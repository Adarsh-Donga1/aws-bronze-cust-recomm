import json
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Callable, List, Optional

from bronze.auth.aws_auth import get_aws_session
from bronze.config.job_params import JobParams
from bronze.config.table_config import TableConfig
from bronze.core.iceberg import merge_df_to_iceberg, save_to_iceberg
from bronze.core.metadata import stamp_metadata
from bronze.utils.cloudwatch_metrics import fetch_ec2_instance_metrics

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MetricSpec:
    """Single CloudWatch metric to collect."""

    metric_name: str
    namespace: str
    unit: str
    stat: str = "Average"
    period: int = 3600
    interval: str = "PT1H"  # aligns with Azure bronze daily rollup logic


@dataclass(frozen=True)
class MetricDefinition:
    """Metrics for a service's primary resources."""

    ec2_instance_specs: List[MetricSpec]
    ebs_volume_specs: List[MetricSpec]
    resource_id_field: str
    table_config: TableConfig
    daily_percentile: float = 0.90

    @property
    def metric_specs(self) -> List[MetricSpec]:
        """All specs (for daily aggregation filters)."""
        return list(self.ec2_instance_specs) + list(self.ebs_volume_specs)


@dataclass(frozen=True)
class ServiceDefinition:
    """AWS service — config plus resource fetch callable."""

    name: str
    namespace: str
    table_config: TableConfig
    fetch_resources: Callable[..., List[dict]]
    metrics: Optional[MetricDefinition] = None


_DAILY_METRICS_SCHEMA = {
    "account_id": "string",
    "aggregation_type": "string",
    "client_id": "string",
    "cloud_name": "string",
    "date": "string",
    "ingestion_timestamp": "string",
    "job_runtime_utc": "timestamp",
    "metric_date": "string",
    "metric_name": "string",
    "metric_unit": "string",
    "metric_value": "double",
    "namespace": "string",
    "region": "string",
    "resource_id": "string",
    "resource_name": "string",
    "service_name": "string",
    "unit": "string",
    "year_month": "string",
}

DAILY_METRICS_TABLE = TableConfig(
    table_name="test_bronze_aws_metrics",
    s3_path_suffix="aws/metrics_daily",
    key_columns=("client_id", "account_id", "resource_id", "metric_date", "metric_name", "aggregation_type"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema=_DAILY_METRICS_SCHEMA,
)

DAILY_METRICS_TABLE_V2 = TableConfig(
    table_name="bronze_aws_daily_metrics",
    s3_path_suffix="aws/daily_metrics",
    key_columns=("client_id", "account_id", "resource_id", "metric_date", "metric_name", "aggregation_type"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema=_DAILY_METRICS_SCHEMA,
)


class AwsServiceRunner:
    """Runs fetch + Iceberg writes for an AWS ServiceDefinition."""

    def __init__(
        self,
        spark,
        params: JobParams,
        definition: ServiceDefinition,
        region: str,
        dry_run: bool = False,
        boto_session=None,
    ):
        self.spark = spark
        self.params = params
        self.definition = definition
        self.region = region
        self.dry_run = dry_run
        self._boto_session = boto_session

    def _session(self):
        if self._boto_session is not None:
            return self._boto_session
        return get_aws_session(
            profile_name=self.params.aws_profile or None,
            region_name=self.region,
            credentials_secret_name=self.params.aws_credentials_secret or None,
            glue_job_runtime=self.params.glue_job_runtime,
        )

    def fetch_only(self) -> dict:
        logger.info("Fetching service: %s region=%s", self.definition.name, self.region)

        from botocore.config import Config

        session = self._session()
        ec2 = session.client("ec2", region_name=self.region)

        resources = self.definition.fetch_resources(
            ec2_client=ec2,
            params=self.params,
            region=self.region,
            account_id=self.params.account_id,
        )

        metrics = None
        if self.definition.metrics and resources:
            # ThreadPoolExecutor uses up to 50 workers; default urllib3 pool is 10 → "Connection pool is full"
            n = len(resources)
            pool = max(50, min(100, n + 20))
            cw_cfg = Config(max_pool_connections=pool)
            cw = session.client("cloudwatch", region_name=self.region, config=cw_cfg)
            metrics = self._fetch_all_metrics(cw, resources)

        logger.info("Fetched service: %s", self.definition.name)
        return {
            "resources": resources,
            "metrics": metrics,
        }

    def save_only(self, fetched: dict) -> dict:
        inv_name = self.definition.table_config.table_name
        results = {}
        write_counts: dict = {}

        resources = fetched.get("resources") or []
        results[inv_name] = resources
        if not self.dry_run and resources:
            write_counts[inv_name] = save_to_iceberg(
                self.spark, resources, self.definition.table_config, self.params
            )
        else:
            write_counts[inv_name] = 0

        metrics = fetched.get("metrics")
        mname = self.definition.metrics.table_config.table_name if self.definition.metrics else None
        if self.definition.metrics and mname is not None:
            if metrics is not None:
                results[mname] = metrics
                if not self.dry_run:
                    write_counts[mname] = save_to_iceberg(
                        self.spark, metrics, self.definition.metrics.table_config, self.params
                    )
                    for tbl, n in (self._save_daily_metrics() or {}).items():
                        write_counts[tbl] = write_counts.get(tbl, 0) + n
                else:
                    write_counts[mname] = 0
            else:
                write_counts[mname] = 0

        results["_write_counts"] = write_counts
        return results

    def run(self) -> dict:
        logger.info("Running service: %s", self.definition.name)
        fetched = self.fetch_only()
        results = self.save_only(fetched)
        results.pop("_write_counts", None)
        logger.info("Completed service: %s", self.definition.name)
        return results

    def _fetch_all_metrics(self, cloudwatch_client, primary_resources: list) -> list:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from datetime import datetime, timezone

        metrics_def = self.definition.metrics
        job_runtime_utc = datetime.now(timezone.utc)

        unit_by_metric = {}
        for spec in metrics_def.metric_specs:
            unit_by_metric.setdefault(spec.metric_name, spec.unit)

        def _fetch_one(resource: dict) -> list:
            resource_id = resource.get(metrics_def.resource_id_field)
            if not resource_id:
                return []

            instance_id = resource.get("instance_id", "")
            if not instance_id:
                return []

            resource_name = resource.get("resource_name") or resource.get("instance_id", "")
            region = resource.get("region", self.region)

            volume_ids = []
            raw = resource.get("ebs_volume_ids")
            if raw:
                try:
                    volume_ids = json.loads(raw) if isinstance(raw, str) else list(raw)
                except json.JSONDecodeError:
                    volume_ids = []

            rows = fetch_ec2_instance_metrics(
                cloudwatch_client,
                instance_id=instance_id,
                volume_ids=volume_ids,
                ec2_specs=metrics_def.ec2_instance_specs,
                ebs_specs=metrics_def.ebs_volume_specs,
                window_days=self.params.window_days,
            )

            for row in rows:
                stamp_metadata(row, self.params)
                row["resource_id"] = resource_id
                row["resource_name"] = resource_name
                row["service_name"] = self.definition.name
                row["namespace"] = row.get("namespace", self.definition.namespace)
                row["region"] = region
                unit = unit_by_metric.get(row["metric_name"], "")
                row["unit"] = unit
                row["metric_unit"] = unit
                row["date"] = row["timestamp"]
                row["metric_date"] = row["timestamp"][:10]
                row["job_runtime_utc"] = job_runtime_utc
                row.pop("timestamp", None)
            return rows

        all_metrics = []
        max_workers = min(50, len(primary_resources)) or 1
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch_one, r): r for r in primary_resources}
            for future in as_completed(futures):
                try:
                    all_metrics.extend(future.result())
                except Exception:
                    resource = futures[future]
                    logger.warning(
                        "Failed to fetch metrics for resource %s",
                        resource.get("resource_id", "unknown"),
                        exc_info=True,
                    )

        if all_metrics:
            by_name = Counter(r.get("metric_name") for r in all_metrics if r.get("metric_name"))
            logger.info(
                "Fetched %d metric rows for %d resources; row counts by metric_name=%s",
                len(all_metrics),
                len(primary_resources),
                dict(sorted(by_name.items())),
            )
        else:
            logger.info("Fetched 0 metric rows for %d resources", len(primary_resources))
        return all_metrics

    def _save_daily_metrics(self) -> dict:
        from datetime import datetime, timezone

        from pyspark.sql import functions as F

        metrics_def = self.definition.metrics
        source_table = metrics_def.table_config
        full_source = f"{self.params.iceberg_catalog}.{self.params.iceberg_database}.{source_table.table_name}"
        service_name = self.definition.name
        percentile = metrics_def.daily_percentile
        window_days = self.params.window_days

        if not self.spark.catalog.tableExists(full_source):
            logger.warning("Source metrics table %s does not exist, skipping daily aggregation.", full_source)
            return {}

        _DAILY_INTERVALS = {"P1D", "PT1D"}
        sub_daily_metrics = []
        p1d_metrics = []
        for spec in metrics_def.metric_specs:
            if spec.interval in _DAILY_INTERVALS:
                p1d_metrics.append(spec)
            else:
                sub_daily_metrics.append(spec)

        client_id = self.params.client_id
        account_id = self.params.account_id
        date_filter = f"CAST(metric_date AS DATE) >= DATE_SUB(CURRENT_DATE(), {window_days})"
        base_filter = f"client_id = '{client_id}' AND account_id = '{account_id}' AND service_name = '{service_name}' AND {date_filter}"
        daily_queries = []

        percentile_label = f"p{int(percentile * 100)}"
        if sub_daily_metrics:
            sub_daily_clauses = [
                f"(metric_name = '{s.metric_name}' AND LOWER(aggregation_type) = '{s.stat.lower()}')"
                for s in sub_daily_metrics
            ]
            sub_daily_filter = " OR ".join(sub_daily_clauses)

            hourly_query = f"""
            SELECT resource_id, metric_name, metric_date,
                AVG(hourly_value) as daily_avg_value,
                FIRST(resource_name) as resource_name,
                FIRST(region) as region,
                '{percentile_label}' as aggregation_type
            FROM (
                SELECT resource_id, metric_name, metric_date,
                    HOUR(CAST(date AS TIMESTAMP)) as hr,
                    PERCENTILE_APPROX(metric_value, {percentile}) as hourly_value,
                    FIRST(resource_name) as resource_name,
                    FIRST(region) as region
                FROM {full_source}
                WHERE {base_filter}
                  AND ({sub_daily_filter})
                GROUP BY resource_id, metric_name, metric_date, HOUR(CAST(date AS TIMESTAMP))
            ) hourly_agg
            GROUP BY resource_id, metric_name, metric_date
            """
            daily_queries.append(hourly_query)

        if p1d_metrics:
            p1d_clauses = []
            for spec in p1d_metrics:
                p1d_clauses.append(
                    f"(metric_name = '{spec.metric_name}' AND LOWER(aggregation_type) = '{spec.stat.lower()}')"
                )
            p1d_filter = " OR ".join(p1d_clauses)

            p1d_agg_cases = " ".join(
                f"WHEN metric_name = '{s.metric_name}' THEN '{s.stat}'" for s in p1d_metrics
            )
            p1d_agg_expr = f"CASE {p1d_agg_cases} ELSE 'Average' END"

            p1d_query = f"""
            SELECT resource_id, metric_name, metric_date,
                metric_value as daily_avg_value,
                resource_name,
                region,
                {p1d_agg_expr} as aggregation_type
            FROM {full_source}
            WHERE {base_filter}
              AND ({p1d_filter})
            """
            daily_queries.append(p1d_query)

        if not daily_queries:
            logger.info("No metrics to aggregate for service %s", service_name)
            return {}

        combined_query = " UNION ALL ".join(daily_queries)
        daily_df = self.spark.sql(combined_query)

        now_ts = datetime.now(timezone.utc)
        now_str = now_ts.strftime("%Y-%m-%d %H:%M:%S")

        daily_df = (
            daily_df.withColumn("client_id", F.lit(self.params.client_id))
            .withColumn("account_id", F.lit(self.params.account_id))
            .withColumn("service_name", F.lit(service_name))
            .withColumn("namespace", F.lit(self.definition.namespace))
            .withColumn("date", F.col("metric_date"))
            .withColumn("metric_value", F.col("daily_avg_value"))
            .withColumn("unit", F.lit(""))
            .withColumn("cloud_name", F.lit("aws"))
            .withColumn("year_month", F.date_format(F.to_date(F.col("metric_date")), "yyyy-MM"))
            .withColumn("ingestion_timestamp", F.lit(now_str))
            .withColumn("job_runtime_utc", F.lit(now_ts))
            .withColumn("metric_unit", F.lit(""))
            .select(*_DAILY_METRICS_SCHEMA.keys())
        )

        daily_df.cache()
        n_source_rows = daily_df.count()
        if n_source_rows == 0:
            logger.info("Daily roll-up query returned 0 rows for %s, skipping daily table merges.", service_name)
            daily_df.unpersist()
            return {}

        client_ids = [self.params.client_id]
        if self.params.additional_client_id:
            client_ids.append(self.params.additional_client_id)

        import re

        for cid in client_ids:
            client_df = daily_df.withColumn("client_id", F.lit(cid))
            safe_cid = re.sub(r"[^A-Za-z0-9_]", "_", cid)
            for table_config in (DAILY_METRICS_TABLE, DAILY_METRICS_TABLE_V2):
                merge_df_to_iceberg(
                    self.spark,
                    client_df,
                    table_config,
                    self.params,
                    source_alias=f"daily_merge_{table_config.table_name}_{safe_cid}",
                )
                logger.info(
                    "[DAILY_METRICS] Merged into %s for client_id=%s",
                    table_config.table_name,
                    cid,
                )

        daily_df.unpersist()
        # Each table receives n_source_rows for every client_id (MERGE of that many source rows per client).
        rpt = {t.table_name: n_source_rows * len(client_ids) for t in (DAILY_METRICS_TABLE, DAILY_METRICS_TABLE_V2)}
        logger.info(
            "[DAILY_METRICS] Roll-up: %d distinct daily rows × %d client_id(s) merged into %s and %s.",
            n_source_rows,
            len(client_ids),
            DAILY_METRICS_TABLE.table_name,
            DAILY_METRICS_TABLE_V2.table_name,
        )
        return rpt
