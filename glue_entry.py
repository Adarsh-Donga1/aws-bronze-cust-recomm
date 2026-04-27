"""AWS Glue entry point — EC2 bronze ingestion (instances + CloudWatch metrics).

Orchestration mirrors ``finomics-custom-recs-bronze/glue_entry.py`` (Azure): parse args,
one Spark session, fetch in parallel per service, sequential Iceberg writes.

On Glue, credentials default to the **Glue job execution role**. For prod-style cross-account
ingestion, set ``ROLE_ARN`` (and optionally ``EXTERNAL_ID``) so the execution role calls
``sts.assume_role`` (same pattern as ``bronze/prod-other-glue.py``). Optional ``RDS_SECRET_ARN`` must be the **PostgreSQL** secret (``host``, ``port``, ``dbname``,
``username``, ``password``) when ``ACTIVE_SERVICES`` is ``ALL`` — then service codes are read from
the ``service_configuration`` table (same idea as Azure ``service_configuration_v2``).
``EXTERNAL_ID`` is only used for ``sts.assume_role`` when ``ROLE_ARN`` is set.
``AWS_PROFILE`` is ignored when ``glue_job_runtime`` is True.
Optional ``AWS_CREDENTIALS_SECRET`` loads static keys only when ``ROLE_ARN`` is not set.

If ``AWS_REGIONS`` is omitted or set to ``ALL`` / ``*`` / ``AUTO``, the job calls ``describe_regions`` and
ingests all EC2 regions enabled for the **same AWS account** that the role or secret resolves to.
``CLIENT_ID`` is Finomics metadata only; it does not select a different AWS account.

Optional ``CLOUD_ACCOUNT_ID`` and/or ``ACCOUNT_IDS`` may list **one or more** 12-digit ids (comma-
separated in one value is supported). The job assumes ``ROLE_ARN`` into **each** account in order
(``arn:aws:iam::<id>:role/<same role name>``) and ingests all regions for each. If both are
empty, ``ROLE_ARN`` is used as-is.
"""

import copy
import logging
import sys
from dataclasses import dataclass, field, replace
from typing import Optional

from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

import bronze.services  # noqa: F401 — registers EC2
from bronze.auth.aws_auth import get_aws_session
from bronze.config.aws_regions import initial_boto_session_region, resolve_ingestion_regions
from bronze.config.cross_account import effective_role_arn
from bronze.config.job_params import JobParams, parse_job_params
from bronze.config.service_config import resolve_active_services
from bronze.core.iceberg import save_to_iceberg
from bronze.core.spark import create_spark_session
from bronze.services.aws.ec2 import log_describe_soft_deny_job_summary, reset_describe_soft_deny_stats
from bronze.services.base import AwsServiceRunner
from bronze.services.registry import get_service_definition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REQUIRED_ARGS = [
    "JOB_NAME",
    "WINDOW_DAYS",
    "ACTIVE_SERVICES",
    "CLIENT_ID",
    "S3_BUCKET",
    "ICEBERG_DATABASE",
]

OPTIONAL_ARGS = [
    "AWS_ACCOUNT_ID",
    "ADDITIONAL_CLIENT_ID",
    "ICEBERG_CATALOG",
    "AWS_REGIONS",
    "AWS_REGION",
    "AWS_CREDENTIALS_SECRET",
    "ROLE_ARN",
    "RDS_SECRET_ARN",
    "EXTERNAL_ID",
    "CLOUD_ACCOUNT_ID",
    "ACCOUNT_IDS",
]


@dataclass
class IngestionTotals:
    """Roll-up for one Glue job: fetched row counts and MERGE batch sizes (list length) per table."""

    rows_fetched_by_table: dict = field(default_factory=dict)
    rows_merged_by_table: dict = field(default_factory=dict)
    fetch_failures: int = 0

    def add_fetched(self, table_name: str, n: int) -> None:
        if n <= 0:
            return
        self.rows_fetched_by_table[table_name] = self.rows_fetched_by_table.get(table_name, 0) + n

    def merge_write_counts(self, wc: dict) -> None:
        for k, v in (wc or {}).items():
            if k.startswith("_"):
                continue
            if isinstance(v, (int, float)):
                self.rows_merged_by_table[k] = self.rows_merged_by_table.get(k, 0) + int(v)


def _log_ingestion_summary(totals: "IngestionTotals") -> None:
    if totals.rows_fetched_by_table or totals.rows_merged_by_table:
        logger.info(
            "[INGESTION SUMMARY] By table — fetched=rows from APIs; merged=sum of row counts in each Iceberg MERGE in this job. "
            "fetched=%s merged=%s",
            totals.rows_fetched_by_table,
            totals.rows_merged_by_table,
        )
    else:
        logger.info(
            "[INGESTION SUMMARY] No instance or raw metric rows were written (see [EC2 DescribeInstances] line above if SCPs denied inventory)."
        )
    if totals.fetch_failures:
        logger.warning(
            "Ingestion thread fetch failures (exceptions, not soft EC2 API empty inventory): %d",
            totals.fetch_failures,
        )


def _resolve_account_id(explicit: str, boto_session) -> str:
    if explicit:
        return explicit
    aid = boto_session.client("sts").get_caller_identity().get("Account")
    if not aid:
        raise RuntimeError("Could not resolve AWS account ID (set AWS_ACCOUNT_ID or use an IAM role).")
    logger.info("Resolved AWS account_id from STS: %s", aid)
    return aid


def _glue_boto_session(params: JobParams, target_account_id):
    """Assume ``ROLE_ARN`` optionally rewritten for ``target_account_id`` (12-digit or None = use ARN as passed)."""
    reg = initial_boto_session_region(list(params.aws_regions))
    ra = (params.role_arn or "").strip()
    if not ra:
        return get_aws_session(
            region_name=reg,
            profile_name=None,
            credentials_secret_name=(params.aws_credentials_secret or None),
            glue_job_runtime=True,
            role_arn=None,
            assume_role_external_id=None,
            rds_secret_arn=(params.rds_secret_arn or None),
        )
    if target_account_id is None:
        role_for = ra
    else:
        role_for = effective_role_arn(ra, target_account_id) or ra
    return get_aws_session(
        region_name=reg,
        profile_name=None,
        credentials_secret_name=(params.aws_credentials_secret or None),
        glue_job_runtime=True,
        role_arn=role_for or None,
        assume_role_external_id=(params.external_id or None),
        rds_secret_arn=(params.rds_secret_arn or None),
    )


def _ingest_regions_for_account(
    spark,
    base_params: JobParams,
    boto_session,
    regions: list,
    totals: Optional[IngestionTotals] = None,
):
    for region in regions:
        logger.info("Processing region: %s", region)
        region_params = replace(base_params, aws_regions=[region])
        runners = []
        for service_name in base_params.active_services:
            definition = get_service_definition(service_name)
            if definition is None:
                logger.warning("Unknown service '%s', skipping.", service_name)
                continue
            runners.append(
                AwsServiceRunner(
                    spark=spark,
                    params=region_params,
                    definition=definition,
                    region=region,
                    dry_run=False,
                    boto_session=boto_session,
                )
            )
        from concurrent.futures import ThreadPoolExecutor, as_completed

        fetched_by_runner = {}
        if runners:
            with ThreadPoolExecutor(max_workers=len(runners)) as executor:
                futures = {executor.submit(r.fetch_only): r for r in runners}
                for future in as_completed(futures):
                    runner = futures[future]
                    try:
                        fetched_by_runner[runner] = future.result()
                    except Exception as ex:
                        # EC2 path usually returns [] on soft API denial; this is a hard failure path.
                        if isinstance(ex, ClientError):
                            code = (ex.response or {}).get("Error", {}).get("Code", "")
                            if code in {
                                "AuthFailure",
                                "UnauthorizedOperation",
                                "AccessDenied",
                                "OptInRequired",
                                "OptinRequired",
                            }:
                                msg = (ex.response or {}).get("Error", {}).get("Message", "") or str(ex)
                                logger.warning(
                                    "Fetch failed for service %s (%s); skipping save phase. %s",
                                    runner.definition.name,
                                    code,
                                    (msg[:400] + "…") if len(msg) > 400 else msg,
                                )
                                fetched_by_runner[runner] = None
                                if totals is not None:
                                    totals.fetch_failures += 1
                                continue
                        logger.error(
                            "Fetch failed for service %s; skipping save phase.",
                            runner.definition.name,
                            exc_info=True,
                        )
                        fetched_by_runner[runner] = None
                        if totals is not None:
                            totals.fetch_failures += 1
        for runner in runners:
            fetched = fetched_by_runner.get(runner)
            if fetched is None:
                continue
            results = runner.save_only(fetched)
            inv_t = runner.definition.table_config.table_name
            if totals is not None:
                metrics_t = None
                if runner.definition.metrics:
                    metrics_t = runner.definition.metrics.table_config.table_name
                totals.add_fetched(inv_t, len(fetched.get("resources") or []))
                if metrics_t:
                    totals.add_fetched(metrics_t, len(fetched.get("metrics") or []))
            wc = results.pop("_write_counts", {}) or {}
            if totals is not None:
                totals.merge_write_counts(wc)
            if base_params.additional_client_id:
                logger.info(
                    "Duplicating data under additional client_id: %s",
                    base_params.additional_client_id,
                )
                extra_wc = _save_for_additional_client(spark, results, runner.definition, region_params)
                if totals is not None and extra_wc:
                    totals.merge_write_counts(extra_wc)


def _save_for_additional_client(spark, results: dict, definition, params: JobParams) -> dict:
    """Re-stamp fetched records with additional_client_id and MERGE again. Returns per-table row counts (same as save)."""
    alt_params = replace(
        params,
        client_id=params.additional_client_id,
        additional_client_id=params.additional_client_id,
    )
    wc: dict = {}

    inv_table = definition.table_config.table_name
    if inv_table in results and results[inv_table]:
        duped = [copy.copy(r) for r in results[inv_table]]
        for r in duped:
            r["client_id"] = params.additional_client_id
        wc[inv_table] = wc.get(inv_table, 0) + save_to_iceberg(
            spark, duped, definition.table_config, alt_params
        )

    if definition.metrics:
        mt = definition.metrics.table_config.table_name
        if mt in results and results[mt]:
            duped_m = [copy.copy(r) for r in results[mt]]
            for r in duped_m:
                r["client_id"] = params.additional_client_id
            wc[mt] = wc.get(mt, 0) + save_to_iceberg(
                spark, duped_m, definition.metrics.table_config, alt_params
            )
    return wc


def main():
    raw_args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    for opt_arg in OPTIONAL_ARGS:
        try:
            raw_args.update(getResolvedOptions(sys.argv, [opt_arg]))
        except Exception:
            logger.info("Optional arg %s not provided, skipping.", opt_arg)

    parsed = parse_job_params(raw_args)
    params = replace(parsed, glue_job_runtime=True)

    tids = list(params.role_target_account_ids)
    run_targets = tids if tids else [None]

    seed_session = _glue_boto_session(params, run_targets[0])
    try:
        ident = seed_session.client("sts").get_caller_identity()
        logger.info(
            "STS seed session: target_param=%s Account=%s Arn=%s",
            run_targets[0] if tids else "ROLE_ARN as-is",
            ident.get("Account"),
            ident.get("Arn"),
        )
    except Exception:
        logger.warning("Could not call sts:GetCallerIdentity for seed session", exc_info=True)

    regions = resolve_ingestion_regions(list(params.aws_regions), seed_session)
    first_acct = _resolve_account_id(params.account_id, seed_session)
    base_params = replace(params, account_id=first_acct, aws_regions=regions, glue_job_runtime=True)

    if tids:
        logger.info(
            "Multi-account: %d target id(s) parsed from CLOUD_ACCOUNT_ID / ACCOUNT_IDS: %s",
            len(tids),
            tids,
        )
    elif params.role_arn:
        logger.info("Cross-account: ROLE_ARN is set; EC2/CloudWatch use assumed-role session.")
    else:
        logger.info("Glue job: glue_job_runtime=True (execution role).")

    spark = None
    try:
        spark = create_spark_session(base_params)

        resolved_services = resolve_active_services(
            active_services=list(base_params.active_services),
            rds_secret_arn=base_params.rds_secret_arn,
            spark=spark,
        )
        base_params = replace(base_params, active_services=resolved_services)

        if base_params.additional_client_id:
            logger.info("Additional client_id configured: %s", base_params.additional_client_id)

        logger.info("Active services: %s", base_params.active_services)
        logger.info("Regions to process: %s", regions)
        logger.info("Iceberg: catalog=%s database=%s warehouse bucket=%s", base_params.iceberg_catalog, base_params.iceberg_database, base_params.s3_bucket)

        ingestion_totals = IngestionTotals()
        reset_describe_soft_deny_stats()

        for target_aid in run_targets:
            try:
                boto_session = _glue_boto_session(params, target_aid)
            except Exception:
                logger.error(
                    "Could not build boto session for target account %s (check trust & role name in that account). Skipping.",
                    target_aid,
                    exc_info=True,
                )
                continue
            try:
                acc_i = boto_session.client("sts").get_caller_identity()
                logger.info(
                    "Per-account STS: target_id_param=%s resolved Account=%s",
                    target_aid if target_aid is not None else "n/a",
                    acc_i.get("Account"),
                )
            except Exception:
                logger.warning("sts:GetCallerIdentity failed for this target", exc_info=True)
            acct = _resolve_account_id(params.account_id, boto_session)
            working = replace(base_params, account_id=acct)
            _ingest_regions_for_account(spark, working, boto_session, regions, ingestion_totals)

        log_describe_soft_deny_job_summary()
        _log_ingestion_summary(ingestion_totals)

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")

    logger.info("Job complete.")


if __name__ == "__main__":
    main()
