import logging
import random
import time

from bronze.config.job_params import JobParams
from bronze.config.table_config import TableConfig

logger = logging.getLogger(__name__)


def _sql_with_retry(spark, sql: str, label: str, retries: int = 5, backoff: float = 3.0) -> None:
    """Execute a Spark SQL statement with exponential-backoff retries."""
    for attempt in range(1, retries + 1):
        try:
            spark.sql(sql)
            return
        except Exception as exc:
            msg = str(exc).lower()
            retryable = (
                "conflicting files" in msg
                or "commitfailedexception" in msg
                or "timeout waiting for connection from pool" in msg
            )
            if retryable and attempt < retries:
                wait = backoff * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logger.warning(
                    "SQL on %s failed (attempt %d/%d), retrying in %.1fs: %s",
                    label, attempt, retries, wait, exc,
                )
                time.sleep(wait)
            else:
                logger.error("SQL on %s failed after %d attempts: %s", label, retries, exc)
                raise


def _full_table_name(params: JobParams, table_config: TableConfig) -> str:
    return f"{params.iceberg_catalog}.{params.iceberg_database}.{table_config.table_name}"


def _ensure_table_exists(spark, params: JobParams, table_config: TableConfig) -> None:
    """Create the Iceberg table if it doesn't already exist."""
    full_name = _full_table_name(params, table_config)
    s3_location = f"s3://{params.s3_bucket}/warehouse/{table_config.s3_path_suffix}"

    columns_ddl = ", ".join(
        f"`{col}` {dtype}" for col, dtype in table_config.column_schema.items()
    )
    partition_ddl = ", ".join(f"`{c}`" for c in table_config.partition_columns)

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
            {columns_ddl}
        )
        USING iceberg
        PARTITIONED BY ({partition_ddl})
        LOCATION '{s3_location}'
    """
    spark.sql(create_sql)


def save_to_iceberg(
    spark,
    records: list,
    table_config: TableConfig,
    params: JobParams,
) -> int:
    """Upsert records into an Iceberg table using MERGE INTO."""
    import pandas as pd
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

    if not records:
        logger.info("No records to save for %s, skipping.", table_config.table_name)
        return 0

    _ensure_table_exists(spark, params, table_config)
    full_name = _full_table_name(params, table_config)

    pdf = pd.DataFrame(records)
    for col in table_config.column_schema:
        if col not in pdf.columns:
            pdf[col] = None
    pdf = pdf[[c for c in table_config.column_schema if c in pdf.columns]]

    # MERGE requires at most one source row per target key; duplicates cause MERGE_CARDINALITY_VIOLATION.
    key_list = list(table_config.key_columns)
    n_before = len(pdf)
    pdf = pdf.drop_duplicates(subset=key_list, keep="last")
    n_dup = n_before - len(pdf)
    if n_dup:
        logger.warning(
            "Removed %d duplicate staging row(s) for %s before MERGE (keys=%s). "
            "Typical cause: CloudWatch returned multiple datapoints mapping to the same key (e.g. same floored timestamp).",
            n_dup,
            table_config.table_name,
            key_list,
        )

    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "long": LongType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
    }
    spark_schema = StructType([
        StructField(col, type_map.get(dtype, StringType()), nullable=True)
        for col, dtype in table_config.column_schema.items()
    ])

    staging_df = spark.createDataFrame(pdf, schema=spark_schema)
    staging_view = f"staging_{table_config.table_name}"
    staging_df.createOrReplaceTempView(staging_view)

    merge_condition = " AND ".join(
        f"target.`{k}` = source.`{k}`" for k in table_config.key_columns
    )
    update_set = ", ".join(
        f"target.`{c}` = source.`{c}`"
        for c in table_config.column_schema
        if c not in table_config.key_columns
    )
    insert_cols = ", ".join(f"`{c}`" for c in table_config.column_schema)
    insert_vals = ", ".join(f"source.`{c}`" for c in table_config.column_schema)

    merge_sql = f"""
        MERGE INTO {full_name} AS target
        USING {staging_view} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    _sql_with_retry(spark, merge_sql, full_name)
    spark.catalog.dropTempView(staging_view)

    row_count = len(pdf)
    logger.info("Saved %d records to %s", row_count, full_name)
    return row_count


def merge_df_to_iceberg(
    spark,
    df,
    table_config: TableConfig,
    params: JobParams,
    source_alias: str = None,
) -> None:
    """Upsert a Spark DataFrame into an Iceberg table via MERGE INTO."""
    full_name = _full_table_name(params, table_config)
    _ensure_table_exists(spark, params, table_config)

    key_cols = list(table_config.key_columns)
    deduped = df.dropDuplicates(key_cols)

    view_name = source_alias or f"merge_source_{table_config.table_name}"
    deduped.createOrReplaceTempView(view_name)

    merge_condition = " AND ".join(
        f"target.`{k}` = source.`{k}`" for k in key_cols
    )
    update_set = ", ".join(
        f"target.`{c}` = source.`{c}`"
        for c in table_config.column_schema
        if c not in key_cols
    )
    insert_cols = ", ".join(f"`{c}`" for c in table_config.column_schema)
    insert_vals = ", ".join(f"source.`{c}`" for c in table_config.column_schema)

    merge_sql = f"""
        MERGE INTO {full_name} AS target
        USING {view_name} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    logger.info("Merging DataFrame into %s on keys %s", full_name, key_cols)
    _sql_with_retry(spark, merge_sql, full_name)
    spark.catalog.dropTempView(view_name)
