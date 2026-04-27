from dataclasses import dataclass


@dataclass(frozen=True)
class TableConfig:
    """Declarative Iceberg table definition."""

    table_name: str
    s3_path_suffix: str
    key_columns: tuple
    partition_columns: tuple
    column_schema: dict  # col_name → Spark SQL type string (e.g. "string", "double")
