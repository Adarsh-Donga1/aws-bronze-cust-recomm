from pyspark.sql import SparkSession

from bronze.config.job_params import JobParams


def create_spark_session(params: JobParams) -> SparkSession:
    """Create a SparkSession configured for Iceberg with the Glue catalog."""
    warehouse_path = f"s3://{params.s3_bucket}/warehouse"

    spark = (
        SparkSession.builder.appName(params.job_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{params.iceberg_catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{params.iceberg_catalog}.warehouse",
            warehouse_path,
        )
        .config(
            f"spark.sql.catalog.{params.iceberg_catalog}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            f"spark.sql.catalog.{params.iceberg_catalog}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            f"spark.sql.catalog.{params.iceberg_catalog}.s3.max-connections",
            "500",
        )
        .config(
            f"spark.sql.catalog.{params.iceberg_catalog}.glue.http-client.apache.max-connections",
            "500",
        )
        .getOrCreate()
    )
    return spark
