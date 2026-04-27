from dataclasses import dataclass, field

from bronze.config.cross_account import parse_aws_target_account_ids


@dataclass(frozen=True)
class JobParams:
    """Parsed and validated Glue job parameters for AWS EC2 bronze."""

    job_name: str
    window_days: int
    active_services: list
    account_id: str
    client_id: str
    additional_client_id: str
    aws_regions: list
    iceberg_catalog: str
    iceberg_database: str
    s3_bucket: str
    aws_profile: str
    aws_credentials_secret: str
    # Prod-style cross-account: Glue execution role calls sts.assume_role (see aws_auth)
    role_arn: str = ""
    rds_secret_arn: str = ""
    external_id: str = ""
    # Each id rewrites ROLE_ARN for that account; empty = use ROLE_ARN without replacement
    role_target_account_ids: tuple = field(default_factory=tuple)
    # True in glue_entry: use IAM job role, not profile / local aws_creds.json (see get_aws_session)
    glue_job_runtime: bool = False


def parse_job_params(raw_args: dict) -> JobParams:
    """Parse raw Glue job args dict into a typed JobParams.

    When ``ACTIVE_SERVICES`` is ``ALL`` (or empty) and ``RDS_SECRET_ARN`` is set, the Glue job loads
    service codes from PostgreSQL table ``service_configuration`` (``cloud_provider='aws'``).
    """
    active_services_raw = raw_args.get("ACTIVE_SERVICES", "")
    active_services = [
        s.strip().upper() for s in active_services_raw.split(",") if s.strip()
    ]

    regions_raw = raw_args.get("AWS_REGIONS", "") or raw_args.get("AWS_REGION", "")
    if regions_raw.strip():
        aws_regions = [r.strip() for r in regions_raw.split(",") if r.strip()]
    else:
        aws_regions = []

    return JobParams(
        job_name=raw_args.get("JOB_NAME", "aws-ec2-bronze-ingestion"),
        window_days=int(raw_args.get("WINDOW_DAYS", "7")),
        active_services=active_services,
        account_id=(raw_args.get("AWS_ACCOUNT_ID") or "").strip(),
        client_id=raw_args["CLIENT_ID"],
        additional_client_id=raw_args.get("ADDITIONAL_CLIENT_ID", ""),
        aws_regions=aws_regions,
        iceberg_catalog=raw_args.get("ICEBERG_CATALOG", "glue_catalog"),
        iceberg_database=raw_args.get("ICEBERG_DATABASE", "bronze"),
        s3_bucket=raw_args["S3_BUCKET"],
        aws_profile=(raw_args.get("AWS_PROFILE") or "").strip(),
        aws_credentials_secret=(raw_args.get("AWS_CREDENTIALS_SECRET") or "").strip(),
        role_arn=(raw_args.get("ROLE_ARN") or raw_args.get("role_arn") or "").strip(),
        rds_secret_arn=(raw_args.get("RDS_SECRET_ARN") or raw_args.get("rds_secret_arn") or "").strip(),
        external_id=(raw_args.get("EXTERNAL_ID") or raw_args.get("external_id") or "").strip(),
        role_target_account_ids=parse_aws_target_account_ids(raw_args),
        glue_job_runtime=False,
    )
