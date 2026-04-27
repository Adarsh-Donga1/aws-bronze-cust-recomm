from datetime import datetime, timezone

from bronze.config.job_params import JobParams


def stamp_metadata(record: dict, params: JobParams) -> dict:
    """Add standard metadata fields to a record dict in-place and return it."""
    now = datetime.now(timezone.utc)
    record["client_id"] = params.client_id
    record["account_id"] = params.account_id
    record["cloud_name"] = "aws"
    record["year_month"] = now.strftime("%Y-%m")
    record["ingestion_timestamp"] = now.isoformat()
    return record
