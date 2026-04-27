"""EC2 region list for multi-region runs (aligns with Azure: list all when not constrained)."""

import logging
import os
from typing import List, Tuple

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Default when ``describe_regions`` is denied: **standard** (non opt-in) commercial regions only.
# Using ``get_available_regions`` (~29+) includes opt-in regions that often return **AuthFailure**
# for ``DescribeInstances`` if the account has not enabled them. Override via env (see below).
STANDARD_COMMERCIAL_EC2_REGIONS: Tuple[str, ...] = (
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-southeast-1",
    "ap-southeast-2",
    "ca-central-1",
    "eu-central-1",
    "eu-north-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "sa-east-1",
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
)


def _fallback_ec2_regions_no_describe_permission(boto3_session) -> List[str]:
    """
    When ``ec2:DescribeRegions`` is denied, choose a region list that does not require that API.

    1) ``EC2_BRONZE_REGION_FALLBACK`` — comma-separated list (highest priority).
    2) ``EC2_BRONZE_USE_FULL_METADATA_REGIONS=1`` — all commercial EC2 names from botocore (no IAM).
    3) Default — :data:`STANDARD_COMMERCIAL_EC2_REGIONS` (avoids opt-in **AuthFailure** noise).
    """
    env = (os.environ.get("EC2_BRONZE_REGION_FALLBACK") or "").strip()
    if env:
        return sorted(r.strip() for r in env.split(",") if r.strip())
    if os.environ.get("EC2_BRONZE_USE_FULL_METADATA_REGIONS", "").lower() in ("1", "true", "yes"):
        return sorted(boto3_session.get_available_regions("ec2", partition_name="aws"))
    return list(STANDARD_COMMERCIAL_EC2_REGIONS)


def list_all_enabled_ec2_regions(boto3_session) -> List[str]:
    """Return EC2 region names for ingestion: prefer ``describe_regions``, else metadata fallback."""
    ec2 = boto3_session.client("ec2", region_name="us-east-1")
    try:
        response = ec2.describe_regions(AllRegions=False)
        names = [r["RegionName"] for r in response.get("Regions", []) if r.get("RegionName")]
        return sorted(names)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("UnauthorizedOperation", "AccessDenied"):
            regions = _fallback_ec2_regions_no_describe_permission(boto3_session)
            src = "EC2_BRONZE_REGION_FALLBACK" if (os.environ.get("EC2_BRONZE_REGION_FALLBACK") or "").strip() else (
                "EC2_BRONZE_USE_FULL_METADATA_REGIONS" if os.environ.get("EC2_BRONZE_USE_FULL_METADATA_REGIONS", "").lower() in ("1", "true", "yes") else "STANDARD_COMMERCIAL_EC2_REGIONS"
            )
            logger.info(
                "ec2:DescribeRegions is not allowed (%s). Using fallback region set (%d regions, source=%s). "
                "Grant ec2:DescribeRegions for the exact enabled list, set EC2_BRONZE_REGION_FALLBACK, EC2_BRONZE_USE_FULL_METADATA_REGIONS, or AWS_REGIONS.",
                code,
                len(regions),
                src,
            )
            return regions
        raise


def initial_boto_session_region(aws_regions: list) -> str:
    """Default region for the boto3 Session (describe_regions, STS); not necessarily the only ingest region."""
    r = aws_regions
    if r and not (len(r) == 1 and str(r[0]).strip().upper() in ("ALL", "*", "AUTO")):
        return str(r[0]).strip()
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"


def is_discover_all_regions(aws_regions: list) -> bool:
    """True when the job should call describe_regions instead of a fixed list."""
    if not aws_regions:
        return True
    if len(aws_regions) == 1 and str(aws_regions[0]).strip().upper() in ("ALL", "*", "AUTO"):
        return True
    return False


def resolve_ingestion_regions(aws_regions: list, boto3_session) -> List[str]:
    """If ``aws_regions`` is empty or a single token ALL/* /AUTO, return all enabled EC2 regions; else the given list."""
    if is_discover_all_regions(aws_regions):
        regions = list_all_enabled_ec2_regions(boto3_session)
        logger.info(
            "EC2 region discovery: %d regions (set AWS_REGIONS to a comma list to limit).",
            len(regions),
        )
        return regions
    return [str(r).strip() for r in aws_regions if str(r).strip()]
