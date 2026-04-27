"""EC2 bronze service — instance inventory + CloudWatch metrics (idle / overprovisioned signals)."""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from botocore.exceptions import ClientError

from bronze.config.table_config import TableConfig
from bronze.core.metadata import stamp_metadata
from bronze.services.base import MetricDefinition, MetricSpec, ServiceDefinition

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Iceberg tables
# ---------------------------------------------------------------------------

EC2_INSTANCES_TABLE = TableConfig(
    table_name="bronze_aws_ec2_instances_v1",
    s3_path_suffix="aws/ec2_instances",
    key_columns=("client_id", "account_id", "resource_id"),
    partition_columns=("client_id", "account_id", "year_month"),
    # Aligned to deployed Iceberg schema (Name tag / ebs list are not stored here—see fetch for metrics-only keys).
    column_schema={
        "resource_id": "string",
        "instance_id": "string",
        "region": "string",
        "availability_zone": "string",
        "instance_type": "string",
        "architecture": "string",
        "instance_state": "string",
        "platform": "string",
        "hypervisor": "string",
        "monitoring_state": "string",
        "launch_time": "string",
        "private_ip": "string",
        "public_ip": "string",
        "vpc_id": "string",
        "subnet_id": "string",
        "tags": "string",
        "raw_data": "string",
        "client_id": "string",
        "account_id": "string",
        "cloud_name": "string",
        "year_month": "string",
        "ingestion_timestamp": "string",
        "job_runtime_utc": "timestamp",
    },
)

EC2_METRICS_TABLE = TableConfig(
    table_name="bronze_aws_metrics_v1",
    s3_path_suffix="aws/ec2_metrics",
    key_columns=("client_id", "account_id", "resource_id", "date", "metric_name", "aggregation_type"),
    partition_columns=("client_id", "account_id", "year_month"),
    column_schema={
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
    },
)

# ---------------------------------------------------------------------------
# CloudWatch metric specs
# Reference: AWS/EC2 + AWS/EBS namespaces; MemoryUtilization requires agent / unified CW agent on supported Nitro instances.
# ---------------------------------------------------------------------------

_EC2_NS = "AWS/EC2"
_EBS_NS = "AWS/EBS"

# period = CloudWatch granularity in **seconds** (e.g. 300=5m, 3600=1h, 86400=1d). Must match ``interval`` intent.
#
# Disk I/O: ``AWS/EC2`` **DiskReadBytes** / **DiskWriteBytes** are for **instance store** only; for typical EBS-backed
# instances they often publish **no** datapoints. EBS traffic is in ``AWS/EBS`` (VolumeId): VolumeReadBytes,
# VolumeWriteBytes, etc. (see EBS_VOLUME_METRIC_SPECS).
EC2_INSTANCE_METRIC_SPECS = [
    MetricSpec("CPUUtilization", _EC2_NS, "Percent", "Average", 300, "PT5M"),
    MetricSpec("MemoryUtilization", _EC2_NS, "Percent", "Average", 300, "PT5M"),
    MetricSpec("NetworkIn", _EC2_NS, "Bytes", "Sum", 86400, "P1D"),
    MetricSpec("NetworkOut", _EC2_NS, "Bytes", "Sum", 86400, "P1D"),
    # Instance store only — kept so bare-metal / instance-store workloads still get rows; often empty for EBS-only.
    MetricSpec("DiskReadBytes", _EC2_NS, "Bytes", "Sum", 86400, "P1D"),
    MetricSpec("DiskWriteBytes", _EC2_NS, "Bytes", "Sum", 86400, "P1D"),
]

EBS_VOLUME_METRIC_SPECS = [
    MetricSpec("VolumeReadBytes", _EBS_NS, "Bytes", "Sum", 86400, "P1D"),
    MetricSpec("VolumeWriteBytes", _EBS_NS, "Bytes", "Sum", 86400, "P1D"),
    MetricSpec("VolumeReadOps", _EBS_NS, "Count", "Sum", 86400, "P1D"),
    MetricSpec("VolumeWriteOps", _EBS_NS, "Count", "Sum", 86400, "P1D"),
]


# Do not fail the whole Glue region loop for expected cross-account / SCP / opt-in issues.
_EC2_DESCRIBE_SOFT_DENY_CODES = frozenset(
    {
        "AuthFailure",
        "UnauthorizedOperation",
        "AccessDenied",
        "OptInRequired",
        "RequestExpired",  # rare clock skew with temp creds
        "OptinRequired",  # alternate spelling from some SDK responses
    }
)

# One INFO summary at job end instead of 100+ per-region WARNINGs (see ``log_describe_soft_deny_job_summary``).
_soft_deny_lock = threading.Lock()
_describe_soft_deny_total: int = 0
_describe_soft_deny_by_code: Dict[str, int] = {}
# Up to 8 (account_id, region, code, detail_snippet) for the summary line
_describe_soft_deny_samples: List[Tuple[str, str, str, str]] = []


def reset_describe_soft_deny_stats() -> None:
    """Call once at start of a Glue run (e.g. from ``glue_entry``) before ingesting accounts/regions."""
    global _describe_soft_deny_total, _describe_soft_deny_by_code, _describe_soft_deny_samples
    with _soft_deny_lock:
        _describe_soft_deny_total = 0
        _describe_soft_deny_by_code = {}
        _describe_soft_deny_samples = []


def get_describe_soft_deny_stats() -> dict:
    with _soft_deny_lock:
        return {
            "total": _describe_soft_deny_total,
            "by_code": dict(_describe_soft_deny_by_code),
            "samples": list(_describe_soft_deny_samples),
        }


def _record_describe_soft_deny(
    code: str,
    region: str,
    account_id: str,
    full_message: str,
) -> None:
    global _describe_soft_deny_total
    hint = "SCP" if "service_control_policy" in (full_message or "") else "IAM/deny"
    with _soft_deny_lock:
        _describe_soft_deny_total += 1
        _describe_soft_deny_by_code[code] = _describe_soft_deny_by_code.get(code, 0) + 1
        if len(_describe_soft_deny_samples) < 8:
            _describe_soft_deny_samples.append(
                (account_id, region, code, hint),
            )
    verbose = (os.environ.get("EC2_BRONZE_VERBOSE_ACCESS", "") or "").lower() in (
        "1",
        "true",
        "yes",
    )
    if verbose:
        msg = (full_message or "")[:500]
        logger.warning(
            "EC2 describe_instances account=%s region=%s: %s; empty inventory. %s",
            account_id,
            region,
            code,
            (msg + "…") if len(full_message or "") > 500 else (full_message or ""),
        )
    else:
        logger.debug(
            "EC2 describe_instances soft deny account=%s region=%s code=%s",
            account_id,
            region,
            code,
        )


def log_describe_soft_deny_job_summary() -> None:
    """Log a single line after all regions/accounts; call from ``glue_entry`` at job end."""
    st = get_describe_soft_deny_stats()
    total = st["total"]
    if not total:
        logger.info(
            "[EC2 DescribeInstances] No access-denial responses in this job (inventory may still be empty where accounts have 0 instances)."
        )
        return
    by_code = st["by_code"]
    samples = st["samples"]
    logger.info(
        "[EC2 DescribeInstances] access denied on %d account-region call(s) (codes: %s). "
        "No instance inventory in those calls, so no instance-level CloudWatch metrics there—fix IAM/SCP if that data is required. "
        "Samples (account_id, region, code, reason): %s. "
        "Set EC2_BRONZE_VERBOSE_ACCESS=1 for a WARNING per call with full text.",
        total,
        by_code,
        samples,
    )


def _name_from_tags(tags: list) -> str:
    if not tags:
        return ""
    for t in tags:
        if t.get("Key") == "Name":
            return t.get("Value") or ""
    return ""


def fetch_ec2_resources(ec2_client, params, region: str, account_id: str) -> list:
    """List EC2 instances in one region and map to bronze inventory rows."""
    job_runtime_utc = datetime.now(timezone.utc)
    records = []

    paginator = ec2_client.get_paginator("describe_instances")
    try:
        for page in paginator.paginate():
            for reservation in page.get("Reservations", []):
                for inst in reservation.get("Instances", []):
                    iid = inst.get("InstanceId")
                    if not iid:
                        continue

                    volume_ids = []
                    for bdm in inst.get("BlockDeviceMappings", []) or []:
                        ebs = bdm.get("Ebs") or {}
                        vid = ebs.get("VolumeId")
                        if vid:
                            volume_ids.append(vid)

                    tags_raw = inst.get("Tags") or []
                    tags_json = json.dumps(tags_raw)

                    arn = f"arn:aws:ec2:{region}:{account_id}:instance/{iid}"
                    st = inst.get("State") or {}
                    mon = inst.get("Monitoring") or {}
                    launch = inst.get("LaunchTime")
                    launch_s = (
                        launch.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        if launch is not None
                        else ""
                    )
                    raw_data = json.dumps(inst, default=str)
                    public_ip = inst.get("PublicIpAddress") or ""
                    if not public_ip and inst.get("NetworkInterfaces"):
                        nis = inst.get("NetworkInterfaces") or []
                        for ni in nis:
                            if ni.get("Association", {}).get("PublicIp"):
                                public_ip = ni["Association"]["PublicIp"]
                                break
                    private_ip = inst.get("PrivateIpAddress") or ""
                    nis = inst.get("NetworkInterfaces") or []
                    if not private_ip and nis:
                        private_ip = (nis[0] or {}).get("PrivateIpAddress") or ""
                    vpc_id = inst.get("VpcId") or ""
                    subnet_id = inst.get("SubnetId") or ""
                    if not subnet_id and nis:
                        subnet_id = (nis[0] or {}).get("SubnetId") or ""
                    if not vpc_id and nis:
                        vpc_id = (nis[0] or {}).get("VpcId") or ""

                    record = {
                        "resource_id": arn,
                        "instance_id": iid,
                        "region": region,
                        "availability_zone": (inst.get("Placement") or {}).get("AvailabilityZone", ""),
                        "instance_type": inst.get("InstanceType") or "",
                        "architecture": inst.get("Architecture") or "",
                        "instance_state": st.get("Name", ""),
                        "platform": (inst.get("PlatformDetails") or inst.get("Platform") or "") or "",
                        "hypervisor": inst.get("Hypervisor") or "",
                        "monitoring_state": mon.get("State", ""),
                        "launch_time": launch_s,
                        "private_ip": private_ip,
                        "public_ip": public_ip,
                        "vpc_id": vpc_id,
                        "subnet_id": subnet_id,
                        "tags": tags_json,
                        "raw_data": raw_data,
                        "job_runtime_utc": job_runtime_utc,
                        # For CloudWatch + metrics / UI only (not in Iceberg instance schema)
                        "resource_name": _name_from_tags(tags_raw) or iid,
                        "ebs_volume_ids": json.dumps(volume_ids),
                        "service_name": "EC2",
                    }
                    stamp_metadata(record, params)
                    record["service_name"] = "EC2"
                    records.append(record)
    except ClientError as e:
        code = (e.response.get("Error") or {}).get("Code", "")
        if code in _EC2_DESCRIBE_SOFT_DENY_CODES:
            full_msg = (e.response.get("Error") or {}).get("Message", "") or str(e)
            _record_describe_soft_deny(code, region, account_id, full_msg)
            return []
        raise

    logger.info("Fetched %d EC2 instances in %s", len(records), region)
    return records


EC2_SERVICE = ServiceDefinition(
    name="EC2",
    namespace=_EC2_NS,
    table_config=EC2_INSTANCES_TABLE,
    fetch_resources=fetch_ec2_resources,
    metrics=MetricDefinition(
        ec2_instance_specs=EC2_INSTANCE_METRIC_SPECS,
        ebs_volume_specs=EBS_VOLUME_METRIC_SPECS,
        resource_id_field="resource_id",
        table_config=EC2_METRICS_TABLE,
    ),
)
