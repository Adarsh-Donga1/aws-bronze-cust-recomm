"""Fetch EC2 / EBS metrics via CloudWatch GetMetricData.

Each :class:`MetricSpec` can use a different ``period`` (seconds) and ``interval`` label; every
``MetricDataQuery`` uses that spec's own ``Period`` in ``MetricStat`` (same pattern as Azure
using different API intervals per metric).
"""

import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

logger = logging.getLogger(__name__)


def _floor_timestamp(ts: datetime, interval_minutes: int) -> datetime:
    total_minutes = ts.hour * 60 + ts.minute
    im = max(1, int(interval_minutes))
    floored_minutes = (total_minutes // im) * im
    return ts.replace(hour=floored_minutes // 60, minute=floored_minutes % 60, second=0, microsecond=0)


def _floor_time_for_datapoint(ts: datetime, meta: dict) -> datetime:
    """Align timestamp to bucket using spec period and interval (P1D → UTC day start)."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)

    inter = (meta.get("interval") or "").strip().upper()
    p = int(meta.get("period_sec", 0) or 0)

    if inter in ("P1D", "PT1D") or p >= 86400:
        return ts.replace(hour=0, minute=0, second=0, microsecond=0)

    if p <= 0:
        p = 3600
    if p < 60:
        return ts.replace(second=0, microsecond=0)

    if p % 60 != 0:
        return ts.replace(microsecond=0)

    interval_minutes = max(1, p // 60)
    return _floor_timestamp(ts, interval_minutes)


def _build_ec2_instance_queries(
    instance_id: str,
    ec2_specs: list,
    id_prefix: str,
) -> Tuple[list, dict]:
    queries = []
    id_to_meta = {}
    for j, spec in enumerate(ec2_specs):
        period = int(spec.period) if spec.period else 60
        # Unique per metric + period (mixed periods in one call)
        qid = f"c{j:02d}{_safe_id_part(spec.metric_name)}p{period}_{id_prefix[:12]}"[:255]
        if qid[0].isdigit():
            qid = f"x{qid}"[:255]

        queries.append(
            {
                "Id": qid,
                "MetricStat": {
                    "Metric": {
                        "Namespace": spec.namespace,
                        "MetricName": spec.metric_name,
                        "Dimensions": [{"Name": "InstanceId", "Value": instance_id}],
                    },
                    "Period": period,
                    "Stat": spec.stat,
                },
                "ReturnData": True,
            }
        )
        id_to_meta[qid] = {
            "metric_name": spec.metric_name,
            "namespace": spec.namespace,
            "stat": spec.stat,
            "merge_group": None,
            "period_sec": period,
            "interval": getattr(spec, "interval", None) or "PT1H",
        }
    return queries, id_to_meta


def _build_ebs_volume_queries(
    volume_id: str,
    ebs_specs: list,
    id_prefix: str,
    vol_idx: int,
) -> Tuple[list, dict]:
    queries = []
    id_to_meta = {}
    for j, spec in enumerate(ebs_specs):
        period = int(spec.period) if spec.period else 60
        qid = f"b{vol_idx:02d}{j:02d}{_safe_id_part(spec.metric_name)}p{period}_{id_prefix[:8]}"[:255]
        if qid[0].isdigit():
            qid = f"x{qid}"[:255]

        queries.append(
            {
                "Id": qid,
                "MetricStat": {
                    "Metric": {
                        "Namespace": spec.namespace,
                        "MetricName": spec.metric_name,
                        "Dimensions": [{"Name": "VolumeId", "Value": volume_id}],
                    },
                    "Period": period,
                    "Stat": spec.stat,
                },
                "ReturnData": True,
            }
        )
        merge_group = f"{spec.metric_name}|{spec.namespace}|{spec.stat}"
        id_to_meta[qid] = {
            "metric_name": spec.metric_name,
            "namespace": spec.namespace,
            "stat": spec.stat,
            "merge_group": merge_group,
            "period_sec": period,
            "interval": getattr(spec, "interval", None) or "PT1H",
        }
    return queries, id_to_meta


def _safe_id_part(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "", name)[:40] or "m"


def _merge_metric_results(metric_results: list, id_to_meta: dict) -> list:
    """Turn GetMetricData MetricDataResults into bronze metric rows (one per metric per timestamp)."""
    rows = []
    for md in metric_results:
        qid = md.get("Id")
        meta = id_to_meta.get(qid)
        if not meta:
            continue
        timestamps = md.get("Timestamps", [])
        values = md.get("Values", [])
        if len(timestamps) != len(values):
            continue
        for ts, val in zip(timestamps, values):
            if val is None:
                continue
            floored = _floor_time_for_datapoint(ts, meta)
            rows.append(
                {
                    "metric_name": meta["metric_name"],
                    "namespace": meta["namespace"],
                    "aggregation_type": meta["stat"],
                    "timestamp": floored.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "metric_value": round(float(val), 6),
                    "_merge_group": meta.get("merge_group"),
                }
            )
    return rows


def _sum_volume_metric_groups(rows: list) -> list:
    """Sum metric_value for rows sharing (timestamp, merge_group) where merge_group is set."""
    buckets = defaultdict(float)
    meta_by_key = {}
    standalone = []

    for r in rows:
        mg = r.get("_merge_group")
        if not mg:
            r = {k: v for k, v in r.items() if k != "_merge_group"}
            standalone.append(r)
            continue
        key = (mg, r["timestamp"], r["metric_name"], r["namespace"], r["aggregation_type"])
        buckets[key] += r["metric_value"]
        meta_by_key[key] = r

    merged = []
    for key, total in buckets.items():
        template = meta_by_key[key]
        merged.append(
            {
                "metric_name": template["metric_name"],
                "namespace": template["namespace"],
                "aggregation_type": template["aggregation_type"],
                "timestamp": template["timestamp"],
                "metric_value": round(total, 6),
            }
        )
    return standalone + merged


def _execute_get_metric_data_chunks(cloudwatch_client, queries: list, id_to_meta: dict, start_time, end_time) -> list:
    all_rows = []
    for chunk_start in range(0, len(queries), 100):
        chunk = queries[chunk_start : chunk_start + 100]
        try:
            resp = cloudwatch_client.get_metric_data(
                MetricDataQueries=chunk,
                StartTime=start_time,
                EndTime=end_time,
            )
            all_rows.extend(_merge_metric_results(resp.get("MetricDataResults", []), id_to_meta))
            next_token = resp.get("NextToken")
            while next_token:
                resp = cloudwatch_client.get_metric_data(
                    MetricDataQueries=chunk,
                    StartTime=start_time,
                    EndTime=end_time,
                    NextToken=next_token,
                )
                all_rows.extend(_merge_metric_results(resp.get("MetricDataResults", []), id_to_meta))
                next_token = resp.get("NextToken")
        except Exception:
            logger.warning(
                "CloudWatch GetMetricData failed (chunk %d-%d)",
                chunk_start,
                chunk_start + len(chunk),
                exc_info=True,
            )
    return all_rows


def fetch_ec2_instance_metrics(
    cloudwatch_client,
    instance_id: str,
    volume_ids: List[str],
    ec2_specs: list,
    ebs_specs: list,
    window_days: int,
) -> list:
    """Fetch instance + EBS metrics; each spec uses its own CloudWatch ``Period``."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=window_days)

    id_prefix = re.sub(r"[^a-zA-Z0-9]", "", instance_id)[:20] or "inst"
    if id_prefix[0].isdigit():
        id_prefix = "i" + id_prefix

    all_queries = []
    id_to_meta: dict = {}

    q_ec2, meta_ec2 = _build_ec2_instance_queries(instance_id, ec2_specs, id_prefix)
    all_queries.extend(q_ec2)
    id_to_meta.update(meta_ec2)

    for vol_idx, vid in enumerate(volume_ids):
        q_ebs, meta_ebs = _build_ebs_volume_queries(vid, ebs_specs, id_prefix, vol_idx)
        all_queries.extend(q_ebs)
        id_to_meta.update(meta_ebs)

    if not all_queries:
        return []

    all_rows = _execute_get_metric_data_chunks(
        cloudwatch_client, all_queries, id_to_meta, start_time, end_time
    )
    return _sum_volume_metric_groups(all_rows)
