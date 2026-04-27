"""Load active AWS service codes from PostgreSQL ``service_configuration`` (Finomics RDS).

Aligns with Azure ``service_configuration_v2`` usage: when ``ACTIVE_SERVICES`` is ``ALL`` (or empty),
query the table filtered by ``cloud_provider = 'aws'``.

``RDS_SECRET_ARN`` must reference the same Secrets Manager JSON shape as Azure
(``host``, ``port``, ``dbname``, ``username``, ``password``) — not assume-role metadata.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from bronze.auth.secrets import get_secret_json
from bronze.services.registry import list_registered_services

logger = logging.getLogger(__name__)

SERVICE_CONFIG_TABLE = "service_configuration"


def get_rds_credentials(secret_arn: str) -> Dict[str, Any]:
    """Load PostgreSQL connection fields from Secrets Manager (same contract as Azure Glue jobs)."""
    if not (secret_arn or "").strip():
        raise ValueError("RDS secret ARN is empty")
    creds = get_secret_json((secret_arn or "").strip())
    if not isinstance(creds, dict):
        raise ValueError("RDS secret must be a JSON object")
    for k in ("host", "port", "dbname", "username", "password"):
        if k not in creds or creds[k] in (None, ""):
            raise KeyError(f"RDS secret JSON must include non-empty '{k}'")
    return creds


def jdbc_url_and_properties(creds: dict) -> Tuple[str, dict]:
    """JDBC URL and Spark properties for PostgreSQL."""
    url = f"jdbc:postgresql://{creds['host']}:{creds['port']}/{creds['dbname']}"
    props = {
        "user": creds["username"],
        "password": creds["password"],
        "driver": "org.postgresql.Driver",
    }
    return url, props


def _query_active_aws_service_codes_sql() -> str:
    return f"""
    SELECT DISTINCT service_code
    FROM {SERVICE_CONFIG_TABLE}
    WHERE LOWER(TRIM(cloud_provider)) = 'aws'
      AND is_active = TRUE
    ORDER BY service_code
    """


def fetch_active_aws_service_codes_spark(spark, secret_arn: str) -> List[str]:
    """Load service_code list using Spark JDBC (Glue / PySpark)."""
    creds = get_rds_credentials(secret_arn)
    url, props = jdbc_url_and_properties(creds)
    q = _query_active_aws_service_codes_sql().strip()
    logger.info("Loading active AWS services from %s via JDBC", SERVICE_CONFIG_TABLE)
    df = spark.read.jdbc(url=url, table=f"({q}) AS service_config_active", properties=props)
    rows = df.collect()
    return [str(r["service_code"]).strip() for r in rows if r["service_code"]]


def fetch_active_aws_service_codes_psycopg2(secret_arn: str) -> List[str]:
    """Load service_code list using psycopg2 (local runner)."""
    try:
        import psycopg2
    except ImportError as e:
        raise RuntimeError(
            "psycopg2 is required to read service_configuration locally. "
            "Install: pip install psycopg2-binary"
        ) from e

    creds = get_rds_credentials(secret_arn)
    logger.info("Loading active AWS services from %s via psycopg2", SERVICE_CONFIG_TABLE)
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(_query_active_aws_service_codes_sql())
            return [str(r[0]).strip() for r in cur.fetchall() if r[0]]
    finally:
        conn.close()


def resolve_active_services(
    *,
    active_services: List[str],
    rds_secret_arn: str,
    spark=None,
) -> List[str]:
    """
    If ``active_services`` is empty or ``['ALL']``, load codes from ``service_configuration`` when
    ``rds_secret_arn`` is set; otherwise default to ``['EC2']``. Non-ALL lists pass through
    (normalized to upper). Unknown DB codes are skipped if not in the service registry.
    """
    raw = [str(s).strip().upper() for s in (active_services or []) if str(s).strip()]
    want_all = (not raw) or (len(raw) == 1 and raw[0] == "ALL")
    if not want_all:
        return raw

    arn = (rds_secret_arn or "").strip()
    if not arn:
        logger.info("ACTIVE_SERVICES is ALL/empty and RDS_SECRET_ARN not set; defaulting to EC2")
        return ["EC2"]

    if spark is not None:
        codes = fetch_active_aws_service_codes_spark(spark, arn)
    else:
        codes = fetch_active_aws_service_codes_psycopg2(arn)

    registered = set(list_registered_services())
    out: List[str] = []
    for c in codes:
        cu = c.strip().upper()
        if cu in registered:
            out.append(cu)
        else:
            logger.warning(
                "service_configuration.service_code %r is not registered in this job; skipping",
                cu,
            )
    if not out:
        logger.warning("No registered services from %s; defaulting to EC2", SERVICE_CONFIG_TABLE)
        return ["EC2"]
    logger.info("Resolved active services from %s: %s", SERVICE_CONFIG_TABLE, out)
    return out
