"""AWS credential resolution for bronze ingestion (aligned with Azure ``azure_auth`` pattern).

Resolution order (when ``glue_job_runtime=True`` — ``glue_entry.py``):

1. **Assume role (prod cross-account)** — if ``role_arn`` is set, the Glue **execution role**
   calls ``sts.assume_role`` (optional job/env ``EXTERNAL_ID``). The returned session is used
   for EC2/CloudWatch. ``RDS_SECRET_ARN`` in Finomics is reserved for **PostgreSQL** (see
   ``service_config``), not for ExternalId. Matches ``prod-other-glue.py`` / FOCUS job pattern.
2. **Static key secret** — if ``credentials_secret_name`` or env ``FINOMICS_AWS_CREDENTIALS_SECRET``
   is set, load JSON with ``aws_access_key_id``, ``aws_secret_access_key``, optional
   ``aws_session_token``, optional ``region_name``. Skipped if ``role_arn`` is set.
3. **Default chain** — ``boto3.Session`` with no profile (IAM job role in Glue).

For local runs (``glue_job_runtime=False``): optional assume role on top of profile/default chain;
static secret and file still apply in the same relative order, with assume role last before return.

See also: ``reference/aws_auth.py`` (older standalone helper; this module supersedes it).
"""

import json
import logging
import os
from typing import Optional

import boto3

from bronze.auth.secrets import get_secret_json

logger = logging.getLogger(__name__)

DEFAULT_LOCAL_CREDS_FILE = os.path.join(os.path.dirname(__file__), "aws_creds.json")


def _resolve_assume_role_external_id(explicit: Optional[str]) -> Optional[str]:
    """From job param or environment only (``RDS_SECRET_ARN`` is used for PostgreSQL, not this)."""
    e = (explicit or "").strip() or (
        os.environ.get("EXTERNAL_ID") or os.environ.get("BRONZE_ASSUME_ROLE_EXTERNAL_ID") or ""
    ).strip()
    return e or None


def _local_base_session_for_assume(
    profile_name: Optional[str],
    region: Optional[str],
) -> boto3.Session:
    """Credentials used only to call ``sts.assume_role`` (profile or default chain, not static files)."""
    prof = (profile_name or os.environ.get("AWS_PROFILE") or "").strip() or None
    if prof:
        return boto3.Session(profile_name=prof, region_name=region)
    return boto3.Session(region_name=region)


def _session_from_assumed_role(
    base_session: boto3.Session,
    role_arn: str,
    region: str,
    external_id: Optional[str],
    role_session_name: str,
) -> boto3.Session:
    """Match ``prod-other-glue.get_credentials`` — temporary creds for API calls."""
    # Unscoped client matches ``boto3.client('sts')`` in prod-other-glue (Glue default chain).
    sts = base_session.client("sts")
    kwargs: dict = {
        "RoleArn": role_arn,
        "RoleSessionName": role_session_name,
        "DurationSeconds": 3600,
    }
    if external_id:
        kwargs["ExternalId"] = external_id
    try:
        resp = sts.assume_role(**kwargs)
    except Exception:
        logger.error("Failed to assume role %s (external_id=%s)", role_arn, "set" if external_id else "none")
        raise
    c = resp["Credentials"]
    logger.info(
        "AWS session: assumed role %s (external_id=%s)",
        role_arn,
        "set" if external_id else "none",
    )
    return boto3.Session(
        aws_access_key_id=c["AccessKeyId"],
        aws_secret_access_key=c["SecretAccessKey"],
        aws_session_token=c["SessionToken"],
        region_name=region,
    )


def get_aws_session(
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    credentials_secret_name: Optional[str] = None,
    local_creds_path: Optional[str] = None,
    glue_job_runtime: bool = False,
    role_arn: Optional[str] = None,
    assume_role_external_id: Optional[str] = None,
    role_session_name: str = "aws-ec2-bronze",
    rds_secret_arn: Optional[str] = None,
) -> boto3.Session:
    """Build a ``boto3.Session`` for EC2 / CloudWatch / STS calls.

    When ``role_arn`` is set, the **Glue execution role** (or local default/profile) is used to
    call ``sts.assume_role``; the returned session matches prod FOCUS / ``prod-other-glue`` behavior.

    Set ``glue_job_runtime=True`` from ``glue_entry.py`` so the Glue execution role is the base
    for ``assume_role`` (no local files). ``AWS_CREDENTIALS_SECRET`` static keys are ignored when
    ``role_arn`` is set.

    ``rds_secret_arn`` is **ignored** by this function (backward compatibility: older entry scripts
    pass the PostgreSQL job parameter here; JDBC for ``service_configuration`` is handled in
    ``service_config`` / ``resolve_active_services``).
    """
    _ = rds_secret_arn  # not used for boto; accepted so Glue scripts do not TypeError
    region = region_name or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

    ra = (role_arn or os.environ.get("ROLE_ARN") or "").strip() or None
    if ra:
        ext = _resolve_assume_role_external_id(assume_role_external_id)
        if not ext and os.environ.get("BRONZE_DISABLE_FINOMICS_DEFAULT_EXTERNAL_ID", "").lower() not in (
            "1",
            "true",
            "yes",
        ):
            from bronze.config.cross_account import DEFAULT_FINOMICS_ASSUME_ROLE_EXTERNAL_ID

            env_default = (os.environ.get("FINOMICS_DEFAULT_ASSUME_ROLE_EXTERNAL_ID") or "").strip()
            if env_default:
                ext = env_default
            else:
                ext = DEFAULT_FINOMICS_ASSUME_ROLE_EXTERNAL_ID
                logger.info(
                    "AssumeRole: EXTERNAL_ID not in job/env; using Finomics default (prod-other-glue). "
                    "Set EXTERNAL_ID or FINOMICS_DEFAULT_ASSUME_ROLE_EXTERNAL_ID, or "
                    "BRONZE_DISABLE_FINOMICS_DEFAULT_EXTERNAL_ID=1 to omit ExternalId.",
                )
        if glue_job_runtime:
            if (os.environ.get("AWS_PROFILE") or "").strip():
                logger.info("Glue job: ignoring AWS_PROFILE; base credentials are the Glue execution role.")
            cred_static = (credentials_secret_name or "").strip() or os.environ.get("FINOMICS_AWS_CREDENTIALS_SECRET", "").strip()
            if cred_static:
                logger.warning(
                    "ROLE_ARN is set: ignoring AWS_CREDENTIALS_SECRET (%s) for boto session; using assume_role only.",
                    cred_static,
                )
            base = boto3.Session(region_name=region or "us-east-1")
        else:
            base = _local_base_session_for_assume(profile_name, region)
        return _session_from_assumed_role(
            base,
            ra,
            region or "us-east-1",
            ext,
            role_session_name,
        )

    if glue_job_runtime:
        profile_name = None
        if (os.environ.get("AWS_PROFILE") or "").strip():
            logger.info(
                "Glue job: ignoring AWS_PROFILE in the environment; using execution role or secret.",
            )

    secret_id = (credentials_secret_name or "").strip() or os.environ.get(
        "FINOMICS_AWS_CREDENTIALS_SECRET", ""
    ).strip()

    if secret_id:
        logger.info("AWS session: loading keys from Secrets Manager (%s)", secret_id)
        secret = get_secret_json(secret_id)
        key_id = secret.get("aws_access_key_id")
        secret_key = secret.get("aws_secret_access_key")
        if not key_id or not secret_key:
            raise KeyError(
                f"Secret '{secret_id}' must include aws_access_key_id and aws_secret_access_key"
            )
        return boto3.Session(
            aws_access_key_id=key_id,
            aws_secret_access_key=secret_key,
            aws_session_token=secret.get("aws_session_token"),
            region_name=region or secret.get("region_name") or "us-east-1",
        )

    creds_file = (local_creds_path or "").strip() or os.environ.get("BRONZE_AWS_CREDS_FILE", "").strip()
    if not creds_file and not glue_job_runtime:
        creds_file = DEFAULT_LOCAL_CREDS_FILE

    if (not glue_job_runtime) and creds_file and os.path.isfile(creds_file):
        try:
            with open(creds_file, "r", encoding="utf-8") as f:
                creds = json.load(f)
            logger.info("AWS session: loading keys from local file (%s)", creds_file)
            return boto3.Session(
                aws_access_key_id=creds.get("aws_access_key_id"),
                aws_secret_access_key=creds.get("aws_secret_access_key"),
                aws_session_token=creds.get("aws_session_token"),
                region_name=region or creds.get("region_name") or "us-east-1",
            )
        except Exception:
            logger.warning(
                "AWS session: failed to read %s; using default credential chain",
                creds_file,
                exc_info=True,
            )

    if glue_job_runtime:
        logger.info(
            "AWS session: job role / default chain (no profile; region=%s)",
            region or "(default)",
        )
        return boto3.Session(region_name=region)

    prof = (profile_name if profile_name is not None else None) or os.environ.get("AWS_PROFILE")
    logger.info(
        "AWS session: default chain (profile=%s, region=%s)",
        prof or "(none)",
        region or "(default)",
    )
    return boto3.Session(profile_name=prof, region_name=region)
