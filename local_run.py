"""Local test runner — EC2 + CloudWatch fetch only (no Spark/Iceberg).

Requires AWS credentials (profile, env, or instance role).

Console (preview):
    python local_run.py --regions us-east-1 --services EC2 --window-days 2

Export JSON + CSV (files named by service; default folder ``output``):
    cd /d D:\\Adarsh-Projects\\finomics\\AWS\\EC2-POC\\aws-ec2-bronze
    python local_run.py --regions us-east-1 --window-days 2 --export-json --export-csv --quiet

Writes ``output\\EC2.json`` and ``output\\EC2\\*.csv`` (override folder with ``--out-dir``).

Environment (optional): LOCAL_EXPORT_JSON=true, LOCAL_EXPORT_CSV=true, LOCAL_EXPORT_DIR=output, LOCAL_EXPORT_QUIET=true
"""

import argparse
import json
import logging
import os
import re

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import pandas as pd
from botocore.exceptions import ClientError

import bronze.services  # noqa: F401
from bronze.auth.aws_auth import get_aws_session
from bronze.config.aws_regions import initial_boto_session_region, resolve_ingestion_regions
from bronze.config.cross_account import effective_role_arn, parse_aws_target_account_ids
from bronze.config.job_params import JobParams
from bronze.config.service_config import resolve_active_services
from bronze.services.base import AwsServiceRunner
from bronze.services.registry import get_service_definition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("local_run")

logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Environment keys that override ~/.aws and SSO; a stale AWS_SESSION_TOKEN in .env causes ExpiredToken.
_AWS_KEY_ENV_OVERRIDES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
)


def _clear_env_aws_key_overrides() -> None:
    """Remove static/session key env vars so boto3 uses shared credentials or SSO (from ``aws sso login``)."""
    cleared = [k for k in _AWS_KEY_ENV_OVERRIDES if k in os.environ]
    for k in _AWS_KEY_ENV_OVERRIDES:
        os.environ.pop(k, None)
    if cleared:
        logger.info(
            "Cleared %s from the environment; credential chain will use your profile/CLI login.",
            ", ".join(cleared),
        )


def _should_use_profile_only(args) -> bool:
    if getattr(args, "use_profile_creds", False):
        return True
    if os.environ.get("BRONZE_USE_PROFILE_CREDENTIALS", "").lower() in ("1", "true", "yes"):
        return True
    return False


def _resolve_account_id(explicit: str, boto_session) -> str:
    if explicit:
        return explicit
    try:
        return boto_session.client("sts").get_caller_identity()["Account"]
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "ExpiredToken":
            msg = (
                "AWS credentials in the environment are expired (ExpiredToken). "
                "This often happens when .env still sets AWS_ACCESS_KEY_ID / AWS_SESSION_TOKEN from an old session, "
                "which overrides a fresh `aws sso login` or default profile.\n"
                "Fix: (1) Comment out or remove those lines from .env, or "
                "(2) Run: python local_run.py ... --use-profile-creds --aws-profile YOUR_PROFILE, or "
                "(3) Set BRONZE_USE_PROFILE_CREDENTIALS=true in .env, or "
                "(4) In cmd: set AWS_SESSION_TOKEN= & set AWS_ACCESS_KEY_ID= (then run again) / run `aws sso login`."
            )
            raise RuntimeError(msg) from e
        raise


def build_params(account_id: str, regions: list, args, active_services: list) -> JobParams:
    return JobParams(
        job_name="local-test",
        window_days=args.window_days,
        active_services=active_services,
        account_id=account_id,
        client_id=args.client_id,
        additional_client_id=getattr(args, "additional_client_id", "") or "",
        aws_regions=regions,
        iceberg_catalog="local",
        iceberg_database="local",
        s3_bucket="local",
        aws_profile=(getattr(args, "aws_profile", None) or "").strip(),
        aws_credentials_secret=(getattr(args, "aws_credentials_secret", None) or "").strip(),
        role_arn=(getattr(args, "role_arn", None) or "").strip(),
        rds_secret_arn=(getattr(args, "rds_secret_arn", None) or "").strip(),
        external_id=(getattr(args, "external_id", None) or "").strip(),
        role_target_account_ids=parse_aws_target_account_ids(
            {
                "CLOUD_ACCOUNT_ID": (getattr(args, "cloud_account_id", None) or "").strip(),
                "ACCOUNT_IDS": (getattr(args, "account_ids", None) or "").strip(),
            }
        ),
        glue_job_runtime=False,
    )


def _safe_filename_part(name: str) -> str:
    return re.sub(r'[^\w\-.]', "_", name)


def print_table(name: str, records: list, max_rows: int) -> None:
    if not records:
        print(f"\n{'='*60}")
        print(f"  {name}: (empty — 0 records)")
        print(f"{'='*60}")
        return

    df = pd.DataFrame(records)
    print(f"\n{'='*60}")
    print(f"  {name}: {len(records)} records, {len(df.columns)} columns")
    print(f"{'='*60}")
    print(f"  Columns: {list(df.columns)}")
    print()
    with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_colwidth", 40):
        print(df.head(max_rows).to_string(index=False))
    if len(records) > max_rows:
        print(f"  ... and {len(records) - max_rows} more rows")
    print()


def write_json_file(path: str, payload: dict) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    logger.info("Wrote JSON: %s", os.path.abspath(path))


def _payload_for_service(aggregated: dict, service_name: str) -> dict:
    """Per-region table payloads for one service."""
    out = {}
    for region, by_service in aggregated.items():
        if service_name in by_service:
            out[region] = by_service[service_name]
    return out


def write_csv_for_service(out_dir: str, service_name: str, aggregated: dict) -> None:
    """``{out_dir}/{SERVICE}/{region}_{table}.csv``"""
    svc_dir = os.path.join(out_dir, _safe_filename_part(service_name))
    os.makedirs(svc_dir, exist_ok=True)
    svc_abs = os.path.abspath(svc_dir)
    for region, by_service in aggregated.items():
        if service_name not in by_service:
            continue
        reg_safe = _safe_filename_part(region)
        for table_name, records in by_service[service_name].items():
            tbl_safe = _safe_filename_part(table_name)
            fname = f"{reg_safe}_{tbl_safe}.csv"
            fpath = os.path.join(svc_abs, fname)
            df = pd.DataFrame(records) if records else pd.DataFrame()
            df.to_csv(fpath, index=False)
            logger.info("Wrote CSV: %s (%d rows)", fpath, len(records))
    logger.info("CSV export directory for %s: %s", service_name, svc_abs)


def main():
    parser = argparse.ArgumentParser(description="Local test runner for AWS EC2 bronze ingestion")
    parser.add_argument(
        "--regions",
        default=os.environ.get("AWS_REGIONS") or os.environ.get("AWS_REGION", "us-east-1"),
        help="Comma-separated regions, or ALL / * / AUTO for all enabled EC2 regions (default: AWS_REGIONS or AWS_REGION or us-east-1)",
    )
    parser.add_argument(
        "--services",
        default=os.environ.get("LOCAL_SERVICES", "EC2"),
        help="Comma-separated service names, or ALL to load from RDS service_configuration_v2 (default: EC2 or LOCAL_SERVICES)",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=int(os.environ.get("LOCAL_WINDOW_DAYS", "2")),
        help="Metrics lookback days (default: 2 or LOCAL_WINDOW_DAYS)",
    )
    parser.add_argument("--max-resources", type=int, default=0, help="Limit metrics fetch to first N resources (0=all)")
    parser.add_argument("--max-rows", type=int, default=20, help="Max rows to display per table")
    parser.add_argument(
        "--client-id",
        default=os.environ.get("CLIENT_ID", "local-test"),
        help="Client ID for metadata stamping (default: CLIENT_ID or local-test)",
    )
    parser.add_argument("--additional-client-id", default="", help="Optional second client_id (not used in dry fetch)")
    parser.add_argument(
        "--account-id",
        default=os.environ.get("AWS_ACCOUNT_ID", ""),
        help="AWS account ID (default: AWS_ACCOUNT_ID or STS)",
    )
    parser.add_argument(
        "--aws-profile",
        default=os.environ.get("AWS_PROFILE", ""),
        help="Shared credentials profile (optional; default credential chain if empty)",
    )
    parser.add_argument(
        "--aws-credentials-secret",
        default=os.environ.get("FINOMICS_AWS_CREDENTIALS_SECRET", ""),
        help="Secrets Manager secret id for JSON keys (optional; ignored if --role-arn is set)",
    )
    parser.add_argument(
        "--role-arn",
        default=os.environ.get("ROLE_ARN", ""),
        help="If set, assume this role (sts.assume_role) after your profile/chain — same as Glue ROLE_ARN",
    )
    parser.add_argument(
        "--rds-secret-arn",
        default=os.environ.get("RDS_SECRET_ARN", ""),
        help="PostgreSQL secret (host, port, dbname, username, password) for ACTIVE_SERVICES=ALL; same as Azure RDS_SECRET_ARN",
    )
    parser.add_argument(
        "--external-id",
        default=os.environ.get("EXTERNAL_ID", ""),
        help="Optional ExternalId for assume_role (prod trust policy)",
    )
    parser.add_argument(
        "--cloud-account-id",
        default=os.environ.get("CLOUD_ACCOUNT_ID", ""),
        help="If set with --role-arn, rewrites ROLE_ARN to this 12-digit account (same role name). FOCUS parity.",
    )
    parser.add_argument(
        "--account-ids",
        default=os.environ.get("ACCOUNT_IDS", ""),
        help="Comma list; first id used like CLOUD_ACCOUNT_ID if --cloud-account-id is not set",
    )
    parser.add_argument(
        "--use-profile-creds",
        action="store_true",
        help="Do not use AWS key env vars (clears them for this process) so your CLI/SSO profile is used. "
        "Use when you get ExpiredToken after 'aws sso login' but .env still has old session keys.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print full aggregated payload as JSON to stdout (does not write a file; use --export-json for files)",
    )
    parser.add_argument(
        "--export-json",
        action="store_true",
        help="Write one JSON file per service: {out_dir}/{SERVICE}.json",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Write CSVs per service under {out_dir}/{SERVICE}/",
    )
    parser.add_argument(
        "--out-dir",
        default=os.environ.get("LOCAL_EXPORT_DIR", "output"),
        metavar="DIR",
        help="Base directory for --export-json and --export-csv (default: output or LOCAL_EXPORT_DIR)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Skip table preview and region headers on stdout (still prints JSON if --json)",
    )
    args = parser.parse_args()
    if os.environ.get("LOCAL_EXPORT_QUIET", "").lower() in ("1", "true", "yes"):
        args.quiet = True
    if os.environ.get("LOCAL_EXPORT_JSON", "").lower() in ("1", "true", "yes"):
        args.export_json = True
    if os.environ.get("LOCAL_EXPORT_CSV", "").lower() in ("1", "true", "yes"):
        args.export_csv = True

    if _should_use_profile_only(args) and not (args.aws_credentials_secret or "").strip():
        _clear_env_aws_key_overrides()

    raw_services = [s.strip().upper() for s in (args.services or "").split(",") if s.strip()]
    resolved_active = resolve_active_services(
        active_services=raw_services,
        rds_secret_arn=(getattr(args, "rds_secret_arn", None) or "").strip(),
        spark=None,
    )

    run_targets = list(
        parse_aws_target_account_ids(
            {
                "CLOUD_ACCOUNT_ID": (getattr(args, "cloud_account_id", None) or "").strip(),
                "ACCOUNT_IDS": (getattr(args, "account_ids", None) or "").strip(),
            }
        )
    ) or [None]
    multi_account = len(run_targets) > 1

    regions_input = [r.strip() for r in args.regions.split(",") if r.strip()]
    profile = (args.aws_profile or "").strip() or None
    cred_secret = (args.aws_credentials_secret or "").strip() or None
    ext = (getattr(args, "external_id", None) or "").strip() or None
    rds_arn = (getattr(args, "rds_secret_arn", None) or "").strip() or None

    def _local_boto_for_target(target_aid):
        raw_ra = (getattr(args, "role_arn", None) or "").strip() or None
        if not raw_ra:
            return get_aws_session(
                region_name=initial_boto_session_region(regions_input),
                profile_name=profile,
                credentials_secret_name=cred_secret,
                role_arn=None,
                assume_role_external_id=ext or None,
                rds_secret_arn=rds_arn,
            )
        role_for = raw_ra if target_aid is None else (effective_role_arn(raw_ra, target_aid) or raw_ra)
        return get_aws_session(
            region_name=initial_boto_session_region(regions_input),
            profile_name=profile,
            credentials_secret_name=cred_secret,
            role_arn=role_for,
            assume_role_external_id=ext or None,
            rds_secret_arn=rds_arn,
        )

    seed = _local_boto_for_target(run_targets[0])
    regions = resolve_ingestion_regions(regions_input, seed)

    aggregated: dict = {}

    for taid in run_targets:
        boto_session = _local_boto_for_target(taid)
        account_id = _resolve_account_id((args.account_id or "").strip(), boto_session)
        for region in regions:
            region_key = f"{account_id}@{region}" if multi_account else region
            if not args.quiet:
                print(f"\n{'#'*60}")
                print(f"  Region: {region} | Data account: {account_id} | target_param: {taid!r}")
                print(f"{'#'*60}")

            params = build_params(account_id, [region], args, resolved_active)
            aggregated.setdefault(region_key, {})

            for service_name in params.active_services:
                definition = get_service_definition(service_name)
                if definition is None:
                    logger.error(
                        "Unknown service '%s'. Available: %s",
                        service_name,
                        ", ".join(__import__("bronze.services.registry", fromlist=["list_registered_services"]).list_registered_services()),
                    )
                    continue

                runner = AwsServiceRunner(
                    spark=None,
                    params=params,
                    definition=definition,
                    region=region,
                    dry_run=True,
                    boto_session=boto_session,
                )
                fetched = runner.fetch_only()
                results = {
                    definition.table_config.table_name: fetched.get("resources") or [],
                }
                if definition.metrics:
                    results[definition.metrics.table_config.table_name] = fetched.get("metrics") or []

                max_res = args.max_resources if args.max_resources > 0 else int(os.environ.get("LOCAL_MAX_RESOURCES", "0"))
                if max_res > 0 and definition.metrics:
                    resource_table = definition.table_config.table_name
                    metrics_table = definition.metrics.table_config.table_name
                    if resource_table in results and metrics_table in results:
                        limited_ids = {
                            r.get(definition.metrics.resource_id_field)
                            for r in results[resource_table][:max_res]
                        }
                        results[metrics_table] = [
                            m for m in results[metrics_table] if m.get("resource_id") in limited_ids
                        ]

                aggregated[region_key][service_name] = results

                if not args.quiet and not args.json:
                    for table_name, records in results.items():
                        print_table(f"{region} / {service_name} / {table_name}", records, args.max_rows)

    if args.json:
        logger.info("Writing aggregated JSON to stdout (--export-json writes output/{SERVICE}.json)")
        # print(json.dumps(aggregated, indent=2, default=str), flush=True)

    out_dir = (args.out_dir or "output").strip() or "output"
    service_list = [s.strip().upper() for s in args.services.split(",") if s.strip()]

    if args.export_json:
        os.makedirs(out_dir, exist_ok=True)
        for svc in service_list:
            payload = _payload_for_service(aggregated, svc)
            if not payload:
                logger.warning("No data for service %s; skipping JSON export.", svc)
                continue
            json_path = os.path.join(out_dir, f"{_safe_filename_part(svc)}.json")
            write_json_file(json_path, payload)

    if args.export_csv:
        os.makedirs(out_dir, exist_ok=True)
        for svc in service_list:
            if not _payload_for_service(aggregated, svc):
                logger.warning("No data for service %s; skipping CSV export.", svc)
                continue
            write_csv_for_service(out_dir, svc, aggregated)


if __name__ == "__main__":
    main()
