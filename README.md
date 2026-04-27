# Finomics AWS EC2 — Bronze layer

AWS Glue job that ingests **EC2 inventory** and **CloudWatch** metrics into **Apache Iceberg** on S3 (same orchestration pattern as the Azure bronze package under `finomics-custom-recs-bronze`).

## Project layout

```
aws-ec2-bronze/
├── glue_entry.py              # AWS Glue entry (use only on Glue)
├── local_run.py               # Local fetch-only (no Spark / no Iceberg)
├── requirements.txt           # Local + tests
├── requirements-glue.txt      # Optional: extra pip deps for Glue job
├── bronze/
│   ├── auth/                  # secrets.py + aws_auth.py (session for API + optional SM keys)
│   ├── config/
│   ├── core/
│   ├── services/aws/ec2.py    # EC2 ServiceDefinition + metric specs
│   └── utils/cloudwatch_metrics.py
```

## Deploy to AWS Glue (dev / prod)

Same packaging flow as Azure bronze:

1. Build a zip of the `bronze/` tree (so `import bronze` works):

   ```bash
   cd aws-ec2-bronze/bronze
   zip -r ../bronze.zip .
   cd ..
   ```

2. Upload **`glue_entry.py`** as the script path and **`bronze.zip`** as **Python library path** (or “extra python files”), per your Glue version’s UI.

3. Use a **Glue 4.0+** (or 5.0) Spark job with the **Iceberg** JARs / settings your platform already uses for other bronze jobs.

4. **Do not** ship `python-dotenv` for Glue — it is only for `local_run.py`. Glue does not need a `.env` file.

### IAM (execution role)

Attach a policy that allows at least:

| Action | Purpose |
|--------|---------|
| `sts:GetCallerIdentity` | Resolve account when `AWS_ACCOUNT_ID` is omitted |
| `ec2:DescribeInstances` | Instance inventory (each target Region) |
| `cloudwatch:GetMetricData` | Metrics |
| `s3:*` on your warehouse prefix | Iceberg data + metadata (align with least privilege in prod) |
| `glue:*` on your catalog DB/tables | As required by your Iceberg-on-Glue setup |
| `secretsmanager:GetSecretValue` | Only if you pass `AWS_CREDENTIALS_SECRET` |

Use **different roles** for dev vs prod by assigning different Glue jobs to different roles (same code, different IAM / different `S3_BUCKET` / `ICEBERG_DATABASE` parameters).

## Glue job parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `JOB_NAME` | Yes | Spark app name |
| `WINDOW_DAYS` | Yes | CloudWatch lookback (days) |
| `ACTIVE_SERVICES` | Yes | e.g. `EC2` |
| `CLIENT_ID` | Yes | Finomics client id (metadata) |
| `S3_BUCKET` | Yes | Iceberg warehouse bucket |
| `ICEBERG_DATABASE` | Yes | Glue database for Iceberg tables |
| `ICEBERG_CATALOG` | No | Default `glue_catalog` |
| `AWS_REGIONS` or `AWS_REGION` | No | Regions to scan; default: job `AWS_REGION` env, else `us-east-1` |
| `AWS_ACCOUNT_ID` | No | 12-digit account; default from STS |
| `ADDITIONAL_CLIENT_ID` | No | Duplicate MERGE under second client_id |
| `AWS_CREDENTIALS_SECRET` | No | Secret id for JSON keys (optional cross-account) |

`AWS_PROFILE` is **not** used on Glue: `glue_entry` sets `glue_job_runtime=True` and the **job role** is used (see `bronze.auth.aws_auth`).

## Local testing

```bash
pip install -r requirements.txt
python local_run.py --regions us-east-1 --window-days 2
```

## Relationship to Azure bronze

- Same **Glue script + zip** deployment pattern and **getResolvedOptions** optional args.
- **Azure** uses `get_azure_credential()` (Secrets Manager → Azure AD); **AWS EC2** uses the **Glue execution role** or optional `AWS_CREDENTIALS_SECRET` for static AWS keys in Secrets Manager.
