import json
import logging

import boto3

logger = logging.getLogger(__name__)

_client = None
_cache = {}


def _get_client():
    global _client
    if _client is None:
        _client = boto3.client("secretsmanager")
    return _client


def get_secret_json(secret_name: str) -> dict:
    """Fetch a JSON secret from AWS Secrets Manager (cached after first call).

    The caller must already have IAM permission to read this secret (Glue job role,
    developer profile, etc.) — same pattern as Azure bronze reading Azure SP secrets.
    """
    if secret_name in _cache:
        return _cache[secret_name]

    client = _get_client()
    response = client.get_secret_value(SecretId=secret_name)
    if "SecretString" not in response:
        raise ValueError(f"Secret '{secret_name}' has no SecretString")

    parsed = json.loads(response["SecretString"])
    _cache[secret_name] = parsed
    return parsed
