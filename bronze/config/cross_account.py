"""Align cross-account role assumption with ``bronze/prod-other-glue.py`` and org patterns."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Same default as ``prod-other-glue.AWSFocusBronzeJob`` when ``EXTERNAL_ID`` is not passed.
# Override via job param ``EXTERNAL_ID`` or env ``EXTERNAL_ID`` / ``BRONZE_ASSUME_ROLE_EXTERNAL_ID``.
DEFAULT_FINOMICS_ASSUME_ROLE_EXTERNAL_ID = "Herbalife-36121070-9cc2-4a91-974d-b98631bf61e4"


def parse_aws_target_account_ids(raw_args: Dict[str, Any]) -> tuple[str, ...]:
    """
    All distinct 12-digit AWS account ids from ``CLOUD_ACCOUNT_ID`` and/or ``ACCOUNT_IDS``.

    Glue/Step Functions often pass a **single** parameter containing a comma-separated list; we parse
    with regex so values like
    ``893999053208,036482137149,098009328036`` (or one blob with no spaces) are handled.
    """
    out: list[str] = []
    seen: set[str] = set()
    for key in ("CLOUD_ACCOUNT_ID", "ACCOUNT_ID", "ACCOUNT_IDS"):
        val = (raw_args.get(key) or "").strip()
        if not val:
            continue
        for m in re.finditer(r"\b(\d{12})\b", val):
            aid = m.group(1)
            if aid not in seen:
                seen.add(aid)
                out.append(aid)
    return tuple(out)


def effective_role_arn(role_arn: str, target_aws_account_id: str) -> str:
    """
    If ``target_aws_account_id`` is a 12-digit id, replace the account segment of a standard
    ``arn:aws:iam::...:role/...`` so you can keep one role *name* while targeting the client
    account (same pattern as many Finomics templates where ``ROLE_ARN`` points at a template
    account but ``CLOUD_ACCOUNT_ID`` / ``ACCOUNT_IDS`` is the data account).
    """
    ra = (role_arn or "").strip()
    ta = (target_aws_account_id or "").strip()
    if not ra or not ta:
        return ra
    if not re.match(r"^\d{12}$", ta):
        logger.warning("Target AWS account id must be 12 digits; ignoring: %r", ta)
        return ra
    m = re.match(r"^(arn:aws:iam::)\d{12}(:role/.+)$", ra)
    if not m:
        logger.warning("ROLE_ARN is not of the form arn:aws:iam::<12-digit>:role/...; not rewriting: %s", ra)
        return ra
    out = f"{m.group(1)}{ta}{m.group(2)}"
    if out != ra:
        logger.info("ROLE_ARN rewritten for target account %s: %s", ta, out)
    return out
