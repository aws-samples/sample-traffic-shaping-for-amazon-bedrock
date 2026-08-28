"""Reusable cdk-nag suppression helper (Python).

Copy this file into a CDK app (e.g. ``infrastructure/nag_suppressions.py``) and
use it INSTEAD of calling ``NagSuppressions`` directly. The wrapper enforces the
one rule that makes suppressions safe: every waiver carries a non-empty,
human-readable reason. A suppression without a justification is a latent
incident -- this helper refuses to create one.

Usage::

    from nag_suppressions import suppress

    suppress(my_fn.role, [
        {"id": "AwsSolutions-IAM4",
         "reason": "AWSLambdaBasicExecutionRole is acceptable for this internal prototype."},
    ])

Pair with the app-entry Aspect (fail-on-violation, the INTERNAL default)::

    import aws_cdk as cdk
    from cdk_nag import AwsSolutionsChecks
    cdk.Aspects.of(app).add(AwsSolutionsChecks())   # verbose default; error-level fails synth
"""

from __future__ import annotations

import re
from typing import Sequence

from constructs import IConstruct
from cdk_nag import NagSuppressions

_MIN_REASON_LEN = 15
_PLACEHOLDER = re.compile(r"^(todo|fixme|tbd|fix later|n/?a|-+|\.+)$", re.IGNORECASE)
_VALID_ID = re.compile(r"^(AwsSolutions-|HIPAA|NIST|PCI)")


def _assert_justified(entry: dict) -> None:
    rule_id = entry.get("id", "")
    reason = (entry.get("reason") or "").strip()
    if not reason:
        raise ValueError(
            f'cdk-nag suppression for "{rule_id}" has an empty reason. '
            "Every waiver must be justified."
        )
    if len(reason) < _MIN_REASON_LEN or _PLACEHOLDER.match(reason):
        raise ValueError(
            f'cdk-nag suppression for "{rule_id}" has a placeholder reason ({reason!r}). '
            f"Write a real justification (>={_MIN_REASON_LEN} chars) or fix the finding."
        )
    if not rule_id or not _VALID_ID.match(rule_id):
        raise ValueError(f"cdk-nag suppression has an invalid rule id: {rule_id!r}.")


def suppress(
    construct: IConstruct,
    entries: Sequence[dict],
    apply_to_children: bool = False,
) -> None:
    """Suppress cdk-nag findings on a construct, enforcing a justification per entry.

    Prefer this over stack-wide suppressions -- scope each waiver as tightly as
    possible.
    """
    if not entries:
        raise ValueError("suppress() called with no entries.")
    for entry in entries:
        _assert_justified(entry)
    NagSuppressions.add_resource_suppressions(
        construct, list(entries), apply_to_children
    )


def suppress_by_path(stack: IConstruct, path: str, entries: Sequence[dict]) -> None:
    """Path-based suppression for constructs you don't hold a reference to."""
    if not entries:
        raise ValueError("suppress_by_path() called with no entries.")
    for entry in entries:
        _assert_justified(entry)
    NagSuppressions.add_resource_suppressions_by_path(stack, path, list(entries))


# NOTE: There is intentionally NO "suppress everything" / wildcard helper.
# Silencing without justification is the failure mode this file exists to prevent.
