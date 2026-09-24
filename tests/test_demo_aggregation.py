"""Regression tests for the demo table's per-attempt throttle accounting, and
for the ceiling override that makes the shaper arm actually shaped (bead a7q.9).

Guards bead 8yk: _aggregate_direct_results() used to derive sim_thr from the
TERMINAL per-request `simulated_throttle` flag, so on the direct+retry arm every
gate rejection that was retried (the `continue` branch of
_direct_call_with_retry) vanished and only the requests that EXHAUSTED their
retries were counted. A real run reported this table:

    arm           offered attempts bedrock_calls  succ   err sim_thr
    shaper             61       61            61    61     0       0
    direct             61       61            37    37     0      24
    direct+retry       61      134            37    37     0      24   <-- wrong

The first two rows satisfy attempts == bedrock_calls + sim_thr; the retry row
does not (134 != 37 + 24). Every attempt either reaches converse() or is
rejected by the simulated gate, so that identity has to hold whenever no
request's attempt count is unknown -- and it is exactly the identity the demo's
headline claim (retry amplification buys attempts, not throughput) rests on.
sim_thr for that run should read 97, and real_thr has the same shape of bug.

These tests feed SYNTHETIC result dicts through the real helper. They make no
Bedrock calls: `make demo` costs real converse() spend and is deliberately not
the validation path for this behavior.

Run: python -m pytest tests/test_demo_aggregation.py -q
"""

import sys
import pathlib
from decimal import Decimal

import pytest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

# Imported unguarded on purpose (bead asx): scripts/demo.py and the modules it
# pulls in must be importable with no config.env and no AWS credentials, so an
# import failure here is a real defect and has to be a hard error. A
# pytest.skip(allow_module_level=True) fallback would leave the suite green while
# these tests silently did not run at all.
from demo import (  # noqa: E402
    _aggregate_direct_results,
    _build_ceiling_override_item,
    _build_filler_prompt,
    _new_virtual_budget,
    _virtual_budget_admit,
    DEMO_BYTES_PER_TOKEN,
    DEMO_MAX_OUTPUT_TOKENS,
    DEMO_REQUEST_COUNT,
    DEMO_TOKENS_PER_REQUEST_ESTIMATE,
    DEMO_TPM_OVERRIDE,
)

OFFERED = 61
ELAPSED_MIN = 1.0
THROTTLE_CODE = "ThrottlingException"

# The nova-2-lite CONFIG item as it is actually deployed (read off
# semaphore-single-table with `make inspect-config MODEL=nova-2-lite` on
# 2026-09-23), Decimal-typed the way boto3's resource API returns it. The
# override has to be replayed against a LIVE-SHAPED item, not a tidy synthetic
# one: the whole defect in this bead was an override that looked right in
# isolation and did nothing to the fields the queue processor reads.
LIVE_CONFIG_ITEM = {
    "pk": "MODEL#us.amazon.nova-2-lite-v1:0",
    "sk": "CONFIG",
    "entity_type": "model_config",
    "model_id": "us.amazon.nova-2-lite-v1:0",
    "api_style": "converse",
    "backend": "runtime",
    "adaptive_queue_threshold": Decimal("50"),
    "adaptive_shift_max": Decimal("0"),
    "rpm_limit": None,
    "rpm_quota_enabled": False,
    "burst_capacity": Decimal("0"),
    "burst_regeneration_rate": Decimal("0"),
    "queue_capacity": Decimal("1000000"),
    "queue_regeneration_rate": Decimal("1000000"),
    "buffer_capacity": Decimal("0"),
    "queue_batch_size": Decimal("10"),
    "tpm_limit": Decimal("8000000"),
    "tpm_burst_capacity": Decimal("0"),
    "tpm_burst_regeneration_rate": Decimal("0"),
    "tpm_queue_capacity": Decimal("6800000"),
    "tpm_queue_regeneration_rate": Decimal("113333.3333"),
    "tpm_buffer_capacity": Decimal("1200000"),
    "output_token_burndown_rate": Decimal("1"),
    "bytes_per_token": Decimal("3"),
    "short_window_sec": Decimal("2"),
    "long_window_sec": Decimal("15"),
}


def _rejected(attempts):
    """A request the simulated gate rejected on every one of its `attempts`
    tries, reaching converse() zero times (the retries-exhausted outcome)."""
    return {
        "ok": False,
        "simulated_throttle": True,
        "real_throttle_code": None,
        "error": None,
        "in": 0,
        "out": 0,
        "ms": 1200.0,
        "attempts": attempts,
        "bedrock_calls": 0,
        "sim_throttle_attempts": attempts,
        "real_throttle_codes": [],
        "completed_ts": 1000.0,
    }


def _succeeded(sim_rejections=0, real_codes=(), in_tok=1800, out_tok=800):
    """A request that ultimately succeeded, after `sim_rejections` gate
    rejections and len(real_codes) real throttles were retried away. attempts is
    every try; bedrock_calls is the subset that got past the gate."""
    real_codes = list(real_codes)
    calls = len(real_codes) + 1
    return {
        "ok": True,
        "simulated_throttle": False,
        "real_throttle_code": None,
        "error": None,
        "in": in_tok,
        "out": out_tok,
        "ms": 5500.0,
        "attempts": sim_rejections + calls,
        "bedrock_calls": calls,
        "sim_throttle_attempts": sim_rejections,
        "real_throttle_codes": real_codes,
        "completed_ts": 1000.0,
    }


def _timed_out():
    """A request still unresolved at the DEMO_DIRECT_TIMEOUT_S wait bound. Its
    true counts are unknowable (the thread may be mid-backoff), so they are None
    -- see _run_direct_arm."""
    return {
        "ok": False,
        "simulated_throttle": False,
        "real_throttle_code": None,
        "error": "timeout",
        "in": 0,
        "out": 0,
        "ms": None,
        "attempts": None,
        "bedrock_calls": None,
        "sim_throttle_attempts": None,
        "real_throttle_codes": None,
        "completed_ts": None,
    }


def _no_retry_arm():
    """The no-retry direct arm's reported run: 37 admitted and succeeded, 24
    rejected. One attempt per request, so per-attempt and per-request throttle
    counts coincide here -- which is why the bug was invisible on this row."""
    return [_succeeded() for _ in range(37)] + [_rejected(1) for _ in range(24)]


def _retry_arm():
    """The direct+retry arm's reported run, decomposed to reproduce its exact
    totals: 134 attempts, 37 bedrock_calls, 37 successes.

    24 requests exhausted all 4 tries (1 initial + DEMO_RETRY_MAX_RETRIES) and
    never reached converse()  -> 96 attempts, 0 calls, 96 gate rejections.
    The 37 that succeeded consumed the remaining 134 - 96 = 38 attempts across
    37 calls -> exactly 1 more gate rejection, on one of them.

    So sim_thr = 96 + 1 = 97, and 37 + 97 = 134. 97 is a derived figure, not a
    magic number; note it EXCEEDS the 61 requests offered, because a request
    rejected four times is counted four times.
    """
    return (
        [_rejected(4) for _ in range(24)]
        + [_succeeded() for _ in range(36)]
        + [_succeeded(sim_rejections=1)]
    )


@pytest.mark.parametrize(
    "label,results,exp_attempts,exp_calls,exp_sim_thr",
    [
        ("direct", _no_retry_arm(), 61, 37, 24),
        ("direct+retry", _retry_arm(), 134, 37, 97),
    ],
)
def test_attempts_equals_calls_plus_sim_thr(label, results, exp_attempts, exp_calls, exp_sim_thr):
    """The invariant, on both arms that flow through this helper."""
    agg = _aggregate_direct_results(results, OFFERED, ELAPSED_MIN)
    assert agg["unknown_attempts"] == 0
    assert agg["attempts"] == exp_attempts
    assert agg["bedrock_calls"] == exp_calls
    assert agg["sim_thr"] == exp_sim_thr
    assert agg["attempts"] == agg["bedrock_calls"] + agg["sim_thr"]
    assert agg["succ"] == 37
    assert agg["err"] == 0


def test_shaper_row_satisfies_the_invariant_by_construction():
    """The shaper row is not aggregated by this helper -- main() builds it from
    constants, because the shaper never retries client-side and its throttles
    are REAL outcomes rather than simulated gate rejections. Pinned here so the
    invariant is stated for all three rows of the table: sim_thr stays 0, and
    attempts == bedrock_calls == the offered count.
    """
    attempts = bedrock_calls = DEMO_REQUEST_COUNT
    sim_thr = 0
    assert attempts == bedrock_calls + sim_thr


def test_retry_arm_terminal_flag_undercounts_by_4x():
    """The bug itself, pinned: counting the terminal per-request flag instead of
    attempts returns 24 for a run that really suffered 97 rejections, and breaks
    the invariant. If this ever equals sim_thr again on this arm, the fix has
    been reverted."""
    results = _retry_arm()
    terminal_flag_count = sum(1 for r in results if r.get("simulated_throttle"))
    agg = _aggregate_direct_results(results, OFFERED, ELAPSED_MIN)
    assert terminal_flag_count == 24
    assert agg["sim_thr"] == 97
    assert agg["attempts"] != agg["bedrock_calls"] + terminal_flag_count


def test_real_thr_counts_throttles_that_were_retried_away():
    """real_thr had the identical bug: a real throttle that was retried and then
    succeeded leaves no terminal real_throttle_code, so it used to be invisible.
    The codes list must survive too -- a nonzero real_thr with an empty code
    list would print "N real throttle(s) ... code(s):" with nothing after it."""
    results = [_succeeded(real_codes=(THROTTLE_CODE, THROTTLE_CODE)), _succeeded()]
    agg = _aggregate_direct_results(results, OFFERED, ELAPSED_MIN)
    assert sum(1 for r in results if r.get("real_throttle_code")) == 0
    assert agg["real_thr"] == 2
    assert agg["real_codes"] == [THROTTLE_CODE]


def test_timed_out_requests_are_excluded_not_counted_as_zero():
    """Wait-bound casualties must drop out of the new counts the same way they
    already drop out of attempts/bedrock_calls, and stay visible via
    unknown_attempts. Counting them as 0 would assert they were never throttled,
    which is a guess -- their real counts are unknowable."""
    baseline = _aggregate_direct_results(_retry_arm(), OFFERED, ELAPSED_MIN)
    with_timeouts = _aggregate_direct_results(
        _retry_arm() + [_timed_out(), _timed_out()], OFFERED, ELAPSED_MIN
    )
    assert with_timeouts["unknown_attempts"] == 2
    for key in ("attempts", "bedrock_calls", "sim_thr", "real_thr"):
        assert with_timeouts[key] == baseline[key], key
    # The invariant is only claimed when nothing is unknown; the timeouts show up
    # as errors instead, which is how the table already reports them.
    assert with_timeouts["err"] == baseline["err"] + 2
    assert with_timeouts["succ"] == baseline["succ"]


def test_rejected_only_arm_never_reaches_bedrock():
    """Degenerate end of the range: every request rejected on every try means
    zero real spend, and the invariant reduces to attempts == sim_thr."""
    agg = _aggregate_direct_results([_rejected(4) for _ in range(61)], OFFERED, ELAPSED_MIN)
    assert agg["bedrock_calls"] == 0
    assert agg["attempts"] == 244
    assert agg["sim_thr"] == 244
    assert agg["attempts"] == agg["bedrock_calls"] + agg["sim_thr"]


def test_empty_results_are_all_zero():
    agg = _aggregate_direct_results([], OFFERED, ELAPSED_MIN)
    assert agg["attempts"] == 0
    assert agg["bedrock_calls"] == 0
    assert agg["sim_thr"] == 0
    assert agg["real_thr"] == 0
    assert agg["unknown_attempts"] == 0


# --- bead a7q.9: the ceiling override, replayed offline against the live item ---


def test_override_keeps_the_live_split_and_does_not_hand_the_queue_the_ceiling():
    """The AC's central assertion: re-deriving at a 100,000 ceiling must scale the
    LIVE 0.85 queue share down to 85,000, NOT give the queue the whole 100,000.

    The live item's share is 6,800,000/8,000,000 = 0.85, and that ratio -- read off
    the deployed item rather than hardcoded -- is what has to survive the override.
    """
    overridden = _build_ceiling_override_item(LIVE_CONFIG_ITEM, LIVE_CONFIG_ITEM["model_id"])

    assert overridden["tpm_limit"] == DEMO_TPM_OVERRIDE == 100_000
    assert overridden["tpm_queue_capacity"] == 85_000
    assert overridden["tpm_queue_capacity"] != DEMO_TPM_OVERRIDE
    assert float(overridden["tpm_queue_regeneration_rate"]) == pytest.approx(1416.6667)
    # The other two thirds of the split scale by the same rule.
    assert overridden["tpm_burst_capacity"] == 0
    assert overridden["tpm_buffer_capacity"] == 15_000
    # Stated as the ratio too, so a future change that lands 85,000 by coincidence
    # rather than by preserving the share still fails here.
    live_share = float(LIVE_CONFIG_ITEM["tpm_queue_capacity"]) / float(
        LIVE_CONFIG_ITEM["tpm_limit"]
    )
    assert live_share == 0.85
    assert (overridden["tpm_queue_capacity"] / overridden["tpm_limit"]) == live_share


def test_override_rewrites_every_field_the_queue_processor_gates_on():
    """The original defect: only tpm_limit moved, so the processor kept pacing on a
    6,800,000-token queue. Every field queue_processor.py:197-198 reads must change.
    """
    overridden = _build_ceiling_override_item(LIVE_CONFIG_ITEM, LIVE_CONFIG_ITEM["model_id"])
    for field in (
        "tpm_limit",
        "tpm_queue_capacity",
        "tpm_queue_regeneration_rate",
        "tpm_buffer_capacity",
    ):
        assert float(overridden[field]) != float(LIVE_CONFIG_ITEM[field]), field


def test_override_merge_keeps_fields_calculate_config_never_emits():
    """calculate_config() does not emit api_style/backend/adaptive_*; the merge onto
    the original item is what keeps them, and dropping them would break the run."""
    overridden = _build_ceiling_override_item(LIVE_CONFIG_ITEM, LIVE_CONFIG_ITEM["model_id"])
    for field in ("api_style", "backend", "adaptive_queue_threshold", "adaptive_shift_max"):
        assert overridden[field] == LIVE_CONFIG_ITEM[field], field
    assert set(LIVE_CONFIG_ITEM) <= set(overridden)


def test_override_writes_the_demos_own_bytes_per_token():
    """The demo writes its own flat bytes_per_token into the item it puts and later
    restores, instead of the live per-model value -- and that field feeds no capacity
    arithmetic, so the split assertions above still hold with it in place."""
    overridden = _build_ceiling_override_item(LIVE_CONFIG_ITEM, LIVE_CONFIG_ITEM["model_id"])
    assert float(overridden["bytes_per_token"]) == pytest.approx(DEMO_BYTES_PER_TOKEN)
    assert float(overridden["bytes_per_token"]) != float(LIVE_CONFIG_ITEM["bytes_per_token"])


def test_both_arms_charge_the_same_tokens_for_the_same_request():
    """The final report claims all arms are judged "by one budget under one
    accounting method". That is only true if the direct arms' virtual gate charges
    what the shaper's config makes budget_manager charge -- the gate used to fall
    through to the library's 4.0 default while the config said 3.0."""
    budget = _new_virtual_budget(1.0)
    charge, est, used = _virtual_budget_admit(
        budget, _build_filler_prompt(), DEMO_MAX_OUTPUT_TOKENS
    )
    assert charge is not None and used == 0
    assert est == DEMO_TOKENS_PER_REQUEST_ESTIMATE


def test_request_profile_is_flat_constants_not_derivations():
    """Bead a7q.10: the three request-profile knobs are flat literals. They must NOT
    come back as arithmetic over the ceiling, the queue share or a per-prompt
    tokenizer measurement -- the demo offers load and measures, it does not predict.
    The offered load only has to comfortably exceed the ceiling so the queue never
    starves, which is asserted as an inequality rather than pinned to a ratio."""
    assert DEMO_BYTES_PER_TOKEN == 5
    assert DEMO_MAX_OUTPUT_TOKENS == 1000
    assert DEMO_REQUEST_COUNT == 70
    offered_tokens = DEMO_REQUEST_COUNT * DEMO_TOKENS_PER_REQUEST_ESTIMATE
    assert offered_tokens > DEMO_TPM_OVERRIDE
