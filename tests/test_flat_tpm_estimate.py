"""Regression tests for the queue processor's flat per-slot TPM estimate.

Guards the overshoot bug fixed 2026-07-22: when a dequeued item carried no
estimated_tokens (burst=0 configs that omit the max-token fields), the flat
fallback collapsed to 1024 tokens. The queue processor paces every token gate —
including the Gate 5 even-spacing pacer — on this value, so a 1024 estimate for
a ~6000-token request let the drain overshoot queue_target_tpm and the account
quota (→ throttles). See scripts/test_queue_overshoot_sim.py for the end-to-end
reproduction; this file locks the helper's behavior directly.

Run: python -m pytest tests/test_flat_tpm_estimate.py -q
"""
import sys
import pathlib
from collections import deque

# Import the queue processor handler from the Lambda handlers path.
HANDLERS = pathlib.Path(__file__).resolve().parents[1] / "infrastructure" / "lambda_handlers"
sys.path.insert(0, str(HANDLERS))
# The handler imports the shared layer at module load.
LAYER = pathlib.Path(__file__).resolve().parents[1] / "infrastructure" / "lambda_layer" / "python"
sys.path.insert(0, str(LAYER))

from queue_processor import _flat_tpm_estimate, _token_gate_sleep  # noqa: E402


def test_explicit_max_tokens_takes_precedence():
    """When default_max_tokens is set, use it (× burndown) + nominal input."""
    cfg = {"default_max_tokens": 1200, "output_token_burndown_rate": 1.0,
           "nominal_input_tokens": 5000}
    assert _flat_tpm_estimate(cfg, queue_target_tpm=5_520_000) == 6200


def test_max_tokens_per_request_alias():
    """max_tokens_per_request is honored when default_max_tokens is absent."""
    cfg = {"max_tokens_per_request": 2000, "output_token_burndown_rate": 1.0}
    assert _flat_tpm_estimate(cfg, queue_target_tpm=0) == 2000


def test_burndown_multiplier_applied():
    cfg = {"default_max_tokens": 1000, "output_token_burndown_rate": 5.0,
           "nominal_input_tokens": 100}
    assert _flat_tpm_estimate(cfg, queue_target_tpm=0) == 5100


def test_falls_back_to_target_implied_average():
    """The live nova-2-lite case: no max-token fields, but target+capacity known.

    5,520,000 / 1,380 = 4,000 tokens/request — a realistic value that keeps the
    pacer honest, NOT the catastrophic 1024.
    """
    cfg = {"queue_capacity": 1380}
    assert _flat_tpm_estimate(cfg, queue_target_tpm=5_520_000) == 4000


def test_never_collapses_to_1024_when_config_bare():
    """With NOTHING useful in config, the floor is 4096 — never the old 1024."""
    assert _flat_tpm_estimate({}, queue_target_tpm=0) == 4096


def test_live_config_would_not_have_reproduced_the_bug():
    """The exact deployed config (no max-token fields, burst=0) now yields 4000,
    not 1024 — so even a missing per-item estimate can't cause the 6x overshoot."""
    live_cfg = {
        "queue_target_tpm": 5_520_000,
        "tpm_queue_capacity": 5_520_000,
        "tpm_queue_regeneration_rate": 92_000,
        "queue_capacity": 1380,
        "queue_regeneration_rate": 23,
        "output_token_burndown_rate": 1,
        "bytes_per_token": 4,
        "backend": "runtime",
        # default_max_tokens / max_tokens_per_request / nominal_input_tokens ABSENT
    }
    est = _flat_tpm_estimate(live_cfg, queue_target_tpm=5_520_000)
    assert est == 4000
    assert est > 1024  # the regression guard


def test_split_gate_checks_only_the_selected_token_dimension():
    dispatch_log = deque([
        (100.0, 5500, 5000, 500),
        (101.0, 5500, 5000, 500),
    ])

    assert _token_gate_sleep(
        dispatch_log, 500, 3, 2.0, 1000, 101.5
    ) > 0
    assert _token_gate_sleep(
        dispatch_log, 5000, 2, 2.0, 20000, 101.5
    ) == 0


def test_split_gate_waits_until_enough_tokens_expire():
    dispatch_log = deque([
        (40.0, 5500, 5000, 500),
        (50.0, 5500, 5000, 500),
        (55.0, 5500, 5000, 500),
    ])

    sleep_for = _token_gate_sleep(
        dispatch_log, 500, 3, 60.0, 1500, 60.0
    )

    assert 40.0 < sleep_for < 40.01
