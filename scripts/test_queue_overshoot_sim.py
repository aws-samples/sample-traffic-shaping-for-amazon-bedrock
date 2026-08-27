#!/usr/bin/env python3
"""
Queue Processor OVERSHOOT reproduction sim (Tier 2c) — estimate-vs-actual drift.

WHY THIS EXISTS
---------------
Live session 2 (2026-07-22) found the queue drain OVERSHOOTS its target:
`queue_target_tpm=5.52M` was configured, but Bedrock saw ~7.6M TPM and threw
~410-530 throttles despite the 60s average being nominally below quota.

The existing dispatch sims (test_queue_processor_sim.py,
test_dispatch_algorithm_sim.py) could NOT reproduce this for two structural
reasons:
  1. They never model Gate 5 (the even-spacing `queue_target_tpm` pacer) — it is
     the PRIMARY rate control in production but exists in no assertion sim.
  2. They use a SINGLE token weight per item and measure TPM off that same
     weight — so the number the gates pace on and the number "Bedrock charges"
     are identical by construction. They are blind to estimate-vs-actual drift.

Config inspection of the deployed nova-2-lite config (your-profile, 2026-07-22)
revealed the mechanism:
    queue_target_tpm            = 5,520,000   (Gate 5 ENABLED)
    tpm_burst_capacity          = 0           (burst disabled)
    default_max_tokens          = <absent>
    max_tokens_per_request      = <absent>
    nominal_input_tokens        = <absent>

  → budget_manager only computes estimated_tokens when tpm_burst_capacity > 0,
    so with burst=0 the queue items carry NO estimate.
  → queue_processor's flat fallback = default_max_tokens(→1024) * burndown(1.0)
    + nominal_input_tokens(→0) = 1024.
  → Every item is PACED as 1024 tokens while Bedrock CHARGES ~6000.

Effect: all three TOKEN gates (2s, 60s, and Gate 5) compute against 1024, so
they think the rate is ~1.4M and NEVER block. Only the RPM-60s gate
(queue_capacity=1380/min = 23 req/s) binds → 23 req/s × 6000 real tokens =
8.28M real TPM → overshoot + throttles.

WHAT THIS SIM MODELS (faithful mirror of queue_processor.py:575-659)
--------------------------------------------------------------------
All five gates in order, per item:
    Gate 1: RPM 2s        Gate 2: TPM 2s (+oversized)     Gate 3: RPM 60s
    Gate 4: TPM 60s       Gate 5: even-spacing pacer (queue_target_tpm)
Gates pace on item.est (the estimate). Throughput/quota compliance is measured
on item.actual (what Bedrock charges). The gap between them is the bug.

NO AWS dependencies — pure Python, fake wall clock.

Usage:
    python3 scripts/test_queue_overshoot_sim.py
    python3 scripts/test_queue_overshoot_sim.py --verbose
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import deque
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sim.core import Item, FakeClock, SimResult, AssertionResult  # noqa: E402


# ══════════════════════════════════════════════════════════════════════════════
# Live workload + config constants (from the deployed nova-2-lite config)
# ══════════════════════════════════════════════════════════════════════════════

QUOTA_TPM       = 8_000_000     # Nova-2-Lite account quota
QUEUE_TARGET_TPM = 5_520_000    # deployed queue_target_tpm (Gate 5 target)
TPM_QUEUE_CAP   = 5_520_000     # deployed tpm_queue_capacity (Gate 4 cap)
TPM_QUEUE_REGEN = 92_000        # deployed tpm_queue_regeneration_rate (tok/s) → Gate 2 cap
QUEUE_CAPACITY  = 1_380         # deployed queue_capacity (Gate 3 cap, req/60s)
QUEUE_REGEN     = 23.0          # deployed queue_regeneration_rate (req/s) → Gate 1 cap

# Backlog must be large enough that EVERY scenario drains for well over 60s, so
# the rolling-60s window actually fills and peak-60s is a meaningful quota check.
# The slowest scenario (~13 req/s) needs ≥ 13×120 ≈ 1,560 items for a 120s run;
# 3,000 gives every scenario a multi-minute steady state. (Fake clock → instant.)
BACKLOG         = 3_000
BATCH_SIZE      = 10
SHORT_WINDOW_SEC = 2.0
DISPATCH_OVERHEAD_MS = 20.0

# The two token numbers that drive the bug.
ACTUAL_TOKENS   = 6_000         # what Bedrock actually charges (~5000 in + ~1000 out)
BROKEN_ESTIMATE = 1_024         # flat fallback when burst=0 and config omits max-token fields
GOOD_ESTIMATE   = 6_753         # what budget_manager WOULD log with burst>0 (5500 in +1200 out est)


@dataclass
class Scenario:
    name: str
    estimate: int          # token count the gates pace on
    actual: int            # token count Bedrock charges
    note: str


SCENARIOS = [
    Scenario(
        name="BROKEN (live: burst=0, flat-1024 estimate)",
        estimate=BROKEN_ESTIMATE, actual=ACTUAL_TOKENS,
        note="Reproduces the live overshoot: gates pace on 1024, Bedrock charges 6000.",
    ),
    Scenario(
        name="FIXED  (estimate == actual, Gate 5 binds correctly)",
        estimate=ACTUAL_TOKENS, actual=ACTUAL_TOKENS,
        note="What a correct estimate produces: Gate 5 holds the real rate at target.",
    ),
    Scenario(
        name="FIXED  (budget-manager 6753 estimate, ~10% over)",
        estimate=GOOD_ESTIMATE, actual=ACTUAL_TOKENS,
        note="Realistic estimate (slight over-count): safely at/under target.",
    ),
]


# ══════════════════════════════════════════════════════════════════════════════
# Faithful 5-gate dispatcher — mirror of queue_processor.py handler loop
# ══════════════════════════════════════════════════════════════════════════════

def run_five_gate_dispatch(
    items: List[Item],
    *,
    queue_regen_rate: float,
    queue_capacity: int,
    tpm_queue_capacity: int,
    tpm_queue_regen_rate: float,
    queue_target_tpm: int,
    batch_size: int = BATCH_SIZE,
    short_window_sec: float = SHORT_WINDOW_SEC,
    dispatch_overhead_ms: float = DISPATCH_OVERHEAD_MS,
    verbose: bool = False,
) -> SimResult:
    """Mirror of queue_processor.py's per-item gate loop (all 5 gates).

    CRITICAL: every gate paces on `item.est` (the estimate), exactly as the live
    code reads item.get('estimated_tokens') or flat_tpm_estimate. Throughput and
    window peaks are recorded on BOTH the estimate stream (dispatch_events) and
    the actual stream (actual_events) so the sim can show the drift.
    """
    result = SimResult(algo_name="5-gate dispatch")
    clock = FakeClock()

    short_window_cap = max(1, int(queue_regen_rate * short_window_sec))
    tpm_2s_cap = int(tpm_queue_regen_rate * short_window_sec) if tpm_queue_regen_rate > 0 else 0
    queue_target_tps = queue_target_tpm / 60.0 if queue_target_tpm > 0 else 0.0
    dispatch_overhead = dispatch_overhead_ms / 1000.0

    # dispatch_log holds the ESTIMATE per dispatch, mirroring the live deque of
    # (ts, item_tokens) where item_tokens is the estimate.
    dispatch_log: deque = deque()
    last_dispatch_ts: Optional[float] = None
    idx = 0

    def prune(horizon: float) -> None:
        cutoff = clock.now - horizon
        while dispatch_log and dispatch_log[0][0] < cutoff:
            dispatch_log.popleft()

    def recent_count(window: float) -> int:
        cutoff = clock.now - window
        return sum(1 for ts, _ in dispatch_log if ts >= cutoff)

    def recent_tokens(window: float) -> int:
        cutoff = clock.now - window
        return sum(tok for ts, tok in dispatch_log if ts >= cutoff)

    def sleep_for_token_budget(item_tokens: int, window: float, cap: int) -> None:
        in_win = sorted(((ts, tok) for ts, tok in dispatch_log if ts >= clock.now - window),
                        key=lambda e: e[0])
        deficit = sum(tok for _, tok in in_win) + item_tokens - cap
        if deficit <= 0:
            return
        freed, sleep_until = 0, clock.now
        for ts, tok in in_win:
            freed += tok
            sleep_until = ts + window
            if freed >= deficit:
                break
        sleep_for = max(0.001, sleep_until - clock.now + 0.005)
        result.sleep_events.append((clock.now, sleep_for))
        clock.sleep(sleep_for)

    while idx < len(items):
        chunk_end = min(idx + batch_size, len(items))
        for i in range(idx, chunk_end):
            item = items[i]
            item_tokens = item.est          # ← gates pace on the ESTIMATE
            prune(60.0)

            # ── Gate 1: RPM 2s ────────────────────────────────────────────────
            if recent_count(short_window_sec) >= short_window_cap:
                in_win = [ts for ts, _ in dispatch_log if ts >= clock.now - short_window_sec]
                oldest = min(in_win) if in_win else clock.now - short_window_sec
                sleep_for = max(0.001, (oldest + short_window_sec) - clock.now + 0.005)
                result.sleep_events.append((clock.now, sleep_for))
                clock.sleep(sleep_for)
                prune(60.0)

            # ── Gate 2: TPM 2s (+ oversized-item path) ────────────────────────
            if tpm_2s_cap > 0:
                t2 = recent_tokens(short_window_sec)
                if item_tokens > tpm_2s_cap:
                    if t2 > 0:
                        in_win = [(ts, tok) for ts, tok in dispatch_log
                                  if ts >= clock.now - short_window_sec]
                        newest = max(ts for ts, _ in in_win) if in_win else clock.now - short_window_sec
                        sleep_for = max(0.001, (newest + short_window_sec) - clock.now + 0.005)
                        result.sleep_events.append((clock.now, sleep_for))
                        clock.sleep(sleep_for)
                        prune(60.0)
                elif t2 + item_tokens > tpm_2s_cap:
                    sleep_for_token_budget(item_tokens, short_window_sec, tpm_2s_cap)
                    prune(60.0)

            # ── Gate 3: RPM 60s ───────────────────────────────────────────────
            if recent_count(60.0) >= queue_capacity:
                in_win_60 = [ts for ts, _ in dispatch_log if ts >= clock.now - 60.0]
                oldest_60 = min(in_win_60) if in_win_60 else clock.now - 60.0
                sleep_for = max(0.001, (oldest_60 + 60.0) - clock.now + 0.005)
                result.sleep_events.append((clock.now, sleep_for))
                clock.sleep(sleep_for)
                prune(60.0)

            # ── Gate 4: TPM 60s ───────────────────────────────────────────────
            if tpm_queue_capacity > 0:
                if recent_tokens(60.0) + item_tokens > tpm_queue_capacity:
                    sleep_for_token_budget(item_tokens, 60.0, tpm_queue_capacity)

            # ── Gate 5: even-spacing pacer (queue_target_tpm) ─────────────────
            if queue_target_tps > 0 and last_dispatch_ts is not None:
                interval = item_tokens / queue_target_tps
                earliest = last_dispatch_ts + interval
                if clock.now < earliest:
                    sleep_for = earliest - clock.now
                    result.sleep_events.append((clock.now, sleep_for))
                    clock.sleep(sleep_for)

            # ── Dispatch ──────────────────────────────────────────────────────
            ts = clock.now
            last_dispatch_ts = ts
            dispatch_log.append((ts, item_tokens))               # estimate stream
            result.dispatch_events.append((ts, item_tokens))     # estimate (gates)
            result.actual_events.append((ts, item.actual))       # actual (Bedrock)
            idx += 1
            clock.advance(dispatch_overhead)

    result.total_sim_time = clock.now
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════════════════════

SEP  = "─" * 76
DSEP = "━" * 76


def run_scenario(sc: Scenario, verbose: bool = False) -> dict:
    items = [Item(tokens=sc.estimate, actual_tokens=sc.actual) for _ in range(BACKLOG)]
    r = run_five_gate_dispatch(
        items,
        queue_regen_rate=QUEUE_REGEN,
        queue_capacity=QUEUE_CAPACITY,
        tpm_queue_capacity=TPM_QUEUE_CAP,
        tpm_queue_regen_rate=TPM_QUEUE_REGEN,
        queue_target_tpm=QUEUE_TARGET_TPM,
        verbose=verbose,
    )

    # Sustained: full-run average. Peak 60s: worst rolling minute.
    est_sustained    = r.effective_tps * 60.0
    actual_sustained = r.effective_actual_tps * 60.0
    actual_peak_60s  = r.max_actual_tokens_in_rolling_window(60.0)
    actual_peak_1s   = r.max_actual_tokens_in_rolling_window(1.0) * 60.0
    eff_rps          = r.effective_rps

    print(f"\n{DSEP}")
    print(f"  {sc.name}")
    print(SEP)
    print(f"  {sc.note}")
    print(f"  estimate/item = {sc.estimate:,} tok   actual/item = {sc.actual:,} tok "
          f"(drift {sc.actual / sc.estimate:.2f}×)")
    print(f"  drain time              : {r.total_sim_time:.1f}s   ({r.total_dispatched} items)")
    print(f"  effective RPS           : {eff_rps:.2f}")
    print(f"  what Gate 5 THINKS (est): {est_sustained:,.0f} TPM sustained "
          f"(target {QUEUE_TARGET_TPM:,})")
    print(f"  what Bedrock CHARGES    : {actual_sustained:,.0f} TPM sustained")
    print(f"  actual peak 60s window  : {actual_peak_60s:,.0f} TPM   "
          f"(quota {QUOTA_TPM:,})")
    over_target = actual_peak_60s > QUEUE_TARGET_TPM
    over_quota  = actual_peak_60s > QUOTA_TPM
    print(f"  → over target?  {'⚠️  YES' if over_target else '✅ no'}    "
          f"over quota (throttles)?  {'⚠️  YES' if over_quota else '✅ no'}")

    return {
        "sc": sc, "actual_peak_60s": actual_peak_60s,
        "over_target": over_target, "over_quota": over_quota,
        "actual_sustained": actual_sustained,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    print("=" * 76)
    print("  QUEUE PROCESSOR OVERSHOOT REPRODUCTION  (estimate-vs-actual drift)")
    print("=" * 76)
    print(f"  Deployed nova-2-lite config: queue_target_tpm={QUEUE_TARGET_TPM:,}, "
          f"tpm_queue_cap={TPM_QUEUE_CAP:,},")
    print(f"  tpm_queue_regen={TPM_QUEUE_REGEN:,}/s, queue_capacity={QUEUE_CAPACITY:,}/min, "
          f"quota={QUOTA_TPM:,} TPM")

    results = [run_scenario(sc, verbose=args.verbose) for sc in SCENARIOS]

    # ── Assertions: the sim must REPRODUCE the bug then PROVE the fix ──────────
    print(f"\n{DSEP}")
    print("  ASSERTIONS — reproduce the bug, then prove the fix")
    print(SEP)
    ar = AssertionResult()

    broken = results[0]
    ar.check(broken["over_target"],
             "BROKEN scenario OVERSHOOTS the queue_target_tpm",
             f"peak60s={broken['actual_peak_60s']:,.0f} > target {QUEUE_TARGET_TPM:,}")
    ar.check(broken["over_quota"],
             "BROKEN scenario exceeds the 8M quota (→ live throttles)",
             f"peak60s={broken['actual_peak_60s']:,.0f} > quota {QUOTA_TPM:,}")

    for fixed in results[1:]:
        ar.check(not fixed["over_quota"],
                 f"FIXED ({fixed['sc'].estimate:,} est) stays under quota",
                 f"peak60s={fixed['actual_peak_60s']:,.0f} ≤ quota {QUOTA_TPM:,}")
        ar.check(fixed["actual_peak_60s"] <= QUEUE_TARGET_TPM * 1.05,
                 f"FIXED ({fixed['sc'].estimate:,} est) holds ~target rate",
                 f"peak60s={fixed['actual_peak_60s']:,.0f} ≈ target {QUEUE_TARGET_TPM:,}")

    print()
    ar.report()
    print()
    if ar.all_passed:
        print("  ✅ Sim reproduces the live overshoot AND proves a correct estimate fixes it.")
        print("     Root cause: gates pace on item estimate; flat-1024 fallback collapses all")
        print("     token gating, leaving only RPM-60s (23 req/s × 6000 real = 8.28M > quota).")
    else:
        print("  ❌ Sim did not behave as expected — review output above.")
    sys.exit(0 if ar.all_passed else 1)


if __name__ == "__main__":
    main()
