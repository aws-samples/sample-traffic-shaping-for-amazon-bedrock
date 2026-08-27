#!/usr/bin/env python3
"""
Min-sleep-floor sweep for the queue processor TOKEN-AWARE dispatch gate (OFFLINE sim).

Purpose
-------
Run 1 (live, shaper-nova, 30 RPS, 1/96/3) threw 882 Bedrock ThrottlingExceptions
even though the AVERAGE dispatch rate was ~7.2M TPM. The gate's TPM-2s sleeps were
tiny (~0.005s), so it held a smooth rate right at the 7.68M queue slice — too close
to the 8M quota to survive once Bedrock's token-bucket burst reserve drained.

This sweep asks a purely mechanical question the sim CAN answer deterministically:
    "For a given minimum-sleep floor on the TPM gate + queue allocation, what
     SUSTAINED and PEAK dispatch TPM does the pacer produce?"

What the sim does NOT model (honest caveat, per sim/core.py): Bedrock's real
sub-minute token bucket, its refill cadence, or async fan-out re-bunching. So the
sim CANNOT tell us whether a config throttles — only the rate it emits. We use it to
predict which (sleep, config) combos land in a near-quota-but-headroom zone, then
live-confirm the promising few.

Usage
-----
    python scripts/sweep_min_sleep_sim.py
    python scripts/sweep_min_sleep_sim.py --doc "reports/queue-processor-pacer-sweep.md"
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import deque
from typing import List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sim.core import Item, FakeClock, SimResult, make_uniform  # noqa: E402


# ── Workload: mirror the live shaper-nova run ────────────────────────────────────
# shaper-nova: prompt_tokens=5000 + max_tokens=1200, bytes_per_token=4.0, burndown 1.0
# → budget_manager logged estimated_tokens=6753 for every request in Run 1.
ITEM_TOKENS = 6753
# 30 RPS x 30s submission = ~900 request backlog fully queued before drain (the
# gate sheds ~99% to the queue at burst=1%). Time-box the sim to this backlog.
BACKLOG = 900
BATCH_SIZE = 10
SHORT_WINDOW_SEC = 2.0
DISPATCH_OVERHEAD_MS = 20.0
QUOTA_TPM = 8_000_000


# ── Configs under test (queue allocation of the 8M TPM quota) ────────────────────
CONFIGS = {
    "1/96/3": {"tpm_queue_capacity": 7_680_000, "queue_capacity": 1920},
    "1/94/5": {"tpm_queue_capacity": 7_520_000, "queue_capacity": 1880},
}
SLEEP_FLOORS = [0.01, 0.015, 0.02, 0.025]


def run_token_aware_with_floor(
    items: List[Item],
    tpm_queue_capacity: int,
    queue_capacity: int,
    min_sleep_floor: float,
    batch_size: int = BATCH_SIZE,
    short_window_sec: float = SHORT_WINDOW_SEC,
    dispatch_overhead_ms: float = DISPATCH_OVERHEAD_MS,
) -> SimResult:
    """TOKEN-AWARE 4-gate dispatch (mirror of queue_processor.py:577-368), with a
    MINIMUM SLEEP FLOOR applied to the TPM gate sleeps (Gate 2 TPM-2s + Gate 4
    TPM-60s). The floor is the lever under test: a bigger floor spaces token
    dispatch further apart, lowering sustained/peak TPM and (in the live system)
    giving Bedrock's bucket time to recharge.
    """
    result = SimResult(algo_name=f"TOKEN-AWARE floor={min_sleep_floor}s")
    clock = FakeClock()

    queue_regen_rate = queue_capacity / 60.0
    tpm_regen_rate = tpm_queue_capacity / 60.0
    short_window_cap = max(1, int(queue_regen_rate * short_window_sec))
    tpm_2s_cap = int(tpm_regen_rate * short_window_sec) if tpm_regen_rate > 0 else 0
    dispatch_overhead = dispatch_overhead_ms / 1000.0
    dispatch_log: deque = deque()
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

    def do_sleep(sleep_for: float, floored: bool) -> None:
        # Token gates honor the min-sleep floor; RPM gates use the raw computed sleep.
        if floored:
            sleep_for = max(sleep_for, min_sleep_floor)
        sleep_for = max(0.001, sleep_for)
        result.sleep_events.append((clock.now, sleep_for))
        clock.sleep(sleep_for)

    def sleep_for_token_budget(item_tokens: int, window: float, cap: int) -> None:
        in_win = sorted(
            ((ts, tok) for ts, tok in dispatch_log if ts >= clock.now - window),
            key=lambda e: e[0],
        )
        current_tokens = sum(tok for _, tok in in_win)
        deficit = current_tokens + item_tokens - cap
        if deficit <= 0:
            # Under budget by the raw math, but still honor the floor so the pacer
            # can't fire back-to-back with no recharge gap.
            do_sleep(0.0, floored=True)
            return
        freed = 0
        sleep_until = clock.now
        for ts, tok in in_win:
            freed += tok
            sleep_until = ts + window
            if freed >= deficit:
                break
        do_sleep(sleep_until - clock.now + 0.005, floored=True)

    while idx < len(items):
        chunk_end = min(idx + batch_size, len(items))
        for i in range(idx, chunk_end):
            item = items[i]
            prune(60.0)

            # Gate 1: RPM 2s (raw sleep, no floor — RPM is not the token constraint)
            if recent_count(short_window_sec) >= short_window_cap:
                in_win = [ts for ts, _ in dispatch_log if ts >= clock.now - short_window_sec]
                oldest = min(in_win) if in_win else clock.now - short_window_sec
                do_sleep((oldest + short_window_sec) - clock.now + 0.005, floored=False)
                prune(60.0)

            # Gate 2: TPM 2s (FLOORED)
            if tpm_2s_cap > 0:
                t2 = recent_tokens(short_window_sec)
                if item.tokens > tpm_2s_cap:
                    if t2 > 0:
                        in_win = [(ts, tok) for ts, tok in dispatch_log
                                  if ts >= clock.now - short_window_sec]
                        newest = max(ts for ts, _ in in_win) if in_win else \
                            clock.now - short_window_sec
                        do_sleep((newest + short_window_sec) - clock.now + 0.005, floored=True)
                        prune(60.0)
                elif t2 + item.tokens > tpm_2s_cap:
                    sleep_for_token_budget(item.tokens, short_window_sec, tpm_2s_cap)
                    prune(60.0)
                else:
                    # Within the 2s token budget — still enforce the recharge floor.
                    do_sleep(0.0, floored=True)

            # Gate 3: RPM 60s (raw sleep, no floor)
            if recent_count(60.0) >= queue_capacity:
                in_win_60 = [ts for ts, _ in dispatch_log if ts >= clock.now - 60.0]
                oldest_60 = min(in_win_60) if in_win_60 else clock.now - 60.0
                do_sleep((oldest_60 + 60.0) - clock.now + 0.005, floored=False)
                prune(60.0)

            # Gate 4: TPM 60s (FLOORED)
            if tpm_queue_capacity > 0:
                t60 = recent_tokens(60.0)
                if t60 + item.tokens > tpm_queue_capacity:
                    sleep_for_token_budget(item.tokens, 60.0, tpm_queue_capacity)
                    prune(60.0)

            ts = clock.now
            result.dispatch_events.append((ts, item.tokens))
            dispatch_log.append((ts, item.tokens))
            idx += 1
            clock.advance(dispatch_overhead)

    result.total_sim_time = clock.now
    return result


# ── Even-spacing (GCRA-style) pacer ──────────────────────────────────────────────
# Instead of a blind sleep floor, pace each item to a TARGET token rate directly:
#     interval = item_tokens / target_tokens_per_second
# The gate ensures at least `interval` seconds elapse between consecutive dispatches,
# so the emitted rate tracks target_tps with NO batch clumping and NO sub-second
# bursts — the peak 1s rate equals the sustained rate by construction. Token-size
# aware: a big item earns a proportionally longer gap. This is the leaky-bucket/GCRA
# approach (Stripe/Lyft/Envoy) the council cited.
TARGET_RATES_TPM = [7_000_000, 7_200_000, 7_400_000, 7_600_000]


def run_even_spacing_pacer(
    items: List[Item],
    target_tpm: int,
    dispatch_overhead_ms: float = DISPATCH_OVERHEAD_MS,
) -> SimResult:
    result = SimResult(algo_name=f"EVEN-SPACING target={target_tpm/1e6:.1f}M TPM")
    clock = FakeClock()
    target_tps = target_tpm / 60.0
    dispatch_overhead = dispatch_overhead_ms / 1000.0
    last_dispatch = None

    for item in items:
        if last_dispatch is not None:
            interval = item.tokens / target_tps  # seconds this item "owns" at target rate
            earliest = last_dispatch + interval
            if clock.now < earliest:
                sleep_for = earliest - clock.now
                result.sleep_events.append((clock.now, sleep_for))
                clock.sleep(sleep_for)
        ts = clock.now
        result.dispatch_events.append((ts, item.tokens))
        last_dispatch = ts
        clock.advance(dispatch_overhead)

    result.total_sim_time = clock.now
    return result


def _run_even_spacing_sweep(items: List[Item], doc_path: str) -> None:
    rows = []
    for target_tpm in TARGET_RATES_TPM:
        r = run_even_spacing_pacer(items, target_tpm=target_tpm)
        rows.append({
            "target_tpm": target_tpm,
            "sustained_tpm": r.effective_tps * 60.0,
            "peak_1s_tpm": r.max_tokens_in_rolling_window(1.0) * 60.0,
            "peak_60s_tpm": r.max_tokens_in_rolling_window(60.0),
            "drain_s": r.total_sim_time,
        })

    hdr = (f"{'target_TPM':>11} {'sustained_TPM':>14} {'peak_1s_TPM':>12} "
           f"{'peak_60s_TPM':>13} {'drain_s':>8} {'<=8M?':>6}")
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        safe = "OK" if r["peak_1s_tpm"] <= QUOTA_TPM else "OVER"
        print(f"{r['target_tpm']:>11,} {r['sustained_tpm']:>14,.0f} "
              f"{r['peak_1s_tpm']:>12,.0f} {r['peak_60s_tpm']:>13,.0f} "
              f"{r['drain_s']:>8.1f} {safe:>6}")

    with open(doc_path, "a", encoding="utf-8") as f:
        f.write("## SIM sweep — EVEN-SPACING pacer (interval = item_tokens / target_rate)\n\n")
        f.write("> Paces each item to a target token rate directly (GCRA/leaky-bucket). "
                "Peak 1s ≈ sustained by construction — no batch clumping. Sim confirms the "
                "RATE shape; live runs confirm no-throttle against Bedrock's real bucket.\n\n")
        f.write("| target TPM | sustained TPM | peak 1s TPM | peak 60s TPM | drain (s) | peak ≤8M? |\n")
        f.write("|---|---|---|---|---|---|\n")
        for r in rows:
            safe = "✅ OK" if r["peak_1s_tpm"] <= QUOTA_TPM else "⚠️ OVER"
            f.write(f"| {r['target_tpm']:,} | {r['sustained_tpm']:,.0f} | "
                    f"{r['peak_1s_tpm']:,.0f} | {r['peak_60s_tpm']:,.0f} | "
                    f"{r['drain_s']:.1f} | {safe} |\n")
        f.write("\n")
    print(f"\nAppended results to: {doc_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pacer sweep (offline sim)")
    parser.add_argument("--doc", default="reports/queue-processor-pacer-sweep.md")
    parser.add_argument("--pacer", choices=["floor", "even-spacing"], default="even-spacing",
                        help="Which pacing strategy to sweep")
    args = parser.parse_args()

    items = make_uniform(BACKLOG, ITEM_TOKENS)

    if args.pacer == "even-spacing":
        _run_even_spacing_sweep(items, args.doc)
        return

    rows = []
    for cfg_name, cfg in CONFIGS.items():
        for floor in SLEEP_FLOORS:
            r = run_token_aware_with_floor(
                items,
                tpm_queue_capacity=cfg["tpm_queue_capacity"],
                queue_capacity=cfg["queue_capacity"],
                min_sleep_floor=floor,
            )
            sustained_tpm = r.effective_tps * 60.0
            peak_1s_tpm = r.max_tokens_in_rolling_window(1.0) * 60.0
            peak_60s_tpm = r.max_tokens_in_rolling_window(60.0)  # tokens in a 60s window = TPM
            drain_s = r.total_sim_time
            rows.append({
                "config": cfg_name,
                "floor": floor,
                "queue_slice": cfg["tpm_queue_capacity"],
                "sustained_tpm": sustained_tpm,
                "peak_1s_tpm": peak_1s_tpm,
                "peak_60s_tpm": peak_60s_tpm,
                "drain_s": drain_s,
                "dispatched": r.total_dispatched,
            })

    # ── Console table ─────────────────────────────────────────────────────────
    hdr = (f"{'config':>8} {'floor':>6} {'queue_slice':>12} {'sustained_TPM':>14} "
           f"{'peak_1s_TPM':>12} {'peak_60s_TPM':>13} {'drain_s':>8} {'<=8M?':>6}")
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        safe = "OK" if r["peak_60s_tpm"] <= QUOTA_TPM else "OVER"
        print(f"{r['config']:>8} {r['floor']:>6.3f} {r['queue_slice']:>12,} "
              f"{r['sustained_tpm']:>14,.0f} {r['peak_1s_tpm']:>12,.0f} "
              f"{r['peak_60s_tpm']:>13,.0f} {r['drain_s']:>8.1f} {safe:>6}")

    # ── Append to results doc ───────────────────────────────────────────────────
    doc_path = args.doc
    new_file = not os.path.exists(doc_path)
    with open(doc_path, "a", encoding="utf-8") as f:
        if new_file:
            f.write("# Ideal Queue Processor Configuration\n\n")
            f.write("Tuning the TOKEN-AWARE dispatch gate's **minimum-sleep floor** and "
                    "**queue TPM allocation** to hold dispatch near the 8M Bedrock quota "
                    "WITHOUT throttling.\n\n")
            f.write("Workload for all runs: shaper-nova — est **6,753 tokens/request** "
                    f"(5000 prompt + 1200 max_tokens), backlog ~{BACKLOG} requests, "
                    f"batch_size={BATCH_SIZE}, short_window={SHORT_WINDOW_SEC}s, "
                    f"quota=8,000,000 TPM.\n\n")
            f.write("## Test parameters recorded per run\n"
                    "- **sleep floor** — minimum seconds the TPM gate sleeps when it trips\n"
                    "- **queue capacity (TPM)** — `tpm_queue_capacity` (the queue's slice of quota)\n"
                    "- **buffer capacity** — held-back fraction (100% − burst − queue)\n\n")

        f.write("## SIM sweep — min-sleep floor x allocation (OFFLINE, no Bedrock)\n\n")
        f.write("> **Sim caveat:** models gate/sleep math only. Reports the dispatch RATE "
                "each config produces; does NOT model Bedrock's token bucket, so it CANNOT "
                "confirm throttling. `peak_60s_TPM <= 8M` is a *necessary* (not sufficient) "
                "condition — live runs confirm no-throttle.\n\n")
        f.write("| config (b/q/buf) | sleep floor (s) | queue cap (TPM) | buffer | "
                "sustained TPM | peak 1s TPM | peak 60s TPM | drain (s) | ≤8M? |\n")
        f.write("|---|---|---|---|---|---|---|---|---|\n")
        for r in rows:
            buf = "3%" if r["config"] == "1/96/3" else "5%"
            safe = "✅ OK" if r["peak_60s_tpm"] <= QUOTA_TPM else "⚠️ OVER"
            f.write(f"| {r['config']} | {r['floor']:.3f} | {r['queue_slice']:,} | {buf} | "
                    f"{r['sustained_tpm']:,.0f} | {r['peak_1s_tpm']:,.0f} | "
                    f"{r['peak_60s_tpm']:,.0f} | {r['drain_s']:.1f} | {safe} |\n")
        f.write("\n")

    print(f"\nAppended results to: {doc_path}")


if __name__ == "__main__":
    main()
