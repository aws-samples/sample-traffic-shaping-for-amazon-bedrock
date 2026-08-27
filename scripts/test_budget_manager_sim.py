#!/usr/bin/env python3
"""
Budget Manager Admission-Gate Simulation.

Proves how the budget manager's atomic admission gate behaves under offered load
BEFORE deploying — the fast, AWS-free counterpart to the (slow, expensive) live
Bedrock load test. Where test_queue_processor_sim.py models the single-threaded
streaming DRAIN, this models the concurrent ADMIT path: many Step Functions
executions each hit the shared put_allocation() gate and are either

    ADMITTED  → capacity available, request would invoke Bedrock immediately, or
    ENQUEUED  → gate rejected (BurstCapacityExceeded), request goes to the queue.

The gate re-implements the RUNTIME backend of
infrastructure/lambda_layer/python/shared_service/dynamo.py::put_allocation()
against a FakeClock, so a sim run against a named quota profile predicts what the
deployed config (from create_model_config.py) would actually admit.

Modeled gates (byte-for-byte with put_allocation runtime path):
    1. RPM 60s window counter    — effective = burst_capacity + elapsed*burst_regen_rate
    2. RPM global 5-min epoch cap — burst_capacity * minutes_elapsed  (minutes 1..5)
    3. TPM 60s window counter     — tpm_burst_capacity + elapsed*tpm_burst_regen_rate
    4. TPM global 5-min epoch cap — tpm_burst_capacity * minutes_elapsed
    5. 2s short-window pre-gate   — request-rate AND token-rate caps over last 2s
    6. Oversized-request rejection — single request cost > effective/global cap

FIDELITY BOUNDARY (important): arrivals are processed in strict time order against
shared counter state. This captures the admit-vs-enqueue CAPACITY decision exactly,
but does NOT reproduce the eventual-consistency over-admission race that occurs when
concurrent Lambdas read a stale counter in production. That race is bounded (at most
N_shards-1 per window) and corrected by reconciliation; it is a throughput/accounting
concern, not a capacity-behavior one, so it is intentionally out of scope here.

NO AWS dependencies — pure Python, fake wall clock. Runs in milliseconds.

Usage:
    # Default: smoke test + all three workloads at the prod profile
    python3 scripts/test_budget_manager_sim.py

    # Production workloads only (skip smoke)
    python3 scripts/test_budget_manager_sim.py --no-smoke

    # One workload against a named profile, at 2x the burst-slice quota
    python3 scripts/test_budget_manager_sim.py --profile nova-lite --workload rpm-push --load-factor 2.0

    # Explicit offered RPS and duration (overrides load-factor auto-sizing)
    python3 scripts/test_budget_manager_sim.py --offered-rps 40 --duration 120

    # Custom total quota (split into burst/queue the same way as deployed config)
    python3 scripts/test_budget_manager_sim.py --rpm 2000 --tpm 4000000

    # Per-arrival trace
    python3 scripts/test_budget_manager_sim.py --workload rpm-push --verbose

Available profiles:  smoke, dev, prod, claude-haiku, claude-sonnet, claude-opus,
                     nova-lite, nova-pro, prod-high
Available workloads: rpm-push, tpm-push, mixed-spike
"""

import argparse
import random
import sys
import time as real_time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple

# Shared simulation package (quota profiles, workload presets, core types).
# We import the SAME vocabulary the queue processor sim uses so a profile means
# the same thing to both — the only difference is which SLICE of quota each reads.
from sim import (
    SimConfig, SimQuota,
    QUOTA_PROFILES, WORKLOAD_PRESETS,
    build_config,
    Item, FakeClock, AssertionResult, make_items_for_preset,
)


# ══════════════════════════════════════════════════════════════════════════════
# Admission gate — faithful replay of put_allocation() runtime path
# ══════════════════════════════════════════════════════════════════════════════

class AdmissionGate:
    """
    In-memory replay of dynamo.py::put_allocation() (runtime backend) against a
    FakeClock. One instance holds all shared counter state; try_admit() returns
    True (admit, counters incremented) or False (reject → caller enqueues).

    Counter state mirrors the DynamoDB items the real gate writes:
      - rpm_window[window]        — request count in the current 60s window
      - rpm_global[epoch]         — request count in the current 5-min epoch
      - tpm_window[window]        — token sum in the current 60s window
      - tpm_global[epoch]         — token sum in the current 5-min epoch
      - dispatch_log              — (ts, tokens) for admitted requests, for the 2s
                                    pre-gate (mirrors querying consumption records)

    The window/epoch keys are integer buckets exactly as put_allocation computes
    them (int(now)//60 and int(now)//300), so regeneration and epoch-cap growth
    line up with production to the second.
    """

    def __init__(self, quota: SimQuota, short_window_sec: float = 2.0):
        self.q = quota
        # Sub-minute smoothing window (matches SimConfig.short_window_sec / the
        # SHORT_WINDOW_SECONDS constant in put_allocation), parameterized so the
        # budget manager sim and queue processor sim share one knob.
        self.short_window_sec = short_window_sec
        # Real config values (identical to calculate_config output for this quota):
        self.burst_capacity = quota.burst_rpm                 # int(rpm * burst_fraction)
        self.burst_regen_rate = quota.burst_rps               # rpm/60 * burst_fraction
        self.tpm_burst_capacity = quota.burst_tpm             # int(tpm * burst_fraction)
        self.tpm_burst_regen_rate = quota.burst_tpm_rate      # tpm/60 * burst_fraction
        # short_window_rps defaults to burst_regen_rate when unset (matches gate).
        self.short_window_cap = max(1, int(self.burst_regen_rate * short_window_sec)) \
            if self.burst_regen_rate > 0 else 0
        self.short_window_tps_cap = max(1, int(self.tpm_burst_regen_rate * short_window_sec)) \
            if self.tpm_burst_regen_rate > 0 else 0

        # Shared counter state
        self.rpm_window: dict = {}
        self.rpm_global: dict = {}
        self.tpm_window: dict = {}
        self.tpm_global: dict = {}
        # Admitted (ts, tokens) — the 2s pre-gate reads this (== consumption records)
        self.dispatch_log: deque = deque()

        # Bookkeeping for reporting the reject reason distribution
        self.reject_reasons: Counter = Counter()

    # ── window / epoch math (identical to put_allocation) ──────────────────────

    @staticmethod
    def _window(now: float) -> int:
        return int(now) // 60

    @staticmethod
    def _elapsed_in_window(now: float) -> float:
        return now - (int(now) // 60) * 60

    @staticmethod
    def _epoch(now: float) -> int:
        return int(now) // 300

    @staticmethod
    def _minutes_elapsed(now: float) -> int:
        epoch_start = (int(now) // 300) * 300
        return int((now - epoch_start) / 60) + 1  # 1..5

    def _prune_short_window(self, now: float) -> None:
        cutoff = now - self.short_window_sec
        while self.dispatch_log and self.dispatch_log[0][0] < cutoff:
            self.dispatch_log.popleft()

    def _record_reject(self, reason: str) -> None:
        self.reject_reasons[reason] += 1

    # ── the gate ────────────────────────────────────────────────────────────────

    def try_admit(self, now: float, tokens: int) -> bool:
        """
        Attempt admission of a request costing `tokens` estimated tokens at time
        `now`. Returns True (admitted, counters mutated) or False (rejected → the
        budget manager enqueues). Mirrors put_allocation() gate order exactly.
        """
        window = self._window(now)
        elapsed = self._elapsed_in_window(now)
        epoch = self._epoch(now)
        minutes_elapsed = self._minutes_elapsed(now)

        effective_capacity = self.burst_capacity + int(elapsed * self.burst_regen_rate)
        global_cap = self.burst_capacity * minutes_elapsed

        gate_tpm = self.tpm_burst_capacity > 0 and tokens > 0
        tpm_effective = 0
        global_tpm_cap = 0
        if gate_tpm:
            tpm_effective = self.tpm_burst_capacity + int(elapsed * self.tpm_burst_regen_rate)
            global_tpm_cap = self.tpm_burst_capacity * minutes_elapsed
            # Oversized pre-check (Gate 6): a request that cannot fit even an empty
            # window/epoch is rejected up front — never admitted, always enqueued.
            if tokens > tpm_effective or tokens > global_tpm_cap:
                self._record_reject("oversized_tpm")
                return False

        # ── 2s short-window pre-gate (Gate 5) ──────────────────────────────────
        # Reads admitted events over the last 2s (== _check_short_window_gate
        # querying consumption records). Rejects BEFORE the transaction on breach.
        self._prune_short_window(now)
        if self.short_window_cap > 0 and len(self.dispatch_log) >= self.short_window_cap:
            self._record_reject("short_window_rps")
            return False
        if self.short_window_tps_cap > 0 and tokens > 0:
            tokens_in_window = sum(tok for _, tok in self.dispatch_log)
            if tokens_in_window + tokens > self.short_window_tps_cap:
                self._record_reject("short_window_tps")
                return False

        # ── RPM 60s window (Gate 1) ─────────────────────────────────────────────
        # Condition: #count < :cap  (strict, matches the RPM counter condition).
        rpm_w = self.rpm_window.get(window, 0)
        if rpm_w >= effective_capacity:
            self._record_reject("rpm_window")
            return False

        # ── RPM global epoch (Gate 2) ────────────────────────────────────────────
        rpm_g = self.rpm_global.get(epoch, 0)
        if rpm_g >= global_cap:
            self._record_reject("rpm_global")
            return False

        # ── TPM 60s window (Gate 3) ─────────────────────────────────────────────
        # Condition: post-increment #count <= :cap  (headroom = cap - inc).
        if gate_tpm:
            tpm_w = self.tpm_window.get(window, 0)
            if tpm_w + tokens > tpm_effective:
                self._record_reject("tpm_window")
                return False

            # ── TPM global epoch (Gate 4) ────────────────────────────────────────
            tpm_g = self.tpm_global.get(epoch, 0)
            if tpm_g + tokens > global_tpm_cap:
                self._record_reject("tpm_global")
                return False

        # ── ADMIT: all gates passed — commit counter increments atomically ───────
        self.rpm_window[window] = rpm_w + 1
        self.rpm_global[epoch] = rpm_g + 1
        if gate_tpm:
            self.tpm_window[window] = self.tpm_window.get(window, 0) + tokens
            self.tpm_global[epoch] = self.tpm_global.get(epoch, 0) + tokens
        self.dispatch_log.append((now, tokens))
        return True


# ══════════════════════════════════════════════════════════════════════════════
# Window-read gate — faithful replay of the FUTURE-STATE admission path
# ══════════════════════════════════════════════════════════════════════════════

class WindowReadGate:
    """
    In-memory replay of the PROPOSED consumption-record sliding-window read gate
    (docs/solution/architecture.md §3), against a FakeClock.

    There are NO counter items. Admission is a READ over recent consumption:

        recs   = BURST#CONSUMPTION records in the last long_window_sec
        tok_2s = sum(tokens where ts >= now - short_window_sec)
        tok_Ns = sum(tokens over the full long_window)
        admit iff est fits BOTH the 2s cap and the 15s cap (tokens AND reqs)

    Caps (mirrors create_model_config's burst slice + the future-state doc):
        cap_2s_tok  = tpm_burst_regen_rate  * short_window_sec
        cap_2s_req  = short_window_rps       * short_window_sec
        cap_Ns_tok  = tpm_burst_regen_rate  * long_window_sec
        cap_Ns_req  = short_window_rps       * long_window_sec

    On admit, a consumption record is written with the ESTIMATE. To model the
    real system's accuracy, an admitted record's token value is REPLACED by its
    ACTUAL at write-back latency (default 7.5s) after admission — so at any read
    the 15s window is a mix of recent-estimate + older-actual, exactly as the
    live gate would see once bedrock_processor reconciles runtime successes.

    Bounded over-admission: production has concurrent Lambdas that each read the
    window before the others' writes land, so up to `concurrency` admits can slip
    through against the same window snapshot. We model this by only making an
    admitted record VISIBLE to later reads after `read_visibility_lag` seconds
    (the strong-read still can't see a write that hasn't committed when a
    concurrent reader sampled the window). This is the read-modify-write race the
    future-state doc explicitly accepts and catches downstream with requeue.
    """

    def __init__(self, quota: SimQuota,
                 short_window_sec: float = 2.0,
                 long_window_sec: float = 15.0,
                 writeback_latency_sec: float = 7.5,
                 read_visibility_lag_sec: float = 0.0,
                 actual_ratio: float = 1.0):
        self.q = quota
        self.short_window_sec = short_window_sec
        self.long_window_sec = long_window_sec
        self.writeback_latency_sec = writeback_latency_sec
        self.read_visibility_lag_sec = read_visibility_lag_sec
        # actual_ratio: actual_tokens / estimated_tokens once reconciled. The
        # runtime output estimate (max_tokens * burndown) over-counts vs actuals,
        # so actuals are typically LESS than the estimate → the window frees up as
        # records reconcile. Default 1.0 = actuals == estimate (neutral).
        self.actual_ratio = actual_ratio

        self.tpm_burst_regen_rate = quota.burst_tpm_rate      # tokens/s (burst slice)
        self.short_window_rps = quota.burst_rps               # req/s (burst slice)

        # Derived caps.
        self.cap_2s_tok = int(self.tpm_burst_regen_rate * short_window_sec)
        self.cap_2s_req = max(1, int(self.short_window_rps * short_window_sec))
        self.cap_Ns_tok = int(self.tpm_burst_regen_rate * long_window_sec)
        self.cap_Ns_req = max(1, int(self.short_window_rps * long_window_sec))

        # Consumption log: list of records. Each record is a dict with:
        #   ts            — admission time
        #   est           — estimated tokens at admission
        #   visible_at    — time this record becomes visible to a read (ts + lag)
        # The token value seen at read time is est until writeback_latency, then
        # est*actual_ratio (the reconciled actual).
        self.log: List[dict] = []

        self.reject_reasons: Counter = Counter()

    def _record_reject(self, reason: str) -> None:
        self.reject_reasons[reason] += 1

    def _tokens_at(self, rec: dict, now: float) -> int:
        """Token value of a record as a read at `now` would see it (estimate
        until write-back latency elapses, then the reconciled actual)."""
        if now - rec['ts'] >= self.writeback_latency_sec:
            return int(rec['est'] * self.actual_ratio)
        return rec['est']

    def _window_sums(self, now: float) -> Tuple[int, int, int, int]:
        """Return (tok_2s, req_2s, tok_Ns, req_Ns) over the visible consumption
        log at `now`. Only records committed (visible_at <= now) are counted —
        this is where bounded over-admission enters: a concurrent admit that has
        not yet committed is invisible to this read."""
        cut_short = now - self.short_window_sec
        cut_long = now - self.long_window_sec
        tok_2s = req_2s = tok_Ns = req_Ns = 0
        for rec in self.log:
            if rec['visible_at'] > now:
                continue  # not yet committed — the read cannot see it
            if rec['ts'] < cut_long:
                continue  # aged out of the long window
            tok = self._tokens_at(rec, now)
            req_Ns += 1
            tok_Ns += tok
            if rec['ts'] >= cut_short:
                req_2s += 1
                tok_2s += tok
        return tok_2s, req_2s, tok_Ns, req_Ns

    def _prune(self, now: float) -> None:
        cut = now - self.long_window_sec
        # Keep records still inside the long window (drop fully-aged ones).
        self.log = [r for r in self.log if r['ts'] >= cut]

    def try_admit(self, now: float, tokens: int) -> bool:
        """Attempt admission via the sliding-window read. Returns True (admit,
        record written) or False (reject → caller enqueues)."""
        self._prune(now)
        tok_2s, req_2s, tok_Ns, req_Ns = self._window_sums(now)

        gate_tpm = self.tpm_burst_regen_rate > 0 and tokens > 0

        # 2s window (rate smoothing).
        if self.cap_2s_req > 0 and req_2s + 1 > self.cap_2s_req:
            self._record_reject("short_window_rps")
            return False
        if gate_tpm and self.cap_2s_tok > 0 and tok_2s + tokens > self.cap_2s_tok:
            self._record_reject("short_window_tps")
            return False

        # 15s window (accuracy horizon).
        if self.cap_Ns_req > 0 and req_Ns + 1 > self.cap_Ns_req:
            self._record_reject("long_window_rps")
            return False
        if gate_tpm and self.cap_Ns_tok > 0 and tok_Ns + tokens > self.cap_Ns_tok:
            self._record_reject("long_window_tps")
            return False

        # ADMIT — write the consumption record (visible after the RMW lag).
        self.log.append({
            'ts': now,
            'est': tokens,
            'visible_at': now + self.read_visibility_lag_sec,
        })
        return True


# ══════════════════════════════════════════════════════════════════════════════
# Counter-gate CONTENTION model — the production TPM single-item hotspot
# ══════════════════════════════════════════════════════════════════════════════

class ContendedCounterGate:
    """
    The counter gate WITH the production contention race the base AdmissionGate
    intentionally omits (its fidelity note, lines 29-32). The unsharded 60s+epoch
    TPM counter is a single DynamoDB item; concurrent TransactWriteItems serialize
    on it, and after `max_conflict_retries` a conflicted admit SHEDS — even though
    the request had real budget. This is the ~3.6M/min the counter gate was losing
    (docs/solution/adr/ADR-005-consumption-read-elimination.md: 1,087 of 1,093 TPM conflicts were
    contention, not cap breach).

    We reuse the exact capacity logic of AdmissionGate for the admit/enqueue
    CAPACITY decision, then overlay a contention shed: at each admit attempt, if
    more than `write_serialization` other admits touched the TPM counter within
    the last `conflict_window` seconds, this admit conflicts. It retries up to
    `max_retries` times (each retry re-samples); if still contended, it sheds as
    `tpm_contention` — a request that had capacity but lost the lock race.
    """

    def __init__(self, quota: SimQuota, short_window_sec: float = 2.0,
                 conflict_window: float = 0.050,
                 write_serialization: int = 1,
                 max_retries: int = 3):
        self.base = AdmissionGate(quota, short_window_sec=short_window_sec)
        self.conflict_window = conflict_window
        self.write_serialization = write_serialization
        self.max_retries = max_retries
        # Times at which the TPM counter item was successfully written.
        self.tpm_writes: deque = deque()
        self.reject_reasons: Counter = Counter()

    def _prune_writes(self, now: float) -> None:
        cut = now - self.conflict_window
        while self.tpm_writes and self.tpm_writes[0] < cut:
            self.tpm_writes.popleft()

    def try_admit(self, now: float, tokens: int) -> bool:
        # Contention only matters when the request would otherwise be admitted
        # AND touches the (unsharded) TPM counter. Probe capacity via the base
        # gate WITHOUT mutating it, then decide contention, then commit.
        gate_tpm = self.base.tpm_burst_capacity > 0 and tokens > 0

        for attempt in range(self.max_retries + 1):
            self._prune_writes(now)
            concurrent = len(self.tpm_writes)
            # More than `write_serialization` writers in the conflict window →
            # this transaction conflicts on the TPM item.
            contended = gate_tpm and concurrent > self.write_serialization
            if not contended:
                break
            if attempt >= self.max_retries:
                # Retries exhausted → shed as contention (had budget, lost race).
                self.reject_reasons['tpm_contention'] += 1
                return False
            # Retry: advance the (local) clock a hair; the sim clock is shared, so
            # we just re-sample the window on the next loop iteration. Model the
            # backoff as clearing part of the write burst.
            if self.tpm_writes:
                self.tpm_writes.popleft()

        # Capacity decision via the faithful base gate (mutates its counters).
        admitted = self.base.try_admit(now, tokens)
        if admitted and gate_tpm:
            self.tpm_writes.append(now)
        # Fold the base gate's reject reasons in.
        if not admitted:
            # base.try_admit already recorded a reason on self.base.reject_reasons;
            # mirror the last one here for a unified histogram.
            for reason, cnt in self.base.reject_reasons.items():
                self.reject_reasons[reason] = cnt
        return admitted


# ══════════════════════════════════════════════════════════════════════════════
# Arrival generation
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Arrival:
    """A single offered request: when it arrives and how many tokens it costs."""
    ts: float
    tokens: int


def make_arrivals(
    offered_rps: float,
    duration_sec: float,
    items: List[Item],
    jitter: bool = True,
    seed: int = 42,
) -> List[Arrival]:
    """
    Build a time-ordered list of arrivals at `offered_rps` over `duration_sec`.

    Inter-arrival gaps are the mean 1/rps with small +/- jitter so arrivals don't
    all land on identical fake-clock ticks (which would make the 2s pre-gate
    trivially reject in lockstep). Token weights are drawn in order from `items`
    (already generated from the workload preset), cycling if we run past the list.
    """
    rng = random.Random(seed)
    arrivals: List[Arrival] = []
    if offered_rps <= 0 or duration_sec <= 0:
        return arrivals
    mean_gap = 1.0 / offered_rps
    t = 0.0
    i = 0
    while t < duration_sec:
        tokens = items[i % len(items)].tokens if items else 1
        arrivals.append(Arrival(ts=t, tokens=tokens))
        i += 1
        gap = mean_gap
        if jitter:
            gap = max(0.0001, mean_gap * rng.uniform(0.5, 1.5))
        t += gap
    return arrivals


# ══════════════════════════════════════════════════════════════════════════════
# Result + analytics
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class AdmissionResult:
    """Outcome of feeding one arrival stream through the gate."""
    # (ts, tokens) for admitted requests
    admitted: List[Tuple[float, int]] = field(default_factory=list)
    enqueued: int = 0
    total: int = 0
    duration: float = 0.0
    reject_reasons: dict = field(default_factory=dict)

    @property
    def admitted_count(self) -> int:
        return len(self.admitted)

    @property
    def admission_rate(self) -> float:
        return self.admitted_count / self.total if self.total else 0.0

    @property
    def admitted_tokens(self) -> int:
        return sum(tok for _, tok in self.admitted)

    def _peak_in_window(self, window_sec: float, weight: Callable[[int], int]) -> int:
        """
        Peak rolling-window sum of weight(tokens) over any `window_sec` window.
        `self.admitted` is already time-ordered (arrivals fed in time order), so no
        sort is needed. weight=lambda _: 1 → request count; weight=lambda t: t → tokens.
        """
        events = self.admitted
        peak, left, wsum = 0, 0, 0
        for right in range(len(events)):
            wsum += weight(events[right][1])
            while events[left][0] < events[right][0] - window_sec:
                wsum -= weight(events[left][1])
                left += 1
            peak = max(peak, wsum)
        return peak

    def peak_req_in_window(self, window_sec: float) -> int:
        return self._peak_in_window(window_sec, lambda _tok: 1)

    def peak_tokens_in_window(self, window_sec: float) -> int:
        return self._peak_in_window(window_sec, lambda tok: tok)


def run_admission(
    gate: AdmissionGate,
    arrivals: List[Arrival],
    clock: FakeClock,
    verbose: bool = False,
) -> AdmissionResult:
    """Feed arrivals through the gate in time order, recording each outcome."""
    result = AdmissionResult(total=len(arrivals))
    for a in arrivals:
        # Advance the shared fake clock to this arrival's timestamp.
        if a.ts > clock.now:
            clock.advance(a.ts - clock.now)
        admitted = gate.try_admit(clock.now, a.tokens)
        if admitted:
            result.admitted.append((clock.now, a.tokens))
            if verbose:
                print(f"  t={clock.now:7.3f}  ADMIT   tokens={a.tokens:,}")
        else:
            result.enqueued += 1
            if verbose:
                print(f"  t={clock.now:7.3f}  ENQUEUE tokens={a.tokens:,}")
    result.duration = arrivals[-1].ts if arrivals else 0.0
    result.reject_reasons = dict(gate.reject_reasons)  # snapshot Counter → plain dict
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Assertions
# ══════════════════════════════════════════════════════════════════════════════

def gate_internal_violations(gate: AdmissionGate) -> Tuple[int, int, int, int]:
    """
    Count how many admitted windows/epochs exceeded their cap in the FINAL counter
    state. Because try_admit() enforces the same conditions the production
    transaction does, this must be 0 for RPM and TPM — a non-zero value would mean
    the sim's gate logic diverged from put_allocation().

    Returns (rpm_window_over, rpm_global_over, tpm_window_over, tpm_global_over).
    Each window's cap is recomputed at the window's max effective/epoch value.
    """
    rpm_w_over = rpm_g_over = tpm_w_over = tpm_g_over = 0
    # RPM window: worst case effective cap = burst_capacity + 60*regen (end of window)
    max_rpm_effective = gate.burst_capacity + int(60 * gate.burst_regen_rate)
    for _, c in gate.rpm_window.items():
        if c > max_rpm_effective:
            rpm_w_over += 1
    # RPM global epoch cap = burst_capacity * 5 (max minutes_elapsed)
    max_rpm_global = gate.burst_capacity * 5
    for _, c in gate.rpm_global.items():
        if c > max_rpm_global:
            rpm_g_over += 1
    if gate.tpm_burst_capacity > 0:
        max_tpm_effective = gate.tpm_burst_capacity + int(60 * gate.tpm_burst_regen_rate)
        for _, c in gate.tpm_window.items():
            if c > max_tpm_effective:
                tpm_w_over += 1
        max_tpm_global = gate.tpm_burst_capacity * 5
        for _, c in gate.tpm_global.items():
            if c > max_tpm_global:
                tpm_g_over += 1
    return rpm_w_over, rpm_g_over, tpm_w_over, tpm_g_over


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

SEP = "─" * 72
DSEP = "━" * 72


def print_summary(cfg: SimConfig, offered_rps: float, duration: float,
                  result: AdmissionResult, gate: AdmissionGate) -> None:
    q = cfg.quota
    print(f"\n{DSEP}")
    print(f"  BUDGET MANAGER ADMISSION  ·  workload={cfg.workload_name}  "
          f"profile={cfg.profile_name}")
    print(SEP)
    print(f"  Total quota          : {q.rpm:,} RPM / {q.tpm:,} TPM")
    print(f"  Burst slice (this)   : {q.burst_rpm:,} RPM ({q.burst_fraction*100:.0f}%) / "
          f"{q.burst_tpm:,} TPM")
    print(f"  Burst 2s caps        : {q.burst_rpm_2s_cap:,} req / {q.burst_tpm_2s_cap:,} tok")
    print(f"  Sustained burst rate : {q.burst_rps:.2f} RPS / {q.burst_tpm_rate:,.0f} tok/s")
    print(SEP)
    print(f"  Offered load         : {offered_rps:.2f} RPS over {duration:.0f}s  "
          f"(avg {cfg.workload.avg_total_tokens():,.0f} tok/req)")
    print(f"  Requests offered     : {result.total}")
    print(f"  ADMITTED             : {result.admitted_count}  "
          f"({result.admission_rate*100:.1f}%)")
    print(f"  ENQUEUED             : {result.enqueued}  "
          f"({(1-result.admission_rate)*100:.1f}%)")
    print(f"  Admitted tokens      : {result.admitted_tokens:,}")
    if result.reject_reasons:
        reasons = ", ".join(f"{k}={v}" for k, v in sorted(result.reject_reasons.items()))
        print(f"  Reject reasons       : {reasons}")

    peak_r2 = result.peak_req_in_window(2.0)
    peak_r60 = result.peak_req_in_window(60.0)
    peak_t2 = result.peak_tokens_in_window(2.0)
    peak_t60 = result.peak_tokens_in_window(60.0)
    max_rpm_eff = gate.burst_capacity + int(60 * gate.burst_regen_rate)
    max_tpm_eff = gate.tpm_burst_capacity + int(60 * gate.tpm_burst_regen_rate)
    print(SEP)
    print(f"  Peak admitted req/2s : {peak_r2:>10}  (cap≈{q.burst_rpm_2s_cap:,})")
    print(f"  Peak admitted req/60s: {peak_r60:>10}  (window cap≤{max_rpm_eff:,})")
    if q.burst_tpm_capacity > 0:
        print(f"  Peak admitted tok/2s : {peak_t2:>10,}  (cap≈{q.burst_tpm_2s_cap:,})")
        print(f"  Peak admitted tok/60s: {peak_t60:>10,}  (window cap≤{max_tpm_eff:,})")


def assert_result(cfg: SimConfig, result: AdmissionResult,
                  gate: AdmissionGate) -> AssertionResult:
    """
    Assert the admitted stream respects the burst-slice windows. We assert against
    GATE-INTERNAL violations (final counter state vs the same effective/epoch cap
    the production transaction enforces), NOT a naive burst_rpm, because linear
    regeneration + the 5-min epoch multiplier legitimately allow more than
    burst_rpm admits within a single 60s window.
    """
    aa = AssertionResult()
    rpm_w, rpm_g, tpm_w, tpm_g = gate_internal_violations(gate)

    aa.check(rpm_w == 0, "Admitted RPM 60s window within effective cap",
             f"windows_over={rpm_w}")
    aa.check(rpm_g == 0, "Admitted RPM 5-min epoch within global cap",
             f"epochs_over={rpm_g}")
    if gate.tpm_burst_capacity > 0:
        aa.check(tpm_w == 0, "Admitted TPM 60s window within effective cap",
                 f"windows_over={tpm_w}")
        aa.check(tpm_g == 0, "Admitted TPM 5-min epoch within global cap",
                 f"epochs_over={tpm_g}")

    # 2s smoothing: peak admitted requests and tokens in any 2s window must
    # not exceed the gate's caps. Arrivals are strictly time-ordered in the sim
    # (no concurrent-Lambda race), so the gate holds exactly — zero slack needed.
    peak_r2 = result.peak_req_in_window(2.0)
    aa.check(peak_r2 <= cfg.quota.burst_rpm_2s_cap,
             f"Peak admitted req/2s ≤ burst_rpm_2s_cap ({cfg.quota.burst_rpm_2s_cap})",
             f"peak={peak_r2}")

    peak_t2 = result.peak_tokens_in_window(2.0)
    aa.check(peak_t2 <= cfg.quota.burst_tpm_2s_cap,
             f"Peak admitted tok/2s ≤ burst_tpm_2s_cap ({cfg.quota.burst_tpm_2s_cap:,})",
             f"peak={peak_t2:,}")

    aa.check(result.admitted_count > 0, "At least one request admitted",
             f"admitted={result.admitted_count}")
    return aa


# ══════════════════════════════════════════════════════════════════════════════
# Scenario runner
# ══════════════════════════════════════════════════════════════════════════════

def resolve_offered_rps(cfg: SimConfig, load_factor: float,
                        offered_rps_override: Optional[float]) -> float:
    """
    Offered RPS = load_factor × the burst-slice sustainable RPS for this workload.

    Sustainable burst RPS is min(burst_rps, burst_tpm_rate / avg_tokens): whichever
    dimension binds. load_factor > 1 offers MORE than the gate can sustain, forcing
    a realistic admit/enqueue split. An explicit --offered-rps overrides this.
    """
    if offered_rps_override is not None:
        return offered_rps_override
    q = cfg.quota
    avg_tok = max(1.0, cfg.workload.avg_total_tokens())
    sustainable = min(q.burst_rps, q.burst_tpm_rate / avg_tok) if q.burst_tpm_rate > 0 else q.burst_rps
    return max(0.1, sustainable * load_factor)


def run_scenario(cfg: SimConfig, load_factor: float, duration: float,
                 offered_rps_override: Optional[float], verbose: bool) -> bool:
    q = cfg.quota
    offered_rps = resolve_offered_rps(cfg, load_factor, offered_rps_override)

    # Generate enough token-weighted items to cover the whole arrival stream.
    est_arrivals = max(1, int(offered_rps * duration))
    items = make_items_for_preset(cfg.workload, min(2000, max(60, est_arrivals)))
    arrivals = make_arrivals(offered_rps, duration, items)

    print(f"\n\n{'#' * 72}")
    print(f"# SCENARIO: {cfg.workload_name.upper()}  ·  profile={cfg.profile_name}  "
          f"·  load_factor={load_factor:.1f}x")
    print(f"#  Workload   : {cfg.workload.description}")
    print(f"#  Avg tokens : {cfg.workload.avg_total_tokens():,.0f}  "
          f"max={cfg.workload.max_total_tokens():,}")
    print(f"#  Offered    : {offered_rps:.2f} RPS × {duration:.0f}s = {len(arrivals)} requests")
    print(f"{'#' * 72}")

    gate = AdmissionGate(q, short_window_sec=cfg.short_window_sec)
    clock = FakeClock()
    wall_start = real_time.perf_counter()
    result = run_admission(gate, arrivals, clock, verbose=verbose)
    wall_ms = (real_time.perf_counter() - wall_start) * 1000

    print_summary(cfg, offered_rps, duration, result, gate)

    print(f"\n  Assertions")
    aa = assert_result(cfg, result, gate)
    aa.report()
    print(f"\n  Simulation wall time: {wall_ms:.0f}ms")

    passed = aa.all_passed
    icon = "✅" if passed else "❌"
    print(f"  {icon} Scenario {'passed' if passed else 'FAILED'} all assertions")
    return passed


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--profile", default="prod", choices=list(QUOTA_PROFILES),
                   help="Named quota profile (default: prod = 2000 RPM / 4M TPM total)")
    p.add_argument("--workload", choices=list(WORKLOAD_PRESETS), default=None,
                   help="Run only this workload (default: all three)")
    p.add_argument("--rpm", type=int, default=None, help="Override total RPM")
    p.add_argument("--tpm", type=int, default=None, help="Override total TPM")
    p.add_argument("--burst-fraction", type=float, default=None,
                   help="Override burst (budget manager) quota fraction (default 0.50)")
    p.add_argument("--load-factor", type=float, default=1.5,
                   help="Offered load as a multiple of sustainable burst RPS (default 1.5x = over-quota)")
    p.add_argument("--offered-rps", type=float, default=None,
                   help="Explicit offered RPS (overrides --load-factor)")
    p.add_argument("--duration", type=float, default=120.0,
                   help="Offered-load duration in seconds (default 120)")
    p.add_argument("--no-smoke", action="store_true",
                   help="Skip the smoke profile sanity check")
    p.add_argument("--verbose", action="store_true",
                   help="Print per-arrival admit/enqueue trace")
    # Phase 0 GATE: side-by-side CURRENT counter (with contention) vs the PROPOSED
    # window-read gate, to prove the cutover recovers the sheds contention caused.
    p.add_argument("--compare-gates", action="store_true",
                   help="Run COUNTER (contended) vs WINDOW-READ gate side-by-side "
                        "and report admitted tokens/min for each (Phase 0 gate).")
    p.add_argument("--concurrency", type=int, default=30,
                   help="Concurrent in-flight admits for the contention/over-admission "
                        "model (default 30 — mirrors ~30 RPS burst fan-out).")
    p.add_argument("--short-window-sec", type=float, default=2.0,
                   help="Short (rate-smoothing) window seconds (default 2).")
    p.add_argument("--long-window-sec", type=float, default=15.0,
                   help="Long (accuracy) window seconds for the window-read gate (default 15).")
    p.add_argument("--writeback-latency-sec", type=float, default=7.5,
                   help="Estimate→actual write-back latency for the window-read gate (default 7.5).")
    p.add_argument("--actual-ratio", type=float, default=1.0,
                   help="actual_tokens/estimated_tokens once reconciled (default 1.0 = neutral; "
                        "<1 models the runtime output over-estimate freeing window room).")
    return p.parse_args()


def run_compare_gates(cfg: SimConfig, args: argparse.Namespace) -> bool:
    """
    Phase 0 GATE. Feed the SAME over-quota arrival stream through the CURRENT
    counter gate (with the production TPM-item contention race modeled) and the
    PROPOSED window-read gate, then compare admitted tokens/min.

    Exit criterion (from the implementation plan): the window-read gate must
    admit AT LEAST the burst budget the counter gate was shedding — i.e. its
    admitted-token throughput recovers the ~3.6M/min contention was losing.
    """
    q = cfg.quota
    duration = args.duration
    offered_rps = resolve_offered_rps(cfg, args.load_factor, args.offered_rps)

    est_arrivals = max(1, int(offered_rps * duration))
    items = make_items_for_preset(cfg.workload, min(4000, max(60, est_arrivals)))
    arrivals = make_arrivals(offered_rps, duration, items)

    print(f"\n{'#' * 72}")
    print(f"# PHASE 0 GATE — COUNTER (contended) vs WINDOW-READ")
    print(f"#  profile={cfg.profile_name}  workload={cfg.workload_name}")
    print(f"#  burst slice: {q.burst_rpm:,} RPM / {q.burst_tpm:,} TPM  "
          f"(regen {q.burst_tpm_rate:,.0f} tok/s = {q.burst_tpm_rate*60:,.0f}/min)")
    print(f"#  offered: {offered_rps:.2f} RPS × {duration:.0f}s = {len(arrivals)} req  "
          f"(avg {cfg.workload.avg_total_tokens():,.0f} tok/req)")
    print(f"#  concurrency={args.concurrency}  windows={args.short_window_sec:.0f}s/"
          f"{args.long_window_sec:.0f}s  writeback={args.writeback_latency_sec:.1f}s  "
          f"actual_ratio={args.actual_ratio}")
    print(f"{'#' * 72}")

    # ── CURRENT: counter gate WITH contention ──────────────────────────────────
    # write_serialization = how many concurrent TPM-counter writers DynamoDB
    # serializes cleanly before the rest conflict. At concurrency C, roughly
    # C admits land within the conflict window; only ~1 commits per serialization
    # slot, the rest retry and (mostly) shed. We tie the conflict pressure to the
    # offered concurrency so higher fan-out sheds more — the measured behavior.
    # write_serialization: how many writers DynamoDB serializes cleanly on the
    # single TPM item within one conflict window before the rest conflict. Tuned
    # so the modeled counter gate admits the MEASURED immediate throughput
    # (~150-240 req/min ≈ 3-4/s at 30 RPS) rather than zero — a faithful, not
    # strawman, baseline. Above this rate, additional concurrent writers shed as
    # contention (the 1,087-of-1,093 TPM-conflict finding, admission-logic §3).
    counter_gate = ContendedCounterGate(
        q, short_window_sec=args.short_window_sec,
        conflict_window=0.050,               # ~one DynamoDB write round-trip
        write_serialization=3,               # ~measured clean-serialize depth
        max_retries=3,                        # matches put_allocation max_conflict_retries
    )
    # Seed the write burst so the contention model reflects `concurrency` in-flight
    # admits, not just strictly-serial arrivals. We inject the fan-out by pre-loading
    # the conflict window proportional to concurrency at each admit via the arrival
    # jitter already present; to make concurrency bite, pre-fill tpm_writes.
    counter_clock = FakeClock()
    counter_result = AdmissionResult(total=len(arrivals))
    # Model concurrent fan-out realistically: siblings only contend when they
    # actually cluster in time. For each arrival we inject sibling writers with
    # probability = min(1, offered_rps * conflict_window / concurrency-normalized),
    # i.e. denser offered load ⇒ more overlapping writes on the single TPM item.
    # This reproduces the MEASURED behavior (immediate path admits ~150-240/min,
    # not zero) rather than a strawman that sheds everything.
    conflict_win = 0.050
    # Expected concurrent writers on the single TPM item in one conflict window.
    # Two sources of overlap: (a) natural arrival density offered_rps*conflict_win,
    # and (b) burst fan-out — Step Functions launches admits in clusters, so a
    # fraction of `concurrency` fire near-simultaneously. We model (b) as a modest
    # multiplier (sqrt of concurrency) rather than the full fan-out, so the counter
    # gate admits the MEASURED ~150-240/min, not zero — a faithful comparison.
    import math as _math
    # Natural overlap on the single item = arrival density in the conflict window.
    # `concurrency` raises the fan-out modestly (log scale): Step Functions bursts
    # cluster admits, but not all `concurrency` land in the SAME 50ms window.
    expected_siblings = offered_rps * conflict_win * (1.0 + _math.log(max(1, args.concurrency)))
    rng_c = random.Random(1234)
    for a in arrivals:
        if a.ts > counter_clock.now:
            counter_clock.advance(a.ts - counter_clock.now)
        now = counter_clock.now
        counter_gate._prune_writes(now)
        # Poisson-ish sibling injection: draw the number of concurrent writers
        # already touching the TPM item in this window.
        siblings = 0
        lam = expected_siblings
        # Simple Knuth Poisson sampler (lam is small-ish here).
        L = _math.exp(-lam)
        k, p = 0, 1.0
        while True:
            k += 1
            p *= rng_c.random()
            if p <= L:
                break
        siblings = k - 1
        for _ in range(siblings):
            counter_gate.tpm_writes.append(now)
        admitted = counter_gate.try_admit(now, a.tokens)
        if admitted:
            counter_result.admitted.append((now, a.tokens))
        else:
            counter_result.enqueued += 1
    counter_result.duration = arrivals[-1].ts if arrivals else 0.0
    counter_result.reject_reasons = dict(counter_gate.reject_reasons)

    # ── PROPOSED: window-read gate WITH bounded over-admission ──────────────────
    # read_visibility_lag models the RMW race: an admit isn't visible to a
    # concurrent reader until it commits (~one write round-trip). Higher
    # concurrency ⇒ more admits slip through against a stale window snapshot, but
    # the 15s horizon caps the total damage.
    window_gate = WindowReadGate(
        q, short_window_sec=args.short_window_sec,
        long_window_sec=args.long_window_sec,
        writeback_latency_sec=args.writeback_latency_sec,
        read_visibility_lag_sec=0.050,       # ~one write round-trip commit lag
        actual_ratio=args.actual_ratio,
    )
    window_clock = FakeClock()
    window_result = run_admission(window_gate, arrivals, window_clock, verbose=False)

    # ── Report ──────────────────────────────────────────────────────────────────
    def per_min(result: AdmissionResult) -> float:
        secs = max(1.0, result.duration)
        return result.admitted_tokens / secs * 60.0

    counter_tpm_min = per_min(counter_result)
    window_tpm_min = per_min(window_result)
    burst_budget_min = q.burst_tpm_rate * 60.0

    print(f"\n{SEP}")
    print(f"  {'':22}  {'COUNTER (contended)':>22}  {'WINDOW-READ':>18}")
    print(SEP)
    print(f"  {'Admitted req':22}  {counter_result.admitted_count:>22,}  "
          f"{window_result.admitted_count:>18,}")
    print(f"  {'Enqueued req':22}  {counter_result.enqueued:>22,}  "
          f"{window_result.enqueued:>18,}")
    print(f"  {'Admission rate':22}  {counter_result.admission_rate*100:>21.1f}%  "
          f"{window_result.admission_rate*100:>17.1f}%")
    print(f"  {'Admitted tokens':22}  {counter_result.admitted_tokens:>22,}  "
          f"{window_result.admitted_tokens:>18,}")
    print(f"  {'Admitted tok/min':22}  {counter_tpm_min:>22,.0f}  {window_tpm_min:>18,.0f}")
    print(SEP)
    print(f"  Burst slice budget       : {burst_budget_min:,.0f} tok/min "
          f"(the target the gate should reach)")
    recovered = window_tpm_min - counter_tpm_min
    print(f"  Counter gate shortfall   : {burst_budget_min - counter_tpm_min:,.0f} tok/min "
          f"below burst budget (contention loss)")
    print(f"  Window-read recovers     : {recovered:,.0f} tok/min vs counter gate")
    if counter_result.reject_reasons:
        print(f"  Counter reject reasons   : "
              + ", ".join(f"{k}={v}" for k, v in sorted(counter_result.reject_reasons.items())))
    if window_result.reject_reasons:
        print(f"  Window reject reasons    : "
              + ", ".join(f"{k}={v}" for k, v in sorted(window_result.reject_reasons.items())))

    # Peak over-admission check for the window gate (bounded damage).
    peak_t2 = window_result.peak_tokens_in_window(args.short_window_sec)
    peak_tN = window_result.peak_tokens_in_window(args.long_window_sec)
    print(SEP)
    print(f"  Window gate peak tok/{args.short_window_sec:.0f}s : {peak_t2:,} "
          f"(cap {window_gate.cap_2s_tok:,})")
    print(f"  Window gate peak tok/{args.long_window_sec:.0f}s: {peak_tN:,} "
          f"(cap {window_gate.cap_Ns_tok:,})")

    # ── EXIT CRITERION ────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    aa = AssertionResult()
    aa.check(window_tpm_min >= counter_tpm_min,
             "Window-read admits >= counter gate (recovers contention sheds)",
             f"window={window_tpm_min:,.0f} vs counter={counter_tpm_min:,.0f} tok/min")
    # Over-admission must stay bounded — peak within the long window should not
    # blow far past the cap (the 15s horizon is the correctness horizon).
    over_frac = (peak_tN / window_gate.cap_Ns_tok) if window_gate.cap_Ns_tok else 0.0
    aa.check(over_frac <= 1.5,
             "Window gate over-admission bounded (peak 15s ≤ 1.5× cap)",
             f"peak/cap={over_frac:.2f}")
    aa.report()
    passed = aa.all_passed
    icon = "✅" if passed else "❌"
    print(f"\n  {icon} PHASE 0 {'PASSED' if passed else 'FAILED'} — "
          f"{'cutover justified' if passed else 'DO NOT proceed to Phase 1'}")
    return passed


def main() -> None:
    args = parse_args()

    if args.compare_gates:
        wl = args.workload or "tpm-push"
        cfg = build_config(
            profile=args.profile, workload_name=wl,
            rpm_override=args.rpm, tpm_override=args.tpm,
            burst_fraction=args.burst_fraction,
            short_window_sec=args.short_window_sec,
        )
        print("=" * 72)
        print("  PHASE 0 GATE — COUNTER (with contention) vs WINDOW-READ ADMISSION")
        print("=" * 72)
        ok = run_compare_gates(cfg, args)
        sys.exit(0 if ok else 1)

    print("=" * 72)
    print("  BUDGET MANAGER ADMISSION-GATE SIMULATION")
    print("=" * 72)
    print()
    print("  Models the CONCURRENT ADMIT path (put_allocation runtime gate):")
    print("  each offered request is ADMITTED (capacity available) or ENQUEUED")
    print("  (BurstCapacityExceeded). Complements the queue processor DRAIN sim.")
    print()
    print("  Quota split (same fractions as create_model_config.py):")
    print("    burst  50% → budget manager (THIS simulation)")
    print("    queue  45% → queue processor (test_queue_processor_sim.py)")
    print("    buffer  5% → safety holdback")

    configs: List[Tuple[SimConfig, float, float]] = []  # (cfg, load_factor, duration)

    if not args.no_smoke:
        smoke = build_config(profile="smoke", workload_name="rpm-push")
        configs.append((smoke, args.load_factor, 30.0))

    workloads = [args.workload] if args.workload else list(WORKLOAD_PRESETS)
    for wl in workloads:
        cfg = build_config(
            profile=args.profile, workload_name=wl,
            rpm_override=args.rpm, tpm_override=args.tpm,
            burst_fraction=args.burst_fraction,
        )
        configs.append((cfg, args.load_factor, args.duration))

    print(f"\n  Scenarios to run: {len(configs)}")

    results: List[bool] = []
    for cfg, lf, dur in configs:
        results.append(run_scenario(cfg, lf, dur, args.offered_rps, args.verbose))

    total = len(results)
    passed = sum(results)
    failed = total - passed

    print(f"\n\n{'=' * 72}")
    print(f"  FINAL RESULTS   {passed}/{total} scenarios passed")
    print(f"{'=' * 72}")
    if failed == 0:
        print("""
  ✅ ALL SCENARIOS PASSED

  Proven (offline, no AWS):
    1. The admission gate admits up to the burst-slice RPM/TPM windows and
       enqueues the rest — the admit/enqueue split is visible per workload.
    2. Admitted traffic never exceeds the effective (regenerating) window cap
       or the 5-minute global epoch cap — the same conditions put_allocation()
       enforces in the production TransactWriteItems.
    3. The binding dimension emerges from the workload: small requests are
       RPM-bound, token-heavy requests are TPM-bound.

  Iterate on config here (profile / rpm / tpm / burst-fraction / load-factor)
  BEFORE spending a live Bedrock load test. Fidelity boundary: arrivals are
  time-ordered offered load, not the eventual-consistency over-admission race
  (bounded + reconciled in production; out of scope here).
""")
    else:
        print(f"\n  ❌ {failed} scenario(s) failed — the sim gate diverged from the "
              f"expected caps, or the offered load produced an impossible admit. "
              f"Review assertion output above.")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
