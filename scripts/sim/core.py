"""
Core simulation primitives: Item, FakeClock, SimResult, AssertionResult,
and workload generators.

These types are algorithm-agnostic — both the queue processor sim and the
(future) budget manager sim import from here.
"""
from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .config import WorkloadPreset, MAX_TOTAL_TOKENS


# ── Item ───────────────────────────────────────────────────────────────────────

@dataclass
class Item:
    """A single queued request.

    Two token counts, deliberately separate — this split is what lets a sim
    reproduce the live estimate-vs-actual drift bug (a flat estimate collapses
    the gates while Bedrock bills the real cost):

      tokens        — the ESTIMATE the dispatch gates pace on (what the queue
                      item carries as estimated_tokens, or the flat fallback).
      actual_tokens — what Bedrock actually CHARGES for this request. Defaults
                      to `tokens` (est == actual) so every existing sim that
                      only sets `tokens` is byte-for-byte unaffected.

    Use `.est` and `.actual` in dispatch/measurement code rather than reading
    the fields directly, so the fallback stays in one place.
    """
    tokens: int = 1
    actual_tokens: Optional[int] = None

    @property
    def est(self) -> int:
        """Token count the gates SEE (paces dispatch)."""
        return self.tokens

    @property
    def actual(self) -> int:
        """Token count Bedrock CHARGES (defaults to the estimate)."""
        return self.actual_tokens if self.actual_tokens is not None else self.tokens


# ── FakeClock ──────────────────────────────────────────────────────────────────

class FakeClock:
    """A wall clock driven by the simulation, not real time.

    Using a fake clock means the simulation completes in milliseconds regardless
    of how many simulated seconds it covers — no threading or real sleeps needed.
    """

    def __init__(self, start: float = 0.0):
        self._now = start

    @property
    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds > 0:
            self._now += seconds

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)


# ── SimResult ──────────────────────────────────────────────────────────────────

@dataclass
class SimResult:
    """Collected outputs from one algorithm run."""
    algo_name: str
    # (sim_timestamp, total_tokens) per dispatched item — `total_tokens` is the
    # ESTIMATE the gates paced on (kept as-is for backward compatibility; every
    # existing sim reads token math off this list).
    dispatch_events: List[Tuple[float, int]] = field(default_factory=list)
    # (sim_timestamp, actual_tokens) per dispatched item — what Bedrock CHARGED.
    # Populated only by actuals-aware sims; when empty, actual_* accessors below
    # transparently fall back to dispatch_events, so old sims are unaffected.
    actual_events:   List[Tuple[float, int]] = field(default_factory=list)
    sleep_events:    List[Tuple[float, float]] = field(default_factory=list)
    total_sim_time:  float = 0.0
    _db_reads: int = 0

    # ── Actuals-aware views (Bedrock-charged TPM, not the estimate) ────────────

    @property
    def _actuals(self) -> List[Tuple[float, int]]:
        """Actual-token events, falling back to the estimate stream if unset."""
        return self.actual_events if self.actual_events else self.dispatch_events

    @property
    def total_actual_tokens(self) -> int:
        return sum(tok for _, tok in self._actuals)

    @property
    def effective_actual_tps(self) -> float:
        """Bedrock-charged tokens per second over the full run."""
        return self.total_actual_tokens / self.total_sim_time if self.total_sim_time > 0 else 0.0

    def max_actual_tokens_in_rolling_window(self, window_sec: float) -> int:
        """Peak ACTUAL (Bedrock-charged) token sum in any rolling window."""
        events = sorted(self._actuals, key=lambda e: e[0])
        if not events:
            return 0
        peak, left, window_sum = 0, 0, 0
        for right in range(len(events)):
            window_sum += events[right][1]
            while events[left][0] < events[right][0] - window_sec:
                window_sum -= events[left][1]
                left += 1
            peak = max(peak, window_sum)
        return peak

    # ── Basic stats ───────────────────────────────────────────────────────────

    @property
    def dispatch_times(self) -> List[float]:
        return [ts for ts, _ in self.dispatch_events]

    @property
    def total_dispatched(self) -> int:
        return len(self.dispatch_events)

    @property
    def total_tokens_dispatched(self) -> int:
        return sum(tok for _, tok in self.dispatch_events)

    @property
    def effective_rps(self) -> float:
        return self.total_dispatched / self.total_sim_time if self.total_sim_time > 0 else 0.0

    @property
    def effective_tps(self) -> float:
        """Tokens per second (token throughput over the full sim duration)."""
        return self.total_tokens_dispatched / self.total_sim_time if self.total_sim_time > 0 else 0.0

    def total_sleep_time(self) -> float:
        return sum(d for _, d in self.sleep_events)

    def total_db_reads(self) -> int:
        return self._db_reads

    # ── Rolling-window analytics (O(n) sliding window) ────────────────────────

    def max_in_rolling_window(self, window_sec: float) -> int:
        """Peak REQUEST count in any rolling window of `window_sec` seconds."""
        events = sorted(self.dispatch_events, key=lambda e: e[0])
        if not events:
            return 0
        peak, left, count = 0, 0, 0
        for right in range(len(events)):
            count += 1
            while events[left][0] < events[right][0] - window_sec:
                count -= 1
                left += 1
            peak = max(peak, count)
        return peak

    def max_tokens_in_rolling_window(self, window_sec: float) -> int:
        """Peak TOKEN SUM in any rolling window of `window_sec` seconds."""
        events = sorted(self.dispatch_events, key=lambda e: e[0])
        if not events:
            return 0
        peak, left, window_sum = 0, 0, 0
        for right in range(len(events)):
            window_sum += events[right][1]
            while events[left][0] < events[right][0] - window_sec:
                window_sum -= events[left][1]
                left += 1
            peak = max(peak, window_sum)
        return peak

    def token_window_violations(self, window_sec: float, cap: int) -> int:
        """Count dispatch events whose rolling window token sum exceeds `cap`."""
        events = sorted(self.dispatch_events, key=lambda e: e[0])
        violations, left, window_sum = 0, 0, 0
        for right in range(len(events)):
            window_sum += events[right][1]
            while events[left][0] < events[right][0] - window_sec:
                window_sum -= events[left][1]
                left += 1
            if window_sum > cap:
                violations += 1
        return violations

    def request_window_violations(self, window_sec: float, cap: int) -> int:
        """Count dispatch events whose rolling window request count exceeds `cap`."""
        events = sorted(self.dispatch_events, key=lambda e: e[0])
        violations, left, count = 0, 0, 0
        for right in range(len(events)):
            count += 1
            while events[left][0] < events[right][0] - window_sec:
                count -= 1
                left += 1
            if count > cap:
                violations += 1
        return violations


# ── AssertionResult ────────────────────────────────────────────────────────────

class AssertionResult:
    """Accumulates pass/fail results for a set of named assertions."""

    def __init__(self) -> None:
        self.passed: List[str] = []
        self.failed: List[str] = []

    def check(self, condition: bool, label: str, detail: str = "") -> None:
        msg = label + (f"  [{detail}]" if detail else "")
        (self.passed if condition else self.failed).append(msg)

    @property
    def all_passed(self) -> bool:
        return len(self.failed) == 0

    def report(self, indent: str = "    ") -> None:
        for msg in self.passed:
            print(f"{indent}✅ PASS  {msg}")
        for msg in self.failed:
            print(f"{indent}❌ FAIL  {msg}")


# ── Workload generators ────────────────────────────────────────────────────────

def make_uniform(n: int, tokens: int) -> List[Item]:
    """n items all with the same token count."""
    return [Item(tokens=min(tokens, MAX_TOTAL_TOKENS)) for _ in range(n)]


def make_mixed(n: int, low: int, high: int, seed: int = 42) -> List[Item]:
    """n items with token count drawn uniformly from [low, high]."""
    rng = random.Random(seed)
    return [Item(tokens=min(rng.randint(low, high), MAX_TOTAL_TOKENS)) for _ in range(n)]


def make_heavy_tail_ranged(
    n: int,
    small_low: int, small_high: int,
    large_low: int, large_high: int,
    large_pct: float = 0.15,
    seed: int = 42,
) -> List[Item]:
    """
    n items split into two populations:
        (1 - large_pct) drawn uniformly from [small_low, small_high]
        large_pct       drawn uniformly from [large_low, large_high]
    All token counts are capped at MAX_TOTAL_TOKENS.
    """
    rng = random.Random(seed)
    items = []
    for _ in range(n):
        if rng.random() < large_pct:
            tok = rng.randint(large_low, large_high)
        else:
            tok = rng.randint(small_low, small_high)
        items.append(Item(tokens=min(tok, MAX_TOTAL_TOKENS)))
    return items


def make_items_for_preset(
    preset: WorkloadPreset,
    num_items: int,
    seed: int = 42,
) -> List[Item]:
    """
    Generate a list of Items according to a WorkloadPreset.

    Non-spike presets:  uniform random within [total_low, total_high].
    Spike presets:      heavy_tail_ranged with base and spike populations.
    """
    if preset.is_spike:
        return make_heavy_tail_ranged(
            num_items,
            small_low=preset.total_low,   small_high=preset.total_high,
            large_low=preset.spike_total_low, large_high=preset.spike_total_high,
            large_pct=preset.spike_pct,
            seed=seed,
        )
    return make_mixed(num_items, low=preset.total_low, high=preset.total_high, seed=seed)
