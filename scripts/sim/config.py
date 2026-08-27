"""
Simulation configuration: quota profiles, workload presets, and SimConfig.

Design principle: the total system quota (RPM + TPM) is stored here and split
into algorithm-specific allocations using the same fractions as
create_model_config.py / calculate_config():

    burst_fraction  (default 0.50) → budget manager's admission slice
    queue_fraction  (default 0.45) → queue processor's drain slice
    buffer_fraction (default 0.05) → safety holdback (neither algorithm uses this)

This means a SimConfig built from "2000 RPM / 4M TPM" with defaults gives the
queue processor sim 900 RPM / 1.8M TPM and (when the budget manager sim is
added) the burst sim 1000 RPM / 2M TPM — matching exactly what DynamoDB config
would write to production.

Every quota parameter in this file is in NATURAL UNITS (RPM, TPM).  Per-second
rates are derived properties, never stored directly.  This matches how operators
reason about Bedrock service quotas in the console.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict

# ── Hard token limits ──────────────────────────────────────────────────────────
# Bedrock models are capped at 200k input context.  We add a flat output
# headroom to get the maximum plausible total tokens a single request can consume.

MAX_INPUT_TOKENS: int = 200_000       # Bedrock context window cap
MAX_TOTAL_TOKENS: int = 204_000       # 200k input + 4k output safety margin


# ── SimQuota ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SimQuota:
    """
    Total system quota expressed in natural units (RPM, TPM) plus the same
    split fractions used by create_model_config.py / calculate_config().

    Each algorithm simulation reads its OWN slice via properties:
        queue processor → .queue_rpm  / .queue_tpm  / .queue_rps  / .queue_tpm_rate
        budget manager  → .burst_rpm  / .burst_tpm  / .burst_rps  / .burst_tpm_rate

    Window caps (.rpm_2s_cap, .tpm_2s_cap) are derived from the queue processor
    slice because that is what test_queue_processor_sim.py needs.  The budget
    manager sim will expose its own cap properties when implemented.
    """
    rpm: int                     # total RPM quota (or 0 if token-quota-only model)
    tpm: int                     # total TPM quota
    burst_fraction: float = 0.50 # fraction allocated to budget manager (burst path)
    queue_fraction: float = 0.45 # fraction allocated to queue processor (drain path)
    buffer_fraction: float = 0.05 # safety holdback — neither algorithm uses this
    notes: str = ""

    # ── Split allocations (mirrors calculate_config() arithmetic) ──────────────

    @property
    def burst_rpm(self) -> int:
        return int(self.rpm * self.burst_fraction) if self.rpm else 0

    @property
    def burst_tpm(self) -> int:
        return int(self.tpm * self.burst_fraction)

    @property
    def queue_rpm(self) -> int:
        return int(self.rpm * self.queue_fraction) if self.rpm else 0

    @property
    def queue_tpm(self) -> int:
        return int(self.tpm * self.queue_fraction)

    # ── Per-second rates ───────────────────────────────────────────────────────

    @property
    def burst_rps(self) -> float:
        return self.burst_rpm / 60.0

    @property
    def burst_tpm_rate(self) -> float:
        """Burst path: tokens per second."""
        return self.burst_tpm / 60.0

    @property
    def queue_rps(self) -> float:
        return self.queue_rpm / 60.0

    @property
    def queue_tpm_rate(self) -> float:
        """Queue path: tokens per second."""
        return self.queue_tpm / 60.0

    # ── Window caps for the QUEUE PROCESSOR slice ──────────────────────────────
    # The queue processor simulation uses these directly as the per-window hard
    # limits in its assertions and algorithm gates.

    @property
    def rpm_2s_cap(self) -> int:
        """Max requests the queue processor may dispatch in any 2-second window."""
        return max(1, int(self.queue_rps * 2))

    @property
    def tpm_2s_cap(self) -> int:
        """Max tokens the queue processor may dispatch in any 2-second window."""
        return int(self.queue_tpm_rate * 2)

    @property
    def queue_capacity(self) -> int:
        """Max requests in the queue processor's 60-second window (= queue_rpm)."""
        return self.queue_rpm

    @property
    def tpm_capacity(self) -> int:
        """Max tokens in the queue processor's 60-second window (= queue_tpm)."""
        return self.queue_tpm

    # ── Window caps for the BUDGET MANAGER (burst) slice ───────────────────────
    # The budget manager admission-gate simulation uses these as the per-window
    # hard limits in its gate math and assertions. They mirror the queue-slice
    # caps above but are derived from the burst fraction, matching exactly what
    # create_model_config.py / calculate_config() writes to DynamoDB:
    #     burst_capacity            = int(rpm * burst_fraction) = burst_rpm
    #     tpm_burst_capacity        = int(tpm * burst_fraction) = burst_tpm
    #     burst_regeneration_rate   = rpm/60 * burst_fraction   = burst_rps
    #     tpm_burst_regeneration_rate = tpm/60 * burst_fraction = burst_tpm_rate

    @property
    def burst_rpm_2s_cap(self) -> int:
        """Max requests the budget manager may admit in any 2-second window."""
        return max(1, int(self.burst_rps * 2))

    @property
    def burst_tpm_2s_cap(self) -> int:
        """Max tokens the budget manager may admit in any 2-second window."""
        return int(self.burst_tpm_rate * 2)

    @property
    def burst_rpm_capacity(self) -> int:
        """Max requests in the budget manager's 60-second window (= burst_rpm)."""
        return self.burst_rpm

    @property
    def burst_tpm_capacity(self) -> int:
        """Max tokens in the budget manager's 60-second window (= burst_tpm)."""
        return self.burst_tpm


# ── WorkloadPreset ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class WorkloadPreset:
    """
    Token distribution for a simulated workload.

    Token counts are specified as INPUT tokens; a flat output_tokens estimate
    is added to every request so total = input + output, matching how the budget
    manager builds estimated_tokens from (prompt_tokens + max_tokens).

    Spike workloads have two populations:
        base  (1 - spike_pct): input drawn uniformly from [input_low, input_high]
        spike (    spike_pct): input drawn uniformly from [spike_input_low, spike_input_high]

    All totals are capped at MAX_TOTAL_TOKENS.
    """
    description: str

    # Base population: input token range + flat output estimate
    input_low: int
    input_high: int
    output_tokens: int = 0

    # Spike tail (leave at 0 / 0.0 for non-spike workloads)
    spike_input_low: int = 0
    spike_input_high: int = 0
    spike_output: int = 0
    spike_pct: float = 0.0   # fraction of requests that are spike items (0–1)

    @property
    def is_spike(self) -> bool:
        return self.spike_pct > 0

    # ── Total token ranges (the values stored in Item.tokens) ─────────────────

    @property
    def total_low(self) -> int:
        return self.input_low + self.output_tokens

    @property
    def total_high(self) -> int:
        return min(self.input_high + self.output_tokens, MAX_TOTAL_TOKENS)

    @property
    def spike_total_low(self) -> int:
        return self.spike_input_low + self.spike_output

    @property
    def spike_total_high(self) -> int:
        return min(self.spike_input_high + self.spike_output, MAX_TOTAL_TOKENS)

    def avg_total_tokens(self) -> float:
        """Weighted average total tokens across both populations."""
        base_avg = (self.total_low + self.total_high) / 2.0
        if not self.is_spike:
            return base_avg
        spike_avg = (self.spike_total_low + self.spike_total_high) / 2.0
        return (1.0 - self.spike_pct) * base_avg + self.spike_pct * spike_avg

    def max_total_tokens(self) -> int:
        if self.is_spike:
            return max(self.total_high, self.spike_total_high)
        return self.total_high


# ── Named quota profiles ───────────────────────────────────────────────────────
# All values are TOTAL system quota (RPM + TPM).  The queue processor and budget
# manager each receive their slice via SimQuota properties.
#
# Fractions default to the same values as create_model_config.py so simulated
# results match what would be deployed: burst=50%, queue=45%, buffer=5%.

QUOTA_PROFILES: Dict[str, SimQuota] = {

    # ── Smoke / dev ────────────────────────────────────────────────────────────
    "smoke": SimQuota(
        rpm=100, tpm=100_000,
        notes=(
            "Smoke test — intentionally small quota to verify the script runs. "
            "Only rpm-push (small items) is exercised at smoke scale; "
            "medium/large workloads would exceed the queue processor's 60-s window."
        ),
    ),
    "dev": SimQuota(
        rpm=600, tpm=100_000,
        notes="Development baseline — matches original test_dispatch_algorithm_sim.py",
    ),

    # ── Production target ──────────────────────────────────────────────────────
    "prod": SimQuota(
        rpm=2_000, tpm=4_000_000,
        notes=(
            "Production target: 2,000 RPM / 4M TPM (total). "
            "Queue processor slice: 900 RPM / 1.8M TPM (45%). "
            "Budget manager slice: 1,000 RPM / 2M TPM (50%)."
        ),
    ),

    # ── Claude on-demand limits ────────────────────────────────────────────────
    "claude-haiku": SimQuota(
        rpm=1_000, tpm=100_000,
        notes="Claude 3 Haiku on-demand",
    ),
    "claude-sonnet": SimQuota(
        rpm=1_000, tpm=200_000,
        notes="Claude 3.5 Sonnet on-demand",
    ),
    "claude-opus": SimQuota(
        rpm=100, tpm=10_000,
        notes="Claude 3 Opus on-demand — very tight quotas, similar to smoke at small scale",
    ),

    # ── Amazon Nova on-demand limits ───────────────────────────────────────────
    "nova-lite": SimQuota(
        rpm=1_000, tpm=1_000_000,
        notes="Amazon Nova Lite — high TPM, token-heavy workloads stay TPM-bound",
    ),
    "nova-pro": SimQuota(
        rpm=400, tpm=400_000,
        notes="Amazon Nova Pro",
    ),

    # ── Provisioned throughput representative ──────────────────────────────────
    "prod-high": SimQuota(
        rpm=2_000, tpm=500_000,
        notes="Representative provisioned throughput tier",
    ),
}

# ── Named workload presets ─────────────────────────────────────────────────────
# Token ranges are specified as INPUT tokens to match how operators think about
# context sizes.  A flat output_tokens estimate is added per request.
#
# At the production profile (4M TPM / 2K RPM total, queue gets 45%):
#   queue_rps      = 15 RPS    (900 RPM / 60)
#   queue_tpm_rate = 30,000 tok/s  (1.8M TPM / 60)
#   rpm_2s_cap     = 30
#   tpm_2s_cap     = 60,000
#
# Binding constraint analysis:
#   rpm-push    avg ~775 tok  → TPM-bound at 38.7 RPS, RPM-bound at 15 → RPM wins
#   tpm-push    avg ~62k tok  → TPM-bound at 0.48 RPS, RPM-bound at 15 → TPM wins
#   mixed-spike avg ~18.7k tok → TPM-bound at 1.6 RPS,  RPM-bound at 15 → TPM wins

WORKLOAD_PRESETS: Dict[str, WorkloadPreset] = {

    "rpm-push": WorkloadPreset(
        description=(
            "Small requests (150–1,000 input + 200 output ≈ 350–1,200 total tokens). "
            "At prod quotas the RPM window saturates before the TPM window; "
            "expected drain rate ≈ queue RPS (15 RPS at prod)."
        ),
        input_low=150,   input_high=1_000,  output_tokens=200,
    ),

    "tpm-push": WorkloadPreset(
        description=(
            "Large requests (20k–100k input + 2k output ≈ 22k–102k total tokens). "
            "Every request is large relative to the 2-second token window; "
            "TOKEN-AWARE uses the oversized-item drain path. "
            "Expected drain rate ≈ queue_tpm_rate / avg_tokens (~0.5 RPS at prod)."
        ),
        input_low=20_000, input_high=100_000, output_tokens=2_000,
    ),

    "nova-live": WorkloadPreset(
        description=(
            "Nova-2-Lite live load-test profile: ~6,200 total tokens/req "
            "(≈6,000 input + 200 output). Matches the 30-RPS at_quota load-test arm used "
            "to validate the shaper. At 8M TPM (4M burst slice) this is TPM-bound "
            "but NOT so token-heavy that a single request saturates the 2s window, "
            "so counter-item contention (not capacity) is the throughput lever."
        ),
        input_low=5_800, input_high=6_200, output_tokens=200,
    ),

    "mixed-spike": WorkloadPreset(
        description=(
            "Medium base traffic (5k–15k input + 1k output ≈ 6k–16k total tokens) "
            "with 15% spike requests (20k–100k input + 2k output ≈ 22k–102k total). "
            "Spike items exceed the 2-second token window; TOKEN-AWARE must "
            "pause for those while flowing medium items through at the TPM-paced rate. "
            "Expected drain rate ≈ 1.6 RPS at prod."
        ),
        input_low=5_000,  input_high=15_000, output_tokens=1_000,
        spike_input_low=20_000, spike_input_high=100_000, spike_output=2_000,
        spike_pct=0.15,
    ),
}


# ── SimConfig ──────────────────────────────────────────────────────────────────

@dataclass
class SimConfig:
    """
    Fully resolved simulation configuration: one quota + one workload.

    Build via build_config() rather than constructing directly; the factory
    handles profile lookup, override merging, and auto-sizing.

    The .expected_effective_rps property drives both auto-sizing and assertion
    targets.  It accounts for three independent rate limits:
        (a) 60-second RPM window:  queue_rpm / 60
        (b) 2-second  RPM window:  rpm_2s_cap / 2  (integer floor may bind below (a))
        (c) TPM rate:              queue_tpm_rate / avg_tokens
    The binding constraint is whichever gives the lowest RPS.
    """
    quota: SimQuota
    workload_name: str
    workload: WorkloadPreset
    profile_name: str = "custom"

    # Simulation mechanics (algorithm-agnostic)
    batch_size: int = 10
    dispatch_overhead_ms: float = 20.0
    short_window_sec: float = 2.0

    # None → auto-sized; set explicitly to override
    num_items_override: Optional[int] = None

    # ── Derived properties ─────────────────────────────────────────────────────

    @property
    def expected_effective_rps(self) -> float:
        """
        Expected throughput of the TOKEN-AWARE algorithm on this config.
        Takes the minimum of FOUR independent rate limits:

            (a) 60s RPM window    queue_rpm / 60
            (b) 2s  RPM window    rpm_2s_cap / 2  (integer floor may bind below (a))
            (c) TPM rate          queue_tpm_rate / avg_tokens
            (d) Dispatch overhead 1000 / dispatch_overhead_ms
                                  — each item spends at least this long in the
                                    fake clock regardless of quota headroom; at
                                    high RPM quotas this becomes the binding limit.
        """
        avg_tok      = max(1.0, self.workload.avg_total_tokens())
        rpm_60s      = self.quota.queue_rps
        rpm_2s       = self.quota.rpm_2s_cap / self.short_window_sec
        tpm_lim      = self.quota.queue_tpm_rate / avg_tok
        overhead_lim = 1000.0 / self.dispatch_overhead_ms
        return min(rpm_60s, rpm_2s, tpm_lim, overhead_lim)

    @property
    def binding_constraint(self) -> str:
        """Which dimension limits throughput: RPM, RPM-2s, TPM, or OVERHEAD."""
        avg_tok      = max(1.0, self.workload.avg_total_tokens())
        rpm_60s      = self.quota.queue_rps
        rpm_2s       = self.quota.rpm_2s_cap / self.short_window_sec
        tpm_lim      = self.quota.queue_tpm_rate / avg_tok
        overhead_lim = 1000.0 / self.dispatch_overhead_ms
        mn = min(rpm_60s, rpm_2s, tpm_lim, overhead_lim)
        # Overhead binds only when it is strictly tighter than the quota limits.
        if overhead_lim == mn and overhead_lim < min(rpm_60s, rpm_2s, tpm_lim):
            return "OVERHEAD"
        if mn == tpm_lim:
            return "TPM"
        if mn == rpm_2s and rpm_2s < rpm_60s:
            return "RPM-2s"
        return "RPM"

    @property
    def num_items(self) -> int:
        """Auto-sized to exercise ~3 full 60-second windows, capped at 500."""
        if self.num_items_override is not None:
            return self.num_items_override
        eff = max(0.01, self.expected_effective_rps)
        return min(500, max(60, int(eff * 180)))

    def describe(self) -> str:
        q = self.quota
        return (
            f"profile={self.profile_name}  "
            f"total={q.rpm:,} RPM / {q.tpm:,} TPM  │  "
            f"queue slice={q.queue_rpm:,} RPM / {q.queue_tpm:,} TPM  │  "
            f"workload={self.workload_name}  │  "
            f"avg_tok={self.workload.avg_total_tokens():,.0f}  │  "
            f"binding={self.binding_constraint}  │  "
            f"expected_rps≈{self.expected_effective_rps:.2f}  │  "
            f"items={self.num_items}"
        )


# ── Factory ────────────────────────────────────────────────────────────────────

def build_config(
    profile: str = "prod",
    workload_name: str = "rpm-push",
    *,
    rpm_override: Optional[int] = None,
    tpm_override: Optional[int] = None,
    burst_fraction: Optional[float] = None,
    queue_fraction: Optional[float] = None,
    buffer_fraction: Optional[float] = None,
    num_items_override: Optional[int] = None,
    batch_size: int = 10,
    dispatch_overhead_ms: float = 20.0,
    short_window_sec: float = 2.0,
) -> SimConfig:
    """
    Build a SimConfig from a named profile + optional overrides.

    CLI overrides (rpm_override, tpm_override, fraction overrides) are applied
    on top of the named profile, so you can say:
        build_config("prod", "tpm-push", tpm_override=2_000_000)
    to test what happens if the TPM quota is halved without touching everything else.

    Args:
        profile:             Key into QUOTA_PROFILES (e.g. "prod", "smoke")
        workload_name:       Key into WORKLOAD_PRESETS (e.g. "rpm-push")
        rpm_override:        Override total RPM (None = use profile value)
        tpm_override:        Override total TPM (None = use profile value)
        burst_fraction:      Override burst split fraction (None = profile default)
        queue_fraction:      Override queue split fraction (None = profile default)
        buffer_fraction:     Override buffer fraction (None = profile default)
        num_items_override:  Override auto-sized item count (None = auto)
        batch_size:          DynamoDB dequeue chunk size
        dispatch_overhead_ms: Per-item dispatch latency (fake clock advance per item)
        short_window_sec:    Sub-minute smoothing window (default 2s, matches production)
    """
    if profile not in QUOTA_PROFILES:
        available = ", ".join(sorted(QUOTA_PROFILES))
        raise ValueError(f"Unknown profile {profile!r}. Available: {available}")
    if workload_name not in WORKLOAD_PRESETS:
        available = ", ".join(sorted(WORKLOAD_PRESETS))
        raise ValueError(f"Unknown workload {workload_name!r}. Available: {available}")

    base = QUOTA_PROFILES[profile]
    quota = SimQuota(
        rpm=rpm_override if rpm_override is not None else base.rpm,
        tpm=tpm_override if tpm_override is not None else base.tpm,
        burst_fraction=burst_fraction   if burst_fraction   is not None else base.burst_fraction,
        queue_fraction=queue_fraction   if queue_fraction   is not None else base.queue_fraction,
        buffer_fraction=buffer_fraction if buffer_fraction  is not None else base.buffer_fraction,
        notes=base.notes,
    )

    return SimConfig(
        quota=quota,
        profile_name=profile,
        workload_name=workload_name,
        workload=WORKLOAD_PRESETS[workload_name],
        batch_size=batch_size,
        dispatch_overhead_ms=dispatch_overhead_ms,
        short_window_sec=short_window_sec,
        num_items_override=num_items_override,
    )
