# Capacity Model — Rationale, Principles, and History

This is the "why" behind the burst/queue/buffer split and the RPM/TPM/iTPM/oTPM dimensions —
not the field reference (see [`../guide/configuration.md`](../guide/configuration.md)) and not
the current mechanism (see [`architecture.md`](architecture.md)). This doc is deliberately kept
free of live quota numbers, because those change constantly (check
[`../guide/configuration.md`](../guide/configuration.md) and `.bedrock_quota_cache.json` for
current values) — what doesn't change as fast is *why* the model is shaped the way it is.

Section 1 is the part that should stay true even as details evolve — read it every time.
Section 2 is the dated history that produced it — read it on demand, when you need the "why did
we end up here" behind a specific decision.

---

## 1. Go-forward framing

### 1.1 One admission lane per model — burst-only or queue-only, never both

Configure each model as **either** burst-dominant (e.g. `85/0/15`) **or** queue-dominant (e.g.
`0/85/15`) — not a blend. `burst_capacity ≤ 0` is the supported way to run queue-only today
(see [`architecture.md` §3](architecture.md#3-admission-gate--sliding-window-consumption-read),
"Burst disabled").

**Why:** the burst path and the queue-drain path are correctly implemented as independent,
separately-scaling mechanisms internally (separate DynamoDB consumption partitions — see
[`architecture.md` §4](architecture.md#4-dynamodb-single-table-design) and §6 — a flood on one
cannot starve the other, by design). But both ultimately dispatch against the **same external,
real Bedrock account-level quota**, and neither gate has any awareness of how much the other is
concurrently consuming against that shared external ceiling. Run both lanes at once and you get
one of two outcomes, both undesirable, and both observed in practice (§2.4-2.6):

- Tuned conservatively: the two lanes don't add up to the sum of their configured capacities —
  combined throughput plateaus below budget, wasting allocated quota.
- Tuned aggressively (drain pacer accurate and fast): the two lanes' independent dispatch streams
  overlap on the real account quota and throttle.

There is no tuning of a blended config that reliably gets you both "close to max throughput" and
"zero throttles" at the same time — because the coordination gap is structural, not a tuning
problem. One lane per model removes the gap entirely: there's only one dispatch stream drawing
against the account quota, so it can be paced against the real ceiling directly (this is what
`architecture.md` §6's Gate 5 even-spacing pacer does for the queue-only path today).

### 1.2 Lane choice = workload latency tolerance

- **Latency-tolerant workloads** → queue-only (`0/85/15`). This is today's default and the safe
  choice for anything batch-shaped.
- **Latency-intolerant workloads** → burst-only (`85/0/15`) **plus failover** for anything that
  doesn't get an immediate burst slot. Burst-only with nowhere for overflow to go just means
  overflow fails — that's only acceptable if something downstream can catch it.

**Target failover design (not yet built):** overflow from a burst-only lane should retry against
a *secondary model or provider* — either a different model on Bedrock (e.g. Sonnet 5 overflow →
Haiku 4.5) or the same model on a different provider path (e.g. Sonnet 5 on Bedrock → Sonnet 5 on
the Anthropic 1P API). There is currently no multi-provider/multi-model routing in this repo —
this is a known gap, not a shipped capability.

### 1.3 TPM is the durable axis; RPM is legacy

RPM mattered at this project's start because some models in active use (Nova, older Claude) had
RPM caps tight enough to bind before TPM did. The industry direction since has been toward
TPM-only quotas — most current-generation models carry no RPM limit at all. Treat RPM support in
this codebase as **maintained for backward compatibility with models that still have an RPM cap,
not as a dimension worth further design investment.** New capacity-model work should default to
thinking in TPM (and, on the Mantle backend, iTPM/oTPM) only.

### 1.4 Known gaps against this framing

The framing above is where this project is headed, not a description of what's fully enforced
today:

- **Nothing stops configuring both lanes at once.** `create_model_config.py` will happily accept
  `burst_fraction>0` and `queue_fraction>0` together; there's no validation forcing the one-lane
  choice.
- **[`../testing/results.md`](../testing/results.md)'s recommendation is stale.** That campaign
  (2026-06-25→27) validated correctness/latency across burst/queue splits, but never pushed load
  high enough to hit the throughput ceiling, so it couldn't see the coordination problem in §1.1.
  Its "burst-dominant" recommendation predates the findings in §2.5-2.6 below and should not be
  followed as-is.
- **[`../guide/configuration.md`](../guide/configuration.md)'s "Bias capacity toward immediate
  admission" worked example** (`BURST_FRACTION=0.85 QUEUE_FRACTION=0.10`) sets both lanes nonzero
  and is wrong under this framing.

---

## 2. History — how we got here

### 2.1 Origin: RPM was a real constraint, not an afterthought

Early in this project, RPM was sometimes the *binding* constraint — some models in scope had low
RPM caps that throttled before TPM did, which is why the capacity model was built with RPM and
TPM as parallel, independently-tracked dimensions from the start (both split into
burst/queue/buffer). See §1.3 for why this is now a declining concern.

### 2.2 MVP → production-ready (early 2026)

[ADR-001](adr/ADR-001-phased-architecture-approach.md) (2026-01-30) and
[ADR-002](adr/ADR-002-mvp-implementation-and-beyond.md) (2026-02-01) established the phased
approach: a queue-based semaphore MVP with graceful degradation, validated by load testing, ahead
of full production hardening.
[ADR-003](adr/ADR-003-production-ready-architecture.md) moved to the single-table DynamoDB design
and set the first concrete capacity split — burst 50% / queue 40% / buffer 10% — explicitly
labeled a **conservative estimate** to be tuned once load-tested.
[Leaky-Bucket-Optimization.md](design/Leaky-Bucket-Optimization.md) is the design basis for that
split: burst and queue as *independently managed* capacities, each checked by a different
component (Budget Manager vs. Queue Processor) — a clean separation of concerns that, years
later, turns out to be exactly the property that makes the coordination gap in §1.1 possible.

### 2.3 The counter-gate generation — and why it was later undone

[ADR-004](adr/ADR-004-counter-write-sharding.md) and
[ADR-005](adr/ADR-005-consumption-read-elimination.md) describe an intermediate architecture
generation: admission moved onto an atomic `TransactWriteItems` counter gate (to eliminate a
DynamoDB hot-partition *read* problem), with ADR-004 addressing write-sharding for that counter
at higher RPM. **This generation was itself later replaced.** The counter design solved the
read-hotspot problem but became a hot-partition *write* problem in its own right, pinning burst
throughput below budget. The current mechanism — a sliding-window read directly over consumption
records, no atomic counters at all — is documented in
[`architecture.md` §3](architecture.md#3-admission-gate--sliding-window-consumption-read) and
§11; the ADRs are kept as a historical record of a design that didn't survive contact with load
testing, not as current guidance.

One artifact from this generation did stick: the Mantle backend's iTPM/oTPM dimensions
(input/output tokens metered as separate account-level quotas by the Anthropic Messages API,
unlike Bedrock's combined TPM) were built **queue-only from day one** — burst forced to zero.
That was the earliest real instance of the one-lane principle in this codebase, arrived at
independently and before the general principle in §1.1 was recognized.

### 2.4 Hot-partition extreme-spike incident (2026-07-06 → 07-09)

A 5× load spike deadlocked the shaper (4.3% success, queue stalled). The fix chain — documented in
[`HOT-PARTITION-FIX-VALIDATION.md`](design/HOT-PARTITION-FIX-VALIDATION.md) — combined the
consumption-read change (§2.3), a Budget Manager memory/compute fix, a DynamoDB expression-syntax
fix, and removing an admission-gate concurrency ceiling that was throttling the shaper's own
Lambda before Bedrock ever saw the traffic. Final validated result: 99.94% success at the same 5×
spike. This incident's root cause was **not** the burst/queue coordination gap — it was Lambda-side
compute and concurrency starvation. It's included here because it's the other major throughput
incident from the same period and it hardened the sliding-window gate (§2.3) that the later
findings (§2.5-2.6) depend on.

### 2.5 The queue-drain pacer was blind, then fixed, then the real problem showed up

This part of the history predates this repo's public git log — it happened in this project's
private pre-publish development, so there's no linkable doc in this repo, but it's worth recording
here because it's the direct evidence behind §1.1:

- **2026-07-13/14 — broken pacer.** A live incident found the queue processor's token-rate gate was
  effectively blind: a capacity-calculation bug always reported full capacity, and a separate
  code path fed it a zero token estimate. The queue drained open-loop, with no real rate control,
  and threw over a thousand Bedrock throttles on one model. Fixed by moving to a token-aware
  sliding-window pacer.
- **2026-07-20 — undershoot, once the pacer was honest.** With the fix in place, an 8M-TPM model
  configured with a 50/45/5 split (burst 4M + queue 3.6M, expecting roughly 7.6M/min combined)
  sustained only ~3.9-4.0M/min — matching the burst lane's regeneration rate *alone*. The queue
  lane wasn't adding throughput on top of burst; it was invisibly capped by the same real ceiling
  burst was already drawing against.
- **2026-07-21 — the principle got named.** Push the queue-drain pacer to be aggressive enough to
  close that undershoot, and the opposite failure appears: the two lanes' independent dispatch
  streams overlap on the real account quota and throttle. Queue-only (burst disabled) was
  validated at 92% of the account's TPM budget with zero throttles — the best result either mode
  achieved. This is where §1.1's principle and the current `0/85/15` default posture come from.

The even-spacing pacer (Gate 5) described in
[`architecture.md` §6](architecture.md#6-queue-drain--rpmtpm-pacing) as "the primary rate
control... the sliding windows alone allowed sub-second bursts that throttled" is the surviving,
current-generation descendant of the 2026-07-13/14 fix.

### 2.6 Quota cache as source of truth (2026-09-16, this branch)

Most recently, `MODEL_MAP` (a hand-maintained alias table of default RPM/TPM values) was retired
as a *correctness* dependency. Real per-account Bedrock quotas are account-specific and change
over time — a hardcoded table can't track that. `get_bedrock_quotas.py` now reads live quota data
into `.bedrock_quota_cache.json`, and `create_model_config.py` resolves TPM as `--tpm` override >
cache, full stop — no third fallback tier. This is a related but distinct theme from the
burst/queue lane story: it's about *where quota numbers come from*, not *how admission is paced*
once you have them.
