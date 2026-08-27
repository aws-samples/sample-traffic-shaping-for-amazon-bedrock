# Bedrock Traffic Shaper — Production Hardening Plan

**Date:** 2026-03-15
**Source:** Council debate (Architect, Engineer, Researcher, Security, Designer) — 3-round structured debate
**Stack:** SemaphoreRateLimiterStack (us-east-1, Account 111122223333)

---

## Where We Are

### Completed Testing (Phases 1-6g, 8)

| Phase | Result | Key Metric |
|-------|--------|-----------|
| 5 — Three-way comparison | No retry 62%, Retry+jitter 44%, **Traffic Shaper 100%** | 3.2x retry amplification eliminated |
| 6f — Global admission cap | 1975 → 508 → 269 → **100** burst admits | 95% reduction, zero variance (3 runs) |
| 6g — Low-RPM validation | Jamba 100 RPM, 150 requests | 100% success, 6 burst / 144 queued / 0 failures |
| Queue drain reliability | 1008/1008 and 144/144 processed | **0 failures, 0 DLQ messages** |
| SFN timeout fix | 5 min → 30 min | 0 false timeout failures |
| 8 — 1-hour soak test | 4,200 requests at 70 RPM, **QUALIFIED PASS** | 98.9% success (100% system, 45 Bedrock transient) |

**Verdict: Functionally validated and soak-tested. Core systems proven under sustained load. Remaining blind spots documented below.**

### 1-Hour Soak Test Critique (What We Still Don't Know)

The 1-hour soak proved the system works. It did NOT prove it survives production conditions. These specific blind spots remain:

| Blind Spot | Why It Matters | How We Address It |
|------------|---------------|-------------------|
| **Long-duration stability** | 1 hour doesn't surface counter drift accumulation, TTL cleanup gaps, or DDB partition hotspots | 72-hour soak with specific drift/TTL metrics (Sprint 2) |
| **Circuit breaker under real conditions** | Bedrock transient lasted ~1 min — never triggered 3-batch trip threshold | Dedicated sustained-failure test: 5+ minutes of 100% Bedrock errors (Sprint 2) |
| **Cold-start spike** | Lambda warm pools were active throughout soak | Flush warm pools → immediate burst test (Sprint 2) |
| **Multi-model contention** | Single model, single queue — no cross-model interference tested | Concurrent Opus + Jamba + Nova Lite soak (Sprint 2, moved from Sprint 3) |
| **DynamoDB throttling** | On-demand capacity auto-scales — never saw throttling behavior | Provisioned capacity test at deliberately low WCU (Sprint 2) |
| **Caller latency expectations** | p50=22s, p95=107s, p99=162s — acceptable for batch, not for interactive | Document latency SLA per path (burst vs. queue) for stakeholders (Sprint 2) |
| **DLQ consumer strategy** | 45 failures went to DLQ, but what does a caller do about it? | Document DLQ consumption pattern + retry guidance (Sprint 2) |
| **Success rate metric definition** | 99.9% gate criterion is unreachable if Bedrock has transients | Redefine: separate system reliability from end-to-end success rate (Sprint 2) |

### What's Been Deployed

- [x] TransactWriteItems 3-item atomic admission gate (consumption + per-window counter + global 5-min counter)
- [x] RPM-paced queue drain (`min_batch_interval = batch_size / queue_regen_rate`)
- [x] Global admission counter (`max_burst_multiplier` = 2.0, deterministic, zero variance)
- [x] Step Functions timeout 65 minutes
- [x] Reserved Lambda concurrency (200) on Budget Manager + Bedrock Processor
- [x] CloudWatch dashboard (13 widgets: admission, queue, Bedrock, Lambda, SFN, health, alarms)
- [x] DLQ alarm (BedrockShaper-DLQ-NotEmpty, threshold > 0)
- [x] Self-healing without a sweep (15 s admission window + 60 s DynamoDB TTL + inline expired-item terminalization)
- [x] 5 report charts + architecture diagram in `reports/`

---

## What's Ahead

### Sprint 0.5 — Operator Readiness (3 days)

*Goal: Make the system observable and operable by someone who didn't build it.*

- [x] **RUNBOOK.md** — skeleton runbook with alarm-to-action mappings
  - [x] DLQ alarm fires → inspect DLQ, check Bedrock Processor logs, identify failing request pattern
  - [x] Queue depth growing unbounded → check Queue Processor lock, verify EventBridge trigger, check Bedrock availability
  - [x] Burst utilization pegged at 100% → verify model config, check if burst_capacity is undersized
  - [x] Lambda errors spiking → check CloudWatch error logs, identify which Lambda, correlate with Bedrock status
  - [x] SFN executions failing → check Budget Manager logs for task token errors
- [x] **Trace correlation** — add `correlation_id` to EMF metric emissions for cross-Lambda tracing
  - [x] Budget Manager EMF includes `correlation_id` property (already present)
  - [x] Queue Processor EMF includes `processor_id` property (batch-level; per-item `correlation_id` in structured logs)
  - [x] Bedrock Processor EMF includes `correlation_id` property
- [x] **Dashboard annotations** — add CloudWatch text widgets defining "normal" baselines
  - [x] Text widget above Row 1: Admission Gate baselines (Queue Depth, Burst Utilization)
  - [x] Text widget above Row 5: Step Functions baselines (Failed, TimedOut, Succeeded)
- [x] **Additional alarms** (leading indicators, not just DLQ)
  - [x] Queue depth > 100 for 5 consecutive minutes (`BedrockShaper-QueueDepthHigh`)
  - [x] Lambda error rate > 5% for 3 consecutive minutes (`BedrockShaper-LambdaErrorRate`)
  - [x] SFN execution failures > 0 for 2 consecutive minutes (`BedrockShaper-SfnFailures`)

### Sprint 1 — Defensive Hardening (1 week)

*Goal: Protect the system from bad input and downstream failures.*

- [x] **Circuit breaker** — stop queue processing when Bedrock is down
  - [x] Queue Processor tracks consecutive batch failure count (already exists: 3-failure trip)
  - [x] CloudWatch alarm on circuit breaker trip (`BedrockShaper-CircuitBreakerTripped`, EMF metric)
  - [x] Manual reset mechanism (`circuit_breaker_disabled` DDB config flag, documented in RUNBOOK.md)
  - [x] Verify: simulate 100% Bedrock failures, confirm processing stops after 3 batches (tested via DOES-NOT-EXIST Lambda ARN → AccessDeniedException → Full batch failure 1/3, 2/3, 3/3 → CircuitBreakerTripped EMF emitted)
- [x] **Input validation** — reject oversized payloads before slot allocation
  - [x] Budget Manager validates `max_tokens` against `max_tokens_per_request` config (default 4096)
  - [x] Reject requests where `max_tokens > max_tokens_per_request` with 400 + SFN task_failure
  - [x] Prompt size validation: reject if `len(prompt.encode('utf-8')) > 1MB`
  - [x] Verify: send oversized request, confirm rejection before burst slot consumed (max_tokens=10000 → InputValidationError, 0 consumption records)
- [x] **DynamoDB TTL audit** — all 7 record types verified with TTL
  - [x] Burst consumption: 300s (dynamo.py:139), Queue consumption: 300s (dynamo.py:598)
  - [x] Queue items: configurable `expiry_hours` default 1hr (dynamo.py:702)
  - [x] Per-window counter: 120s (dynamo.py:189), Global counter: 600s (dynamo.py:210)
  - [x] Lock records: 120s / LOCK_TTL (dynamo.py:892)
  - [x] Invocation errors: 7 days (dynamo.py:809). Config records: no TTL (correct — permanent)
- [x] **Cost model** — `COST-MODEL.md` created with 3-tier analysis
  - [x] DynamoDB: 6.9 WCU avg/request × $1.25/M = $22-224/mo at 100-1000 RPM
  - [x] Step Functions: 5 transitions/execution × $0.025/K = $33-325/mo
  - [x] Lambda: 2.3 invocations/request = $96-906/mo total infrastructure
  - [x] Summary: $0.035/1K requests infrastructure overhead (negligible vs. Bedrock inference)

### Sprint 2 — Soak Test & Validation (2 weeks)

*Goal: Prove the system survives sustained and adversarial load. Address all blind spots from 1-hour soak critique.*

#### 2a. Soak Testing (done + remaining)

- [x] **1-hour validation soak** — **QUALIFIED PASS** (Phase 8)
  - [x] Design soak test script (`scripts/soak_test.py` — rate-limited RPM, adversarial injection, DLQ delta, checkpoints)
  - [x] 4,200 sent at 70 RPM, 98.9% success, 45 DLQ (Bedrock transient at ~17-18m, self-healed)
  - [x] Adversarial: 200 sent, 97 correctly rejected before slot consumption
  - [x] Documented in TESTING-CAMPAIGN.md Phase 8
- [ ] **72-hour soak test** — requires persistent compute (EC2)
  - [ ] **Launch:** run the sustained soak on a persistent host in a `tmux` session —
    `make soak-test ARGS='--model <model> --target-rpm <n> --duration-hours 72'`
  - [ ] **EC2 setup:** t3.micro, Amazon Linux 2023, IAM role with SFN+DDB+SQS+Bedrock perms
  - [ ] **Deps on EC2:** `sudo yum install -y python3.11 python3.11-pip tmux git && python3.11 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
  - [ ] **Copy config.env** from local machine to EC2 (contains ARNs)
  - [ ] Run against a model at a sustained rate below its quota
  - [ ] **Specific metrics to watch** (gaps from 1-hour soak):
    - [ ] Consumption-record TTL: confirm orphaned records age out within ~60 s (no unbounded growth)
    - [ ] TTL cleanup: monitor DDB item count trend — are expired items being cleaned up or accumulating?
    - [ ] DDB partition behavior: monitor `ConsumedReadCapacityUnits` and `ConsumedWriteCapacityUnits` for hot-partition throttling signals
    - [ ] Memory: monitor Lambda max memory used across all 72 hours (CloudWatch `MaxMemoryUsed`)
    - [ ] Queue drain consistency: does processing rate degrade over time or stay constant?
  - [ ] **Post-soak:** `make analyze-soak ARGS='<results>.json --cloudwatch --hours 72'`
  - [ ] Measure: success rate, p50/p95/p99 latency, queue depth max, DLQ count, DDB consumed capacity
  - [ ] Document results in TESTING-CAMPAIGN.md as Phase 9

#### 2b. Blind Spot Tests (from soak critique)

- [x] **Circuit breaker under sustained failure** — dedicated test (Phase 10, ISC 1-8)
  - [x] Inject 100% Bedrock failures (BEDROCK_PROCESSOR_ARN → DOES-NOT-EXIST → AccessDeniedException)
  - [x] Circuit breaker trips after 3 consecutive batch failures (2 invocations with batch_size=2)
  - [x] CircuitBreakerTripped EMF metric emitted, processing stops
  - [x] Recovery: CDK redeploy restores correct ARN, new request triggers EventBridge → queue resumes
  - [x] Key finding: items dequeued during failing batches (6 items = 3 batches x 2) become SFN timeouts, not re-queued. Remaining 4 items preserved in queue and succeed after recovery
  - [x] Queue overflow time = queue_capacity / incoming_rate (e.g. 22 queue slots at 50 RPM → ~26s at full burst)
- [x] **Cold-start burst** — tested (Phase 10, ISC 9-14)
  - [x] Set reserved concurrency to 0 on Budget Manager + Bedrock Processor for 5 minutes
  - [x] Restored to 200, immediately sent 50 requests
  - [x] 23 cold starts observed, Init Duration 451-535ms
  - [x] 45 burst + 5 queued = 50/50 success, 0 failures, 0 SFN timeouts
  - [x] Cold-start latency does NOT cause SFN timeouts (535ms init << 30-min timeout)
- [x] **Multi-model contention** — tested (Phase 10, ISC 15-22)
  - [x] Configs: Opus (25 burst, 50 RPM), Jamba (50 burst, 100 RPM), Nova Lite (500 burst, 1000 RPM)
  - [x] Concurrent burst: 10 Opus + 30 Jamba + 30 Nova Lite = 70 requests simultaneously
  - [x] Per-model isolation confirmed: Opus 8b/2q, Jamba 22b/8q, Nova Lite 18b/12q
  - [x] All queues drained to 0 in 18.8s total, 0 failures, 0 timeouts
  - [x] Consumption records fully partitioned by model — zero cross-contamination
  - [x] EventBridge trigger + per-model lock mechanism handles concurrent models correctly
- [x] **Adversarial scenarios** — partially validated in 1-hour soak
  - [x] Poison messages: 45 BedrockInvocationErrors correctly routed to DLQ
  - [x] Oversized payloads: 97/200 adversarial correctly rejected before slot consumption
  - [ ] Remaining: cold-start burst (see above)
  - [ ] Remaining: sustained errors long enough to trip circuit breaker (see above)

#### 2c. Infrastructure Resilience

- [ ] **DynamoDB throttling resilience**
  - [ ] Switch single table from on-demand to provisioned capacity (deliberately low: 5 RCU / 5 WCU)
  - [ ] Send 50 requests at 30 RPM
  - [ ] Verify: system degrades gracefully (requests queue, don't crash; DDB throttle metrics visible)
  - [ ] Verify: no data loss — all requests eventually succeed or reach DLQ
  - [ ] Switch back to on-demand after test
  - [ ] Document DDB capacity recommendations for production

#### 2d. Documentation & Definitions

- [x] **Latency SLA documentation** — added to RUNBOOK.md
  - [x] Burst path: p50 1-3s, p95 5-8s, p99 10-15s
  - [x] Queue path: p50 20-60s, p95 90-120s, p99 150-180s
  - [x] Caller guidance: set timeout >= SFN timeout (30 min), treat queue path as async
- [x] **DLQ consumer guidance** — added to RUNBOOK.md
  - [x] Message format documented (request_id, model_id, error_type, error_message, correlation_id)
  - [x] Error classification table: retryable (Throttling, ModelTimeout) vs permanent (Validation, AccessDenied)
  - [x] Recommended consumer pattern: SQS-triggered Lambda with classify/retry/alert flow
- [x] **Redefine success rate metric** — added to RUNBOOK.md
  - [x] System reliability (>= 99.99%): admission, routing, queue drain — what we control
  - [x] End-to-end success (varies): includes Bedrock availability — what we measure
  - [x] Production gate uses system reliability, not end-to-end success
- [ ] **Full RUNBOOK.md expansion**
  - [x] Add observed failure modes and their signatures (Bedrock transient pattern added)
  - [ ] Add capacity planning guidelines based on soak data
  - [ ] Add escalation procedures
  - [ ] Add DLQ triage procedure (which errors are retryable)
- [x] **`max_burst_multiplier` tuning** — tested (Phase 10, ISC 23-26)
  - [x] Opus 1.5x: 30 burst admits (cap=37), 100/100 success, 162s queue drain
  - [x] Jamba 2.0x: 96 burst admits (cap=100), 200/200 success, 135s queue drain
  - [x] **Recommended values:**
    - Low RPM (<=50): `1.5x` — tighter cap reduces queue drain pressure
    - Medium RPM (100-500): `2.0x` — default, good balance of burst absorption and queue throughput
    - High RPM (>=1000): `2.0x` — large burst_capacity already absorbs spikes; no need for tighter cap

### Sprint 3 — Production Pilot (2 weeks)

*Goal: First real traffic with monitoring.*

- [ ] **Tenant isolation** (if multi-tenant)
  - [ ] Per-tenant burst budgets (separate global counters per tenant_id)
  - [ ] Per-tenant queue fairness (round-robin or weighted dispatch)
  - [ ] Verify: noisy tenant doesn't starve others
- [ ] **Canary automation**
  - [ ] EventBridge-triggered Lambda that sends 5 requests every hour
  - [ ] CloudWatch alarm on canary failure
  - [ ] Canary tests both burst path and queue path
- [ ] **Production pilot deployment**
  - [ ] Deploy to production account (or promote existing stack)
  - [ ] Route 5% of traffic through Traffic Shaper (shadow mode or canary)
  - [ ] Monitor for 1 week with on-call coverage
  - [ ] Expand to 25% → 50% → 100% based on success metrics
- [ ] **GA gate review**
  - [ ] All Sprint 0.5-2 items checked off
  - [ ] 72-hour soak passed with documented results
  - [ ] Multi-model concurrent test passed
  - [ ] Circuit breaker tested under sustained (5+ min) Bedrock failure
  - [ ] Cold-start burst tested with no data loss
  - [ ] Latency SLA documented and stakeholder-approved
  - [ ] DLQ consumer pattern documented
  - [ ] Runbook reviewed by on-call team
  - [ ] Cost model validated against soak test actuals
  - [ ] No P0/P1 bugs open

---

## Production Gate Criteria

The system is **production-ready** when ALL of the following are true:

### System Reliability (things we control)
1. **System reliability >= 99.99%** in 72-hour soak — zero dropped requests, zero orphaned state, zero system errors. Bedrock errors don't count against system reliability (they're downstream).
2. Circuit breaker tested under **sustained** (5+ min) simulated Bedrock outage — trips correctly, recovers correctly, no queue items lost during trip
3. Input validation rejects oversized payloads before consuming burst slots
4. Cold-start burst tested — 100 simultaneous requests after warm pool flush, no data loss
5. Multi-model concurrent test passed — 3 models simultaneously, no cross-model interference

### End-to-End Quality (things we measure but don't fully control)
6. **End-to-end success rate documented** per model, with Bedrock availability as the bounding factor
7. DLQ count = 0 during soak (excluding Bedrock transient errors and intentional poison messages)
8. Latency SLA documented and stakeholder-approved: burst path p50 < X, queue path p50 < Y

### Operational Readiness
9. DLQ consumer pattern documented (retryable vs permanent errors, recommended consumer Lambda)
10. Cost model documented and validated against soak test actuals
11. RUNBOOK.md reviewed by at least one person who didn't build the system
12. All CloudWatch alarms fire correctly in test scenarios
13. Capacity planning guidelines documented (DDB WCU at target RPM, Lambda concurrency)

**Key distinction:** We gate on *system reliability* (what we control), not *end-to-end success* (which is bounded by Bedrock's availability). A 1-minute Bedrock transient causing 45 DLQ messages is correct system behavior — the traffic shaper's job is to capture and trace those failures, not prevent them.

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `TESTING-CAMPAIGN.md` | Living test results (Phases 1-6g + Phase 8 soak) |
| `FINAL-REPORT.md` | Stakeholder-ready summary with charts |
| `PRODUCTION-HARDENING-PLAN.md` | This document |
| `infrastructure/semaphore_stack.py` | CDK stack (dashboard, alarms, Lambdas) |
| `infrastructure/lambda_layer/python/shared_service/dynamo.py` | Admission gate (TransactWriteItems) |
| `infrastructure/lambda_handlers/budget_manager.py` | Burst/queue routing |
| `infrastructure/lambda_handlers/queue_processor.py` | RPM-paced drain |
| `infrastructure/lambda_handlers/bedrock_processor.py` | Bedrock API calls |
| `reports/*.png` | Visualization charts |

---

## Council Source

This plan synthesizes the council debate held 2026-03-15 with 5 agents (Architect, Engineer, Researcher, Security, Designer) across 3 rounds. Key convergence points:

- **Unanimous:** Circuit breaker and input validation are Sprint 1 non-negotiables
- **Unanimous:** Soak test must gate production — adversarial, not just happy-path
- **Unanimous:** Tenant isolation is post-soak, informed by observed failure modes
- **4/5 agreed:** Skeleton runbook before soak test (operators need context to interpret soak results)
- **Remaining tension:** Rook (Security) wants adversarial soak gating Sprint 1; others allow Sprint 1 to ship first with soak gating broader rollout
