# Testing — Bedrock Traffic Shaper

This is the index for the shaper's testing: **how the tests are structured, where the
authoritative results live, and how to read a success rate honestly.** Start here, then follow
the pointers below.

To understand or operate the shaper itself (rather than test it), see
[`../solution/architecture.md`](../solution/architecture.md) and
[`../solution/runbook.md`](../solution/runbook.md).

> **Note — the `/baseline/{arm}` comparison arms were removed.** A later re-architecture deleted the
> `/baseline/{retry,jitter}` direct-inference control path (endpoint, `DirectArmFn`,
> `baseline_ingress_fn`, the baseline SQS queue/DLQ). The shaper `/invoke` reproduction steps below
> remain valid. Anything referring to a `/prod/baseline/*` path or "baseline arms" describes how the
> historical comparison was run and is no longer runnable against the current stack; the baseline
> numbers already captured in [`results.md`](results.md) are retained as historical record.

---

## The test layers

There are four layers of tests, from fastest/offline to slowest/live. Only the live layer produces
authoritative numbers.

| Layer | What it is | How to run |
|-------|-----------|------------|
| **Unit (pytest)** | Fast, offline correctness tests under `tests/` — admission expressions, runtime-TPM reconciliation units, and other invariants that have bitten before. | `pytest` |
| **Simulation harness** | Offline algorithm proofs under `scripts/sim/` (fake clock, shared quota/workload vocabulary) that reason about queue-drain and admission behavior with no AWS calls. | `make test-queue-processor-sim`, `make test-budget-manager-sim` |
| **Direct-SDK sanity checks** | Live-stack smoke and small load runs driven from local Python — the dev sanity checks. | `make test`, `make test-budget-manager`, `make test-direct-bedrock`, `make test-multi-model` |
| **Soak** | Sustained-traffic runs (with adversarial injection) against a live stack; any duration via `--duration-hours`. | `make soak-test` |

Authoritative load-test numbers come from load testing against a deployed stack; the results and
methodology are consolidated in [`results.md`](results.md).

## Map of this directory

| Doc | Purpose |
|-----|---------|
| [`results.md`](results.md) | **Authoritative load-test results** — the full config × model × shape matrix, baseline comparisons, cost analysis, and scalability limits. Its **Appendix** folds in the dated primary findings and recovered evidence (hot-partition ceiling, burst over-admission, throttle sweep, extreme-spike deadlock and fix, Nova/Sonnet-5 data points). Read this before quoting any number. |

---

## The one rule and the honesty gate

Two things are worth internalizing before you run anything or read a result.

**One rule: if a number enters the authoritative results doc, it came from a load test against a
deployed stack.** Not a laptop script, not a hand-run `curl`, not a spreadsheet. The
`scripts/test_*.py` helpers are **dev sanity checks** — they are never a results source. Sanity-check
locally with those; **measure** with a load test. *(They also historically served as the executable
spec of the removed `retry` / `jitter` baseline arms.)*

**Honesty gate — score on the reconciled outcome, never the HTTP code.** The shaper's `/invoke`
ingress maps API Gateway directly onto Step Functions `StartExecution`. That integration returns
HTTP `200` as soon as the state machine *starts* — **not** when the request *succeeds*. The real
terminal outcome (queued → drained → Bedrock throttle → DLQ, Lambda-concurrency throttle, or
queue-TTL expiry) commits *after* that `200` and is invisible to the client. A run that was
88.6% end-to-end can report ~100% if you trust the HTTP column. **Score on the reconciled
`RequestOutcome`, correlated by `correlation_id`** — see [`results.md`](results.md) §3 and the
reconciliation notes below.

---

## Reproducing a load test

### 1. Deploy and configure

```bash
make deploy                                  # deploy SemaphoreRateLimiterStack
make create-config MODEL=<model> BURST_CAPACITY=<n>   # set the capacity split for the run
make clean                                   # reset DynamoDB state (preserves CONFIG)
```

### 2. Drive load

Point a load generator at `POST /invoke` (IAM/SigV4-signed) at the offered rate you want to test,
co-located with the shaper (us-east-1) to avoid a cross-region latency confound. Whatever generator
you use, aggregate offered RPS should be sized against your confirmed Bedrock quota so the run
actually exercises the overload path. For local sanity runs, the `make test-*` targets drive the
same `/invoke` contract from a Python worker fleet.

`SHAPER_PATH` selects the ingress path. The current stack exposes only `/prod/invoke` (shaper).
*(Historically the same load plan also drove `/prod/baseline/retry` and `/prod/baseline/jitter` —
one plan, three arms — so the admission algorithm was the only variable between arms. Those baseline
arms were removed in the re-architecture and are no longer runnable.)*

### 3. Verify honestly, not by HTTP 200

```bash
make inspect-consumption                               # capacity consumption
make inspect-queue-single                              # queued items
```

Confirm that `throttled` is split out from `error` (a Bedrock `429` is not a generic `503`), that
DLQ depth is `0`, and that the reconciled `succeeded / N` — **not** the HTTP `200` count — is your
success signal.

---

## Fleet sizing: avoid the Little's-Law artifact

> *Historical — applies to the removed baseline-arm comparison. Retained as sizing methodology.*

When comparing the shaper against synchronous baseline arms, **you cannot size both arms with one
thread count**, or you manufacture a fake shaper win.

- The **baseline arms are synchronous** — a thread is held for the full model latency. A sync
  fleet's throughput is capped at `threads / L_sync` (Little's Law). At a shaped-under-load Opus
  latency of ~30.9s, a 1,000-thread fleet caps at `1000 / 30.9 ≈ 32 rps` — a small fraction of a
  high-rps target.
- The **shaper ingress is asynchronous** — the response returns in ~0.2s and the work drains
  behind a timer, so the fleet has no such cap.

Size each column with the latency for its ingress mode:

- **baseline (sync):** `threads = ceil(RPS × L_sync)`. If this exceeds ~1,000, the arm is
  infeasible for that model — do not run synchronous reactive-retry against it (e.g. Opus, whose
  ~30s single-call latency leaves no room for retries under a 29s ceiling).
- **shaper (async):** `threads ≈ ceil(RPS × submit_latency)`, `submit_latency ≈ 0.2s`, plus
  headroom. Small fleets can drive large RPS.

Aggregate RPS across a load-generator fleet is `taskCount × per-task PEAK_RPS`; size `taskCount`
against your confirmed quota.

---

## Reconciliation notes (getting an honest denominator)

- **`correlation_id` must be globally unique across the whole fleet.** Building it from
  `threadNum + currentTimeMillis()` collides across generator tasks (thread #5 exists in every task,
  and wall-clock ticks together), silently merging distinct requests in the denominator. Salt it
  with a UUID, optionally keeping `threadNum` as a human-readable prefix.
- **A reconciler that drops duplicate and malformed `correlation_id`s is fail-safe:** it should
  report the dropped rate and abort if that rate exceeds ~1% (above that, the id-minting is broken
  and the run is void).
- **Never poll `GET /result/{id}` from the load path.** Polling over the measured ingress inflates
  offered load, can trip the regional WAF rate rule, and inflates `GetItem` cost — corrupting the
  very thing you are measuring. Score after the run from the EMF `RequestOutcome` reconciliation
  (or an opt-in webhook). Production clients still poll by default; polling is banned only from the
  measured load path.

Each run should capture its arm, model, shape, sizing, and ingress mode so it is reproducible and
self-describing.
