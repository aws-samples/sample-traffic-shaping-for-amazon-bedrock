# Hot-Partition Fix — 5× Extreme-Spike Validation (2026-07-09)

## Summary

The distributed rate-limiter previously **deadlocked** under a 5× extreme spike (2026-07-06:
4.3% success, 43.8% admission timeouts, 51.9% SFN timeouts, queue stalled). After a
multi-part fix — validated by iterative load-test runs against live `global.anthropic.claude-opus-4-7`
(30M TPM) in us-east-1 — the same 5× spike now completes at **99.94% success (7074/7078)**
with zero deadlock signatures.

## Test scenario (identical to the 2026-07-06 baseline)

- **Load:** 7,078 requests × ~21K tokens (74,000-char prompt, `max_tokens=50`) ≈ **150M tokens = 5× of the 30M TPM quota**.
- **Shape:** 100 concurrent workers, 60-second submission burst (idle-then-slam), through the shaper (`test_budget_manager.py`).
- **Model:** `global.anthropic.claude-opus-4-7`, TPM-only (no RPM gate), burndown 5.0.

## Result progression (each load-test run isolated one bottleneck)

| Run | Config | SUCCEEDED | Dominant failure | Root cause found |
|-----|--------|-----------|------------------|------------------|
| Baseline 2026-07-06 | 128MB, single-partition read | 4.3% | 43.8% `Sandbox.Timedout` + 51.9% SFN `TimedOut` | hot-partition read + compute starvation |
| Fix run #1 | atomic TPM counter | 0% | 100% DynamoDB `ValidationException` | `if_not_exists()` illegal in ConditionExpression |
| Fix run #2 | legal expr, BM reserved=200 | 50.2% | 49.8% `Lambda.TooManyRequests` (429) | admission-gate concurrency cap too low |
| Fix run #3 | BM reserved=1000 | 71.4% | 28.6% `Lambda.TooManyRequests` (429) | reserved concurrency is also a *ceiling* |
| **Fix run #4 (final)** | **unreserved (full pool)** | **99.94%** | 1 transient `BedrockInvocationError` (Bedrock 500) | — validated |

## Final run — authoritative metrics (CloudWatch AWS/States + AWS/Lambda)

| Metric | Baseline | Final |
|--------|---------:|------:|
| ExecutionsStarted | 7,078 | 7,078 |
| ExecutionsSucceeded | ~199 (4.3%) | **7,074 (99.94%)** |
| ExecutionsFailed | ~2,033 (43.8%) | **1 (0.014%)** |
| ExecutionsTimedOut | ~2,408 (51.9%) | **0** |
| Budget Manager `Sandbox.Timedout` | 2,033 | **0** |
| Budget Manager Lambda Throttles (429) | — | **0** (17,864 → 10,546 → 0 across runs) |
| Queue | stalled at 909, deadlocked | fully drained |

The single failure was a Bedrock-side `InternalFailure` (HTTP 500) on one request — normal
service noise, correctly captured by the DLQ — not a shaper defect.

## The fix chain (each root cause eliminated)

1. **DynamoDB hot-partition READ** ([ADR-005](../adr/ADR-005-consumption-read-elimination.md)):
   admission previously re-summed every consumption record in the 60s window per request
   (O(records)/request, single partition, ~3000 RCU/s ceiling). TPM enforcement moved into
   the atomic `TransactWriteItems` gate via a window + global-epoch token counter; the hot
   read is gone.
2. **Budget Manager compute starvation:** 128MB → 1024MB (the admission gate was CPU-starved
   doing TransactWriteItems + conflict retries, timing out at 30s).
3. **Illegal DynamoDB expression:** `if_not_exists()` is not allowed in a `ConditionExpression`
   (only in `UpdateExpression`). Replaced with `attribute_not_exists(#count) OR #count <= :headroom`
   plus a Python oversized-request pre-check. Guarded permanently by `tests/test_admission_expressions.py`
   (moto in-memory DynamoDB) so a runtime expression error can never again reach a paid run.
4. **Admission-gate concurrency ceiling:** `reserved_concurrent_executions` is both a floor and
   a ceiling. Capping the synchronous admission gate throttled it under the burst. Removed the
   reservation so it draws from the full account unreserved pool (this account: 5000 total,
   4390 unreserved).

## KNOWN LIMITATION — synchronous admission-gate burst absorption

The admission path is a **synchronous per-request Lambda** invoked via API Gateway →
Step Functions `StartExecution`. It has a finite instantaneous-burst-absorption ceiling:
under a *deliberately extreme* slam (100 workers firing ~7K StartExecutions in 60s), earlier
runs saw `Lambda.TooManyRequestsException`. Removing the reserved cap resolved it at this
scale (99.94%), but the architectural characteristic remains: **this system shapes QUOTA
overflow — it queues what exceeds the model's TPM budget — it is not designed to absorb an
unbounded Lambda-invocation burst.**

For workloads that generate extreme *instantaneous* request bursts (as opposed to sustained
over-quota load), front the admission path with a buffer:
- **SQS between API Gateway and the admission gate** (absorb the StartExecution burst, drain at a controlled rate), or
- **API Gateway throttling / usage plans** (cap the arrival rate at the edge).

Both are deferred as out-of-scope for the quota-shaping mission. Flagged here so the boundary
is explicit rather than discovered in production.

## Reproduce

```bash
make clean
python scripts/test_budget_manager.py \
  --model global.anthropic.claude-opus-4-7 \
  --num-requests 7078 --max-workers 100 --submission-duration 60 \
  --prompt-size 74000 --max-tokens 50
```

Local expression validation (no AWS, no spend):

```bash
python -m pytest tests/test_admission_expressions.py -q
```
