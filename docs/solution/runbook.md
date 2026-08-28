# Bedrock Traffic Shaper — Operator Runbook

**Stack:** SemaphoreRateLimiterStack
**Dashboard:** [BedrockTrafficShaper](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=BedrockTrafficShaper)
**Region:** us-east-1

---

## Quick Debug Sequence

**Always start here.** Query DynamoDB tables before checking logs — table state tells you what happened, logs tell you why.

```bash
make inspect-queue-single    # Queued items waiting to be processed
make inspect-consumption     # Burst capacity consumption records
make inspect-config          # Model configuration (burst_capacity, RPM, etc.)
make logs-errors             # Filter for error messages across all Lambdas
```

All `inspect-*` commands accept `MODEL=<alias>` (default `opus`), e.g. `make inspect-queue-single MODEL=nova-2-lite`.

---

## Inspection & Log Commands

The CloudWatch dashboard URL is in `config.env` as `DASHBOARD_URL`.

**Inspect table state:**

| Command | Shows |
|---------|-------|
| `make inspect-config` | Model `CONFIG` record (quota, capacity split, windows) |
| `make inspect-queue-single` | Queued items waiting to be processed (and any LOCK records) |
| `make inspect-consumption` | Burst-capacity consumption records |
| `make inspect-queue-capacity` | Queue-capacity consumption records |
| `make inspect-tpm-consumption` | Token (TPM) consumption in the consumption records |
| `make inspect-dlq` | Up to 10 DLQ messages, non-destructive (0 visibility timeout) |

**Read logs:**

| Command | Shows |
|---------|-------|
| `make logs-recent` | Last 10 min across Budget Manager + Queue Processor |
| `make logs-budget-recent` | Budget Manager, last 10 min |
| `make logs-queue-recent` | Queue Processor, last 10 min |
| `make logs-bedrock-recent` | Bedrock Processor, last 10 min |
| `make logs-errors` | Errors/exceptions/failures across the Lambdas, last 30 min |

**Stream logs live (follow mode):** `make tail-budget`, `make tail-queue`, `make tail-bedrock`.

> The `logs-*` and `tail-*` commands read `config.env` for log group names, so they only work after a successful `make setup`.

---

## Alarm Response Procedures

### DLQ Alarm: `BedrockShaper-DLQ-NotEmpty`

**Severity:** P1 — a request failed permanently and was sent to the dead-letter queue.

**Symptoms:** DLQ Depth widget shows messages > 0.

Terminal failures — both `throttled`/429 and `error`/503 — are captured to the DLQ.

**Actions:**
1. Check DLQ contents (non-destructive, 0 visibility timeout): `make inspect-dlq` (or `aws sqs receive-message --queue-url $DLQ_URL --max-number-of-messages 10`)
2. Read the message body — it contains `request_id`, `model_id`, `error_type`, `error_message`, `correlation_id`
3. Correlate with Bedrock Processor logs: `make logs-errors` and search for the `request_id`
4. Common causes:
   - `ThrottlingException` — Bedrock rate limit hit despite RPM pacing. Check if `queue_regen_rate` in config matches actual model RPM
   - `ValidationException` — malformed payload. Check the original request content
   - `ModelTimeoutException` — Bedrock took too long. Check model health in AWS console
5. After investigation, purge processed messages: `make drain-dlq` (purges **all** messages — only run once you've captured what you need) (or `aws sqs purge-queue --queue-url $DLQ_URL`)

### Queue Depth Alarm: `BedrockShaper-QueueDepthHigh`

**Severity:** P2 — requests are accumulating faster than they're being drained.

**Symptoms:** Queue Depth widget growing steadily. Processing Rate widget flat or zero.

**Actions:**
1. Check if Queue Processor is running: `make inspect-queue-single` — look for LOCK records
2. Check Queue Processor logs: `make tail-queue`
3. Common causes:
   - **Lock stuck:** Processor crashed mid-execution. Run `make clean` to reset locks (preserves CONFIG). (There is no scheduled sweep — the lock is heartbeat-guarded and a crashed owner's lock is superseded once its heartbeat lapses.)
   - **Bedrock throttling:** Processing Rate is low because Bedrock is rejecting calls. Check Bedrock Processor logs for ThrottlingException
   - **EventBridge not triggering:** Verify the schedule rule is enabled in EventBridge console
   - **Capacity exhausted:** `make inspect-consumption` — if QUEUE consumption records are maxed, the queue is full. Wait for TTL expiry or increase `queue_capacity` in config
4. If stuck, trigger processor manually: `make create-config MODEL=<model>` (recreates config which triggers EventBridge)

### Lambda Error Rate Alarm: `BedrockShaper-LambdaErrorRate`

**Severity:** P2 — one or more Lambda functions are failing.

**Symptoms:** Lambda Errors widget shows non-zero for specific function(s).

**Actions:**
1. Identify which Lambda: check the Lambda Errors widget — each function is a separate line
2. Check logs for that function:
   - Budget Manager: `make tail-budget`
   - Queue Processor: `make tail-queue`
   - Bedrock Processor: check CloudWatch logs directly (no make shortcut)
3. Common causes by function:
   - **Budget Manager:** DynamoDB TransactWriteItems failure (capacity, condition check). Check `make inspect-consumption` for counter state
   - **Queue Processor:** Lock contention, Bedrock Processor invoke failure. Check for `TooManyRequestsException` in logs
   - **Bedrock Processor:** Model errors, payload issues, SFN callback failures. Check DLQ for details
4. If errors are transient (< 1%), monitor for self-resolution. If persistent, check AWS Health Dashboard for service issues

### Circuit Breaker Alarm: `BedrockShaper-CircuitBreakerTripped`

**Severity:** P1 — Queue Processor stopped processing because Bedrock is consistently failing.

**Symptoms:** CircuitBreakerTripped in alarm status. Processing Rate drops to 0. Queue Depth growing.

**Actions:**
1. Check Bedrock service health: AWS Health Dashboard + Bedrock console for the model
2. Check Bedrock Processor logs for error pattern: `make logs-errors`
3. Common causes:
   - **Bedrock outage:** Model unavailable. Wait for AWS to resolve, then queue will auto-drain on next processor invocation
   - **IAM permission revoked:** Bedrock Processor can't call `bedrock-runtime:InvokeModel`
   - **Model deprecated:** Model ID no longer valid. Update model config
4. After root cause resolved, queue processing resumes automatically on next EventBridge trigger
5. To force immediate resume: `make create-config MODEL=<alias>` (triggers EventBridge)
6. To temporarily disable circuit breaker during investigation:
   ```bash
   aws dynamodb update-item --table-name semaphore-single-table \
     --key '{"pk":{"S":"MODEL#<model_id>#CONFIG"},"sk":{"S":"CONFIG"}}' \
     --update-expression "SET circuit_breaker_disabled = :v" \
     --expression-attribute-values '{":v":{"BOOL":true}}'
   ```
   **Remember to re-enable after investigation** (set to `false`).

### SFN Failures Alarm: `BedrockShaper-SfnFailures`

**Severity:** P2 — Step Functions executions are failing (not timing out — failing).

**Symptoms:** SFN Failed widget shows non-zero.

**Actions:**
1. Open Step Functions console: check recent failed executions
2. Look at execution input/output — the error will be in the task state
3. Common causes:
   - **Budget Manager Lambda error:** The Lambda itself crashed (OOM, timeout, unhandled exception)
   - **Task token expired:** Token was held too long before callback. Check if Budget Manager is slow
   - **IAM permission issue:** Lambda can't call `SendTaskSuccess`/`SendTaskFailure`
4. Cross-reference with Lambda Errors widget to see if the root cause is a Lambda failure

---

## Normal Baselines

| Metric | Normal Range | Investigate When |
|--------|-------------|-----------------|
| Queue Depth | 0-10 during steady traffic | > 50 sustained |
| Burst Utilization | 0-80% | Pegged at 100% for > 2 minutes |
| Processing Rate | Matches incoming request rate | Drops to 0 while queue has items |
| Lambda Errors | 0 | Any non-zero sustained > 1 minute |
| SFN Failed | 0 | Any non-zero |
| SFN TimedOut | 0 | > 0 indicates queue backlog or SFN timeout too short |
| DLQ Depth | 0 | Any non-zero |
| Bedrock Latency P50 | 500-2000ms (model-dependent) | > 5000ms |
| Circuit Breaker | Not tripped | Any trip event |

---

## Querying Throttle & Admission-Source Metrics

The Bedrock Processor emits per-request EMF counters dimensioned by **admission
source** — `immediate` (burst gate) vs `queued` (queue drain) — so you can answer
"how many requests went through immediately vs queued?" and "which path produced the
throttles?" directly from CloudWatch, without walking log streams. See the dashboard's
**Requests by Source** and **Throttles by Source** widgets, or query the CLI directly.

**Metrics** (namespace `BedrockShaper`, dimensions `ServiceName=TrafficShaper`, `model_id`, `source`):

| Metric | Meaning |
|--------|---------|
| `RequestsProcessed` | count=1 per request the processor handled (both success + throttle) |
| `BedrockThrottles` | count=1 only when Bedrock throttled that request |

`source` is exactly one of `immediate` or `queued`. Throttle-rate-by-source =
`BedrockThrottles / RequestsProcessed` for that source.

```bash
# Requests + throttles by source for a model over a run window (adjust --start-time/--end-time).
# Repeat with source=queued. Use a wide --period to get a single window total.
MODEL=us.amazon.nova-2-lite-v1:0
for METRIC in RequestsProcessed BedrockThrottles; do
  for SRC in immediate queued; do
    echo -n "$METRIC source=$SRC: "
    aws cloudwatch get-metric-statistics --namespace BedrockShaper --metric-name $METRIC \
      --dimensions Name=ServiceName,Value=TrafficShaper Name=model_id,Value=$MODEL Name=source,Value=$SRC \
      --start-time 2026-07-15T19:48:00Z --end-time 2026-07-15T20:05:00Z \
      --period 3600 --statistics Sum --region us-east-1 \
      --query 'Datapoints[].Sum' --output text
  done
done
```

**Cross-check for trust:** the sum of `BedrockThrottles` across both sources should equal
the independent `AWS/Bedrock InvocationThrottles` metric for the same model + window
(verified exact match, 2026-07-15). If they diverge, suspect a window boundary or a
missing `source` value — not a silent metric.

```bash
aws cloudwatch get-metric-statistics --namespace AWS/Bedrock --metric-name InvocationThrottles \
  --dimensions Name=ModelId,Value=$MODEL \
  --start-time 2026-07-15T19:48:00Z --end-time 2026-07-15T20:05:00Z \
  --period 3600 --statistics Sum --region us-east-1 --query 'Datapoints[].Sum' --output text
```

> **Note:** these metrics reflect only the shaper's own processor path. Direct-Bedrock
> comparison-arm traffic never touches the processor, so it does not appear here — use
> `AWS/Bedrock InvocationThrottles` for that. The `source` dimension only exists for runs
> **after** the metric was added (deployed 2026-07-15).

### Queue drain diagnosis (did it exceed one 13-min window? did the self-reschedule fire?)

The queue processor is capped at `MAX_RUNTIME` (13 min) per invocation. When it exits with a
full-batch backlog it emits a `QueueProcessingRequired` EventBridge event to wake a successor
(logged as `Backlog remains — rescheduling drain` + `Re-triggered queue processor`). To confirm
a drain completed and whether it needed successor invocations:

```bash
source config.env
aws logs filter-log-events --log-group-name "$QUEUE_PROCESSOR_LOG_GROUP" --region us-east-1 \
  --start-time $(python3 -c "import datetime;print(int(datetime.datetime(2026,7,15,19,48,0,tzinfo=datetime.timezone.utc).timestamp()*1000))") \
  --filter-pattern '?"Acquired processor lock" ?"Queue processing complete" ?"rescheduling drain" ?"Re-triggered queue"' \
  --query 'events[*].message' --output text
```

- **Multiple `Acquired processor lock` + `rescheduling drain` emits** = the drain spanned more
  than one invocation and the EventBridge self-reschedule (successor hand-off) worked.
- **Total drain wall time** = first `Acquired` → last `Queue processing complete`. If it exceeds
  13 min, at least one successor was required — confirm a matching `rescheduling drain` emit exists,
  else the backlog would have stranded (the pre-fix failure mode: 4,979 `ExecutionsTimedOut`).
- **`ExecutionTimedOut` (SFN) should be 0.** Any non-zero at exactly 65 min = a stranded backlog the
  drain never reached.

---

## Observed Failure Modes (from Soak Testing)

### Bedrock Transient Failure Burst
**Observed:** Phase 8 soak test, ~17-18 minutes into 1-hour run at 70 RPM.
**Pattern:** 42 `BedrockInvocationError` failures in ~1 minute, then zero failures for remaining 42 minutes.
**Signature:** DLQ depth jumps suddenly, then stabilizes. Queue depth stays low. RPM pacing continues normally.
**Impact:** 1.1% failure rate over the full hour. All failures correctly captured in DLQ.
**Action:** No intervention needed — system self-heals. Check DLQ messages for the error pattern. If the transient event lasts > 3 minutes, expect circuit breaker to trip (3 consecutive full-batch failures). See circuit breaker alarm procedure above.

### Rate Limiter Catch-Up Burst (Test Script Only)
**Observed:** Phase 8 soak test, at ~4.3m and ~59.3m.
**Pattern:** Test script's timer-based rate limiter falls behind during checkpoint calculation, then catches up by sending a burst of ~200 requests in < 1 minute.
**Signature:** Sudden spike in incoming requests, queue depth spikes, then drains normally.
**Impact:** System handled gracefully — all requests admitted and processed. Not a system issue; this is a test harness artifact. Fixed in `soak_test.py` (schedule reset when > 2 intervals behind).

---

## Latency SLA (Expected Response Times)

Requests take one of two paths through the system. Callers should set timeouts based on the queue path, not the burst path.

| Path | Description | p50 | p95 | p99 |
|------|-------------|-----|-----|-----|
| **Burst (immediate)** | Request gets a burst slot, sent directly to Bedrock | 1-3s | 5-8s | 10-15s |
| **Queue (deferred)** | Burst full, request queued, processed at RPM pace | 20-60s | 90-120s | 150-180s |

**Queue path latency depends on:**
- Queue depth at time of admission (more items ahead = longer wait)
- `queue_regen_rate` (RPM pacing — higher RPM = faster drain)
- `queue_batch_size` (items per batch, default 10)
- Bedrock model latency (Opus ~3-5s, Jamba ~1-2s, Nova Lite ~0.5-1s)

**Caller guidance:**
- Set client timeout >= SFN timeout (30 minutes) to avoid premature abandonment
- Burst path callers: expect sub-10s responses most of the time
- Queue path callers: treat as async — response arrives via SFN callback
- If p95 queue latency is unacceptable, increase `burst_capacity` to reduce queuing

---

## DLQ Consumer Guidance

### Message Format

Each DLQ message body contains:

```json
{
  "request_id": "load_test_42",
  "model_id": "us.amazon.nova-2-lite-v1:0",
  "prompt": "...",
  "max_tokens": 100,
  "error_type": "BedrockInvocationError",
  "error_message": "Bedrock returned error: ThrottlingException",
  "correlation_id": "abc-123-def",
  "timestamp": "2026-03-15T19:06:56.804035"
}
```

### Error Classification

| Error Type | Retryable? | Action |
|------------|-----------|--------|
| `ThrottlingException` | Yes | Re-submit after 30s backoff. Bedrock was temporarily overloaded. |
| `BedrockInvocationError` | Maybe | Check `error_message` — transient service errors are retryable, model errors are not. |
| `ModelTimeoutException` | Yes | Bedrock took too long. Re-submit with same payload. |
| `ValidationException` | No | Payload is malformed. Fix the request before retrying. |
| `AccessDeniedException` | No | IAM permission issue. Check Bedrock Processor role. |
| `InputValidationError` | No | Request exceeded `max_tokens_per_request` or prompt size limit. Reduce payload. |

### Recommended Consumer Pattern

1. **Lambda DLQ consumer** triggered by SQS event source mapping
2. Parse message body, classify error type
3. For retryable errors: re-submit to Step Functions state machine (with backoff)
4. For permanent errors: alert owner + log to observability platform
5. Delete message from DLQ after processing

**Important:** The SQS visibility timeout should be longer than your consumer Lambda timeout to prevent double-processing.

---

## Success Metrics Definition

### System Reliability vs End-to-End Success

The Traffic Shaper has two distinct success metrics:

| Metric | Definition | Target | What Counts Against It |
|--------|-----------|--------|----------------------|
| **System reliability** | Requests correctly admitted, routed, and processed by the Traffic Shaper infrastructure | >= 99.99% | Dropped requests, orphaned state, admission gate errors, queue processing failures, SFN infrastructure failures |
| **End-to-end success** | Requests that return a successful Bedrock response to the caller | Varies by model | Everything above + Bedrock throttling, model errors, service outages |

**Key principle:** We gate production readiness on *system reliability* (what we control), not *end-to-end success* (which is bounded by Bedrock's availability).

A Bedrock transient causing 45 DLQ messages in a 4,200-request soak test is **correct system behavior** — the Traffic Shaper's job is to capture, trace, and surface those failures, not prevent them.

**Observed baselines:**
- System reliability: 100% across all tests (0 dropped requests, 0 orphaned state)
- End-to-end success: 98.9% in 1-hour soak (45 Bedrock transients out of 4,200 requests)

---

## Key Configuration

| Parameter | Location | Impact |
|-----------|----------|--------|
| `burst_capacity` | DDB CONFIG record | Max burst admits per window |
| `max_burst_multiplier` | DDB CONFIG record | Global cap = burst_capacity * multiplier |
| `queue_capacity` | DDB CONFIG record | Max queue depth per window |
| `queue_regen_rate` | DDB CONFIG record | RPM pacing (should match model RPM) |
| `max_tokens_per_request` | DDB CONFIG record | Max output tokens per request (default 4096) |
| `circuit_breaker_disabled` | DDB CONFIG record | Set `true` to bypass 3-failure circuit breaker |
| Reserved concurrency | CDK stack (200) | Max concurrent Lambda invocations |
| SFN timeout | CDK stack (30 min) | Max time for request lifecycle |

**To update model config:** `make create-config MODEL=<alias> BURST_CAPACITY=<N>`

**To reset system state:** `make clean` (preserves CONFIG, clears consumption/queue/locks)
