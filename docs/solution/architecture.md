<!--
Consolidated "how it works" reference. Grounded in the live handler + CDK source:
  infrastructure/lambda_handlers/{budget_manager,bedrock_processor,queue_processor,
                                  finalizer_fn,outcome_stream_fn,result_fn}.py
  infrastructure/lambda_layer/python/shared_service/{dynamo,bedrock_client}.py
  infrastructure/semaphore_stack.py
Canonical decision record: docs/solution/adr/ (ADR-001..005). Design rationale (the "why"):
docs/solution/design/ (indexed by docs/solution/design/README.md). This doc is the single
"how the shaper works" reference; when it disagrees with an older design note, the code wins.
-->

# Architecture — Bedrock Traffic Shaper

A distributed rate limiter for Amazon Bedrock built entirely on AWS-native services (DynamoDB,
Lambda, Step Functions, EventBridge, API Gateway, S3). It solves the GenAI traffic-spike problem
by **queueing** requests that exceed a model's quota instead of rejecting them — trading latency
for reliability so async workloads see high success rates with no client-side throttle storms.
Every request resolves to exactly one honest, client-readable terminal outcome.

> Diagrams: `architecture/traffic-shaper-architecture.drawio` (editable source),
> `architecture/current-state-architecture.svg`, `architecture/future-state-architecture.svg`.
> The SVGs predate the sliding-window admission gate and the honest-outcomes layer described
> below; regenerate from the `.drawio` before relying on them for the current design.

---

## 1. Components

| Component | Role |
|-----------|------|
| **API Gateway** | `POST /invoke` ingress (`AWS_IAM` / SigV4). Integrates *directly* with Step Functions `StartExecution` via an `AwsIntegration` — no ingress Lambda, so the front door cannot throttle like a Lambda pool. `GET /result/{request_id}` is a separate method. A **regional** WAFv2 Web ACL is associated with the `prod` stage. |
| **Step Functions workflow** | One Standard state machine per request: `ReserveBudget` (`waitForTaskToken`) → `Success`. 65-minute execution timeout (must exceed the 60-minute queue-item TTL). X-Ray tracing enabled. |
| **Budget Manager Lambda** | Admission gate. Reads recent consumption over a sliding window; admits (invokes Bedrock Processor) or enqueues overflow. |
| **Bedrock Processor Lambda** | Backend-aware Bedrock caller for both immediate and queued paths. Reconciles token estimates to actuals, persists the completion to S3, writes the terminal status, then sends the SFN task callback. |
| **Queue Processor Lambda** | Event-driven, single-owner (lock-guarded) background drain. Paces dequeued items with in-memory rate gates and forwards each to Bedrock Processor. |
| **Finalizer Lambda** | EventBridge target on SFN execution-status changes (`FAILED`/`TIMED_OUT`/`ABORTED`). Records an honest terminal outcome for the rare paths where no per-request writer committed one. |
| **Outcome Stream Lambda** | DynamoDB Streams handler. The single authoritative emitter of the `RequestOutcome` metric — projects each committed terminal transition into exactly one EMF data point. |
| **Result Lambda** | `GET /result/{request_id}` poll endpoint: reads the terminal-status item, presigns the S3 output. No `states:*` calls. |
| **DynamoDB single table** | Unified state: model configuration, burst/queue consumption records, queue items, processor locks, and per-request terminal status. Streams enabled (`NEW_AND_OLD_IMAGES`). |
| **S3 output bucket** | Holds inference completion bodies (KMS-encrypted, 2-day lifecycle expiry). The status item stores only an `output_ref`; `/result` presigns it. |
| **DLQ (SQS)** | Captures failed Bedrock invocations for inspection. |

---

## 2. Request lifecycle (Step Functions callback pattern)

The system uses `waitForTaskToken`: a single Step Functions execution stays open for the whole
request and is resolved by **exactly one** callback. Immediate and queued requests converge on the
same Bedrock Processor and the same execution — one unified path, full observability.

```
POST /invoke ──AwsIntegration──► StartExecution (Standard SM)
    │
    └─ ReserveBudget task (waitForTaskToken) → Budget Manager
         ├─ Capacity available → invoke Bedrock Processor async (InvocationType='Event')
         │                       Processor calls Bedrock, persists body to S3, writes
         │                       SUCCEEDED status, then SendTaskSuccess(task_token, result)
         │                       → execution RESUMES with the Bedrock response
         │
         └─ Over-capacity → enqueue {task_token, execution_arn, payload-less metadata} to DynamoDB,
                            write QUEUED (202) status, trigger the drain (EventBridge).
                            The task token is HELD — the execution stays RUNNING (no resume at
                            enqueue); the client sees 202 via GET /result.
                            (later) EventBridge → Queue Processor dequeues (paced) →
                            Bedrock Processor (describe_execution to recover payload) →
                            Bedrock → S3 + terminal status → SendTaskSuccess → execution RESUMES
```

**Path 1 — Immediate (`source: immediate`):** Budget Manager checks the sliding window; if there
is headroom, it invokes Bedrock Processor asynchronously with the `request_payload`. Bedrock
Processor calls Bedrock and sends the single callback that resumes the execution.

**Path 2 — Queued (`source: queued`):** if the window is full, Budget Manager enqueues the
`task_token` + `execution_arn`, writes a `QUEUED` (202) status, and triggers the drain — but it
does **not** resume the execution. The task token stays held and the execution stays RUNNING (this
is why the 65-minute SM timeout exceeds the 60-minute queue TTL); the client observes the `202` by
polling `GET /result`. EventBridge triggers the Queue Processor, which dequeues (rate-paced) and
invokes Bedrock Processor with the `execution_arn`; Bedrock Processor calls `describe_execution` to
recover the original payload, calls Bedrock, and sends the one callback that resumes the execution.

Both paths converge on Bedrock Processor, so every request completes inside its Step Functions
execution carrying `source: immediate|queued` for analytics.

### Callback-pattern implementation notes

- **State machine input:** the task passes `"input.$": "$"` (the entire input) plus
  `sfn.JsonPath.task_token` and `$$.Execution.Id`, so the Lambda can extract a flexible payload
  and hold the token/ARN (`semaphore_stack.py`).
- **Lambda-throttle retry:** `ReserveBudget` retries `Lambda.TooManyRequestsException` /
  `Lambda.SdkClientException` (3× exponential backoff) before catching `States.ALL` → Fail.
- **DynamoDB float rejection:** request payloads with Python `float` values (e.g. `temperature`)
  are handled as `Decimal` on the DynamoDB round-trip and converted back before the Bedrock call.
- **256 KB SFN payload limit:** the completion body is written to S3 and referenced by
  `output_ref` on the status item, so large responses never ride in the execution output or the
  DynamoDB item (see §7).

---

## 3. Admission gate — sliding-window consumption read

The correctness core is a **sliding-window read over the consumption records**, not an atomic
counter. Admission is a *read* of recent consumption; the consumption records are the single
source of truth (`dynamo.py::put_allocation` → `_enforce_window_gate`).

> **This replaced an earlier `TransactWriteItems` atomic-counter gate with write-sharding
> (ADR-004/005).** The counter items were the DynamoDB single-partition hotspot that pinned burst
> throughput far below budget; the window-read gate removed them entirely. The ADRs describe that
> superseded design; the sliding-window model that is actually implemented is described here and in
> §11 below.

### How it works

On every reserve, Budget Manager issues **one strongly-consistent query** of the last
`LONG_WINDOW_SECONDS` (default **15 s**) of the `MODEL#{model_id}#BURST#CONSUMPTION` partition,
then admits iff the request's token estimate, added to recent consumption, fits **both** windows:

| Window | Cap | Purpose |
|--------|-----|---------|
| **2 s** (`SHORT_WINDOW_SECONDS`) | `regen_rate × 2` tokens **and** `rps × 2` requests | rate smoothing — keeps dispatch under Bedrock's sub-minute token bucket |
| **15 s** (`LONG_WINDOW_SECONDS`) | `regen_rate × 15` tokens **and** `rps × 15` requests | accuracy horizon — long enough that reconciled *actuals* dominate the window (Bedrock latency ~7.5 s < 15 s) |

On admit, Budget Manager writes the consumption record with the **estimate** (a single
`put_item`); Bedrock Processor overwrites it with the **actual** token usage ~7.5 s later, well
inside the 15 s window (§5). On any breach it raises `BurstCapacityExceeded` and the caller
enqueues. A single request whose estimate cannot fit even an empty long window is rejected up
front (oversized-request guard).

Before the read, Budget Manager sleeps a fixed 0–40 ms jitter to de-correlate thundering herds —
a weak optimization, **not** the safety net.

### Correctness contract

This is a **shaper, not a hard transactional semaphore.** The strongly-consistent read sees a
Lambda's own recent writes but not concurrent uncommitted admits, so concurrent reservations can
each read the window before the others' writes land — **bounded over-admission** is accepted by
design. The window horizon *is* the correctness horizon: any drift (crash, over-admit) ages out
within `LONG_WINDOW_SECONDS`, so **no reconciliation pass is needed for admission correctness**.
When over-admission does reach Bedrock and throttles, the throttle surfaces as an honest terminal
`429` (§8) rather than being silently retried.

> **Note on "requeue-on-throttle":** several `dynamo.py` / `budget_manager.py` comments cite
> "requeue-on-throttle" as the downstream catch for over-admission. That mechanism (re-enqueue a
> burst-admitted throttle for a paced retry) was **designed but is NOT implemented** — the
> current behavior is throttle → terminal `429` + DLQ for both paths
> (`bedrock_processor.py`). The 15 s window self-healing, not requeue, is today's drift catch.

### Burst disabled (`burst_capacity ≤ 0`)

A config with `burst_capacity ≤ 0` rejects every request so all traffic queues — used to exercise
the queue path in isolation. (This replaced a legacy footgun where `burst_capacity = 0` meant
"admit everything.")

---

## 4. DynamoDB single-table design

One table (`semaphore-single-table`) holds every entity, keyed by generic `pk` / `sk` with
entity-type prefixes and TTL-based garbage collection. Streams are enabled
(`NEW_AND_OLD_IMAGES`) to drive the outcome metric (§8).

```
PK: pk (STRING)   SK: sk (STRING)   TTL: ttl (NUMBER)   Stream: NEW_AND_OLD_IMAGES
```

**Entity types:**

1. **Configuration** — `pk = MODEL#{model_id}`, `sk = CONFIG`. Holds `burst_capacity`,
   `queue_capacity`, regeneration rates, RPM/TPM (and mantle iTPM/oTPM) fields, `queue_batch_size`,
   `backend`, `queue_target_tpm`, window overrides, and TTL/limit knobs. Read by every component.

2. **Consumption records** — separate partitions for burst vs queue so they scale independently:
   - `pk = MODEL#{model_id}#BURST#CONSUMPTION`, `sk = {timestamp_ms}#{request_id}` (**TTL 60 s**)
   - `pk = MODEL#{model_id}#QUEUE#CONSUMPTION`, `sk = {timestamp_ms}#{request_id}` (TTL 5 min)
   These are the source of truth for the sliding-window capacity calculation. Each record carries
   `estimated_tokens` (and, on the mantle backend, `estimated_input_tokens` /
   `estimated_output_tokens`), overwritten with actuals after the call.

3. **Queue items** — `pk = MODEL#{model_id}#QUEUE#ITEMS`, `sk = {timestamp_ms}#{priority}#{request_id}`.
   FIFO, carries `task_token` + `execution_arn` + `tenant_id`/`correlation_id` + the pre-call token
   estimate, `expires_at` (ISO-8601), TTL 1 hour.

4. **Processor locks** — `pk = MODEL#{model_id}#LOCK`, `sk = PROCESSOR#{slot}`. Heartbeat-based,
   `LOCK_TTL = 120 s` for self-healing (see `docs/solution/design/Queue-Processor-Trigger-Improvements.md`).

5. **Request terminal status** — `pk = REQUEST#{request_id}`, `sk = STATUS`. The honest-outcomes
   record (§8): `state ∈ {PENDING,QUEUED,SUCCEEDED,FAILED}`, `reason`, precomputed `http_status`,
   `output_ref` (S3 pointer, never the body), `tenant_id`, `correlation_id`, `source`, `arm`,
   `attempts`, `duration_ms`, TTL 24 h. **Keyed on `request_id` alone** — `tenant_id` is a stored,
   read-time-checked attribute, never part of the key.

6. **Invocation errors** — `pk = MODEL#{model_id}#INVOCATION#ERRORS`, `sk = {timestamp_ms}#{request_id}`,
   TTL 7 days. Debug breadcrumbs written by the queue processor on per-item failures.

There are **no admission-counter items** in the current design (removed with ADR-004/005's counter
gate; see §3 and §11).

---

## 5. Token estimation and actual-usage reconciliation

Admission gates on a **pre-call estimate** because Bedrock deducts quota up front. Budget Manager
estimates input tokens from prompt UTF-8 byte length (`bytes_per_token`, +10% safety margin) and
output tokens from `max_tokens` (× `output_token_burndown_rate` on the runtime combined-TPM path;
1:1 on the mantle split-quota path). The estimate is written onto the consumption record and
carried onto the queue item.

After a successful call, Bedrock Processor **reconciles the record to actuals**
(`reconcile_consumption`): the runtime Converse response returns `usage.inputTokens` /
`usage.outputTokens`, and the mantle/OpenAI paths return `usage.input_tokens` /
`usage.output_tokens`. The over-counted estimate is overwritten with real usage ~7.5 s in, so the
next window read (§3) paces on truth rather than the ceiling. Reconciliation is best-effort — a
miss simply ages out of the 15 s window.

---

## 6. Queue drain — RPM/TPM pacing

The Queue Processor is a **single-owner** loop (guarded by the heartbeat lock) that drains the
`QUEUE#ITEMS` partition and forwards each item to Bedrock Processor. Because one locked loop is the
sole dispatch authority, its in-memory rate view is authoritative and needs no cross-instance
atomic coordination.

- **Bounded dequeue as the "is there work?" signal.** A `Query(Limit=batch_size)` fetches the
  oldest items; an empty result means drained. This replaced a per-loop `Select='COUNT'` scan of
  the whole partition that spiked RCU and self-throttled.
- **In-memory dispatch gates.** A rolling dispatch log drives up to five gates per item: RPM 2 s,
  token 2 s (combined TPM, or split iTPM/oTPM on mantle), RPM 60 s, token 60 s, and — when
  `queue_target_tpm > 0` — an **even-spacing pacer (Gate 5)** that spaces each dispatch
  `item_tokens / (target/60)` seconds after the previous one. Gate 5 is the primary rate control;
  it holds the *actual* Bedrock arrival rate at the target with no batch clumping (the sliding
  windows alone allowed sub-second bursts that throttled). The dispatch log resyncs from DynamoDB
  actuals every 60 s to correct estimate drift.
- **Fire-and-forget dispatch.** Each Bedrock Processor invoke is submitted to a deliberately tiny
  (1-worker) thread pool so the ~40 ms async-invoke handoff overlaps the pacer sleep instead of
  serializing on it — the gates, not network latency, bound throughput. A larger pool re-created
  the sub-second bursts the pacer exists to prevent.
- **Self-handoff.** A single invocation is capped at ~13 min (below the 15-min Lambda ceiling). If
  it exits with the queue still draining at full clip, it emits a `QueueProcessingRequired`
  EventBridge event to wake a successor (after releasing its lock).
- **Circuit breaker.** Three consecutive full-chunk failures trip an exit (emitting a
  `CircuitBreakerTripped` metric), unless disabled via config.

Burst and queue draw from **separate consumption partitions**, so a flood on the immediate path
cannot starve the queue drain.

---

## 7. Self-healing (no scheduled reconciliation)

There is **no scheduled reconciliation Lambda** — the earlier 60 s sweep was removed. Correctness
self-heals three ways:

1. **Admission drift ages out.** The window read only ever sees the last 15 s of consumption, and
   post-call token reconciliation (§5) overwrites estimates with actuals inside that horizon — so
   admission correctness does **not** depend on any sweep (§3).
2. **Orphaned consumption records** are reclaimed by their 60 s DynamoDB TTL.
3. **Expired queue items** are terminalized **inline in the dequeue path** (`batch_dequeue_items`):
   an item past its `expires_at` gets an honest `FAILED` / `queue_expired` / `504` terminal status
   (idempotent — §8) and its row is deleted, so the drain never spends Bedrock quota on a
   logically-expired request. Items whose held execution simply times out are covered by the
   Finalizer (§8).

---

## 8. Honest outcomes — one terminal per request

Every request resolves to exactly one client-readable terminal outcome, recorded durably before
the client is told anything conclusive. This closes the pathology where an
`APIGW → StartExecution` integration returns `200` unconditionally and every real failure (queue
drop, throttle, timeout) commits *after* the 200 and is invisible.

### Terminal-status item and exactly-once transition

The `REQUEST#{request_id}` / `STATUS` item (§4, entity 5) is the record. Every terminal writer uses
one conditional `UpdateItem` (`write_terminal_status`):

```
ConditionExpression: attribute_not_exists(pk) OR #state IN (:pending, :queued)
```

This permits `absent | PENDING | QUEUED → terminal` and blocks `terminal → terminal`. The first
writer wins; every later writer catches `ConditionalCheckFailedException` and returns `False`
(loser swallows). The condition allows a **fresh create** with no prior PENDING, which is how the
`/invoke` path works today (see the note below).

### Who writes what

- **Budget Manager** writes `QUEUED` (202) on enqueue, and `FAILED` / `validation_error` (400) on
  oversized-input / bad-`model_id` rejections.
- **Bedrock Processor** writes the terminal `SUCCEEDED` (200, with `output_ref`) **after**
  persisting the body to S3 and **before** `send_task_success` — the "write-gates-signal" rule. If
  the terminal write (or the S3 put) fails, it does **not** signal success; the task token times
  out → SFN `TimedOut` → the Finalizer records the truth. There is deliberately **no SUCCEEDED
  backstop**. On a Bedrock failure it writes `throttled` (429) or `error` (503) — see §9.
- **Finalizer** (EventBridge on SFN status change) records terminal outcomes for `FAILED` /
  `TIMED_OUT` / `ABORTED`. For `TIMED_OUT` it reads the item: still `QUEUED` → `queue_expired`
  (504), else `timed_out` (504). It never calls `DescribeExecution` (identity is threaded through
  the SM I/O onto the event) — a control-plane call under load would re-introduce the SFN throttle.
- **Dequeue path (inline)** writes `queue_expired` (504) for expired queue items (§7).

### The metric is emitted off the stream, not by the writer

The `UpdateItem` and a metric `print` are two non-atomic side effects — a writer that commits then
dies would lose the metric permanently (and every retry's conditional write then fails). So the
**Outcome Stream Lambda** is the *single* `RequestOutcome` emitter: it projects each committed
`PENDING|QUEUED|absent → terminal` transition off the DynamoDB stream into exactly one EMF point,
dimensioned `[ServiceName, model_id, source, arm, outcome]` with `tenant_id` / `correlation_id` /
`duration_ms` as properties. The conditional write stays the exactly-once *record* gate; the stream
makes the *metric* liveness-independent.

### The client contract — `GET /result/{request_id}`

The Result Lambda is a pure read + S3 presign (no `states:*`; the item never stores an
executionArn, so ARN leakage is structurally impossible). It enforces tenant isolation at read
time (caller SigV4 principal vs stored `tenant_id`) and maps state → HTTP:

| Situation | Code |
|-----------|------|
| absent / `PENDING` / `QUEUED` | **202** + `Retry-After` |
| `SUCCEEDED` | **200** + presigned output URL |
| `FAILED` `reason=throttled` (or `ingress_throttled`) | **429** |
| `FAILED` `reason=error` | **503** |
| `FAILED` `reason=timed_out` / `queue_expired` | **504** |
| `FAILED` `reason=validation_error` | **400** |

> **Implementation divergence to review:** the design specified an *SM-first-state PENDING
> PutItem* on `/invoke`. That is **not** in the current state machine (`ReserveBudget → Success`
> only). On `/invoke`, PENDING is never written up front — the status item is created lazily by the
> first terminal (or QUEUED) writer, and `/result` treats an absent item as `202/PENDING`.
> The exactly-once semantics still hold (the conditional permits a fresh create), but a request
> that never reaches any writer has no record — the offline `correlation_id` reconciliation
> (`ingress_lost` bucket) is what catches that case.

---

## 9. Multi-backend invocation and 429 vs 503 classification

`bedrock_client.client_for(config)` dispatches on `(backend, api_style)` and fails closed on any
unknown pair. Three wire shapes are implemented behind one uniform `BedrockResponse`:

| Backend / style | Path | Quota shape |
|-----------------|------|-------------|
| `runtime` / `converse` (default) | `bedrock-runtime.Converse` | combined TPM |
| `mantle` / `messages` | SigV4 POST to the Mantle endpoint (Anthropic Messages) | split iTPM / oTPM |
| `mantle` / `responses` | SigV4 POST to the Mantle endpoint (OpenAI Responses) | split iTPM / oTPM |

boto3 client-side retries are **disabled** (`total_max_attempts=1`) so a Bedrock throttle surfaces
on the first attempt into the shaper's accounting instead of being silently absorbed (boto3's
default retry mode hid ~96% of throttles in an earlier run). Next-generation Claude models reject
non-default sampling params, so `temperature`/`top_p`/`top_k` are stripped by model-ID substring.

### How 429 (account quota) vs 503 (serving capacity) are classified

The upstream distinction is: **429 = account rate/TPM quota exceeded; 503 = model serving capacity
exceeded ("rate exceeds available capacity").** How the shaper maps that to its own terminal
`reason` differs by backend, and this is load-bearing:

- **Runtime Converse path:** only `ThrottlingException` / `TooManyRequestsException` /
  `ServiceQuotaExceededException` are classified `throttled` (→ terminal `429`). Every other error
  (including any HTTP 5xx / serving-capacity condition) is `not throttled` (→ terminal `503`).
- **Mantle paths (Messages and Responses):** `is_throttled = status in (429, 503)` — **both** an
  upstream `429` (account quota) **and** an upstream `503` (serving capacity) are folded into
  `throttled`, so on the mantle backend a serving-capacity `503` currently surfaces to the client
  as a terminal **`429`**, not `503`. This is the verified root cause of the "Sonnet 5 503-vs-429"
  finding: the two conditions are distinct upstream but not distinguished at the shaper's client
  boundary on the mantle path. (The runtime path does distinguish them.)

Bedrock Processor turns the client result into the terminal `reason`/`http_status`: `throttled → 429`,
any other failure `→ error / 503`; validation failures `→ 400`; SFN `TimedOut`/`queue_expired → 504`
(`bedrock_processor.py`, `finalizer_fn.py`, `result_fn.py`). `throttled` is reserved strictly for
real Bedrock throttles — a `Lambda.TooManyRequestsException` catch→Fail maps to `error`/`503`, never
`throttled`.

---

## 10. Observability

Metrics are emitted as CloudWatch EMF to stdout, namespace `BedrockShaper`, service `TrafficShaper`:

- `RequestOutcome` (count) — the reconciled per-request terminal truth, emitted once off the stream
  (§8), dimensioned by `model_id`, `source`, `arm`, `outcome`.
- `RequestsProcessed` / `BedrockThrottles` (Bedrock Processor) — operational counters dimensioned
  by admission `source` (immediate vs queued), answering "which path throttles."
- `InputTokens` / `OutputTokens` — actual usage per call.
- `BedrockLatency`, `RequestQueued`, `ProcessingRate`, `RequeuedOnThrottle`,
  `CircuitBreakerTripped`, `OrphanedRecordsSwept`.

Correlation IDs thread through all Lambdas; X-Ray tracing is enabled on the state machine.

---

## 11. Decision record & design rationale

- **Canonical decisions:** `docs/solution/adr/` — ADR-001 (phased approach) → ADR-002 (MVP) →
  ADR-003 (production-ready leaky-bucket) → ADR-004 (counter write-sharding) → ADR-005
  (consumption-read elimination).
- **Superseded design:** ADR-004/005 describe the `TransactWriteItems` atomic-counter admission
  gate with RPM write-sharding. That design was replaced by the **sliding-window consumption read**
  (§3), now implemented in `dynamo.py::put_allocation`. Read the ADRs for the historical reasoning;
  read this doc for current behavior.
- **Design rationale notes:** `docs/solution/design/` (indexed in
  `docs/solution/design/README.md`) — leaky-bucket optimization, queue-processor triggers, and
  hot-partition fix validation.
- **Operating the shaper:** `docs/solution/runbook.md`. **Cost:** `docs/solution/cost-model.md`.
  **Roadmap:** `docs/solution/production-hardening.md`. **Load testing:** `docs/testing/`.
