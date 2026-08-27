# The Invoke API

Clients submit work to the shaper by POSTing to the API Gateway `/invoke`
endpoint. Submission is asynchronous: `/invoke` starts a Step Functions execution
and returns immediately, and the caller retrieves the final outcome by polling
`GET /result/{request_id}`.

This page documents the request/response contract, authentication, and the
outcome semantics (200 / 429 / 503 and friends). It is grounded in
[`semaphore_stack.py`](../../infrastructure/semaphore_stack.py),
[`bedrock_processor.py`](../../infrastructure/lambda_handlers/bedrock_processor.py),
and [`result_fn.py`](../../infrastructure/lambda_handlers/result_fn.py).

---

## Endpoints

| Method + path | Auth | Purpose |
|---------------|------|---------|
| `POST /invoke` | `AWS_IAM` (SigV4) | Submit a request. Integrated directly with Step Functions `StartExecution` — no Lambda proxy. Returns immediately (async). |
| `GET /result/{request_id}` | `AWS_IAM` (SigV4) | Poll for the outcome of a submitted request. Returns 202 while pending, then a terminal code. |

The API has a single front door:

- **API Gateway URL** (`API_GATEWAY_URL` in `config.env`) — SigV4 with the
  `execute-api` service. A **regional** WAFv2 Web ACL is associated with the API
  Gateway `prod` stage and applies per-IP rate limits at the edge of the API.

---

## Authentication (SigV4 / `AWS_IAM`)

Both `/invoke` and `/result` use IAM authorization. Every request must be SigV4
signed by an IAM principal (`execute-api:Invoke` on the API). There is no API key
and no Cognito pool — the SigV4 principal is the identity.

Sign requests with any SigV4-capable client, for example
[`awscurl`](https://github.com/okigan/awscurl):

```bash
source config.env

awscurl --service execute-api --region "$AWS_REGION" \
  -X POST "${API_GATEWAY_URL}invoke" \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "demo-req-1",
    "model_id": "us.amazon.nova-2-lite-v1:0",
    "prompt": "Summarize the theory of relativity in one sentence."
  }'
```

Because `/invoke` is a direct `StartExecution` integration, you can also submit
the identical payload straight to the state machine (this is what `make test`
does under the hood):

```bash
source config.env

aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --input '{"request_id":"demo-req-1","model_id":"us.amazon.nova-2-lite-v1:0","prompt":"Hello"}'
```

---

## Request body

`POST /invoke` takes a JSON body that becomes the Step Functions execution input.

| Field | Required | Meaning |
|-------|----------|---------|
| `request_id` | yes | Caller-supplied unique ID. It is the idempotency key and the key of the terminal-status item you later read at `/result/{request_id}`. |
| `model_id` | yes | **Full** Bedrock model ID (not a `create-config` alias), e.g. `us.amazon.nova-2-lite-v1:0`. Must have a matching `CONFIG` record. |
| `prompt` | yes* | The user prompt text. |
| `max_tokens` | no | Max output tokens for the call (defaults apply if omitted). |
| `temperature` | no | Sampling temperature. Only forwarded when the model accepts sampling params; next-generation Claude models reject non-default sampling values, so it is stripped for them. |
| `request_payload` | no | Alternative: supply `{prompt, max_tokens, temperature}` as a nested object instead of loose top-level fields. |

\* Either a top-level `prompt` or a `request_payload.prompt` must resolve to a
non-empty prompt. A request with no valid prompt is failed as a validation error
rather than having a prompt fabricated for it.

> **Input size limit.** The payload rides inside the Step Functions execution
> state, which has a 256 KB limit — this bounds per-request input size. Payload
> decoupling (SQS/S3) is on the roadmap.

---

## Submission response

`POST /invoke` returns synchronously with the result of `StartExecution`. It does
**not** contain the Bedrock response — the request is still being admitted and
processed. Submission-time status codes:

| Status | Meaning |
|--------|---------|
| `200` | Execution started. Body carries the `StartExecution` response (`executionArn`, `startDate`). |
| `429` | `ingress_throttled` — the `StartExecution` account-level request rate was exceeded at the front door. Retry with backoff. |
| `400` | Malformed submission (a 4xx from `StartExecution`). |
| `500` | A 5xx from `StartExecution`. |

Once you have a `200`, poll `/result/{request_id}` for the actual outcome.

---

## Polling for the outcome: `GET /result/{request_id}`

`ResultFn` reads the terminal-status item and, on success, returns a presigned S3
URL to the response body. It never returns a Step Functions ARN. Tenant isolation
is enforced at read time: a caller whose SigV4 principal differs from the item's
owning `tenant_id` gets `403` (items submitted through the direct `/invoke`
integration carry `tenant_id = unknown` and are gated by IAM auth alone).

```bash
source config.env

awscurl --service execute-api --region "$AWS_REGION" \
  -X GET "${API_GATEWAY_URL}result/demo-req-1"
```

### Outcome status codes

| Status | `status` / `reason` | Meaning |
|--------|---------------------|---------|
| `202` | `PENDING` / `QUEUED` (or item absent yet) | Not terminal. Re-poll after the `Retry-After` interval. |
| `200` | `SUCCEEDED` | Body ready. Response includes `output_url` — a presigned GET URL for the inference response body. |
| `429` | `FAILED` / `throttled` | Bedrock throttled the request — its rate/TPM quota was exceeded. Safe to resubmit after backoff. |
| `503` | `FAILED` / `error` | A non-throttle Bedrock invocation error or an unexpected processor error. |
| `504` | `FAILED` / `timed_out` or `queue_expired` | The request did not complete before its deadline / the queued item's TTL expired. |
| `400` | `FAILED` / `validation_error` | The request was malformed (missing model, missing/invalid payload). |

Example success body:

```json
{
  "status": "SUCCEEDED",
  "request_id": "demo-req-1",
  "output_url": "https://<bucket>.s3.amazonaws.com/outputs/demo-req-1.json?X-Amz-..."
}
```

Fetch `output_url` with a plain HTTP GET (it is presigned and expires in ~1 hour)
to retrieve the full Bedrock response body.

---

## Outcome semantics: 429 vs 503

The distinction is deliberate and honest — the shaper reports what actually
happened at Bedrock rather than papering over it:

- **`429` (throttled).** The Bedrock call raised `ThrottlingException`,
  `TooManyRequestsException`, or `ServiceQuotaExceededException` — i.e. the
  account's Bedrock rate/TPM quota was exceeded for that model despite pacing.
  This is a transient, retry-after-backoff condition.
- **`503` (error).** Any other Bedrock invocation failure or an unexpected
  processor error (for example a model serving-capacity / availability error, or a
  transient service error that is not a quota throttle).

Both terminal failures are also written to the Dead Letter Queue with the
`request_id`, `model_id`, `error_type`, `error_message`, and `correlation_id` for
investigation (see [`../solution/runbook.md`](../solution/runbook.md)).

Client retries on throttling are intentionally **not** performed inside the Bedrock
call (boto3 retries are disabled) so that a throttle surfaces honestly to the
shaper's accounting and DLQ instead of being silently absorbed.

---

## Latency expectations

Callers should size timeouts for the queue path, not the burst path. The ranges below are
**illustrative** — observed in the load-test campaign, not guaranteed SLAs; actual latency depends
on the model, payload size, and offered load (queue-path latency grows with backlog under sustained
overload). See the measured results in [`../../docs/testing/results.md`](../../docs/testing/results.md).

| Path | p50 | p95 |
|------|-----|-----|
| Burst (immediate) | ~1-3s | ~5-8s |
| Queue (deferred) | ~20-60s | ~90-120s |

Treat the queue path as asynchronous: submit, then poll `/result` until it returns
a terminal code. Full SLA detail is in
[`../solution/runbook.md`](../solution/runbook.md).
