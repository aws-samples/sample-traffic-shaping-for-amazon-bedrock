"""Bedrock Processor Lambda - Calls Bedrock and sends Step Functions task callback."""

import json
import os
import random
import boto3
import time
from botocore.config import Config
from botocore.exceptions import ClientError

from shared_service import DynamoService, client_for


# Environment variables
SINGLE_TABLE_NAME = os.environ.get('SINGLE_TABLE_NAME')
DLQ_URL = os.environ.get('DLQ_URL')
OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET')
if not DLQ_URL:
    print("WARNING: DLQ_URL not configured — failed requests will not be captured")
if not OUTPUT_BUCKET:
    print("WARNING: OUTPUT_BUCKET not configured — success bodies cannot be persisted to S3")

# Disable boto3 client-side retries on the Bedrock call. boto3's default retry
# mode silently absorbs throttles (~96% of Bedrock 429s never surfaced in the
# 2026-07-13 run — 1,425 throttles collapsed to 55 logged). With no retries a
# throttle surfaces on the first attempt so the shaper's own accounting/DLQ path
# sees it instead of being blind to backend pressure. total_max_attempts=1 means
# exactly one attempt (no retries). See docs/testing/results.md (Appendix — "Bug 1 root cause").
_NO_RETRY_CONFIG = Config(retries={'total_max_attempts': 1, 'mode': 'standard'})

# Pre-invoke arrival jitter (milliseconds). The queue processor paces the async
# INVOKE of this Lambda evenly (Gate 5 spaces submits ~65ms apart), but AWS's
# async-invoke scheduler delivers those invocations to Bedrock-processor instances
# in bursts — so evenly-metered submits arrive at Bedrock's SUB-SECOND token bucket
# clustered, breaching it even when the 60s average is well under quota (observed:
# 119 throttles at a 4.67M/min drain vs 5.52M target / 8M quota). A small random
# sleep here — the closest point to the API call, downstream of the async-delivery
# bunching — re-spreads those clustered arrivals across the second. It costs only
# ~JITTER_MS/2 mean added latency (negligible vs ~7.5s converse) and does NOT lower
# throughput; it only smooths arrival timing. 0 = disabled. Env-tunable so the
# window can be swept without a redeploy.
#
# Sizing: Bedrock's bucket sustains ~22 req/s (8M TPM / 6000 tok). A dequeue batch
# is 10 items; if AWS delivers a batch clustered, spreading it under 22 req/s needs
# ~450ms. But the drain is already paced to ~13 req/s (below the bucket), so the
# bunching is only PARTIAL — 250ms is a balanced first cut (spreads to ~40 req/s
# instantaneous, roughly halving the observed peaks) at ~125ms mean added latency.
# Sweep upward (→500ms) via env/context if 250 doesn't clear the residual throttles.
BEDROCK_INVOKE_JITTER_MS = int(os.environ.get('BEDROCK_INVOKE_JITTER_MS', '250'))

# Clients
sfn_client = boto3.client('stepfunctions')
bedrock_runtime = boto3.client('bedrock-runtime', config=_NO_RETRY_CONFIG)
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')


# Next-generation Claude models (Opus 4.7+, Sonnet 4.6+) return HTTP 400 when
# sampling parameters (temperature, top_p, top_k) are sent with non-default
# values. Detect these by model-ID substring so the fix needs no config read in
# the processor (keeps the Tier 1 unblock to a single file). See
# docs/solution/architecture.md §9 (sampling-param stripping).
_STRIP_SAMPLING_SUBSTRINGS = (
    'claude-opus-4-7',
    'claude-opus-4-8',
    'claude-opus-5',
    'claude-sonnet-4-6',
    'claude-sonnet-5',
)


def should_strip_sampling_params(model_id: str) -> bool:
    """True if this model rejects sampling params (next-gen Claude)."""
    return any(s in (model_id or '') for s in _STRIP_SAMPLING_SUBSTRINGS)


def _write_terminal_with_retry(dynamo_service, *, request_id, state, http_status,
                               tenant_id, correlation_id, model_id, source,
                               reason=None, output_ref=None, duration_ms=None,
                               attempts=1, max_tries=3):
    """Write the terminal status item with a bounded retry (circuit-breaker style).

    Returns True if the conditional UpdateItem COMMITTED (won or lost the
    exactly-once transition — both mean the terminal state is settled in DDB).
    Returns False only when every attempt raised (DDB unreachable / persistent
    error) — the honest-outcomes contract: the caller MUST NOT signal success on
    False; let the task token time out so SM TimedOut → finalizer records the
    truth. A distinct 'TerminalWriteFailure' marker is logged for the on-call
    alarm (do not mistake it for a Bedrock throttle)."""
    if not request_id:
        # No pk — cannot address the terminal item. Unknowable request.
        print("WARNING: skipping terminal write — request_id is unknowable")
        return False
    last_err = None
    for attempt in range(1, max_tries + 1):
        try:
            dynamo_service.write_terminal_status(
                request_id=request_id,
                state=state,
                reason=reason,
                http_status=http_status,
                tenant_id=tenant_id,
                correlation_id=correlation_id,
                model_id=model_id,
                arm='shaper',
                source=source,
                output_ref=output_ref,
                attempts=attempts,
                duration_ms=duration_ms,
            )
            # Returned without raising → DDB write committed (win or loser-swallow).
            return True
        except Exception as e:
            last_err = e
            print(f"WARNING: terminal write attempt {attempt}/{max_tries} failed: "
                  f"request_id={request_id}, state={state}, error={e}")
            if attempt < max_tries:
                time.sleep(0.1 * attempt)
    print(f"ERROR: TerminalWriteFailure — request_id={request_id}, state={state}, "
          f"reason={reason} did not commit after {max_tries} tries: {last_err}")
    return False


def send_to_dlq(request_id, model_id, error_type, error_message, execution_arn, correlation_id=''):
    """Send failed request details to the Dead Letter Queue. Fails silently if DLQ_URL is not set or SQS errors."""
    if not DLQ_URL:
        print(f"WARNING: DLQ_URL not configured — dropping failed request: request_id={request_id}, correlation_id={correlation_id}, error_type={error_type}")
        return
    try:
        message_body = {
            'request_id': request_id,
            'model_id': model_id,
            'error_type': error_type,
            'error_message': error_message,
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S%z', time.gmtime()),
            'execution_arn': execution_arn,
            'correlation_id': correlation_id
        }
        sqs_client.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps(message_body)
        )
        print(f"Sent failure to DLQ: request_id={request_id}")
    except Exception as e:
        print(f"WARNING: Failed to send to DLQ: {e}")


def emit_bedrock_latency_metric(model_id, duration_ms, correlation_id=''):
    """Emit an EMF (Embedded Metric Format) metric for BedrockLatency to stdout."""
    emf = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": "BedrockShaper",
                "Dimensions": [["ServiceName", "model_id"]],
                "Metrics": [{"Name": "BedrockLatency", "Unit": "Milliseconds"}]
            }]
        },
        "ServiceName": "TrafficShaper",
        "model_id": model_id,
        "correlation_id": correlation_id,
        "BedrockLatency": duration_ms
    }
    print(json.dumps(emf))


def emit_request_metrics(model_id, source, throttled, correlation_id=''):
    """Emit per-request EMF counters dimensioned by admission SOURCE.

    The processor is the single choke point every request passes through, and it
    already knows both facts here: `source` ('immediate' = burst gate, 'queued' =
    queue drain) and whether Bedrock throttled. Emitting them with `source` as a
    CloudWatch dimension makes the immediate-vs-queued split AND the throttle-source
    split first-class metrics — answerable with a SEARCH() dashboard expression
    instead of hand-walking log streams. See docs/testing/results.md (Appendix)
    (burst path produced ~78% of throttles in the 2026-07-15 run — a fact that
    previously had to be reconstructed manually).

    RequestsProcessed is emitted for every request; BedrockThrottles only when the
    call was throttled, so a throttle-rate-by-source is RequestsProcessed vs
    BedrockThrottles per source in the dashboard.
    """
    metrics = [{"Name": "RequestsProcessed", "Unit": "Count"}]
    values = {"RequestsProcessed": 1}
    if throttled:
        metrics.append({"Name": "BedrockThrottles", "Unit": "Count"})
        values["BedrockThrottles"] = 1
    emf = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": "BedrockShaper",
                "Dimensions": [["ServiceName", "model_id", "source"]],
                "Metrics": metrics
            }]
        },
        "ServiceName": "TrafficShaper",
        "model_id": model_id,
        "source": source,
        "correlation_id": correlation_id,
        **values
    }
    print(json.dumps(emf))


def emit_token_metrics(model_id, source, actual_input_tokens, actual_output_tokens, correlation_id=''):
    """Emit EMF InputTokens/OutputTokens counters for a successful call.

    The clients already parse Bedrock's real usage (Converse usage.inputTokens/
    outputTokens; mantle & OpenAI usage.input_tokens/output_tokens) onto the result
    — this makes that usage a first-class CloudWatch metric so per-run token totals
    and iTPM/oTPM are answerable with a SUM()/window query instead of being lost
    when the queue item is deleted (the runtime path previously recorded NO actual
    tokens anywhere — testing-report token-total gap, 2026-08-04). Dimensioned by
    `source` (immediate/queued) to match RequestsProcessed. A throttled/failed call
    reports no usage, so it emits nothing and contributes zero tokens — the same
    convention as the co-developer's local harness (529s → zero tokens)."""
    ai = 0 if actual_input_tokens is None else int(actual_input_tokens)
    ao = 0 if actual_output_tokens is None else int(actual_output_tokens)
    if ai == 0 and ao == 0:
        return  # no usage reported (e.g. throttle/error) — contributes zero, like the 529 convention
    emf = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": "BedrockShaper",
                "Dimensions": [["ServiceName", "model_id", "source"], ["ServiceName", "model_id"]],
                "Metrics": [{"Name": "InputTokens", "Unit": "Count"},
                            {"Name": "OutputTokens", "Unit": "Count"}]
            }]
        },
        "ServiceName": "TrafficShaper",
        "model_id": model_id,
        "source": source,
        "correlation_id": correlation_id,
        "InputTokens": ai,
        "OutputTokens": ao,
    }
    print(json.dumps(emf))


def emit_requeued_on_throttle(model_id, correlation_id=''):
    """Emit an EMF counter when a burst-admitted request is re-enqueued after a
    Bedrock throttle (instead of dead-lettered). Lets the dashboard distinguish a
    paced retry from a terminal failure."""
    emf = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": "BedrockShaper",
                "Dimensions": [["ServiceName", "model_id"]],
                "Metrics": [{"Name": "RequeuedOnThrottle", "Unit": "Count"}]
            }]
        },
        "ServiceName": "TrafficShaper",
        "model_id": model_id,
        "correlation_id": correlation_id,
        "RequeuedOnThrottle": 1
    }
    print(json.dumps(emf))


def invoke_bedrock_model(model_id, request_payload):
    """
    Invoke a Bedrock foundation model using Converse API.

    Args:
        model_id: The Bedrock model ID (e.g., 'us.anthropic.claude-opus-4-20250514-v1:0')
        request_payload: The request payload containing the prompt and parameters

    Returns:
        dict: Response from Bedrock including:
            - success: bool
            - response_body: dict (if successful)
            - error: str (if failed)
            - throttled: bool
            - duration_ms: float
    """
    start_time = time.time()

    try:
        # Use Converse API - unified interface for all models
        inference_config = {'maxTokens': request_payload.get('max_tokens', 1024)}
        # Only send temperature when the model accepts sampling params AND the
        # caller actually provided one. Next-gen Claude models 400 otherwise.
        if not should_strip_sampling_params(model_id) and 'temperature' in request_payload:
            inference_config['temperature'] = request_payload['temperature']

        response = bedrock_runtime.converse(
            modelId=model_id,
            messages=[
                {
                    'role': 'user',
                    'content': [
                        {
                            'text': request_payload.get('prompt', 'Hello, how are you?')
                        }
                    ]
                }
            ],
            inferenceConfig=inference_config
        )

        duration_ms = (time.time() - start_time) * 1000

        print(f"Bedrock invocation successful: model={model_id}, duration={duration_ms:.2f}ms")

        return {
            'success': True,
            'response_body': response,
            'throttled': False,
            'duration_ms': duration_ms
        }

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        error_message = e.response.get('Error', {}).get('Message', '')
        duration_ms = (time.time() - start_time) * 1000

        # Check if it's a throttling error
        is_throttled = error_code in ['ThrottlingException', 'TooManyRequestsException', 'ServiceQuotaExceededException']

        if is_throttled:
            print(f"Bedrock throttled: model={model_id}, error={error_code}, duration={duration_ms:.2f}ms")
        else:
            print(f"Bedrock invocation failed: model={model_id}, error={error_code}, message={error_message}, duration={duration_ms:.2f}ms")

        return {
            'success': False,
            'error': f"{error_code}: {error_message}",
            'throttled': is_throttled,
            'duration_ms': duration_ms
        }

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        print(f"Unexpected error invoking Bedrock: model={model_id}, error={str(e)}, duration={duration_ms:.2f}ms")

        return {
            'success': False,
            'error': str(e),
            'throttled': False,
            'duration_ms': duration_ms
        }


def _build_thinking(model_config):
    """Build the adaptive-thinking block from model config, or None when unset."""
    if model_config.get('thinking_type') != 'adaptive':
        return None
    thinking = {'type': 'adaptive'}
    display = model_config.get('thinking_display')
    if display:
        thinking['display'] = display
    return thinking


def invoke_via_client(model_id, request_payload, model_config):
    """
    Backend-aware invocation via the BedrockClient abstraction.

    Returns the SAME dict shape as invoke_bedrock_model() plus optional
    'actual_input_tokens' / 'actual_output_tokens' (mantle reports them
    separately; runtime Converse reports them too). Dispatches on the config's
    backend/api_style; client_for() fails closed on unknown backends.
    """
    client = client_for(model_config)
    strip_sampling = bool(model_config.get('strip_sampling_params', should_strip_sampling_params(model_id)))
    prompt = request_payload.get('prompt', 'Hello, how are you?')
    max_tokens = request_payload.get('max_tokens') or int(model_config.get('default_max_tokens', 1024))
    temperature = request_payload.get('temperature')
    beta_headers = model_config.get('beta_headers') or None

    result = client.invoke(
        model_id=model_id,
        prompt=prompt,
        max_tokens=max_tokens,
        thinking=_build_thinking(model_config),
        effort=model_config.get('effort'),
        strip_sampling=strip_sampling,
        temperature=temperature,
        beta_headers=beta_headers,
    )
    return {
        'success': result.success,
        'response_body': result.response_body,
        'error': result.error,
        'throttled': result.throttled,
        'duration_ms': result.duration_ms,
        'actual_input_tokens': result.actual_input_tokens,
        'actual_output_tokens': result.actual_output_tokens,
    }


def reconcile_consumption(dynamo_service, model_id, allocation_id,
                          actual_input_tokens, actual_output_tokens):
    """
    Reconcile the BURST consumption record to the ACTUAL token usage — for BOTH
    runtime and mantle successes.

    This is the write-back that makes the sliding-window admission read accurate
    (docs/solution/architecture.md §5). Pre-call admission wrote an
    ESTIMATE (byte-heuristic input + a max_tokens*burndown output ceiling that
    OVER-counts). After the call, Bedrock returns the real input/output token
    usage (Converse `usage.inputTokens`/`usage.outputTokens`; mantle reports them
    too), surfaced on the client result as actual_input_tokens/actual_output_tokens.
    We overwrite the record's estimated_tokens (and the split input/output fields)
    with actuals so the next window read sees real consumption — replacing the
    over-counted estimate ~7.5s in, well inside the 15s accuracy window.

    allocation_id is the '{timestamp_ms}#{request_id}' written by put_allocation,
    matching the BURST#CONSUMPTION sort key exactly. Best-effort: a failure here
    must never fail the request (the call already succeeded). Because the window
    horizon self-heals, a missed reconciliation just ages out — no drift builds up.
    """
    if actual_input_tokens is None and actual_output_tokens is None:
        return
    if not allocation_id or '#' not in allocation_id:
        print(f"WARNING: cannot reconcile mantle consumption — bad allocation_id={allocation_id!r}")
        return
    try:
        table = dynamo_service.single_table
        set_parts = ['estimated_tokens = :combined']
        values = {}
        combined = (actual_input_tokens or 0) + (actual_output_tokens or 0)
        values[':combined'] = combined
        if actual_input_tokens is not None:
            set_parts.append('estimated_input_tokens = :ai')
            values[':ai'] = int(actual_input_tokens)
        if actual_output_tokens is not None:
            set_parts.append('estimated_output_tokens = :ao')
            values[':ao'] = int(actual_output_tokens)
        table.update_item(
            Key={'pk': f'MODEL#{model_id}#BURST#CONSUMPTION', 'sk': allocation_id},
            UpdateExpression='SET ' + ', '.join(set_parts),
            ConditionExpression='attribute_exists(pk) AND attribute_exists(sk)',
            ExpressionAttributeValues=values,
        )
        print(f"Reconciled consumption to actuals: model={model_id}, sk={allocation_id}, "
              f"actual_in={actual_input_tokens}, actual_out={actual_output_tokens}")
    except Exception as e:
        # Reconciliation is best-effort. There is no reconciliation Lambda anymore —
        # the sliding-window admission gate self-heals (any missed write-back ages
        # out of the 15s window) and TTL clears the record. A failure here just
        # leaves the estimate in place until it ages out.
        print(f"WARNING: consumption reconciliation failed (non-fatal): {e}")


def handler(event, context):
    """
    Process Bedrock request and send callback to Step Functions.

    Expected event:
        - task_token: Step Functions task token for callback
        - model_id: Bedrock model ID
        - request_id: Unique request identifier
        - tenant_id: Tenant identifier (threaded into terminal-status writes)
        - correlation_id: Correlation identifier
        - request_payload: Payload for Bedrock invocation (immediate path)
        - execution_arn: Step Functions execution ARN (queued path - resolve payload via describe_execution)
        - allocation_id: Allocation ID from budget manager
    """
    # Log only non-sensitive metadata — never the full event (it carries the user
    # prompt in request_payload, plus the task_token bearer credential).
    print(f"Bedrock Processor received: request_id={event.get('request_id')}, "
          f"model_id={event.get('model_id')}, correlation_id={event.get('correlation_id')}, "
          f"source={event.get('source')}")

    task_token = event.get('task_token')
    model_id = event.get('model_id')
    # Cato C-5: NO 'unknown' default. If request_id (the terminal-item pk) did not
    # propagate, we cannot write an honest terminal status and must not proceed.
    request_id = event.get('request_id')
    tenant_id = event.get('tenant_id')
    request_payload = event.get('request_payload', {})
    execution_arn = event.get('execution_arn')
    allocation_id = event.get('allocation_id')
    correlation_id = event.get('correlation_id', '')
    # source ∈ {immediate,queued}; set before any early return so terminal writes
    # always carry an honest source dimension.
    source = 'immediate'

    # Construct the shared DynamoService early so every path (including early
    # returns) can write a terminal status.
    dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)

    if not task_token:
        # No task token → no callback channel exists. Cannot signal SFN; the
        # execution will surface the missing-token failure on its own.
        print(f"ERROR: task_token is required")
        return {'statusCode': 400, 'error': 'task_token is required'}

    # Cato C-5: request_id / tenant_id must have propagated. If either is missing,
    # treat as an error — write a terminal status where we can (request_id known)
    # and fail the task; do NOT proceed to spend Bedrock quota on an untraceable
    # request.
    if not request_id or not tenant_id:
        error_msg = (f"missing propagation: request_id={request_id!r}, "
                     f"tenant_id={tenant_id!r}")
        print(f"ERROR: {error_msg}")
        if request_id:
            _write_terminal_with_retry(
                dynamo_service,
                request_id=request_id, state='FAILED', reason='error',
                http_status=503, tenant_id=tenant_id, correlation_id=correlation_id,
                model_id=model_id, source=source,
            )
        sfn_client.send_task_failure(
            taskToken=task_token,
            error='PropagationError',
            cause=error_msg
        )
        return {'statusCode': 400, 'error': error_msg}

    if not model_id:
        print(f"ERROR: model_id is required")
        # Honest terminal status: a missing model_id is a validation error.
        _write_terminal_with_retry(
            dynamo_service,
            request_id=request_id, state='FAILED', reason='validation_error',
            http_status=400, tenant_id=tenant_id, correlation_id=correlation_id,
            model_id=model_id, source=source,
        )
        # Send failure callback
        sfn_client.send_task_failure(
            taskToken=task_token,
            error='ValidationError',
            cause='model_id is required'
        )
        return {'statusCode': 400, 'error': 'model_id is required'}

    try:
        # Resolve request_payload from execution_arn if not provided directly.
        # This handles the queued path where payload stays in Step Function state.
        if not request_payload and execution_arn:
            source = 'queued'
            print(f"Resolving payload from execution_arn: {execution_arn}")
            exec_info = sfn_client.describe_execution(executionArn=execution_arn)
            original_input = json.loads(exec_info['input'])

            # Extract request_payload from original Step Function input.
            # Only carry temperature forward if the caller actually set one —
            # invoke_bedrock_model() decides whether to send it per model.
            request_payload = original_input.get('request_payload') or {
                'prompt': original_input.get('prompt'),
                'max_tokens': original_input.get('max_tokens', 100),
                **({'temperature': original_input['temperature']}
                   if 'temperature' in original_input else {})
            }
            # User prompts may contain PII/PHI — log only the length, never the content.
            print(f"Resolved payload from execution: prompt_len={len(request_payload.get('prompt', ''))}")

        # Fail if we have no valid payload — fabricating a prompt masks data loss bugs
        if not request_payload or not request_payload.get('prompt'):
            error_msg = f"No valid request_payload resolved for request_id={request_id}"
            print(f"ERROR: {error_msg}")
            # Honest terminal status: a missing/invalid payload is a validation error.
            _write_terminal_with_retry(
                dynamo_service,
                request_id=request_id, state='FAILED', reason='validation_error',
                http_status=400, tenant_id=tenant_id, correlation_id=correlation_id,
                model_id=model_id, source=source,
            )
            send_to_dlq(
                request_id=request_id,
                model_id=model_id,
                error_type='MissingPayloadError',
                error_message=error_msg,
                execution_arn=execution_arn,
                correlation_id=correlation_id
            )
            sfn_client.send_task_failure(
                taskToken=task_token,
                error='MissingPayloadError',
                cause=error_msg
            )
            return {'statusCode': 400, 'error': error_msg}

        # Call Bedrock via the backend-aware client. Fetch model config to pick
        # runtime (Converse) vs mantle (Messages) — client_for() dispatches on
        # config['backend']. Fail-safe: if config is unreadable, fall back to the
        # legacy runtime converse path so runtime traffic is never blocked by a
        # config read hiccup.
        model_config = {}
        try:
            model_config = dynamo_service.get_model_config(model_id) or {}
        except Exception as cfg_err:
            print(f"WARNING: could not load model_config for {model_id}: {cfg_err} — using runtime converse fallback")

        # Arrival jitter: re-spread async-delivery-bunched invocations across the
        # second so they don't clump against Bedrock's sub-minute token bucket
        # (see BEDROCK_INVOKE_JITTER_MS). Applied immediately before the call.
        if BEDROCK_INVOKE_JITTER_MS > 0:
            time.sleep(random.uniform(0, BEDROCK_INVOKE_JITTER_MS / 1000.0))  # nosec B311

        print(f"Invoking Bedrock: request_id={request_id}, model_id={model_id}, "
              f"backend={model_config.get('backend', 'runtime')}, correlation_id={correlation_id}")
        if model_config:
            bedrock_response = invoke_via_client(model_id, request_payload, model_config)
        else:
            bedrock_response = invoke_bedrock_model(model_id, request_payload)

        duration_ms = bedrock_response.get('duration_ms')

        # Reconcile the consumption record to ACTUAL token usage for BOTH runtime
        # and mantle successes. This write-back is what makes the sliding-window
        # admission read accurate: the over-counted estimate is replaced by the
        # real usage ~7.5s in, inside the 15s window. Only runs when the client
        # surfaced actuals (Converse and mantle both do).
        if (bedrock_response.get('success') and allocation_id
                and (bedrock_response.get('actual_input_tokens') is not None
                     or bedrock_response.get('actual_output_tokens') is not None)):
            try:
                reconcile_consumption(
                    dynamo_service, model_id, allocation_id,
                    bedrock_response.get('actual_input_tokens'),
                    bedrock_response.get('actual_output_tokens'),
                )
            except Exception as rec_err:
                print(f"WARNING: consumption reconciliation failed (non-fatal): {rec_err}")

        # Emit EMF metric for Bedrock latency (success or failure)
        emit_bedrock_latency_metric(model_id, bedrock_response.get('duration_ms', 0), correlation_id=correlation_id)

        # Emit per-request operational counters dimensioned by admission source
        # (immediate=burst gate, queued=queue drain), tagging throttles with the path
        # that produced them. `source` was set above ('immediate' by default, 'queued'
        # when the payload was resolved from execution_arn). NOTE: these RequestsProcessed
        # /BedrockThrottles counters are OPERATIONAL telemetry, distinct from the
        # exactly-once RequestOutcome terminal metric that OutcomeStreamFn emits off the
        # DDB stream (Cato C-2) — they answer "which admission path throttles" in near
        # real time; RequestOutcome is the reconciled per-request terminal truth.
        emit_request_metrics(model_id, source, bedrock_response.get('throttled', False),
                             correlation_id=correlation_id)

        # Emit token counters from the ACTUAL usage the client parsed. Only on
        # success; throttled/failed calls report no usage and contribute zero
        # tokens (matches the co-developer harness convention). This is what makes
        # per-run Input/Output token totals + iTPM/oTPM queryable from CloudWatch.
        if bedrock_response.get('success'):
            emit_token_metrics(
                model_id, source,
                bedrock_response.get('actual_input_tokens'),
                bedrock_response.get('actual_output_tokens'),
                correlation_id=correlation_id,
            )

        # Bedrock failed — write honest terminal status, DLQ, then task failure.
        # (requeue-on-throttle intentionally excluded: throttles stay terminal.)
        if not bedrock_response.get('success'):
            error_msg = bedrock_response.get('error', 'Unknown Bedrock error')
            # Branch: a Bedrock throttle is reason='throttled'/429; anything else
            # is reason='error'/503. 'throttled' is reserved for real Bedrock
            # throttles (Cato C-8) — never used for our own validation failures.
            if bedrock_response.get('throttled'):
                terminal_reason, terminal_http = 'throttled', 429
            else:
                terminal_reason, terminal_http = 'error', 503
            _write_terminal_with_retry(
                dynamo_service,
                request_id=request_id, state='FAILED', reason=terminal_reason,
                http_status=terminal_http, tenant_id=tenant_id,
                correlation_id=correlation_id, model_id=model_id, source=source,
                duration_ms=duration_ms,
            )

            send_to_dlq(
                request_id=request_id,
                model_id=model_id,
                error_type='BedrockInvocationError',
                error_message=error_msg,
                execution_arn=execution_arn,
                correlation_id=correlation_id
            )

            print(f"Sending task failure callback: request_id={request_id}, correlation_id={correlation_id}, error={error_msg}")
            sfn_client.send_task_failure(
                taskToken=task_token,
                error='BedrockInvocationError',
                cause=error_msg
            )
            return {
                'statusCode': 502,
                'error': error_msg,
                'request_id': request_id
            }

        # Bedrock succeeded. Write-gates-signal (design §"Write-gates-signal, and
        # NO success backstop"): persist the body to S3 and commit the SUCCEEDED
        # terminal item (with output_ref) BEFORE send_task_success. If the terminal
        # write ultimately fails, do NOT signal success — let the task token time
        # out so SM TimedOut → the FAILED/TIMED_OUT finalizer records honestly.
        output_ref = None
        if OUTPUT_BUCKET:
            output_key = f"outputs/{request_id}.json"
            try:
                s3_client.put_object(
                    Bucket=OUTPUT_BUCKET,
                    Key=output_key,
                    Body=json.dumps(bedrock_response.get('response_body'), default=str).encode('utf-8'),
                    ContentType='application/json',
                )
                output_ref = f"s3://{OUTPUT_BUCKET}/{output_key}"
                print(f"Persisted inference body: request_id={request_id}, output_ref={output_ref}")
            except Exception as s3_err:
                # S3 body persistence failed — treat as a terminal-write failure:
                # do not signal a success we cannot back with a retrievable body.
                print(f"ERROR: TerminalWriteFailure — failed to persist body to S3: "
                      f"request_id={request_id}, error={s3_err}")
                return {
                    'statusCode': 500,
                    'error': f"failed to persist output body: {s3_err}",
                    'request_id': request_id
                }
        else:
            print(f"WARNING: OUTPUT_BUCKET unset — SUCCEEDED terminal for request_id={request_id} will have no output_ref")

        committed = _write_terminal_with_retry(
            dynamo_service,
            request_id=request_id, state='SUCCEEDED', reason=None,
            http_status=200, output_ref=output_ref, tenant_id=tenant_id,
            correlation_id=correlation_id, model_id=model_id, source=source,
            duration_ms=duration_ms,
        )
        if not committed:
            # Honest-outcomes contract: the terminal SUCCEEDED write did not
            # commit. Do NOT send_task_success — let the token time out so the
            # finalizer records the truth (TerminalWriteFailure marker already
            # logged for the on-call alarm).
            print(f"ERROR: TerminalWriteFailure — not signaling success for "
                  f"request_id={request_id}; letting task token time out")
            return {
                'statusCode': 500,
                'error': 'terminal SUCCEEDED write did not commit; success withheld',
                'request_id': request_id
            }

        callback_output = {
            'allocation_id': allocation_id,
            'queued': False,  # Required for Choice state routing
            'source': source,  # 'immediate' or 'queued' - tracks which path was taken
            'bedrock_response': bedrock_response,
            'request_id': request_id,
            'output_ref': output_ref,
            'processed_by': 'bedrock_processor',
            'correlation_id': correlation_id
        }

        print(f"Sending task success callback: request_id={request_id}, correlation_id={correlation_id}")
        # default=str: some models' Converse responses carry bytes fields (observed
        # on grok-4.6 and jamba). Without the fallback, json.dumps raises "Object of
        # type bytes is not JSON serializable" and a SUCCESSFUL call is spuriously
        # failed to the DLQ. Mirrors the S3 body write above (line ~695).
        sfn_client.send_task_success(
            taskToken=task_token,
            output=json.dumps(callback_output, default=str)
        )

        return {
            'statusCode': 200,
            'body': {
                'message': 'Bedrock invocation complete, callback sent',
                'request_id': request_id
            }
        }

    except Exception as e:
        error_message = str(e)
        print(f"ERROR processing request: request_id={request_id}, correlation_id={correlation_id}, error={error_message}")

        # Honest terminal status: an unexpected processor error is reason='error'/503.
        _write_terminal_with_retry(
            dynamo_service,
            request_id=request_id, state='FAILED', reason='error',
            http_status=503, tenant_id=tenant_id, correlation_id=correlation_id,
            model_id=model_id, source=source,
        )

        # Send to DLQ BEFORE task_failure callback (so we don't lose the message if callback fails)
        send_to_dlq(
            request_id=request_id,
            model_id=model_id,
            error_type=type(e).__name__,
            error_message=error_message,
            execution_arn=execution_arn,
            correlation_id=correlation_id
        )

        # Send failure callback
        try:
            sfn_client.send_task_failure(
                taskToken=task_token,
                error='BedrockProcessorError',
                cause=error_message
            )
        except Exception as callback_error:
            print(f"ERROR sending failure callback: {callback_error}")

        return {
            'statusCode': 500,
            'error': error_message
        }
