"""Budget Manager Lambda - Handles budget reservation and release."""

import json
import logging
import os
import random
import re
import time
import uuid
import boto3
from datetime import datetime
from typing import Dict, Any, Optional

# Import shared service layer (Phase 2: Leaky bucket implementation)
from shared_service import (
    DynamoService,
    estimate_request_tokens,
    estimate_request_tokens_split,
    BurstCapacityExceeded,
)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SINGLE_TABLE_NAME = os.environ.get('SINGLE_TABLE_NAME')
BEDROCK_PROCESSOR_ARN = os.environ.get('BEDROCK_PROCESSOR_ARN')

# Clients
eventbridge = boto3.client('events')
lambda_client = boto3.client('lambda')
sfn_client = boto3.client('stepfunctions')

# Constants
SEMAPHORE_ID = "semaphore#default"  # Used in EventBridge events for queue processor


def _record_status(dynamo_service, *, request_id, state, http_status, reason,
                   correlation_id, tenant_id, model_id, source):
    """Best-effort honest-outcomes status write (Objective 3).

    Records a lifecycle transition on the single-table REQUEST#{id}/STATUS item via
    Agent-A's conditional write_terminal_status helper. Never crashes the handler —
    the status record is observability, not the request's critical path (the Step
    Functions callback still governs the caller's actual result). Mirrors the
    non-fatal posture of the EMF emission and send_task_failure blocks below.

    QUEUED (202) marks the PENDING->QUEUED transition (not terminal — a later queue
    drain writes the true terminal); FAILED/validation_error (400) marks an
    ingress-rejected request that never consumed Bedrock quota.
    """
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
        )
    except Exception as e:
        logger.warning(f"Failed to write {state} status (non-fatal): {e}")


def _enqueue_on_reject(dynamo_service, model_id, request_id, task_token, execution_arn,
                       correlation_id, tenant_id, reason, extra=None,
                       estimated_tokens=0, estimated_input_tokens=0, estimated_output_tokens=0):
    """Enqueue a rejected request and return the (False, metadata) tuple.

    Shared by the mantle admission path for both gate-reject and over-consumption
    rollback cases (mirrors the runtime path's enqueue blocks).

    The estimated_* args are the pre-call token estimate computed here (where the
    prompt is available) so the queue processor can log each request's cost at
    dispatch without re-resolving the prompt.
    """
    try:
        dynamo_service.enqueue_request(
            model_id=model_id, request_id=request_id,
            task_token=task_token, execution_arn=execution_arn,
            correlation_id=correlation_id, tenant_id=tenant_id,
            estimated_tokens=estimated_tokens,
            estimated_input_tokens=estimated_input_tokens,
            estimated_output_tokens=estimated_output_tokens,
        )
        logger.info(f"Enqueued request ({reason}): request_id={request_id}, "
                    f"model_id={model_id}, correlation_id={correlation_id}")
    except Exception as e:
        logger.error(f"Failed to enqueue to single table: {e}")
        raise
    meta = {'queued': True, 'reason': reason, 'correlation_id': correlation_id, 'tenant_id': tenant_id}
    if extra:
        meta.update(extra)
    return (False, meta)


def _try_reserve_mantle(*, dynamo_service, model_id, request_id, task_token, execution_arn,
                        request_payload, correlation_id, tenant_id, config,
                        burst_capacity, burst_regen_rate, rpm_quota_enabled,
                        short_window_sec=0.0, long_window_sec=0.0):
    """
    Mantle iTPM/oTPM split admission via the sliding-window read gate.

    Three dimensions gate, honoring rpm_quota_enabled:
      - RPM: only when rpm_quota_enabled (next-gen models skip it entirely).
      - iTPM: input tokens per minute (independent quota).
      - oTPM: output tokens per minute (independent quota, the tighter one).

    put_allocation(backend='mantle', ...) reads the last long_window_sec of
    consumption records and rejects up front when the request would breach the 2s
    or 15s cap on any dimension. Bounded over-admission from concurrent reads is
    accepted and caught downstream by requeue-on-throttle — no post-gate verify or
    rollback, and no reconciliation Lambda.
    """
    # Split-token config (required on mantle — create-config enforces presence).
    itpm_burst_capacity = int(config.get('itpm_burst_capacity', 0))
    itpm_burst_regen_rate = float(config.get('itpm_burst_regeneration_rate', 0))
    otpm_burst_capacity = int(config.get('otpm_burst_capacity', 0))
    otpm_burst_regen_rate = float(config.get('otpm_burst_regeneration_rate', 0))
    bytes_per_token = float(config.get('bytes_per_token', 4.0))
    # Sub-minute (2s) request-rate cap. Next-gen mantle models set rpm_quota_enabled
    # =False (no RPM signal to default from), so an explicit short_window_rps is
    # required to enable the 2s gate on those; absent ⇒ 0 (gate off for that model).
    short_window_rps = float(config.get('short_window_rps', 0))

    prompt = (request_payload or {}).get('prompt', '')
    max_tokens = (request_payload or {}).get('max_tokens', 100)
    est_input, est_output = estimate_request_tokens_split(
        prompt=prompt, max_tokens=max_tokens, bytes_per_token=bytes_per_token
    )
    estimated_tokens = est_input + est_output
    logger.info(f"Mantle split estimate: itpm~{est_input}, otpm~{est_output} "
                f"(input_bytes~{len((prompt or '').encode('utf-8'))}, max_tokens={max_tokens}, "
                f"bytes_per_token={bytes_per_token}, rpm_enabled={rpm_quota_enabled})")

    # Step 2: Atomic 3-way admission gate.
    try:
        allocation_result = dynamo_service.put_allocation(
            model_id, request_id, estimated_tokens=estimated_tokens,
            correlation_id=correlation_id,
            burst_capacity=burst_capacity, burst_regen_rate=burst_regen_rate,
            backend='mantle', rpm_quota_enabled=rpm_quota_enabled,
            estimated_input_tokens=est_input, estimated_output_tokens=est_output,
            itpm_burst_capacity=itpm_burst_capacity, itpm_burst_regen_rate=itpm_burst_regen_rate,
            otpm_burst_capacity=otpm_burst_capacity, otpm_burst_regen_rate=otpm_burst_regen_rate,
            short_window_rps=short_window_rps,
            short_window_sec=short_window_sec, long_window_sec=long_window_sec,
        )
    except BurstCapacityExceeded:
        logger.info(f"Mantle admission gate rejected: request_id={request_id}, "
                    f"correlation_id={correlation_id}, tenant_id={tenant_id}")
        return _enqueue_on_reject(
            dynamo_service, model_id, request_id, task_token, execution_arn,
            correlation_id, tenant_id, 'mantle_admission_gate',
            extra={'burst_capacity': burst_capacity},
            estimated_tokens=estimated_tokens,
            estimated_input_tokens=est_input, estimated_output_tokens=est_output,
        )

    timestamp_ms = allocation_result['timestamp_ms']

    # The mantle window-read gate already enforced iTPM + oTPM (+ optional RPM)
    # against the 2s/15s consumption windows. Reaching this point means every
    # enabled dimension had headroom. There is no post-gate verify and no rollback:
    # bounded over-admission from concurrent reads is accepted and caught downstream
    # by requeue-on-throttle; the 15s window horizon self-heals any drift.
    logger.info(f"Mantle capacity available (window-read gate), proceeding: request_id={request_id}, "
                f"correlation_id={correlation_id}, tenant_id={tenant_id}")
    return (True, {
        'queued': False,
        'available_itpm': None,  # counter-enforced; no post-gate availability computed
        'available_otpm': None,
        'available_rpm': None,
        'estimated_tokens': estimated_tokens,
        'estimated_input_tokens': est_input,
        'estimated_output_tokens': est_output,
        'timestamp_ms': timestamp_ms,
        'burst_capacity': burst_capacity,
        'correlation_id': correlation_id,
        'tenant_id': tenant_id,
    })


def try_reserve_allocation_leaky_bucket(
    dynamo_service: DynamoService,
    model_id: str,
    request_id: str,
    task_token: Optional[str] = None,
    execution_arn: Optional[str] = None,
    request_payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
    tenant_id: str = 'unknown',
    config: Optional[Dict[str, Any]] = None
):
    """
    Optimistic write-then-verify pattern for burst capacity.
    Gates on BOTH RPM and TPM — whichever dimension is tighter.

    Args:
        task_token: Step Functions task token for callback
        execution_arn: Step Functions execution ARN for payload resolution
        request_payload: Request payload with prompt/max_tokens for TPM estimation
        config: Pre-fetched model config (avoids duplicate DynamoDB read)

    Returns:
        tuple: (success: bool, metadata: dict)
    """
    # Step 1: Get model config (use pre-fetched if available)
    try:
        if config is None:
            config = dynamo_service.get_effective_capacity(model_id)
        adaptive_shift = config.get('_adaptive_shift', 0)
        if adaptive_shift:
            logger.info(f"Adaptive capacity: shifted {adaptive_shift} tokens from burst to queue for model={model_id}")
        burst_capacity = int(config['burst_capacity'])
        burst_regen_rate = float(config['burst_regeneration_rate'])
        # TPM config (optional — gracefully degrade if not configured)
        tpm_burst_capacity = int(config.get('tpm_burst_capacity', 0))
        tpm_burst_regen_rate = float(config.get('tpm_burst_regeneration_rate', 0))
        burndown_rate = float(config.get('output_token_burndown_rate', 1.0))
        # Backend selects the admission shape. Absent ⇒ 'runtime' (legacy configs).
        backend = config.get('backend', 'runtime')
        # RPM is optional per-config: next-gen runtime AND mantle no-RPM models set
        # rpm_quota_enabled=False. Absent ⇒ True (legacy RPM-gated configs).
        rpm_quota_enabled = bool(config.get('rpm_quota_enabled', True))
        # Sub-minute (2s) rate cap. Requests/sec admitted per 2s window; smooths
        # instantaneous dispatch to ~the sustained quota rate. Absent ⇒ 0, which
        # makes put_allocation fall back to burst_regeneration_rate (≈ quota req/s).
        short_window_rps = float(config.get('short_window_rps', 0))
        # Sliding-window admission horizons (future-state gate). Absent ⇒ 0, which
        # makes put_allocation fall back to its class defaults (2s / 15s).
        short_window_sec = float(config.get('short_window_sec', 0))
        long_window_sec = float(config.get('long_window_sec', 0))
    except KeyError:
        raise ValueError(f"Model config not found: {model_id}. Run 'make create-config MODEL=...'")
    except Exception as e:
        raise ValueError(f"Error loading model config for {model_id}: {e}")

    # De-correlate concurrent Lambda invocations before hitting the atomic admission
    # gate. Spreads thundering herds across a short window so the eventually-consistent
    # counter read has time to reflect prior commits — reducing false admits and wasted
    # TransactWriteItems attempts. Fixed 0–40ms; negligible at low traffic, meaningful
    # when 30–100 Lambdas fire simultaneously against a nearly-full window.
    time.sleep(random.uniform(0, 0.040))  # nosec B311  # non-crypto: admission jitter

    # Mantle backend uses the iTPM/oTPM split 3-way admission gate. Dispatch early
    # so the runtime path below stays byte-identical to its pre-Tier-2 behavior.
    if backend == 'mantle':
        return _try_reserve_mantle(
            dynamo_service=dynamo_service, model_id=model_id, request_id=request_id,
            task_token=task_token, execution_arn=execution_arn,
            request_payload=request_payload, correlation_id=correlation_id,
            tenant_id=tenant_id, config=config,
            burst_capacity=burst_capacity, burst_regen_rate=burst_regen_rate,
            rpm_quota_enabled=rpm_quota_enabled,
            short_window_sec=short_window_sec, long_window_sec=long_window_sec,
        )

    # Step 1b: Estimate TPM cost of this request.
    #
    # ALWAYS compute the estimate when a payload is present — NOT only when
    # tpm_burst_capacity > 0. The estimate serves two independent consumers:
    #   (1) the burst TPM gate (only active when tpm_burst_regen_rate > 0), and
    #   (2) the queue item's estimated_tokens, which the QUEUE PROCESSOR paces on.
    # Gating this on tpm_burst_capacity meant burst=0 configs enqueued items with
    # NO estimate, so the queue processor fell back to a flat 1024-token estimate
    # while Bedrock charged ~6000 — collapsing every token gate (incl. the Gate 5
    # even-spacing pacer) and letting the drain overshoot queue_target_tpm and the
    # account quota (→ throttles). Reproduced in scripts/test_queue_overshoot_sim.py.
    # Computing it unconditionally is safe: the burst TPM gate stays keyed on
    # tpm_burst_regen_rate > 0 (dynamo.put_allocation), so a non-zero estimate here
    # does NOT enable that gate when burst is disabled.
    estimated_tokens = 0
    if request_payload:
        prompt = request_payload.get('prompt', '')
        max_tokens = request_payload.get('max_tokens', 100)
        bytes_per_token = float(config.get('bytes_per_token', 4.0))
        estimated_tokens = estimate_request_tokens(
            prompt=prompt,
            max_tokens=max_tokens,
            burndown_rate=burndown_rate,
            bytes_per_token=bytes_per_token
        )
        logger.info(f"TPM estimate: {estimated_tokens} tokens "
                    f"(input_bytes~{len((prompt or '').encode('utf-8'))}, max_tokens={max_tokens}, "
                    f"burndown={burndown_rate}x, bytes_per_token={bytes_per_token})")

    # Step 2: Sliding-window read admission gate — see put_allocation(). One
    # strongly-consistent read of the last 15s of consumption records enforces both
    # the 2s rate cap and the 15s accuracy cap on RPS and TPS; if the request would
    # breach either, BurstCapacityExceeded is raised and the request is enqueued.
    # On admit, the consumption record is written with the estimate (reconciled to
    # actuals ~7.5s later by bedrock_processor). No counters, no transaction.
    try:
        allocation_result = dynamo_service.put_allocation(
            model_id, request_id, estimated_tokens=estimated_tokens,
            correlation_id=correlation_id,
            burst_capacity=burst_capacity, burst_regen_rate=burst_regen_rate,
            tpm_burst_capacity=tpm_burst_capacity,
            tpm_burst_regen_rate=tpm_burst_regen_rate,
            rpm_quota_enabled=rpm_quota_enabled,
            short_window_rps=short_window_rps,
            short_window_sec=short_window_sec,
            long_window_sec=long_window_sec,
        )
    except BurstCapacityExceeded:
        # Atomic admission gate rejected — no consumption record written, no rollback needed
        logger.info(f"Burst capacity gate rejected: request_id={request_id}, "
                    f"correlation_id={correlation_id}, tenant_id={tenant_id}")

        queue_partition_id = model_id
        try:
            dynamo_service.enqueue_request(
                model_id=queue_partition_id,
                request_id=request_id,
                task_token=task_token,
                execution_arn=execution_arn,
                correlation_id=correlation_id,
                tenant_id=tenant_id,
                estimated_tokens=estimated_tokens,
            )
            logger.info(f"Enqueued request (burst gate): request_id={request_id}, "
                        f"model_id={queue_partition_id}, correlation_id={correlation_id}, "
                        f"estimated_tokens={estimated_tokens}")
        except Exception as e:
            # Log-then-raise: exception propagates to the caller (SFN retry/DLQ), which
            # owns terminal handling. Use warning here to avoid double-counting as a
            # handled error at this layer (semgrep: logging-error-without-handling).
            logger.warning(f"Failed to enqueue to single table: {e}")
            raise

        return (False, {
            'queued': True,
            'reason': 'burst_capacity_gate',
            'burst_capacity': burst_capacity,
            'correlation_id': correlation_id,
            'tenant_id': tenant_id
        })

    timestamp_ms = allocation_result['timestamp_ms']
    current_time = allocation_result['timestamp']

    # The window-read gate already enforced both RPS and TPS against the 2s/15s
    # consumption windows. Reaching this point means the request had headroom in
    # both. There is no post-gate over-consumption check and nothing to roll back:
    # bounded over-admission from concurrent reads is accepted and caught downstream
    # by requeue-on-throttle; the 15s window horizon self-heals any drift.
    logger.info(f"Capacity available (window-read gate), proceeding: request_id={request_id}, "
                f"correlation_id={correlation_id}, tenant_id={tenant_id}, "
                f"tpm_gated={tpm_burst_capacity > 0 and estimated_tokens > 0}")
    return (True, {
        'queued': False,
        'available_capacity': burst_capacity,  # counter-gated; sentinel for EMF utilization math
        'available_tpm': None,                 # no post-gate availability computed (counter-enforced)
        'estimated_tokens': estimated_tokens,
        'timestamp_ms': timestamp_ms,
        'burst_capacity': burst_capacity,
        'correlation_id': correlation_id,
        'tenant_id': tenant_id
    })


def trigger_queue_processor(model_id: str, dynamo_service: DynamoService):
    """
    Trigger queue processor via EventBridge event.
    Uses heartbeat-based lock in single table for stale lock detection.

    If a processor crashed, its lock will have an expired TTL. This function
    checks if a lock is active (not stale) before triggering.

    Self-healing: If lock TTL is expired, triggers new processor which will
    overwrite the stale lock. Recovery within 2 minutes vs 48 hours for
    DynamoDB TTL cleanup.

    Multiple Budget Managers may detect stale state and trigger multiple
    Queue Processors - only one will win the lock acquisition.

    Args:
        model_id: The Bedrock model ID to process queue for
        dynamo_service: DynamoService instance for lock operations
    """
    # Check if an active processor is running (lock exists and not stale)
    if dynamo_service.is_processor_lock_active(model_id):
        logger.info(f"Queue processor already running (active lock): model={model_id}")
        return

    # No active processor - trigger one
    # Note: Multiple Budget Managers may reach here simultaneously if lock is stale
    # That's OK - Queue Processor uses atomic acquire_processor_lock() to ensure
    # only one wins
    logger.info(f"No active processor detected, triggering Queue Processor: model={model_id}")

    try:
        eventbridge.put_events(
            Entries=[{
                'Source': 'budget-manager',
                'DetailType': 'QueueProcessingRequired',
                'Detail': json.dumps({
                    'semaphore_id': SEMAPHORE_ID,
                    'model_id': model_id,
                    'timestamp': datetime.utcnow().isoformat()
                })
            }]
        )
        logger.info(f"Triggered queue processor via EventBridge for model: {model_id}")
    except Exception as e:
        logger.error(f"Failed to trigger queue processor via EventBridge: {e}")
        # Don't fail the enqueue operation if trigger fails


def handler(event, context):
    """Handle budget reservation using leaky bucket algorithm."""
    # Log only non-sensitive metadata — never the full event (it carries the user
    # prompt in request_payload, plus the task_token bearer credential).
    logger.info(f"Budget Manager received: action={event.get('action')}, "
                f"model_id={event.get('model_id')}, request_id={event.get('request_id')}, "
                f"correlation_id={event.get('correlation_id')}, tenant_id={event.get('tenant_id')}")

    # Initialize shared service
    dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)

    action = event.get('action', 'reserve')

    if action == 'reserve':
        request_id = event.get('request_id', 'unknown')
        model_id = event.get('model_id')
        task_token = event.get('task_token')
        execution_arn = event.get('execution_arn')

        # P2: Generate correlation_id for distributed tracing
        correlation_id = event.get('correlation_id') or str(uuid.uuid4())[:12]
        # P0: Extract tenant_id for per-tenant observability
        tenant_id = event.get('tenant_id', 'unknown')

        logger.info(f"Reserve request: request_id={request_id}, correlation_id={correlation_id}, "
                    f"tenant_id={tenant_id}, model_id={model_id}")

        # Extract request_payload from event or construct from input
        # Supports both:
        #   - {request_payload: {prompt: "..."}} - explicit payload
        #   - {input: {prompt: "..."}} - loose params passed via Step Functions
        request_payload = event.get('request_payload', {})
        if not request_payload:
            # Try to extract from nested input object (passed by Step Functions)
            input_obj = event.get('input', {})
            if input_obj.get('prompt'):
                request_payload = {
                    'prompt': input_obj.get('prompt'),
                    'max_tokens': input_obj.get('max_tokens', 100),
                    # Carry temperature only if explicitly provided; the Bedrock
                    # Processor strips it for next-gen Claude models regardless.
                    **({'temperature': input_obj['temperature']}
                       if 'temperature' in input_obj else {})
                }
            elif input_obj.get('request_payload'):
                request_payload = input_obj.get('request_payload')

        # Validate required fields
        if not model_id or not isinstance(model_id, str):
            error_msg = "model_id is required and must be a non-empty string"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'error': error_msg,
                'queued': False
            }

        # Validate model_id format: alphanumeric, dots, hyphens, colons, underscores only
        # Max 256 chars (Bedrock model IDs are typically under 100 chars)
        if len(model_id) > 256 or not re.match(r'^[a-zA-Z0-9._:/-]+$', model_id):
            error_msg = f"model_id format invalid: must be <= 256 chars, alphanumeric/dots/hyphens/colons/underscores/slashes only, got: {model_id[:64]!r}"
            logger.error(error_msg)
            _record_status(
                dynamo_service, request_id=request_id, state='FAILED',
                http_status=400, reason='validation_error',
                correlation_id=correlation_id, tenant_id=tenant_id,
                model_id=model_id, source='immediate',
            )
            return {
                'statusCode': 400,
                'error': error_msg,
                'queued': False
            }

        if not task_token:
            error_msg = "task_token is required for Step Functions callback"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'error': error_msg,
                'queued': False
            }

        # Input validation: reject oversized payloads BEFORE consuming burst slots
        # Also pre-fetches config to avoid duplicate DynamoDB read in try_reserve_allocation_leaky_bucket()
        config = None
        try:
            config = dynamo_service.get_effective_capacity(model_id)
            max_tokens_per_request = int(config.get('max_tokens_per_request', 4096))
        except Exception:
            max_tokens_per_request = 4096  # Safe default if config unavailable

        requested_max_tokens = int(request_payload.get('max_tokens', 100)) if request_payload else 100
        if requested_max_tokens > max_tokens_per_request:
            error_msg = (f"max_tokens ({requested_max_tokens}) exceeds limit ({max_tokens_per_request}). "
                         f"Reduce max_tokens or update max_tokens_per_request in model config.")
            logger.error(f"Input validation rejected: {error_msg}")
            try:
                sfn_client.send_task_failure(
                    taskToken=task_token,
                    error='InputValidationError',
                    cause=error_msg
                )
            except Exception as sfn_err:
                logger.error(f"Failed to send task failure callback: {sfn_err}")
            _record_status(
                dynamo_service, request_id=request_id, state='FAILED',
                http_status=400, reason='validation_error',
                correlation_id=correlation_id, tenant_id=tenant_id,
                model_id=model_id, source='immediate',
            )
            return {
                'statusCode': 400,
                'error': error_msg,
                'queued': False
            }

        prompt_bytes = len((request_payload.get('prompt', '') if request_payload else '').encode('utf-8'))
        max_prompt_bytes = 1_048_576  # 1 MB
        if prompt_bytes > max_prompt_bytes:
            error_msg = (f"Prompt size ({prompt_bytes} bytes) exceeds limit ({max_prompt_bytes} bytes / 1MB). "
                         f"Reduce prompt size.")
            logger.error(f"Input validation rejected: {error_msg}")
            try:
                sfn_client.send_task_failure(
                    taskToken=task_token,
                    error='InputValidationError',
                    cause=error_msg
                )
            except Exception as sfn_err:
                logger.error(f"Failed to send task failure callback: {sfn_err}")
            _record_status(
                dynamo_service, request_id=request_id, state='FAILED',
                http_status=400, reason='validation_error',
                correlation_id=correlation_id, tenant_id=tenant_id,
                model_id=model_id, source='immediate',
            )
            return {
                'statusCode': 400,
                'error': error_msg,
                'queued': False
            }

        # Try to reserve using leaky bucket (with TPM estimation from payload)
        # Pass pre-fetched config to avoid duplicate get_effective_capacity() call
        success, metadata = try_reserve_allocation_leaky_bucket(
            dynamo_service, model_id, request_id, task_token, execution_arn,
            request_payload=request_payload,
            correlation_id=correlation_id,
            tenant_id=tenant_id,
            config=config
        )

        # Emit EMF metrics for observability (never crash the handler).
        # QueueDepth was dropped here: it required a get_queue_depth() Select='COUNT'
        # scan of the ENTIRE queue partition on EVERY admission (once per request).
        # Under a submission burst that scan spiked RCU on the single QUEUE#ITEMS hot
        # partition past its ceiling and threw ThrottlingException. RequestQueued comes
        # straight from metadata (no DB read) and is retained; backlog depth, if needed,
        # should be derived from a bounded read or CloudWatch, not a per-request scan.
        try:
            metric_defs = [
                {"Name": "RequestQueued", "Unit": "Count"},
            ]
            emf = {
                "_aws": {
                    "Timestamp": int(time.time() * 1000),
                    "CloudWatchMetrics": [{
                        "Namespace": "BedrockShaper",
                        "Dimensions": [["ServiceName", "model_id"]],
                        "Metrics": metric_defs
                    }]
                },
                "ServiceName": "TrafficShaper",
                "model_id": model_id,
                "correlation_id": correlation_id,
                "tenant_id": tenant_id,
                "RequestQueued": 1 if metadata.get('queued') else 0,
            }
            print(json.dumps(emf))
        except Exception as e:
            logger.warning(f"EMF metric emission failed (non-fatal): {e}")

        if success:
            # Capacity available - invoke Bedrock Processor async
            allocation_id = f"{metadata['timestamp_ms']}#{request_id}"
            logger.info(f"Allocation successful, invoking Bedrock Processor async: request_id={request_id}, "
                        f"model_id={model_id}, correlation_id={correlation_id}, tenant_id={tenant_id}")

            if not request_payload:
                # This is a bug: capacity was reserved but no payload exists to send.
                # Send task failure callback and return error instead of silently fabricating a prompt.
                error_msg = (f"BUG: request_payload is empty after capacity reservation. "
                             f"request_id={request_id}, model_id={model_id}")
                logger.error(error_msg)
                try:
                    sfn_client.send_task_failure(
                        taskToken=task_token,
                        error='MissingRequestPayload',
                        cause=error_msg
                    )
                except Exception as sfn_err:
                    logger.error(f"Failed to send task failure callback: {sfn_err}")
                return {
                    'statusCode': 500,
                    'error': error_msg,
                    'request_id': request_id
                }

            # Invoke Bedrock Processor asynchronously with direct payload
            lambda_client.invoke(
                FunctionName=BEDROCK_PROCESSOR_ARN,
                InvocationType='Event',  # Async invocation
                Payload=json.dumps({
                    'task_token': task_token,
                    'model_id': model_id,
                    'request_id': request_id,
                    'request_payload': request_payload,
                    'allocation_id': allocation_id,
                    'correlation_id': correlation_id,
                    'tenant_id': tenant_id
                })
            )

            # Lambda exits, Step Functions waits for callback from processor
            return {
                'statusCode': 200,
                'body': {
                    'message': 'Bedrock processor invoked async',
                    'request_id': request_id,
                    'allocation_id': allocation_id,
                    'correlation_id': correlation_id,
                    'tenant_id': tenant_id
                }
            }
        else:
            # Queued - trigger queue processor
            # Don't send callback here - Queue Processor will resume via Bedrock Processor
            # Honest-outcomes: mark the PENDING->QUEUED transition (202, non-terminal).
            # Single convergence point for BOTH the runtime burst-gate enqueue and the
            # mantle _enqueue_on_reject path — both surface here as success=False. The
            # true terminal (SUCCEEDED/FAILED) is written later when the queue drains.
            _record_status(
                dynamo_service, request_id=request_id, state='QUEUED',
                http_status=202, reason=None,
                correlation_id=correlation_id, tenant_id=tenant_id,
                model_id=model_id, source='queued',
            )

            try:
                trigger_queue_processor(model_id, dynamo_service)
            except Exception as e:
                logger.error(f"Failed to trigger processor: {e}")
                # Don't fail the request if trigger fails

            logger.info(f"Request enqueued, awaiting processing: request_id={request_id}, "
                        f"correlation_id={correlation_id}, tenant_id={tenant_id}")
            return {
                'statusCode': 200,
                'body': {
                    'message': 'Request enqueued, awaiting processing',
                    'request_id': request_id,
                    'correlation_id': correlation_id,
                    'tenant_id': tenant_id
                }
            }

    elif action == 'release':
        # No-op in leaky bucket (TTL handles cleanup)
        # Keep for Step Functions compatibility
        return {
            'statusCode': 200,
            'body': {
                'message': 'Release acknowledged (TTL handles cleanup)',
                'allocation_id': event.get('allocation_id')
            }
        }

    else:
        return {
            'statusCode': 400,
            'body': {'error': f'Unknown action: {action}'}
        }
