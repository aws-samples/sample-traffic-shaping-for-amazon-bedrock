"""Queue Processor Lambda - Phase 3 - Single table queue with leaky bucket and batch processing."""

import boto3
import os
import logging
import json
import time
from collections import deque
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from shared_service import DynamoService
from typing import Dict, Any, List, Tuple

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SINGLE_TABLE_NAME = os.environ.get('SINGLE_TABLE_NAME')
BEDROCK_PROCESSOR_ARN = os.environ.get('BEDROCK_PROCESSOR_ARN')

# Clients
lambda_client = boto3.client('lambda')
eventbridge = boto3.client('events')

# EventBridge event constants — mirror budget_manager's QueueProcessingRequired
# emit so the same rule (source ∈ {budget-manager, queue-processor}) re-triggers
# this Lambda. See semaphore_stack.py:312.
SEMAPHORE_ID = "semaphore#default"

# Import lock constants from shared service
from shared_service.dynamo import LOCK_HEARTBEAT_INTERVAL

# Sub-minute window for the RPM/TPM 2s gates (mirrors the sim's short_window_sec).
SHORT_WINDOW_SEC = 2.0

# Fire-and-forget dispatch pool size. Each dequeued item's Bedrock-processor invoke
# is submitted to this pool so the ~40ms async-invoke handoff runs on a worker thread
# and OVERLAPS the pacer's inter-item sleep, instead of stacking serially on top of it.
# The serial invoke cost was what capped the single loop at ~10 req/s / 4.3M TPM even
# though the pacer targeted ~55ms/item (see queue-processor-throughput-session-summary).
#
# Sized DELIBERATELY SMALL (not a generous pool): in-flight invokes ≈ invoke_latency /
# pacer_interval ≈ 40ms / ~57ms < 1, so ~1 invoke is ever in flight at the target rate.
# A large pool (tried 10) lets a freshly-dequeued batch fire ~10 invokes near-
# simultaneously, re-creating the sub-second Bedrock burst the even-spacing pacer exists
# to prevent — throttles clustered in single minutes even with 12% quota headroom. Three
# workers keep the handoff off the critical path with one slot of slack for a slow invoke,
# WITHOUT letting the pool bunch calls faster than the pacer spaced the submissions.
DISPATCH_POOL_WORKERS = 1


def _flat_tpm_estimate(config: Dict[str, Any], queue_target_tpm: int = 0) -> int:
    """Per-slot TPM estimate for a dequeued item that carries NO per-request estimate.

    This is a FALLBACK; the accurate path is the budget manager's estimated_tokens
    on the queue item (see budget_manager Step 1b, now computed unconditionally).
    The fallback still matters as defense in depth: the queue processor paces every
    token gate (incl. the Gate 5 even-spacing pacer) on this value, so a fallback
    that badly under-counts real cost collapses all token gating and lets the drain
    overshoot queue_target_tpm and the account quota (→ throttles). See
    scripts/test_queue_overshoot_sim.py for the reproduction.

    Preference order:
      1. Explicit config: default_max_tokens (× burndown) + nominal_input_tokens.
      2. queue_target_tpm / queue_capacity — the target-implied average tokens per
         request. This is the safest quota-derived default: pacing on it holds the
         real rate at ~target even when the per-request estimate is missing.
      3. A conservative 4096-token floor — NOT 1024. 1024 is smaller than almost any
         real request (a bare prompt + a few hundred output tokens), so it was the
         amplifier that turned a missing estimate into a 6× overshoot.
    """
    default_max_tokens = config.get('default_max_tokens', config.get('max_tokens_per_request'))
    if default_max_tokens is not None:
        burndown_rate = float(config.get('output_token_burndown_rate', 1.0))
        nominal_input_tokens = int(config.get('nominal_input_tokens', 0))
        return int(int(default_max_tokens) * burndown_rate) + nominal_input_tokens

    queue_capacity = int(config.get('queue_capacity', 0) or 0)
    if queue_target_tpm > 0 and queue_capacity > 0:
        return max(1, int(queue_target_tpm / queue_capacity))

    return 4096


def _sleep_for_token_budget(dispatch_log, item_tokens: int, window_sec: float,
                            cap: int, now: float) -> float:
    """Return the minimum sleep until (tokens_in_window + item_tokens) <= cap.

    Walks the in-window dispatch entries oldest-first, accumulating freed tokens,
    and returns the delay until the last needed entry rolls off the window edge.
    Mirrors sleep_for_token_budget() in run_proposed_token_aware() (the sim). Used
    by both the 2s and 60s TPM gates — the sleep target is NOT a fixed interval.
    """
    in_win = sorted(
        ((ts, tok) for ts, tok in dispatch_log if ts >= now - window_sec),
        key=lambda e: e[0],
    )
    deficit = sum(tok for _, tok in in_win) + item_tokens - cap
    if deficit <= 0:
        return 0.0
    freed = 0
    sleep_until = now
    for ts, tok in in_win:          # oldest first
        freed += tok
        sleep_until = ts + window_sec
        if freed >= deficit:
            break
    return max(0.001, sleep_until - now + 0.005)


def _token_gate_sleep(dispatch_log, item_tokens: int, token_index: int,
                      window_sec: float, cap: int, now: float) -> float:
    """Return the sleep required for one token dimension to fit its window."""
    if cap <= 0 or item_tokens <= 0:
        return 0.0

    in_window = [
        (entry[0], entry[token_index])
        for entry in dispatch_log
        if entry[0] >= now - window_sec and entry[token_index] > 0
    ]
    consumed = sum(tokens for _, tokens in in_window)

    if item_tokens > cap:
        if not in_window:
            return 0.0
        newest = max(timestamp for timestamp, _ in in_window)
        return max(0.001, newest + window_sec - now + 0.005)

    if consumed + item_tokens <= cap:
        return 0.0

    return _sleep_for_token_budget(
        in_window, item_tokens, window_sec, cap, now
    )


def trigger_successor(model_id: str) -> None:
    """Emit a QueueProcessingRequired event so a FRESH invocation resumes the drain.

    The queue processor is capped at ~13 min (MAX_RUNTIME) before the 15-min Lambda
    ceiling, so a single invocation cannot drain a backlog larger than one window.
    The intended design is for the processor to wake its own successor on exit; the
    EventBridge rule already whitelists source 'queue-processor' (semaphore_stack.py:312)
    and the Lambda already has events:PutEvents (semaphore_stack.py:267). This is the
    missing code emit. MUST be called AFTER the processor lock is released so the
    successor can acquire the lock instead of bouncing off the one we still hold.
    """
    try:
        eventbridge.put_events(
            Entries=[{
                'Source': 'queue-processor',
                'DetailType': 'QueueProcessingRequired',
                'Detail': json.dumps({
                    'semaphore_id': SEMAPHORE_ID,
                    'model_id': model_id,
                    'timestamp': datetime.utcnow().isoformat()
                })
            }]
        )
        logger.info(f"Re-triggered queue processor via EventBridge for model: {model_id}")
    except Exception as e:
        # Don't fail the drain if the re-trigger emit fails; a later budget-manager
        # enqueue (or the next scheduled trigger) can still wake the processor.
        logger.error(f"Failed to re-trigger queue processor via EventBridge: {e}")


def try_reserve_queue_capacity_batch(
    dynamo_service: DynamoService,
    model_id: str,
    batch_size: int,
    processor_id: str
) -> Tuple[bool, Dict[str, Any]]:
    """
    Reserve batch capacity using leaky bucket pattern.
    Reserves up to batch_size tokens based on available capacity.
    Gates on BOTH RPM and TPM — whichever dimension is tighter.

    Args:
        dynamo_service: DynamoDB service instance
        model_id: The Bedrock model ID
        batch_size: Desired number of tokens to reserve
        processor_id: Processor ID for request tracking

    Returns:
        tuple: (success: bool, metadata: dict)
            - success: True if any capacity was reserved
            - metadata: Contains 'reserved' count, 'available_capacity', or 'wait_seconds'
    """
    # Step 1: Get model config (required)
    try:
        config = dynamo_service.get_effective_capacity(model_id)
        adaptive_shift = config.get('_adaptive_shift', 0)
        if adaptive_shift:
            logger.info(f"Adaptive capacity: shifted {adaptive_shift} tokens from burst to queue for model={model_id}")
        queue_capacity = int(config['queue_capacity'])
        queue_regen_rate = float(config['queue_regeneration_rate'])
        # TPM config (optional)
        tpm_queue_capacity = int(config.get('tpm_queue_capacity', 0))
        tpm_queue_regen_rate = float(config.get('tpm_queue_regeneration_rate', 0))
        # Tier 2: mantle split-quota queue config (optional; only set on backend=mantle)
        backend = config.get('backend', 'runtime')
        itpm_queue_capacity = int(config.get('itpm_queue_capacity', 0))
        itpm_queue_regen_rate = float(config.get('itpm_queue_regeneration_rate', 0))
        otpm_queue_capacity = int(config.get('otpm_queue_capacity', 0))
        otpm_queue_regen_rate = float(config.get('otpm_queue_regeneration_rate', 0))
        # Stage-1a flat per-slot TPM estimate. The processor writes consumption
        # records BEFORE it dequeues items, so it has no per-request prompt at
        # reserve time — seed each record with a config-derived flat estimate so
        # the 60s TPM window-sum is non-zero and trackable. Reconcile-to-actuals
        # (stage 1b) will later overwrite these with real usage. Uses the shared
        # _flat_tpm_estimate() so it can never collapse to the catastrophic 1024
        # default (see the helper's docstring + test_queue_overshoot_sim.py).
        burndown_rate = float(config.get('output_token_burndown_rate', 1.0))
        nominal_input_tokens = int(config.get('nominal_input_tokens', 0))
        flat_tpm_estimate = _flat_tpm_estimate(config, int(config.get('queue_target_tpm', 0)))
        flat_output_estimate = max(1, flat_tpm_estimate - nominal_input_tokens)
        # Sub-minute (2s) rate cap — same smoothing the burst gate applies, done here
        # as a FREE in-memory filter of the 60s records already fetched below (the
        # queue processor holds a single-owner lock, so a read-then-decide is race-free
        # unlike the concurrent burst path, which uses an atomic counter instead).
        # Defaults to queue_regeneration_rate (≈ the drain's sustained quota req/s).
        short_window_rps = float(config.get('short_window_rps', 0)) or queue_regen_rate
    except KeyError:
        raise ValueError(f"Model config not found: {model_id}. Run 'make create-config MODEL=...'")
    except Exception as e:
        raise ValueError(f"Error loading model config for {model_id}: {e}")

    # Step 2: Query current consumption to determine available capacity
    consumption_records = dynamo_service.query_queue_consumption_records(
        model_id=model_id,
        window_seconds=60,
        consistent_read=True
    )

    current_time = time.time()
    available_capacity = dynamo_service.calculate_available_tokens(
        capacity=queue_capacity,
        consumption_records=consumption_records,
        regeneration_rate=queue_regen_rate,
        current_time=current_time
    )

    # Step 2a: Sub-minute (2s) rate cap. FREE in-memory filter of the 60s records
    # already fetched above — count how many were dispatched in the last 2s and cap
    # THIS batch's reservation so the 2s total stays under short_window_cap. This
    # smooths the drain to ~the sustained quota rate, preventing the same
    # instantaneous-over-dispatch that drains Bedrock's sub-minute bucket. Race-free:
    # the queue processor holds a single-owner lock, so this reader is the only writer.
    SHORT_WINDOW_SECONDS = 2
    cutoff_ms = int((current_time - SHORT_WINDOW_SECONDS) * 1000)
    recent_records = [
        r for r in consumption_records
        if int(r['sk'].split('#')[0]) >= cutoff_ms
    ]

    # RPS dimension: cap requests dispatched per 2s.
    short_window_cap = max(1, int(short_window_rps * SHORT_WINDOW_SECONDS))
    recent_2s = len(recent_records)
    short_window_headroom = max(0, short_window_cap - recent_2s)

    # TPS dimension: cap TOKENS dispatched per 2s (sized from the per-second TPM queue
    # regen rate). This is the sub-minute token bound — the dimension that maps to
    # mantle's token-only quota and to any large-request workload. Convert token
    # headroom into a slot count using the flat per-slot estimate. Both dimensions are
    # a FREE in-memory filter of the 60s records already fetched (no extra read).
    tps_slot_headroom = short_window_headroom  # default: TPS not gating
    if tpm_queue_regen_rate > 0 and flat_tpm_estimate > 0:
        short_window_tps_cap = max(1, int(tpm_queue_regen_rate * SHORT_WINDOW_SECONDS))
        recent_tokens_2s = sum(int(r.get('estimated_tokens', 0) or 0) for r in recent_records)
        token_headroom = max(0, short_window_tps_cap - recent_tokens_2s)
        tps_slot_headroom = token_headroom // flat_tpm_estimate

    # Bind on whichever sub-minute dimension is tighter (mirrors the 60s min-of-both).
    short_window_headroom = min(short_window_headroom, tps_slot_headroom)
    if short_window_headroom <= 0:
        # 2s window full on RPS or TPS — wait for it to roll rather than dispatch a spike.
        logger.info(f"Sub-minute cap reached: recent_2s={recent_2s}, rps_cap={short_window_cap}, "
                    f"tps_slot_headroom={tps_slot_headroom}, waiting")
        return (False, {
            'reason': 'short_window_rate_cap',
            'available_capacity': available_capacity,
            'recent_2s': recent_2s,
            'short_window_cap': short_window_cap,
            'wait_seconds': 1.0,
        })

    # Step 2b: Check TPM capacity (if configured)
    available_tpm = None
    tpm_limited_batch = batch_size
    if tpm_queue_capacity > 0:
        available_tpm = dynamo_service.calculate_available_tpm(
            tpm_capacity=tpm_queue_capacity,
            consumption_records=consumption_records,
            tpm_regeneration_rate=tpm_queue_regen_rate,
            current_time=current_time
        )
        logger.info(f"TPM queue check: available_tpm={available_tpm:.0f}, "
                    f"tpm_queue_capacity={tpm_queue_capacity}")

        if available_tpm <= 0:
            wait_seconds = max(1.0, (1 - available_tpm) / tpm_queue_regen_rate) if tpm_queue_regen_rate > 0 else 10
            logger.info(f"No TPM queue capacity: {available_tpm:.0f}, wait_seconds={wait_seconds:.2f}")
            return (False, {
                'reason': 'no_tpm_capacity',
                'available_capacity': available_capacity,
                'available_tpm': available_tpm,
                'wait_seconds': wait_seconds
            })

    # Step 2c: Mantle split-quota gate — drain only when BOTH iTPM and oTPM queue
    # capacity is available (min-of-both). oTPM (the tighter 2M bucket) usually binds first.
    if backend == 'mantle' and (itpm_queue_capacity > 0 or otpm_queue_capacity > 0):
        available_itpm = dynamo_service.calculate_available_split_tpm(
            tpm_capacity=itpm_queue_capacity, consumption_records=consumption_records,
            tpm_regeneration_rate=itpm_queue_regen_rate, dimension='INPUT', current_time=current_time,
        )
        available_otpm = dynamo_service.calculate_available_split_tpm(
            tpm_capacity=otpm_queue_capacity, consumption_records=consumption_records,
            tpm_regeneration_rate=otpm_queue_regen_rate, dimension='OUTPUT', current_time=current_time,
        )
        logger.info(f"Mantle queue check: available_itpm={available_itpm:.0f}, available_otpm={available_otpm:.0f}")
        tighter, regen = (available_otpm, otpm_queue_regen_rate)
        if available_itpm < available_otpm:
            tighter, regen = (available_itpm, itpm_queue_regen_rate)
        if tighter <= 0:
            wait_seconds = max(1.0, (1 - tighter) / regen) if regen > 0 else 10
            logger.info(f"No mantle queue capacity (tighter dim={tighter:.0f}), wait_seconds={wait_seconds:.2f}")
            return (False, {
                'reason': 'no_mantle_capacity',
                'available_capacity': available_capacity,
                'available_itpm': available_itpm,
                'available_otpm': available_otpm,
                'wait_seconds': wait_seconds,
            })

    # Step 3: Determine how many tokens we can reserve (RPM dimension)
    if available_capacity <= 0:
        # Calculate wait time until 1 token is available
        wait_seconds = (1 - available_capacity) / queue_regen_rate
        logger.info(f"No RPM queue capacity available: {available_capacity:.2f}, wait_seconds={wait_seconds:.2f}")
        return (False, {
            'reason': 'no_capacity',
            'available_capacity': available_capacity,
            'available_tpm': available_tpm,
            'wait_seconds': wait_seconds
        })

    # Reserve up to available capacity, but not more than requested batch_size, and
    # never more than the sub-minute (2s) headroom — this is what actually smooths
    # the drain rate below Bedrock's instantaneous bucket limit.
    actual_reserve = min(batch_size, int(available_capacity), short_window_headroom)
    if actual_reserve < 1:
        # Fractional capacity available but less than 1
        wait_seconds = (1 - available_capacity) / queue_regen_rate
        return (False, {
            'reason': 'insufficient_capacity',
            'available_capacity': available_capacity,
            'available_tpm': available_tpm,
            'wait_seconds': wait_seconds
        })

    # Step 4: Write consumption records for each reserved token
    consumption_records_written = []
    timestamp_base = int(time.time() * 1000)

    for i in range(actual_reserve):
        request_id = f'{processor_id}_batch_{timestamp_base}_{i}'
        try:
            # Stage 1a: seed the consumption record with the flat per-slot estimate
            # so the TPM window-sum is non-zero and trackable. Mantle splits the
            # estimate across input/output; runtime tracks the combined value only.
            if backend == 'mantle':
                result = dynamo_service.put_queue_consumption(
                    model_id, request_id,
                    estimated_tokens=flat_tpm_estimate,
                    estimated_input_tokens=nominal_input_tokens,
                    estimated_output_tokens=flat_output_estimate,
                )
            else:
                result = dynamo_service.put_queue_consumption(
                    model_id, request_id, estimated_tokens=flat_tpm_estimate,
                )
            consumption_records_written.append({
                'request_id': request_id,
                'timestamp_ms': result['timestamp_ms']
            })
        except Exception as e:
            logger.warning(f"Failed to write consumption record {i}: {e}")
            # Continue - we'll reserve what we can

    reserved_count = len(consumption_records_written)

    if reserved_count == 0:
        return (False, {
            'reason': 'write_failed',
            'available_capacity': available_capacity
        })

    # TPM consumed in the last 60s = sum of estimated_tokens over the records already
    # queried above (free — no extra read). This is the "consumed" signal the dispatch
    # log pairs with each request's own estimate to compute would_exceed.
    tpm_consumed_last_60s = sum(
        int(r.get('estimated_tokens', 0) or 0) for r in consumption_records
    )

    logger.info(f"Batch capacity reserved: requested={batch_size}, reserved={reserved_count}, "
                f"available={available_capacity:.2f}, flat_tpm_estimate={flat_tpm_estimate}, "
                f"tpm_consumed_last_60s={tpm_consumed_last_60s}")

    return (True, {
        'reserved': reserved_count,
        'available_capacity': available_capacity,
        'consumption_records': consumption_records_written,
        'flat_tpm_estimate': flat_tpm_estimate,
        'tpm_consumed_last_60s': tpm_consumed_last_60s,
        'tpm_queue_capacity': tpm_queue_capacity,
    })


def process_single_item(item: Dict[str, Any], model_id: str) -> Dict[str, Any]:
    """
    Forward item to Bedrock Processor for execution.

    Bedrock Processor will:
    - Call describe_execution to get original payload from execution_arn
    - Invoke Bedrock
    - Send Step Functions callback

    Args:
        item: Queue item with task_token, execution_arn, request_id
        model_id: Bedrock model ID for invocations

    Returns:
        Dict with 'item', 'success', 'error' keys
    """
    request_id = item.get('request_id', 'unknown')
    correlation_id = item.get('correlation_id', '')
    task_token = item.get('task_token')
    execution_arn = item.get('execution_arn')
    # Honest-outcomes (Cato C-5): bedrock_processor requires tenant_id to have
    # propagated or it fails the request without spending quota. enqueue_request
    # stores it on the queue item; forward it through the async invoke so the
    # queued path carries the same propagation the immediate (SFN) path does.
    tenant_id = item.get('tenant_id')

    if not task_token or not execution_arn:
        logger.error(f"Missing task_token or execution_arn: request_id={request_id}, correlation_id={correlation_id}")
        return {'item': item, 'success': False, 'error': 'Missing task_token or execution_arn'}

    try:
        # Invoke Bedrock Processor with execution_arn (it will resolve payload)
        lambda_client.invoke(
            FunctionName=BEDROCK_PROCESSOR_ARN,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps({
                'task_token': task_token,
                'model_id': model_id,
                'request_id': request_id,
                'tenant_id': tenant_id,
                'execution_arn': execution_arn,  # Processor will call describe_execution
                'correlation_id': correlation_id
            })
        )
        logger.info(f"Forwarded to Bedrock Processor: request_id={request_id}, correlation_id={correlation_id}")
        return {'item': item, 'success': True, 'error': None}

    except Exception as e:
        logger.error(f"Failed to invoke Bedrock Processor: request_id={request_id}, correlation_id={correlation_id}, error={e}")
        return {'item': item, 'success': False, 'error': str(e)}


def handler(event, context):
    """Process queued requests using batch dequeuing and parallel execution."""
    logger.info(f"Queue Processor triggered: {json.dumps(event)}")

    # Extract model_id from EventBridge event
    model_id = event.get('detail', {}).get('model_id')
    if not model_id:
        logger.error("No model_id in event, cannot process queue")
        return {'processed': 0, 'error': 'No model_id in event'}

    logger.info(f"Processing queue for model: {model_id}")

    # Initialize shared service
    dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)

    # Load configuration from DynamoDB (with fallbacks)
    try:
        config = dynamo_service.get_effective_capacity(model_id)
        adaptive_shift = config.get('_adaptive_shift', 0)
        if adaptive_shift:
            logger.info(f"Adaptive capacity: shifted {adaptive_shift} tokens from burst to queue for model={model_id}")
        batch_size = int(config.get('queue_batch_size', 10))
        queue_capacity = int(config.get('queue_capacity', 100))
        queue_regen_rate = float(config.get('queue_regeneration_rate', 0.75))
        # TOKEN-AWARE dispatch config (optional; 0 = TPM gating disabled)
        tpm_queue_capacity = int(config.get('tpm_queue_capacity', 0))
        tpm_queue_regen_rate = float(config.get('tpm_queue_regeneration_rate', 0))
        backend = config.get('backend', 'runtime')
        itpm_queue_capacity = int(config.get('itpm_queue_capacity', 0))
        itpm_queue_regen_rate = float(config.get('itpm_queue_regeneration_rate', 0))
        otpm_queue_capacity = int(config.get('otpm_queue_capacity', 0))
        otpm_queue_regen_rate = float(config.get('otpm_queue_regeneration_rate', 0))
        # Sub-minute rate cap defaults to the sustained queue quota (req/s)
        short_window_rps = float(config.get('short_window_rps', 0)) or queue_regen_rate
        # EVEN-SPACING pacer target (tokens/min). When > 0, each item waits
        # item_tokens / (target/60) seconds since the previous dispatch — a GCRA/
        # leaky-bucket pacer that holds the ACTUAL Bedrock arrival rate at `target`
        # with no batch clumping or sub-second bursts (the 2s-window gates allowed
        # 9-13M 1s peaks that throttled — see "Ideal queue processor configuration").
        # 0 = disabled (fall back to the four sliding-window gates only).
        queue_target_tpm = int(config.get('queue_target_tpm', 0))
        # Flat per-slot TPM estimate — used when a dequeued item carries no
        # per-request estimate (runtime path). Mirrors try_reserve_*'s derivation.
        flat_tpm_estimate = _flat_tpm_estimate(config, queue_target_tpm)
    except (KeyError, Exception) as e:
        logger.warning(f"Config not found for {model_id}, using defaults: {e}")
        batch_size = 10
        queue_capacity = 100
        queue_regen_rate = 0.75
        tpm_queue_capacity = 0
        tpm_queue_regen_rate = 0
        backend = 'runtime'
        itpm_queue_capacity = 0
        itpm_queue_regen_rate = 0
        otpm_queue_capacity = 0
        otpm_queue_regen_rate = 0
        short_window_rps = queue_regen_rate
        # 4096, not 1024: a missing config should not seed a per-slot estimate
        # smaller than a real request and collapse the token gates. See
        # _flat_tpm_estimate() / test_queue_overshoot_sim.py.
        flat_tpm_estimate = 4096
        queue_target_tpm = 0

    MAX_RUNTIME = 13 * 60  # 13 minutes (2 min buffer before Lambda timeout)

    # Derived window caps for the four in-memory dispatch gates (mirror the sim).
    rpm_2s_cap = max(1, int(short_window_rps * SHORT_WINDOW_SEC))
    tpm_2s_cap = int(tpm_queue_regen_rate * SHORT_WINDOW_SEC) if tpm_queue_regen_rate > 0 else 0
    itpm_2s_cap = int(itpm_queue_regen_rate * SHORT_WINDOW_SEC) if itpm_queue_regen_rate > 0 else 0
    otpm_2s_cap = int(otpm_queue_regen_rate * SHORT_WINDOW_SEC) if otpm_queue_regen_rate > 0 else 0
    # Even-spacing pacer: target tokens/second (0 = disabled).
    queue_target_tps = queue_target_tpm / 60.0 if queue_target_tpm > 0 else 0.0
    logger.info(f"Config loaded: batch_size={batch_size}, queue_capacity={queue_capacity}, "
                f"rpm_2s_cap={rpm_2s_cap}, tpm_2s_cap={tpm_2s_cap}, "
                f"itpm_2s_cap={itpm_2s_cap}, otpm_2s_cap={otpm_2s_cap}, "
                f"tpm_queue_capacity={tpm_queue_capacity}, flat_tpm_estimate={flat_tpm_estimate}, "
                f"queue_target_tpm={queue_target_tpm}")

    # Acquire processing lock using heartbeat-based locking
    processor_id = context.aws_request_id
    if not dynamo_service.acquire_processor_lock(model_id, processor_id):
        logger.info("Another processor running (active lock exists), exiting")
        return {'processed': 0, 'message': 'Another processor running'}

    logger.info(f"Acquired processor lock: model={model_id}, processor_id={processor_id}")

    # Whether to wake a successor invocation on exit. Set True only when the last
    # dispatched batch was FULL — a strong "there is still work and we were draining
    # at full clip, not starved" signal — so we hand off before the 13-min ceiling
    # instead of stranding the backlog (Bug 2). The flag resets to False at the top
    # of every invocation, so a successor that immediately hits an empty queue or a
    # long capacity wait will NOT re-trigger, which prevents a busy re-trigger loop.
    should_reschedule = False

    try:
        processed_count = 0
        failed_count = 0
        consecutive_batch_failures = 0
        last_heartbeat = time.time()
        start_time = time.time()

        # Rolling in-memory dispatch log of
        # (timestamp, combined_tokens, input_tokens, output_tokens).
        dispatch_log = deque()
        last_resync = time.time()
        # Even-spacing pacer state: wall-clock time of the previous dispatch. The next
        # item may not dispatch until item_tokens/target_tps seconds after this.
        last_dispatch_ts = None

        # Persistent fire-and-forget dispatch pool. Each item's invoke is submitted here
        # (a ~µs handoff) so the ~40ms blocking invoke runs on a worker thread and overlaps
        # the pacer sleep, keeping the paced loop's per-item cost off the critical path.
        # Success/failure is reconciled at the chunk boundary (futures have resolved by
        # then), preserving the per-item error-recording and circuit-breaker semantics.
        dispatch_executor = ThreadPoolExecutor(max_workers=DISPATCH_POOL_WORKERS)

        while time.time() - start_time < MAX_RUNTIME:
            # Heartbeat check - refresh lock TTL every LOCK_HEARTBEAT_INTERVAL seconds
            if time.time() - last_heartbeat >= LOCK_HEARTBEAT_INTERVAL:
                if not dynamo_service.refresh_processor_heartbeat(model_id, processor_id):
                    logger.warning(f"Lost lock ownership, exiting: model={model_id}")
                    return {
                        'processed': processed_count,
                        'failed': failed_count,
                        'status': 'lock_lost'
                    }
                logger.info(f"Heartbeat refreshed: model={model_id}, processed={processed_count}")
                last_heartbeat = time.time()

            # Periodic re-sync: every 60s, rebuild dispatch_log from DynamoDB actuals.
            # This corrects estimate-vs-actual drift (and recovers a warm successor's
            # window) without paying a read per item.
            if time.time() - last_resync >= 60.0:
                try:
                    records = dynamo_service.query_queue_consumption_records(
                        model_id=model_id, window_seconds=60, consistent_read=False
                    )
                    rebuilt = deque()
                    for r in records:
                        ts = int(r['sk'].split('#')[0]) / 1000.0
                        combined = int(r.get('estimated_tokens') or 0)
                        input_tokens = int(r.get('estimated_input_tokens') or 0)
                        output_tokens = int(r.get('estimated_output_tokens') or 0)
                        rebuilt.append((ts, combined, input_tokens, output_tokens))
                    dispatch_log = deque(sorted(rebuilt, key=lambda e: e[0]))
                    logger.info(f"dispatch_log resynced from DynamoDB: {len(dispatch_log)} records")
                except Exception as e:
                    logger.warning(f"dispatch_log resync failed (non-fatal): {e}")
                last_resync = time.time()

            # Dequeue a chunk from DynamoDB. This is JUST a DB fetch (no reservation);
            # the four in-memory gates below pace each item individually. The bounded
            # dequeue (Limit=batch_size) is also the loop's sole "is there work?" signal
            # — an empty result means the queue is drained. This replaces a per-iteration
            # get_queue_depth() Select='COUNT' scan that read the ENTIRE queue partition
            # every loop, spiking RCU past the single-partition ceiling and throttling.
            items = dynamo_service.batch_dequeue_items(model_id, batch_size)
            if not items:
                # Truly drained — no successor needed. Clear any earlier full-batch
                # signal so we don't wake a fresh invocation onto an empty queue.
                logger.info(f"Queue empty, stopping (total processed={processed_count})")
                should_reschedule = False
                break

            logger.info(f"Dequeued {len(items)} items for streaming dispatch "
                        f"(total processed={processed_count})")

            # A full chunk means the queue is still deep — hand off to a successor on
            # exit (Bug 2). A short chunk means the queue is nearly drained, so leave
            # the flag alone (it stays False from this invocation's start unless an
            # earlier full chunk already set it).
            if len(items) >= batch_size:
                should_reschedule = True

            # Per-item streaming dispatch behind four in-memory gates. Each item's invoke
            # is submitted to the fire-and-forget pool (keyed to its item for reconciliation);
            # chunk-level success/failure is tallied after the item loop, preserving the
            # "3 consecutive full-chunk failures → exit" circuit-breaker semantics.
            chunk_success = 0
            chunk_failed = 0
            chunk_errors = []
            chunk_futures = []
            for item in items:
                # Prune entries older than 60s (both windows are subsets of 60s).
                cutoff = time.time() - 60.0
                while dispatch_log and dispatch_log[0][0] < cutoff:
                    dispatch_log.popleft()

                now = time.time()
                item_tokens = int(item.get('estimated_tokens') or flat_tpm_estimate)
                item_input_tokens = int(item.get('estimated_input_tokens') or 0)
                item_output_tokens = int(item.get('estimated_output_tokens') or 0)

                # ── Gate 1: RPM 2s ────────────────────────────────────────────────
                recent_2s_count = sum(
                    1 for entry in dispatch_log
                    if entry[0] >= now - SHORT_WINDOW_SEC
                )
                if recent_2s_count >= rpm_2s_cap:
                    in_win = [
                        entry[0] for entry in dispatch_log
                        if entry[0] >= now - SHORT_WINDOW_SEC
                    ]
                    sleep_for = max(0.001, (min(in_win) + SHORT_WINDOW_SEC) - now + 0.005)
                    logger.info(f"Gate RPM-2s: count={recent_2s_count}>={rpm_2s_cap}, sleeping {sleep_for:.3f}s")
                    time.sleep(sleep_for)
                    now = time.time()

                # ── Gate 2: token 2s windows ───────────────────────────────────────
                if backend == 'mantle':
                    for label, tokens, index, cap in (
                        ('iTPM', item_input_tokens, 2, itpm_2s_cap),
                        ('oTPM', item_output_tokens, 3, otpm_2s_cap),
                    ):
                        sleep_for = _token_gate_sleep(
                            dispatch_log, tokens, index,
                            SHORT_WINDOW_SEC, cap, now
                        )
                        if sleep_for > 0:
                            logger.info(
                                f"Gate {label}-2s: item={tokens}, cap={cap}, "
                                f"sleeping {sleep_for:.3f}s"
                            )
                            time.sleep(sleep_for)
                            now = time.time()
                elif tpm_2s_cap > 0:
                    sleep_for = _token_gate_sleep(
                        dispatch_log, item_tokens, 1,
                        SHORT_WINDOW_SEC, tpm_2s_cap, now
                    )
                    if sleep_for > 0:
                        logger.info(
                            f"Gate TPM-2s: item={item_tokens}, cap={tpm_2s_cap}, "
                            f"sleeping {sleep_for:.3f}s"
                        )
                        time.sleep(sleep_for)
                        now = time.time()

                # ── Gate 3: RPM 60s ────────────────────────────────────────────────
                recent_60s_count = sum(
                    1 for entry in dispatch_log if entry[0] >= now - 60.0
                )
                if recent_60s_count >= queue_capacity:
                    in_win_60 = [
                        entry[0] for entry in dispatch_log
                        if entry[0] >= now - 60.0
                    ]
                    sleep_for = max(0.001, (min(in_win_60) + 60.0) - now + 0.005)
                    logger.info(f"Gate RPM-60s: count={recent_60s_count}>={queue_capacity}, sleeping {sleep_for:.3f}s")
                    time.sleep(sleep_for)
                    now = time.time()

                # ── Gate 4: token 60s windows ──────────────────────────────────────
                if backend == 'mantle':
                    for label, tokens, index, cap in (
                        ('iTPM', item_input_tokens, 2, itpm_queue_capacity),
                        ('oTPM', item_output_tokens, 3, otpm_queue_capacity),
                    ):
                        sleep_for = _token_gate_sleep(
                            dispatch_log, tokens, index, 60.0, cap, now
                        )
                        if sleep_for > 0:
                            logger.info(
                                f"Gate {label}-60s: item={tokens}, cap={cap}, "
                                f"sleeping {sleep_for:.3f}s"
                            )
                            time.sleep(sleep_for)
                            now = time.time()
                elif tpm_queue_capacity > 0:
                    sleep_for = _token_gate_sleep(
                        dispatch_log, item_tokens, 1,
                        60.0, tpm_queue_capacity, now
                    )
                    if sleep_for > 0:
                        logger.info(
                            f"Gate TPM-60s: item={item_tokens}, "
                            f"cap={tpm_queue_capacity}, sleeping {sleep_for:.3f}s"
                        )
                        time.sleep(sleep_for)
                        now = time.time()

                # ── Gate 5: EVEN-SPACING pacer ─────────────────────────────────────
                # Primary rate control when queue_target_tpm > 0. Holds the ACTUAL
                # Bedrock arrival rate at the target by spacing each dispatch
                # item_tokens/target_tps seconds after the previous one — no batch
                # clumping, so peak 1s ≈ sustained (the 2s-window gates alone allowed
                # 9-13M 1s bursts that throttled). The four gates above remain as a
                # safety ceiling. Sim-validated in "Ideal queue processor configuration".
                if queue_target_tps > 0 and last_dispatch_ts is not None:
                    interval = item_tokens / queue_target_tps
                    earliest = last_dispatch_ts + interval
                    now = time.time()
                    if now < earliest:
                        sleep_for = earliest - now
                        time.sleep(sleep_for)

                # ── Dispatch (fire-and-forget) ─────────────────────────────────────
                # Submit to the pool rather than calling inline — the ~40ms blocking
                # invoke now overlaps the next item's pacer sleep instead of adding to
                # the loop's per-item cost. The pacer clock advances at submit time
                # (which is when the request enters the dispatch stream), not at the
                # invoke's completion, so the emitted rate tracks the target.
                dispatch_ts = time.time()
                last_dispatch_ts = dispatch_ts
                dispatch_log.append((
                    dispatch_ts,
                    item_tokens,
                    item_input_tokens,
                    item_output_tokens,
                ))
                chunk_futures.append(
                    (item, dispatch_executor.submit(process_single_item, item, model_id))
                )

            # Reconcile the chunk: block on each future (they have largely resolved during
            # the paced item loop, so this is near-free) and tally success/failure exactly
            # as the old inline path did — preserving error recording and the circuit breaker.
            for item, future in chunk_futures:
                try:
                    result = future.result(timeout=60)
                except Exception as e:
                    result = {'item': item, 'success': False, 'error': str(e)}
                if result['success']:
                    chunk_success += 1
                    processed_count += 1
                else:
                    chunk_failed += 1
                    failed_count += 1
                    error_reason = result['error'] or 'Unknown error'
                    chunk_errors.append(error_reason)
                    dynamo_service.record_invocation_error(
                        model_id=model_id,
                        request_id=item.get('request_id', 'unknown'),
                        execution_arn=item.get('execution_arn'),
                        error=error_reason
                    )

            # Log chunk summary with error details when failures occur
            if chunk_errors:
                logger.info(f"Chunk complete: {chunk_success} success, {chunk_failed} failed, "
                           f"total processed={processed_count}, errors={chunk_errors}")
            else:
                logger.info(f"Chunk complete: {chunk_success} success, {chunk_failed} failed, "
                           f"total processed={processed_count}")

            # Circuit breaker: track consecutive full-chunk failures
            if chunk_success == 0 and chunk_failed > 0:
                consecutive_batch_failures += 1
                logger.warning(f"Full chunk failure ({consecutive_batch_failures}/3)")
                if consecutive_batch_failures >= 3:
                    # Reload config to pick up any operator changes to circuit_breaker_disabled
                    # (config at handler start may be minutes stale in the 13-min processing loop)
                    try:
                        fresh_config = dynamo_service.get_effective_capacity(model_id)
                    except Exception:
                        fresh_config = config  # Fall back to stale config if reload fails
                    if fresh_config.get('circuit_breaker_disabled'):
                        logger.warning("Circuit breaker would trip but is DISABLED via config — continuing")
                        consecutive_batch_failures = 0
                    else:
                        logger.error("Circuit breaker tripped: 3 consecutive chunk failures, exiting")
                        # Emit EMF metric for CloudWatch alarm
                        try:
                            cb_emf = {
                                "_aws": {
                                    "Timestamp": int(time.time() * 1000),
                                    "CloudWatchMetrics": [{
                                        "Namespace": "BedrockShaper",
                                        "Dimensions": [["ServiceName", "model_id"]],
                                        "Metrics": [{"Name": "CircuitBreakerTripped", "Unit": "Count"}]
                                    }]
                                },
                                "ServiceName": "TrafficShaper",
                                "model_id": model_id,
                                "processor_id": processor_id,
                                "CircuitBreakerTripped": 1
                            }
                            print(json.dumps(cb_emf))
                        except Exception:
                            logger.debug("Failed to emit circuit breaker EMF", exc_info=True)
                        break
            else:
                consecutive_batch_failures = 0

            # Emit EMF metrics for CloudWatch. QueueDepth was dropped along with the
            # per-chunk get_queue_depth() Select='COUNT' scan (full-partition read →
            # RCU spike → throttling). ProcessingRate is emitted per chunk; backlog
            # depth, if needed later, should come from a bounded read, not a scan.
            try:
                emf = {
                    "_aws": {
                        "Timestamp": int(time.time() * 1000),
                        "CloudWatchMetrics": [{
                            "Namespace": "BedrockShaper",
                            "Dimensions": [["ServiceName", "model_id"]],
                            "Metrics": [
                                {"Name": "ProcessingRate", "Unit": "Count"}
                            ]
                        }]
                    },
                    "ServiceName": "TrafficShaper",
                    "model_id": model_id,
                    "processor_id": processor_id,
                    "ProcessingRate": chunk_success
                }
                print(json.dumps(emf))
            except Exception:
                logger.debug("Failed to emit EMF metrics", exc_info=True)

        logger.info(f"Queue processing complete: model={model_id}, processed={processed_count}, failed={failed_count}")
        return {
            'processed': processed_count,
            'failed': failed_count,
            'status': 'complete'
        }

    finally:
        # Shut the dispatch pool down so worker threads don't leak across warm-container
        # invocations. wait=True lets any in-flight invokes finish; every chunk already
        # reconciled its own futures, so nothing here is left unaccounted for.
        try:
            dispatch_executor.shutdown(wait=True)
        except Exception:
            logger.debug("Dispatch pool shutdown failed", exc_info=True)

        # Release lock only if we still own it
        if dynamo_service.release_processor_lock(model_id, processor_id):
            logger.info(f"Released processor lock: model={model_id}")
        else:
            logger.warning(f"Could not release lock (already lost ownership): model={model_id}")

        # Hand off to a fresh invocation if we exited with the queue still draining
        # at full clip (Bug 2). Emit AFTER releasing the lock so the successor can
        # acquire it rather than bouncing off the lock we just held.
        if should_reschedule:
            logger.info(f"Backlog remains — rescheduling drain: model={model_id}")
            trigger_successor(model_id)
