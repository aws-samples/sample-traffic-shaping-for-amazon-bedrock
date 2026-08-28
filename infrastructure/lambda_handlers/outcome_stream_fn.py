"""Outcome Stream Fn — DynamoDB Streams (NEW_AND_OLD_IMAGES) handler.

The SINGLE authoritative emitter of the RequestOutcome EMF metric (Cato C-2:
writer-emit is only at-most-once — a terminal writer that commits the item then
dies before printing the metric would lose it forever, and every retry's
conditional write then fails, permanently poisoning RequestOutcome(succeeded),
the exact signal OBJ2 calibrates on). By projecting the committed
PENDING|QUEUED -> terminal transition off the stream, the metric is
liveness-independent: the conditional UpdateItem stays the exactly-once *record*
gate; this function turns that one committed transition into exactly one metric.

Behavior (BUILD_CONTRACT §RequestOutcome EMF):
  - Filter to REQUEST#*/STATUS items (entity_type == "request_status").
  - Emit RequestOutcome (count=1) when NewImage.state is terminal
    (SUCCEEDED|FAILED) AND OldImage state was PENDING|QUEUED or the item is new
    (INSERT / no OldImage). Ignore terminal->terminal and non-terminal transitions.
  - Map state+reason -> outcome ∈ {succeeded, throttled, error, timed_out,
    queue_expired, ingress_throttled}.
  - EMF dimensions: [ServiceName, model_id, source, arm, outcome].
    tenant_id / correlation_id / attempts / duration_ms are PROPERTIES.
  - At-least-once metric is fine (stream may redeliver; re-emits the same point).

Never returns a partial-batch failure for a metric-emit hiccup — a stream record
that can't be parsed is logged and skipped so one poison record can't wedge the
shard iterator.
"""

import json
import os
import time

from boto3.dynamodb.types import TypeDeserializer


SINGLE_TABLE_NAME = os.environ.get('SINGLE_TABLE_NAME')

EMF_NAMESPACE = "BedrockShaper"
SERVICE_NAME = "TrafficShaper"

_TERMINAL_STATES = {'SUCCEEDED', 'FAILED'}
_NON_TERMINAL_STATES = {'PENDING', 'QUEUED'}

_deserializer = TypeDeserializer()


def _deserialize_image(image):
    """Convert a DynamoDB Streams image (type-descriptor JSON) to a plain dict.
    Returns {} for a missing image (e.g. INSERT has no OldImage)."""
    if not image:
        return {}
    out = {}
    for key, typed_value in image.items():
        try:
            out[key] = _deserializer.deserialize(typed_value)
        except Exception:  # noqa: BLE001 — skip any single undeserializable attr
            out[key] = None
    return out


def _map_outcome(state, reason):
    """Map terminal state + reason to the RequestOutcome dimension value."""
    if state == 'SUCCEEDED':
        return 'succeeded'
    # state == 'FAILED' — the reason carries the real classification.
    return {
        'throttled': 'throttled',
        'ingress_throttled': 'ingress_throttled',
        'error': 'error',
        'timed_out': 'timed_out',
        'queue_expired': 'queue_expired',
        'validation_error': 'error',  # 400-class collapses to the error bucket for the metric
    }.get(reason, 'error')


def _coerce_int(value, default=None):
    """DynamoDB numbers deserialize to Decimal; coerce to int for EMF."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _emit_request_outcome(new_image):
    """Print one RequestOutcome EMF blob to stdout (house style — matches
    bedrock_processor.emit_bedrock_latency_metric)."""
    model_id = new_image.get('model_id') or 'unknown'
    source = new_image.get('source') or 'queued'
    arm = new_image.get('arm') or 'shaper'
    outcome = _map_outcome(new_image.get('state'), new_image.get('reason'))

    emf = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": EMF_NAMESPACE,
                "Dimensions": [["ServiceName", "model_id", "source", "arm", "outcome"]],
                "Metrics": [{"Name": "RequestOutcome", "Unit": "Count"}],
            }],
        },
        "ServiceName": SERVICE_NAME,
        "model_id": model_id,
        "source": source,
        "arm": arm,
        "outcome": outcome,
        # Properties (not dimensions) — high-cardinality context.
        "tenant_id": new_image.get('tenant_id') or '',
        "correlation_id": new_image.get('correlation_id') or '',
        "request_id": new_image.get('request_id') or '',
        "attempts": _coerce_int(new_image.get('attempts'), 1),
        "RequestOutcome": 1,
    }
    duration_ms = _coerce_int(new_image.get('duration_ms'))
    if duration_ms is not None:
        emf["duration_ms"] = duration_ms

    print(json.dumps(emf))


def _should_emit(new_image, old_image, event_name):
    """True iff this is a PENDING|QUEUED|absent -> terminal(SUCCEEDED|FAILED)
    transition on a request_status item."""
    # Filter to the terminal-status item only.
    if new_image.get('entity_type') != 'request_status':
        return False

    new_state = new_image.get('state')
    if new_state not in _TERMINAL_STATES:
        # Non-terminal transition (e.g. PENDING->QUEUED) — ignore.
        return False

    old_state = old_image.get('state') if old_image else None

    # INSERT straight to terminal, or no prior image -> emit.
    if event_name == 'INSERT' or old_state is None:
        return True

    # Terminal reached from a non-terminal state -> emit exactly this transition.
    if old_state in _NON_TERMINAL_STATES:
        return True

    # old_state already terminal (terminal->terminal, shouldn't happen given the
    # conditional write, but stream may redeliver) -> skip to avoid double count
    # on genuine re-transitions. Redelivery of the SAME record still re-emits the
    # same point (at-least-once accepted per contract) because DynamoDB replays
    # the original (old=non-terminal) image, not a synthesized terminal->terminal.
    return False


def handler(event, context):
    """DynamoDB Streams target. Emits RequestOutcome for terminal transitions."""
    records = event.get('Records', []) or []
    emitted = 0
    scanned = 0

    for record in records:
        scanned += 1
        try:
            event_name = record.get('eventName')  # INSERT | MODIFY | REMOVE
            if event_name == 'REMOVE':
                # TTL/delete churn — never a terminal-outcome transition.
                continue

            ddb = record.get('dynamodb') or {}
            new_image = _deserialize_image(ddb.get('NewImage'))
            if not new_image:
                continue
            old_image = _deserialize_image(ddb.get('OldImage'))

            if _should_emit(new_image, old_image, event_name):
                _emit_request_outcome(new_image)
                emitted += 1
        except Exception as e:  # noqa: BLE001 — one poison record must not wedge the shard
            print(f"WARNING: outcome stream record skipped (non-fatal): {e}")
            continue

    if emitted:
        print(f"OutcomeStreamFn: emitted {emitted} RequestOutcome metric(s) "
              f"from {scanned} stream record(s)")
    return {'status': 'ok', 'scanned': scanned, 'emitted': emitted}
