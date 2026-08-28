"""Finalizer Fn — EventBridge "Step Functions Execution Status Change" handler.

Records an honest terminal outcome for executions that end in FAILED, TIMED_OUT,
or ABORTED — the rare paths where no per-request writer got to commit a terminal
status (design §OBJECTIVE 3, "Write-gates-signal, and NO success backstop": there
is deliberately NO SUCCEEDED finalizer; success is written by bedrock_processor
before send_task_success).

Hard rule (RedTeam #9, Cato): this handler NEVER calls states:DescribeExecution.
Correlation/identity are threaded into the state-machine I/O and arrive on the
EventBridge event detail (detail.input / detail.output are JSON strings). A
control-plane DescribeExecution under load would re-introduce the SFN throttle.

Mapping (BUILD_CONTRACT + Cato C-8):
  TIMED_OUT  -> read the terminal item:
                  still QUEUED  -> reason=queue_expired, http_status=504
                  otherwise     -> reason=timed_out,     http_status=504
  FAILED     -> reason=error, http_status=503
                (includes a Lambda.TooManyRequestsException catch→Fail — that is
                 'error', NOT 'throttled'; 'throttled' is reserved for Bedrock
                 throttles surfaced by bedrock_processor. Cato C-8.)
  ABORTED    -> reason=error, http_status=503

Idempotent: write_terminal_status uses a conditional UpdateItem that only permits
PENDING|QUEUED -> terminal. If bedrock_processor (or the queue-expiry path) already
wrote a terminal status, the helper returns False and this finalizer no-ops — the
first writer always wins. EMF is emitted by OutcomeStreamFn off the DDB stream, not
here.
"""

import json
import os

from shared_service import DynamoService


SINGLE_TABLE_NAME = os.environ.get('SINGLE_TABLE_NAME')

# EventBridge statuses this finalizer records. SUCCEEDED is intentionally absent.
_HANDLED_STATUSES = {'FAILED', 'TIMED_OUT', 'ABORTED'}


def _load_json(maybe_json):
    """Best-effort parse of a JSON string (SM input/output are strings on the
    EventBridge detail). Returns {} for None/blank/malformed."""
    if not maybe_json:
        return {}
    if isinstance(maybe_json, dict):
        return maybe_json
    try:
        parsed = json.loads(maybe_json)
        return parsed if isinstance(parsed, dict) else {}
    except (ValueError, TypeError):
        return {}


def _first(*values):
    """Return the first truthy value, else None."""
    for v in values:
        if v:
            return v
    return None


def _extract_identity(detail):
    """Pull request_id/correlation_id/model_id/tenant_id from the SM I/O threaded
    onto the EventBridge detail. Prefers output (later, richer) then input.

    The SM threads these through its state I/O per the design; nested locations
    are checked defensively because the exact envelope depends on which state the
    execution died in.
    """
    sm_input = _load_json(detail.get('input'))
    sm_output = _load_json(detail.get('output'))

    # Nested payload the processor callback echoes, when present.
    nested = {}
    if isinstance(sm_output.get('bedrock_response'), dict):
        nested = sm_output

    def pick(key):
        return _first(sm_output.get(key), nested.get(key), sm_input.get(key))

    return {
        'request_id': pick('request_id'),
        'correlation_id': pick('correlation_id'),
        'model_id': pick('model_id'),
        'tenant_id': pick('tenant_id'),
    }


def _finalize_one(dynamo, detail):
    """Record the terminal outcome for a single execution-status-change detail.
    Returns a small result dict for logging/aggregation."""
    status = detail.get('status')
    execution_arn = detail.get('executionArn')

    if status not in _HANDLED_STATUSES:
        # SUCCEEDED and any RUNNING/etc. transitions are not our job.
        return {'skipped': True, 'status': status, 'executionArn': execution_arn}

    ident = _extract_identity(detail)
    request_id = ident['request_id']

    if not request_id:
        # Without a request_id we cannot key the terminal item. Fail loud — a
        # missing id means the SM I/O threading regressed (do NOT DescribeExecution).
        print(f"ERROR: finalizer could not extract request_id from execution "
              f"{execution_arn} (status={status}); identity={ident}")
        return {'error': 'missing_request_id', 'status': status,
                'executionArn': execution_arn}

    # Decide reason/http_status.
    if status == 'TIMED_OUT':
        # Distinguish a queue-TTL expiry (still QUEUED at timeout) from an
        # in-flight timeout. Read our own state — deterministic, no TTL-lag
        # dependence, no control-plane call.
        current_state = None
        try:
            resp = dynamo.single_table.get_item(
                Key={'pk': f'REQUEST#{request_id}', 'sk': 'STATUS'}
            )
            current_state = (resp.get('Item') or {}).get('state')
        except Exception as e:  # noqa: BLE001 — degrade to timed_out on read failure
            print(f"WARNING: finalizer GetItem failed for request_id={request_id}: {e}")
        if current_state == 'QUEUED':
            reason, http_status = 'queue_expired', 504
        else:
            reason, http_status = 'timed_out', 504
    else:
        # FAILED or ABORTED (incl. Lambda.TooManyRequestsException catch→Fail).
        # 'error', never 'throttled' — Cato C-8.
        reason, http_status = 'error', 503

    try:
        won = dynamo.write_terminal_status(
            request_id=request_id,
            state='FAILED',
            reason=reason,
            http_status=http_status,
            tenant_id=ident['tenant_id'],
            correlation_id=ident['correlation_id'],
            model_id=ident['model_id'],
            source='queued',
        )
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: finalizer write_terminal_status failed for "
              f"request_id={request_id} (status={status}): {e}")
        # Re-raise so the EventBridge target records a delivery failure and retries;
        # the conditional write is idempotent, so a retry is safe.
        raise

    if won:
        print(f"Finalized terminal outcome: request_id={request_id}, "
              f"sfn_status={status}, reason={reason}, http={http_status}, "
              f"correlation_id={ident['correlation_id']}")
    else:
        # A per-request writer already committed a terminal — expected on the
        # happy-ish path where a real reason was recorded before timeout fired.
        print(f"Finalizer no-op (already terminal): request_id={request_id}, "
              f"sfn_status={status}")

    return {'request_id': request_id, 'status': status, 'reason': reason,
            'http_status': http_status, 'won': won}


def handler(event, context):
    """EventBridge target. Accepts a single event (normal) or a batch shape."""
    dynamo = DynamoService(single_table_name=SINGLE_TABLE_NAME)

    # EventBridge delivers one event per invocation; support a Records batch
    # defensively for any pipe/replay wrapper.
    records = event.get('Records')
    if records:
        results = []
        for rec in records:
            detail = (rec.get('detail')
                      or _load_json(rec.get('body')).get('detail')
                      or {})
            results.append(_finalize_one(dynamo, detail))
        return {'status': 'ok', 'results': results}

    detail = event.get('detail') or {}
    return {'status': 'ok', 'result': _finalize_one(dynamo, detail)}
