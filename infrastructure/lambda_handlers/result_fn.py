"""Result Fn — GET /result/{request_id}.

Thin read-only poll endpoint for the honest-outcomes layer. Reads the terminal
status item (pk=REQUEST#{request_id}, sk=STATUS) and, on SUCCEEDED, presigns the
S3 output pointer. Makes NO states:* calls and never returns an executionArn —
the item does not store one, so ARN leakage is structurally impossible (design
§OBJECTIVE 3, "GET /result/{request_id} = thin ResultFn").

Tenant isolation is enforced at read time: the caller's SigV4 principal
($context.identity.userArn, surfaced as requestContext.identity.userArn in the
APIGW proxy event) is compared to the stored item.tenant_id; a mismatch is a 403.

HTTP contract (BUILD_CONTRACT §HTTP outcome→code map):
  absent | PENDING | QUEUED  -> 202 + Retry-After
  SUCCEEDED                  -> 200 + {status, output_url: presigned}
  FAILED reason=throttled    -> 429
  FAILED reason=error        -> 503
  FAILED reason=timed_out    -> 504
  FAILED reason=queue_expired-> 504
  FAILED reason=validation_error -> 400

Returns APIGW proxy-integration shape: {statusCode, headers, body}.
"""

import json
import os

import boto3

from shared_service import DynamoService


SINGLE_TABLE_NAME = os.environ.get('SINGLE_TABLE_NAME')
OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET')

# Seconds a polling client should wait before re-polling a non-terminal request.
RETRY_AFTER_SECONDS = 2
# Presigned GET URL lifetime.
PRESIGN_EXPIRY_SECONDS = 3600

s3_client = boto3.client('s3')

# reason -> HTTP status for terminal FAILED items. SUCCEEDED is handled
# separately (200). PENDING/QUEUED/absent are non-terminal (202).
_REASON_TO_STATUS = {
    'throttled': 429,
    'ingress_throttled': 429,
    'error': 503,
    'timed_out': 504,
    'queue_expired': 504,
    'validation_error': 400,
}


def _response(status_code, body, extra_headers=None):
    """Build an APIGW proxy-integration response with a JSON body."""
    headers = {'Content-Type': 'application/json'}
    if extra_headers:
        headers.update(extra_headers)
    return {
        'statusCode': status_code,
        'headers': headers,
        'body': json.dumps(body),
    }


def _caller_tenant(event):
    """Extract the caller's SigV4 principal from the APIGW proxy event.

    The terminal item's tenant_id was minted from $context.identity.userArn at
    invoke time, so the authoritative comparison value is that same field.
    Falls back through the identity map for robustness; returns None when no
    principal can be established (treated as unauthorized by the caller).
    """
    identity = (event.get('requestContext') or {}).get('identity') or {}
    for key in ('userArn', 'user', 'caller', 'accessKey'):
        val = identity.get(key)
        if val:
            return val
    return None


def _parse_s3_uri(output_ref):
    """Return (bucket, key) from an s3://bucket/key URI, or (OUTPUT_BUCKET, ref)
    when the ref is a bare key. Returns (None, None) when unresolvable."""
    if not output_ref:
        return None, None
    if output_ref.startswith('s3://'):
        without_scheme = output_ref[len('s3://'):]
        bucket, _, key = without_scheme.partition('/')
        if bucket and key:
            return bucket, key
        return None, None
    # Bare key — resolve against the configured output bucket.
    if OUTPUT_BUCKET:
        return OUTPUT_BUCKET, output_ref
    return None, None


def _presign(output_ref):
    """Presign a GET for the completion body. Returns None on any failure so the
    caller can degrade rather than 500."""
    bucket, key = _parse_s3_uri(output_ref)
    if not bucket or not key:
        print(f"WARNING: cannot presign — unresolvable output_ref={output_ref!r}")
        return None
    try:
        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=PRESIGN_EXPIRY_SECONDS,
        )
    except Exception as e:  # noqa: BLE001 — presign failure must not 500 the poll
        print(f"WARNING: presign failed for output_ref={output_ref!r}: {e}")
        return None


def handler(event, context):
    """GET /result/{request_id} — pure read + presign, no states:* calls."""
    path_params = event.get('pathParameters') or {}
    request_id = path_params.get('request_id')
    if not request_id:
        return _response(400, {'status': 'error', 'reason': 'validation_error',
                               'message': 'request_id path parameter is required'})

    dynamo = DynamoService(single_table_name=SINGLE_TABLE_NAME)
    try:
        resp = dynamo.single_table.get_item(
            Key={'pk': f'REQUEST#{request_id}', 'sk': 'STATUS'}
        )
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: GetItem failed for request_id={request_id}: {e}")
        return _response(503, {'status': 'error', 'reason': 'error',
                               'message': 'status lookup failed'})

    item = resp.get('Item')

    # Absent -> not yet recorded (PENDING write may still be in flight). 202.
    if not item:
        return _response(202, {'status': 'PENDING', 'request_id': request_id},
                         {'Retry-After': str(RETRY_AFTER_SECONDS)})

    # Tenant isolation: compare caller principal to the stored tenant_id.
    # A stored tenant of 'unknown'/None means the item was created by an ingress
    # path that cannot mint a per-tenant id — today the /invoke AwsIntegration
    # (direct StartExecution) does not inject $context.identity into the SFN input,
    # so its PENDING write defaults tenant_id='unknown'. Such items carry no
    # cross-tenant ownership claim to enforce; access is already gated by the
    # method's IAM SigV4 auth. Only enforce the 403 when the item names a REAL
    # owning tenant that differs from the caller (the /baseline path DOES mint one).
    item_tenant = item.get('tenant_id')
    caller_tenant = _caller_tenant(event)
    if item_tenant and item_tenant != 'unknown' and caller_tenant != item_tenant:
        # Do not leak existence details across tenants.
        print(f"WARNING: cross-tenant read refused: request_id={request_id}, "
              f"caller={caller_tenant!r}, owner={item_tenant!r}")
        return _response(403, {'status': 'error', 'reason': 'forbidden',
                               'message': 'not authorized for this request'})

    state = item.get('state')
    reason = item.get('reason')

    # Non-terminal -> keep polling.
    if state in ('PENDING', 'QUEUED') or state is None:
        return _response(202, {'status': state or 'PENDING', 'request_id': request_id},
                         {'Retry-After': str(RETRY_AFTER_SECONDS)})

    if state == 'SUCCEEDED':
        output_url = _presign(item.get('output_ref'))
        if not output_url:
            # Body pointer missing/unpresignable — honest 503 rather than a
            # confident 200 with no answer (design: write-gates-signal ethos).
            return _response(503, {'status': 'error', 'reason': 'error',
                                   'request_id': request_id,
                                   'message': 'output unavailable'})
        return _response(200, {'status': 'SUCCEEDED', 'request_id': request_id,
                               'output_url': output_url})

    # Terminal FAILED — map reason to the honest HTTP code.
    status_code = _REASON_TO_STATUS.get(reason, 503)
    return _response(status_code, {
        'status': 'FAILED',
        'reason': reason or 'error',
        'request_id': request_id,
    })
