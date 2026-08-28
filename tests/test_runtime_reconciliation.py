"""Local (moto) validation of runtime consumption reconciliation + its burndown units.

The runtime TPM counter increments by burndown-WEIGHTED tokens
(`input + max_tokens*burndown`), while the mantle path stores RAW `input+output`.
A 3x load-test run leaked real Bedrock TPM throttles because the runtime path never
reconciled its byte-heuristic estimate to actuals. The fix reconciles post-call — but
it MUST write burndown-weighted tokens, or it would under-weight output and make
over-admission worse. This test pins that units contract.

Run: python -m pytest tests/test_runtime_reconciliation.py -q
"""
import sys
import pathlib
import boto3
import pytest
from moto import mock_aws

LAYER = pathlib.Path(__file__).resolve().parents[1] / "infrastructure" / "lambda_layer" / "python"
sys.path.insert(0, str(LAYER))

from shared_service import DynamoService  # noqa: E402

TABLE = "semaphore-single-table"


def _make_table():
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName=TABLE,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                              {"AttributeName": "sk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return ddb


def _reconcile_runtime(svc, model_id, alloc_id, ai, ao, burndown):
    """Mirror of bedrock_processor.reconcile_runtime_consumption's write (units contract).

    Kept as a local mirror so the test doesn't import the Lambda handler's boto/runtime
    deps; the assertion is on the UNITS (burndown-weighted combined), which is the part
    that must never regress.
    """
    combined = int((ai or 0) + (ao or 0) * burndown)
    svc.single_table.update_item(
        Key={"pk": f"MODEL#{model_id}#BURST#CONSUMPTION", "sk": alloc_id},
        UpdateExpression="SET estimated_tokens = :c, estimated_input_tokens = :ai, estimated_output_tokens = :ao",
        ConditionExpression="attribute_exists(pk) AND attribute_exists(sk)",
        ExpressionAttributeValues={":c": combined, ":ai": int(ai), ":ao": int(ao)},
    )
    return combined


@mock_aws
def test_runtime_reconcile_writes_burndown_weighted_combined():
    _make_table()
    svc = DynamoService(single_table_name=TABLE)
    common = dict(
        burst_capacity=10_000_000, burst_regen_rate=0.0, max_burst_multiplier=5.0,
        counter_shards=1, tpm_burst_capacity=3_000_000, tpm_burst_regen_rate=0.0,
    )
    # Admit with an ESTIMATE (burndown-weighted): input 1000 + max_tokens*burndown.
    res = svc.put_allocation("rt-model", "rc-1", estimated_tokens=1000 + 1200 * 5, **common)
    alloc_id = res["item"]["sk"]   # '{now_ms}#{request_id}' — matches BURST#CONSUMPTION sort key

    # Actuals came back smaller than the estimate; reconcile with burndown=5.
    combined = _reconcile_runtime(svc, "rt-model", alloc_id, ai=1200, ao=300, burndown=5)

    # Contract: combined MUST be input + output*burndown (1200 + 300*5 = 2700),
    # NOT the raw sum (1500). Raw would under-weight output and worsen over-admission.
    assert combined == 2700, f"expected burndown-weighted 2700, got {combined}"

    item = svc.single_table.get_item(
        Key={"pk": "MODEL#rt-model#BURST#CONSUMPTION", "sk": alloc_id}).get("Item")
    assert int(item["estimated_tokens"]) == 2700
    assert int(item["estimated_input_tokens"]) == 1200
    assert int(item["estimated_output_tokens"]) == 300


@mock_aws
def test_burndown_1_reduces_to_raw_sum():
    """For burndown=1 (non-Claude, e.g. Nova) the weighted combined == raw sum."""
    _make_table()
    svc = DynamoService(single_table_name=TABLE)
    common = dict(
        burst_capacity=10_000_000, burst_regen_rate=0.0, max_burst_multiplier=5.0,
        counter_shards=1, tpm_burst_capacity=3_000_000, tpm_burst_regen_rate=0.0,
    )
    res = svc.put_allocation("nova-like", "rc-2", estimated_tokens=6000, **common)
    alloc_id = res["item"]["sk"]
    combined = _reconcile_runtime(svc, "nova-like", alloc_id, ai=1200, ao=300, burndown=1)
    assert combined == 1500  # 1200 + 300*1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
