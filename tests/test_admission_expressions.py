"""Local (moto) validation of the sliding-window admission gate.

The burst admission gate is now a strongly-consistent READ over the last
`long_window_sec` seconds of the BURST#CONSUMPTION partition (no counters, no
TransactWriteItems) — see dynamo.py::put_allocation / _enforce_window_gate. moto
executes real DynamoDB queries/puts in-memory, so these tests exercise the actual
read-gate logic against a real table.

Run: python -m pytest tests/test_admission_expressions.py -q
"""
import sys
import pathlib
import boto3
import pytest
from moto import mock_aws

# Import the shared service layer from the Lambda layer path.
LAYER = pathlib.Path(__file__).resolve().parents[1] / "infrastructure" / "lambda_layer" / "python"
sys.path.insert(0, str(LAYER))

from shared_service import DynamoService, BurstCapacityExceeded  # noqa: E402

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


# A TPM-gated runtime model. burst_capacity>0 enables the gate; rpm disabled so
# only the token dimension binds. Regen rates set the caps:
#   cap_short_tok = tpm_burst_regen_rate * 2
#   cap_long_tok  = tpm_burst_regen_rate * 15
# With tpm_burst_regen_rate=100_000 tok/s: 2s cap = 200k, 15s cap = 1.5M.
TPM_MODEL = dict(
    burst_capacity=10_000_000,        # sentinel; rpm_quota_enabled False → no req cap
    burst_regen_rate=0.0,
    tpm_burst_capacity=6_000_000,     # unused by the window gate (kept for signature)
    tpm_burst_regen_rate=100_000.0,   # → 2s cap 200k, 15s cap 1.5M
    rpm_quota_enabled=False,
)


def _put(svc, model_id, request_id, tokens):
    return svc.put_allocation(
        model_id, request_id, estimated_tokens=tokens,
        burst_capacity=TPM_MODEL["burst_capacity"],
        burst_regen_rate=TPM_MODEL["burst_regen_rate"],
        tpm_burst_capacity=TPM_MODEL["tpm_burst_capacity"],
        tpm_burst_regen_rate=TPM_MODEL["tpm_burst_regen_rate"],
        rpm_quota_enabled=TPM_MODEL["rpm_quota_enabled"],
    )


@mock_aws
def test_admits_when_window_empty():
    """An empty window admits a normal request and writes the consumption record."""
    _make_table()
    svc = DynamoService(single_table_name=TABLE)
    res = _put(svc, "m1", "r1", 50_000)
    assert res["item"]["estimated_tokens"] == 50_000
    # Record was written to the BURST#CONSUMPTION partition.
    recs = svc.query_consumption_records("m1", "BURST", window_seconds=15, consistent_read=True)
    assert len(recs) == 1


@mock_aws
def test_oversized_single_request_rejected_up_front():
    """A single request bigger than the 15s cap can never fit → reject up front."""
    _make_table()
    svc = DynamoService(single_table_name=TABLE)
    # 15s cap = 100_000 * 15 = 1.5M. Ask for 2M.
    with pytest.raises(BurstCapacityExceeded):
        _put(svc, "m2", "big", 2_000_000)


@mock_aws
def test_short_window_cap_binds():
    """Filling the 2s token cap sheds the next request (short window binds)."""
    _make_table()
    svc = DynamoService(single_table_name=TABLE)
    # 2s cap = 200k. Admit 150k, then 60k would push 2s window to 210k > 200k.
    _put(svc, "m3", "a", 150_000)
    with pytest.raises(BurstCapacityExceeded):
        _put(svc, "m3", "b", 60_000)


@mock_aws
def test_long_window_cap_binds_when_short_ok():
    """The 15s cap binds even when each request fits the 2s cap.

    Craft records that are all older than 2s (so the 2s window is empty) but sum
    near the 15s cap, then a new request should be rejected by the LONG window.
    """
    import time as _t
    _make_table()
    svc = DynamoService(single_table_name=TABLE)
    table = svc.single_table
    now_ms = int(_t.time() * 1000)
    # Seed 15 records of 100k each at 2.5s..13.7s ago (outside the 2s window, inside
    # the 15s window) = 1.5M == the 15s cap.
    for i in range(15):
        ts = now_ms - (2500 + i * 800)
        table.put_item(Item={
            "pk": "MODEL#m4#BURST#CONSUMPTION",
            "sk": f"{ts}#seed{i}",
            "estimated_tokens": 100_000,
            "count": 1,
        })
    # 2s window empty, but 15s window already at 1.5M (== cap). Any token request rejects.
    with pytest.raises(BurstCapacityExceeded):
        _put(svc, "m4", "late", 50_000)


@mock_aws
def test_rpm_dimension_binds_when_enabled():
    """With rpm_quota_enabled and a request-rate cap, the 2s request count binds."""
    _make_table()
    svc = DynamoService(single_table_name=TABLE)
    # short_window_rps=1 → 2s req cap = 2, 15s req cap = 15. No token gate.
    common = dict(
        burst_capacity=100, burst_regen_rate=1.0,
        tpm_burst_capacity=0, tpm_burst_regen_rate=0.0,
        rpm_quota_enabled=True, short_window_rps=1.0,
    )
    svc.put_allocation("m5", "r1", estimated_tokens=0, **common)
    svc.put_allocation("m5", "r2", estimated_tokens=0, **common)
    # Third within 2s exceeds the 2-req cap.
    with pytest.raises(BurstCapacityExceeded):
        svc.put_allocation("m5", "r3", estimated_tokens=0, **common)


@mock_aws
def test_mantle_split_windows():
    """Mantle gates iTPM and oTPM independently over the window."""
    _make_table()
    svc = DynamoService(single_table_name=TABLE)
    common = dict(
        backend="mantle", rpm_quota_enabled=False,
        burst_capacity=10_000_000, burst_regen_rate=0.0,
        itpm_burst_capacity=1, itpm_burst_regen_rate=100_000.0,  # iTPM 2s cap 200k
        otpm_burst_capacity=1, otpm_burst_regen_rate=10_000.0,   # oTPM 2s cap 20k (tighter)
    )
    # First request fits both. Second breaches the tighter oTPM 2s cap.
    svc.put_allocation("m6", "r1", estimated_tokens=30_000,
                       estimated_input_tokens=15_000, estimated_output_tokens=15_000, **common)
    with pytest.raises(BurstCapacityExceeded):
        svc.put_allocation("m6", "r2", estimated_tokens=30_000,
                           estimated_input_tokens=15_000, estimated_output_tokens=15_000, **common)


@mock_aws
def test_burst_disabled_rejects_everything():
    """burst_capacity<=0 (0% burst) rejects every request so it all queues.

    Pins the fix that replaced the former legacy admit-all passthrough: with burst
    disabled, put_allocation must raise BurstCapacityExceeded (caller enqueues)
    rather than silently admitting the request and bypassing the queue.
    """
    _make_table()
    svc = DynamoService(single_table_name=TABLE)
    with pytest.raises(BurstCapacityExceeded):
        svc.put_allocation(
            "m7", "r1", estimated_tokens=5_000,
            burst_capacity=0, burst_regen_rate=0.0,
            tpm_burst_capacity=0, tpm_burst_regen_rate=0.0,
            rpm_quota_enabled=False,
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
