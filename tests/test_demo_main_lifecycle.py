"""Offline tests for demo.py's run lifecycle: the CONFIG override/backup/restore
around main(), run errors and warnings, stop and drain-timeout cleanup, outcome
accounting, and the WAF limit lookup. Every AWS call is faked; nothing here
reads config.env's live resources.

Run: python -m pytest tests/test_demo_main_lifecycle.py -q
"""

import copy
import json
import pathlib
import sys
import threading
from decimal import Decimal

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))
import demo  # noqa: E402

MODEL = "us.amazon.nova-2-lite-v1:0"
ORIGINAL = {
    "pk": f"MODEL#{MODEL}",
    "sk": "CONFIG",
    "model_id": MODEL,
    "backend": "runtime",
    "rpm_limit": None,
    "tpm_limit": Decimal("8000000"),
    "tpm_burst_capacity": Decimal("0"),
    "tpm_queue_capacity": Decimal("6800000"),
    "tpm_queue_regeneration_rate": Decimal("113333.3333"),
    "tpm_buffer_capacity": Decimal("1200000"),
    "output_token_burndown_rate": Decimal("1"),
    "bytes_per_token": Decimal("3"),
    "short_window_sec": Decimal("2"),
    "long_window_sec": Decimal("15"),
}


def _client_error(code, op="Op"):
    return ClientError({"Error": {"Code": code, "Message": f"arn:aws:x:123456789012 {code}"}}, op)


class FakeTable:
    """CONFIG table stand-in that honours the two condition expressions demo.py
    uses. `log` is shared with the arm stubs so call order can be asserted."""

    def __init__(self, item, log):
        self.item, self.log = copy.deepcopy(item), log
        self.fail_restores = 0  # restore puts that raise before the condition is checked

    def get_item(self, Key, ConsistentRead=False):
        return {"Item": copy.deepcopy(self.item)} if self.item is not None else {}

    def put_item(self, Item, ConditionExpression=None, ExpressionAttributeValues=None):
        marker = (self.item or {}).get("demo_override_run")
        if ConditionExpression == "demo_override_run = :mine":
            if self.fail_restores:
                self.fail_restores -= 1
                raise _client_error("ProvisionedThroughputExceededException", "PutItem")
            if marker != ExpressionAttributeValues[":mine"]:
                raise _client_error("ConditionalCheckFailedException", "PutItem")
        elif ConditionExpression == "attribute_not_exists(demo_override_run)" and marker:
            raise _client_error("ConditionalCheckFailedException", "PutItem")
        self.log.append("put:override" if "demo_override_run" in Item else "put:original")
        self.item = copy.deepcopy(Item)


def _served_direct(i):
    return {
        "idx": i,
        "ok": True,
        "outcome": "served",
        "simulated_throttle": False,
        "in": 10,
        "out": 5,
        "ms": 100.0,
        "elapsed_ms": 100.0,
        "status": 200,
        "attempts": 1,
        "bedrock_calls": 1,
        "sim_throttle_attempts": 0,
        "real_throttle_codes": [],
        "error_code": None,
        "completed_ts": 1.0,
    }


@pytest.fixture(autouse=True)
def clean_run_state(monkeypatch):
    """No live WAF read, no stray stop flag, a fresh run log, no recording."""
    monkeypatch.setitem(demo._waf_cache, "limits", (200, 300, "test default"))
    monkeypatch.setattr(demo, "_event_path", None)
    demo._STOP.clear()
    demo._run_log["errors"].clear()
    demo._run_log["warnings"].clear()
    yield
    demo._STOP.clear()


def _make_harness(monkeypatch, tmp_path, stub_shaper=True):
    """main() with every AWS edge faked. Arms are stubs (tweak `h` before main());
    stub_shaper=False keeps the real shaper arm."""
    log = []
    table = FakeTable(ORIGINAL, log)
    h = {"log": log, "table": table, "tmp": tmp_path, "events": tmp_path / "run.jsonl"}
    h.update(
        shaper=lambda h, state, n: [{"status": 200, "ms": 50.0, "outcome": "served"}] * n,
        direct=lambda h, out, arm, n: out.update(
            results=[_served_direct(i) for i in range(1, n + 1)], start=0.0, end=1.0
        ),
    )

    class Resource:
        def Table(self, name):
            return table

    monkeypatch.setattr(demo.config_loader, "get_config_with_aws_check", lambda: {})
    monkeypatch.setattr(demo.boto3, "resource", lambda *a, **k: Resource())
    monkeypatch.setattr(demo, "_load_api_url", lambda _: "https://x.execute-api.us-east-1.a/p")
    monkeypatch.setattr(demo.signal, "signal", lambda *a: None)
    monkeypatch.setattr(demo, "_shaper_tokens", lambda *a: (100.0, 50.0, 150.0))
    monkeypatch.setattr(demo, "DEMO_METRICS_POLL_INTERVAL_S", 0)
    monkeypatch.setattr(demo, "DEMO_RESTORE_BACKOFF_S", 0)
    monkeypatch.setattr(demo, "DEMO_TMP_DIR", str(tmp_path))

    def shaper(*args, state, **kwargs):
        n = sum(c for _, c in args[6])
        state.update(lock=threading.Lock(), results={}, sent_ids={}, arns={}, pending=0, sent=n)
        log.append("shaper")
        return h["shaper"](h, state, n)

    def direct(*args, **kwargs):
        h["direct"](h, args[6], args[7], sum(c for _, c in args[4]))

    def cleanup(state, *args):
        log.append(f"cleanup:{args[-1]}")

    if stub_shaper:
        monkeypatch.setattr(demo, "_run_shaper_arm", shaper)
    monkeypatch.setattr(demo, "_run_direct_arm", direct)
    monkeypatch.setattr(demo, "_cleanup_shaper", cleanup)
    return h


@pytest.fixture
def harness(monkeypatch, tmp_path):
    return _make_harness(monkeypatch, tmp_path)


def _main(h, *argv):
    demo.main([MODEL, "--profile", "10:4", "--events", str(h["events"]), *argv])


def _summary(h):
    return json.loads(h["events"].with_name("run-summary.json").read_text())


def _run_errors(h):
    events = [json.loads(line) for line in h["events"].read_text().splitlines()]
    return [e for e in events if e["event"] == "run_error"]


# ── CONFIG lifecycle ───────────────────────────────────────────────────────────


def test_success_overrides_then_restores_and_drops_the_backup(harness):
    _main(harness)
    assert harness["log"] == ["put:override", "shaper", "put:original"]
    assert harness["table"].item == ORIGINAL  # the marker went with the override
    assert list(harness["tmp"].glob("*-config-backup.json")) == []
    s = _summary(harness)
    assert s["meta"]["run_error"] is None and s["meta"]["warnings"] == []
    assert s["arms"]["shaper"]["served"] == 4 and s["arms"]["shaper"]["tokens"] == 150


def test_the_override_carries_a_run_marker(harness):
    seen = {}
    harness["shaper"] = lambda h, state, n: seen.update(h["table"].item) or []
    _main(harness)
    assert seen["tpm_limit"] == demo.DEMO_TPM_OVERRIDE and seen["demo_override_run"]


def test_a_shaper_arm_exception_still_restores_and_fails_the_run(harness):
    def boom(h, state, n):
        raise RuntimeError("kaput arn:aws:iam::123456789012:role/x")

    harness["shaper"] = boom
    with pytest.raises(SystemExit) as exc:
        _main(harness)
    assert exc.value.code == 1
    assert harness["log"][0] == "put:override" and harness["log"][-1] == "put:original"
    s = _summary(harness)
    assert s["meta"]["run_error"] == "demo run failed: RuntimeError"  # no message text
    assert s["arms"]["shaper"]["unresolved"] == 4 and s["arms"]["shaper"]["error"]
    errors = _run_errors(harness)
    assert errors[0]["reason"] == "run_failed" and "123456789012" not in errors[0]["message"]


def test_a_stop_cleans_up_the_shaper_before_the_restore(harness):
    def stopped(h, state, n):
        demo._STOP.set()
        return [{"status": 200, "ms": 5.0, "outcome": "served"}] + [
            {"status": None, "ms": None, "outcome": "cancelled"}
        ]

    harness["shaper"] = stopped
    _main(harness)
    assert harness["log"] == ["put:override", "shaper", "cleanup:stop", "put:original"]
    s = _summary(harness)
    assert s["meta"]["stopped"] is True
    sh = s["arms"]["shaper"]
    assert (sh["offered"], sh["served"], sh["cancelled"], sh["unsent"]) == (4, 1, 1, 2)


def test_a_drain_timeout_cancels_open_requests_before_the_restore(harness):
    def timed_out(h, state, n):
        state["pending"] = 1
        return [{"status": 200, "ms": 5.0, "outcome": "served"}] * (n - 1) + [
            {"status": None, "ms": None, "outcome": "unresolved"}
        ]

    harness["shaper"] = timed_out
    _main(harness)
    assert harness["log"] == ["put:override", "shaper", "cleanup:drain timeout", "put:original"]
    assert any("drain timed out" in w for w in _summary(harness)["meta"]["warnings"])


def test_real_quota_never_writes_config(harness):
    _main(harness, "--real-quota")
    assert harness["log"] == ["shaper"]
    assert list(harness["tmp"].glob("*-config-backup.json")) == []


def test_a_failed_restore_keeps_the_backup_and_prints_the_command(harness, capsys):
    harness["table"].fail_restores = demo.DEMO_RESTORE_ATTEMPTS
    with pytest.raises(SystemExit) as exc:
        _main(harness)
    assert exc.value.code == 1
    (backup,) = harness["tmp"].glob("*-config-backup.json")
    typed = json.loads(backup.read_text())
    assert typed["tpm_limit"] == {"N": "8000000"} and typed["rpm_limit"] == {"NULL": True}
    out = capsys.readouterr().out
    assert f"--item file://{backup}" in out and "aws dynamodb put-item" in out
    assert _run_errors(harness)[-1]["reason"] == "config_restore_failed"
    assert backup.name in _summary(harness)["meta"]["run_error"]
    assert str(harness["tmp"]) not in _summary(harness)["meta"]["run_error"]  # basename only


def test_a_restore_that_succeeds_on_retry_is_verified(harness):
    harness["table"].fail_restores = 1
    _main(harness)
    assert harness["table"].item == ORIGINAL and harness["log"][-1] == "put:original"


def test_a_config_already_carrying_a_marker_is_refused(harness, capsys):
    harness["table"].item = {**ORIGINAL, "demo_override_run": "other-run"}
    with pytest.raises(SystemExit) as exc:
        _main(harness)
    assert exc.value.code == 2 and harness["log"] == []
    out = capsys.readouterr().out
    assert "other-run" in out and "other-run-config-backup.json" in out


def test_restore_skips_an_item_another_run_has_taken_over(harness):
    def takeover(h, state, n):
        h["table"].item = {**ORIGINAL, "tpm_limit": Decimal("1"), "demo_override_run": "other"}
        return [{"status": 200, "ms": 5.0, "outcome": "served"}] * n

    harness["shaper"] = takeover
    _main(harness)  # a warning, not a run error
    assert harness["table"].item["demo_override_run"] == "other"
    assert harness["log"] == ["put:override", "shaper"]
    assert any("another demo run" in w for w in _summary(harness)["meta"]["warnings"])


# ── preflight in main ──────────────────────────────────────────────────────────


def test_force_waives_the_waf_check_only(harness):
    _main(harness, "--profile", "60:300", "--force")  # WAF only: runs, with a warning
    assert harness["log"][0] == "put:override"
    assert any("--force" in w for w in _summary(harness)["meta"]["warnings"])


def test_force_does_not_waive_queue_expiry(harness, capsys):
    with pytest.raises(SystemExit) as exc:
        _main(harness, "--profile", "3000:3100", "--force")  # busts WAF and expiry
    assert exc.value.code == 2 and harness["log"] == []
    out = capsys.readouterr().out
    assert "expire" in out and "Pass --force" not in out


@pytest.mark.parametrize("tpm_limit", [Decimal("0"), None])
def test_real_quota_without_a_tpm_limit_is_refused(harness, capsys, tpm_limit):
    harness["table"].item = {**ORIGINAL, "tpm_limit": tpm_limit}
    with pytest.raises(SystemExit) as exc:
        _main(harness, "--real-quota", "--profile", "10:1")
    assert exc.value.code == 2 and harness["log"] == []
    assert "no tpm_limit" in capsys.readouterr().out


def test_a_profile_that_rounds_to_nothing_for_this_model_exits_2(harness, capsys):
    harness["table"].item = {**ORIGINAL, "output_token_burndown_rate": Decimal("10")}
    with pytest.raises(SystemExit) as exc:
        _main(harness, "--profile", "10@0.1x")  # 1 request at nova's 1x, 0 at a 10x model's
    assert exc.value.code == 2 and harness["log"] == []
    assert "offers no requests" in capsys.readouterr().out


@pytest.mark.parametrize("spec", ["1:100000000", "3601:10"])
def test_oversized_profiles_are_rejected_at_parse(spec):
    with pytest.raises(ValueError, match="limit"):
        demo.parse_load_profile(spec)


# ── arm failures ───────────────────────────────────────────────────────────────


def test_a_crashed_direct_arm_fails_the_run_and_keeps_the_other_arms(harness):
    def direct(h, out, arm, n):
        if arm == "direct":
            out.update(results=[_served_direct(1)], start=0.0)
            raise RuntimeError("kaput")
        out.update(results=[_served_direct(i) for i in range(1, n + 1)], start=0.0, end=1.0)

    harness["direct"] = direct
    with pytest.raises(SystemExit) as exc:
        _main(harness)
    assert exc.value.code == 1
    s = _summary(harness)
    d = s["arms"]["direct"]
    assert (d["offered"], d["served"], d["unresolved"], d["unsent"]) == (4, 1, 3, 0)
    assert d["error"] == "RuntimeError"
    assert all(r.get("error") == "RuntimeError" for r in d["requests"][1:])
    assert s["arms"]["direct+retry"]["served"] == 4 and s["arms"]["shaper"]["served"] == 4
    assert "direct arm crashed: RuntimeError" in s["meta"]["run_error"]
    assert any(e["reason"] == "direct_arm_crashed" for e in _run_errors(harness))


def test_systematic_errors_flag_the_comparison_invalid(harness):
    def rejected(h, state, n):
        state["submit_failures"] = {"403": n}
        return [{"status": 403, "ms": 5.0, "outcome": "failed"}] * n

    def denied(h, out, arm, n):
        row = {**_served_direct(1), "ok": False, "outcome": "failed", "status": 500}
        out.update(results=[{**row, "error_code": "AccessDeniedException"}] * n, start=0, end=1)

    harness["shaper"], harness["direct"] = rejected, denied
    _main(harness)  # warnings, not run errors
    warnings = _summary(harness)["meta"]["warnings"]
    assert sum("comparison invalid" in w for w in warnings) == 3
    assert any("403 x4" in w for w in warnings)
    assert any("AccessDeniedException x4" in w for w in warnings)
    assert _summary(harness)["arms"]["shaper"]["failed"] == 4


def test_a_crashing_worker_keeps_the_rest_of_its_arm(monkeypatch):
    def call(brt, idx, *args):
        if idx == 2:
            raise RuntimeError("kaput")
        return _served_direct(idx)

    monkeypatch.setattr(demo.boto3, "client", lambda *a, **k: object())
    monkeypatch.setattr(demo, "_direct_call", call)
    out = {}
    demo._guarded_direct_arm(
        "us-east-1", "m", "p", 100, [(0.03, 3)], demo._new_virtual_budget(1.0), out, "direct", 0
    )
    assert "error" not in out and out["crashed_requests"] == 1
    assert [r["outcome"] for r in out["results"]] == ["served", "failed", "served"]
    assert out["results"][1]["error"] == "RuntimeError"


# ── shaper arm: poller, stop, status reads ─────────────────────────────────────


class FakeDynamo:
    def __init__(self, fail=None, state="SUCCEEDED"):
        self.fail, self.state = fail, state

    def batch_get_item(self, RequestItems):
        if self.fail:
            raise self.fail
        keys = RequestItems["t"]["Keys"]
        return {"Responses": {"t": [{"pk": k["pk"], "state": self.state} for k in keys]}}


def _shaper(monkeypatch, indices, submit=None, dynamo=None, **kw):
    monkeypatch.setattr(demo, "_paced_indices", lambda start, profile: iter(indices))
    monkeypatch.setattr(
        demo,
        "_submit_sized",
        submit or (lambda api, arm, *rest: {"request_id": rest[-1], "submit_status": 202}),
    )
    monkeypatch.setattr(demo, "DEMO_STATUS_POLL_INTERVAL_S", 0.01)
    state = {}
    results = demo._run_shaper_arm(
        "http://unused", "m", "p", demo.time.time(), dynamo or FakeDynamo(), "t", state=state, **kw
    )
    return results, state


def test_a_crashed_poller_is_a_run_error(monkeypatch):
    def boom(*a):
        raise KeyError("bug")

    monkeypatch.setattr(demo, "_read_status_batch", boom)
    results, state = _shaper(monkeypatch, [1])
    assert results[0]["outcome"] == "unresolved" and state["pending"] == 1
    assert demo._run_log["errors"] == ["shaper status poller crashed: KeyError"]


def test_a_poller_crash_through_main_exits_1_and_cleans_up(monkeypatch, tmp_path):
    h = _make_harness(monkeypatch, tmp_path, stub_shaper=False)
    monkeypatch.setattr(demo, "_read_status_batch", lambda *a: 1 / 0)
    monkeypatch.setattr(demo, "_paced_indices", lambda start, profile: iter([1, 2, 3, 4]))
    monkeypatch.setattr(
        demo,
        "_submit_sized",
        lambda api, arm, *rest: {"request_id": rest[-1], "submit_status": 202},
    )
    with pytest.raises(SystemExit) as exc:
        _main(h)
    assert exc.value.code == 1
    assert h["log"] == ["put:override", "cleanup:drain timeout", "put:original"]
    assert "poller crashed: ZeroDivisionError" in _summary(h)["meta"]["run_error"]


def test_persistent_status_read_failures_end_the_drain(monkeypatch):
    monkeypatch.setattr(demo, "DEMO_STATUS_MAX_FAILED_PASSES", 3)
    dynamo = FakeDynamo(fail=_client_error("AccessDeniedException", "BatchGetItem"))
    results, state = _shaper(monkeypatch, [1], dynamo=dynamo)
    assert state["pending"] == 1
    assert demo._run_log["errors"] == [
        "shaper STATUS reads failed 3 passes in a row (AccessDeniedException); stopped polling"
    ]


def test_queued_posts_are_not_sent_after_a_stop(monkeypatch):
    posts = []

    def submit(api, arm, *rest):
        posts.append(arm)
        demo._STOP.set()
        return {"request_id": rest[-1], "submit_status": 202, "execution_arn": "arn:1"}

    results, state = _shaper(monkeypatch, [1, 2], submit=submit, submit_workers=1)
    assert posts == ["shaper:req1"]
    assert [r["outcome"] for r in results] == ["cancelled", "unsent"]
    assert list(state["sent_ids"]) == [1] and state["arns"] == {state["sent_ids"][1]: "arn:1"}


def test_an_ingress_rejection_is_failed_with_slot_latency(monkeypatch):
    submit = lambda api, arm, *rest: {"request_id": rest[-1], "submit_status": 403}  # noqa: E731
    results, state = _shaper(monkeypatch, [1], submit=submit)
    assert results[0]["outcome"] == "failed" and results[0]["ms"] >= 0
    assert state["submit_failures"] == {"403": 1}


def test_sent_events_carry_the_models_estimate(monkeypatch, tmp_path):
    path = tmp_path / "run.jsonl"
    demo._start_events(str(path), 0.0)
    _shaper(monkeypatch, [1], est_tokens=6820)
    demo._direct_call(
        object(), 1, "m", "p", 1, demo._new_virtual_budget(5.0, level=0), "d", est_tokens=6820
    )
    sent = [json.loads(line) for line in path.read_text().splitlines() if '"sent"' in line]
    assert [e["est"] for e in sent] == [6820, 6820]


# ── shaper cleanup accounting ──────────────────────────────────────────────────


def test_stop_failures_are_counted_and_deletes_survive_them(monkeypatch):
    outcomes = {
        "arn:throttled": _client_error("ThrottlingException", "StopExecution"),
        "arn:gone": _client_error("ExecutionDoesNotExist", "StopExecution"),
        "arn:offline": EndpointConnectionError(endpoint_url="https://states"),
    }

    class Sfn:
        def stop_execution(self, executionArn, **kw):
            if executionArn in outcomes:
                raise outcomes[executionArn]

    class Table:
        def query(self, **kw):
            return {"Items": [{"pk": "q", "sk": "1", "request_id": "a"}]}

        def batch_writer(self):
            class Batch:
                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return False

                def delete_item(self, Key):
                    pass

            return Batch()

    class Dynamo:
        def Table(self, name):
            return Table()

    monkeypatch.setattr(demo.boto3, "client", lambda *a, **k: Sfn())
    arns = ["arn:ok", "arn:throttled", "arn:gone", "arn:offline"]
    counts = demo._cancel_outstanding(Dynamo(), "t", "m", ["a"], arns, "us-east-1")
    assert (counts["deleted"], counts["stopped"], counts["failed_stops"]) == (1, 1, 2)
    assert sorted(counts["errors"]) == [
        "StopExecution ClientError (ThrottlingException)",
        "StopExecution EndpointConnectionError",
    ]


def test_a_delete_failure_still_runs_the_stops(monkeypatch):
    class Dynamo:
        def Table(self, name):
            class Table:
                def query(self, **kw):
                    raise _client_error("ThrottlingException", "Query")

            return Table()

    stopped = []

    class Sfn:
        def stop_execution(self, executionArn, **kw):
            stopped.append(executionArn)

    monkeypatch.setattr(demo.boto3, "client", lambda *a, **k: Sfn())
    counts = demo._cancel_outstanding(Dynamo(), "t", "m", ["a"], ["arn:1"], "us-east-1")
    assert stopped == ["arn:1"] and counts["stopped"] == 1 and counts["deleted"] == 0
    assert counts["errors"] == ["queue delete ClientError (ThrottlingException)"]


def test_cleanup_accumulates_both_passes_and_fails_the_run_on_errors(monkeypatch, tmp_path):
    path = tmp_path / "run.jsonl"
    demo._start_events(str(path), 0.0)
    passes = iter(
        [
            {"deleted": 2, "stopped": 1, "failed_stops": 1, "errors": ["StopExecution X"]},
            {"deleted": 1, "stopped": 0, "failed_stops": 0, "errors": ["queue delete Y"]},
        ]
    )
    seen = []

    def cancel(dynamodb, table, model, ids, arns, region):
        seen.append((sorted(ids), arns))
        return next(passes)

    monkeypatch.setattr(demo, "_cancel_outstanding", cancel)
    monkeypatch.setattr(demo, "DEMO_STOP_ENQUEUE_SETTLE_S", 0)
    state = {
        "lock": threading.Lock(),
        "sent_ids": {1: "a", 2: "b", 3: "c"},
        "results": {3: {"status": 200}},
        "arns": {"a": "arn:a"},
    }
    demo._cleanup_shaper(state, None, "t", "m", "us-east-1", "stop")
    assert seen == [(["a", "b"], ["arn:a", None]), (["a", "b"], [])]
    events = [json.loads(line) for line in path.read_text().splitlines()]
    cleanup = next(e for e in events if e["event"] == "cleanup")
    assert (cleanup["deleted"], cleanup["stopped"], cleanup["failed_stops"]) == (3, 1, 1)
    assert cleanup["open"] == 2 and cleanup["unknown_arn"] == 1
    assert demo._run_log["errors"] and demo._run_log["warnings"]
    assert "StopExecution X; queue delete Y" in demo._run_log["errors"][0]
    assert any(e["event"] == "run_error" and e["reason"] == "cleanup_failed" for e in events)


# ── outcome accounting ─────────────────────────────────────────────────────────


def test_stopped_run_accounting_pads_to_the_profile():
    profile = [(10, 4), (10, 4)]
    per_arm = {
        "shaper": [
            {"status": 200, "ms": 1000.0, "outcome": "served"},
            {"status": 403, "ms": 50.0, "outcome": "failed"},
            {"status": None, "ms": None, "outcome": "cancelled"},
        ],
        "direct": [
            _served_direct(1),
            {**_served_direct(2), "ok": False, "outcome": "cancelled", "elapsed_ms": None},
        ],
    }
    s = demo._build_summary(profile, per_arm, {}, {"stopped": True})
    for arm, a in s["arms"].items():
        assert a["offered"] == 8 == sum(a[o] for o in demo.DEMO_OUTCOMES), arm
        assert sum(p["offered"] for p in a["phases"]) == a["offered"]
        for p in a["phases"]:
            assert sum(p[o] for o in demo.DEMO_OUTCOMES) == p["offered"]
    sh = s["arms"]["shaper"]
    assert (sh["served"], sh["failed"], sh["cancelled"], sh["unsent"]) == (1, 1, 1, 5)
    assert sh["latency_all"]["n"] == 2  # the ingress rejection is timed
    d = s["arms"]["direct"]
    assert (d["failed"], d["cancelled"], d["unsent"]) == (0, 1, 6)


def test_a_stopped_direct_call_is_cancelled_not_failed(monkeypatch):
    demo._STOP.set()
    res = demo._direct_call(object(), 1, "m", "p", 1, demo._new_virtual_budget(1.0), "d")
    assert res["outcome"] == "cancelled" and res["elapsed_ms"] is None


def test_markdown_shows_outcome_columns_banners_and_warnings():
    s = demo._build_summary(
        [(10, 2)],
        {"shaper": [{"status": 200, "ms": 1.0, "outcome": "served"}]},
        {},
        {"stopped": True, "run_error": "X", "warnings": ["comparison invalid: Y"]},
    )
    text = demo._summary_markdown(s)
    assert "| shaper | 2 | 1 (50%) | 0 | 0 | 0 | 1 |" in text
    assert "**Run error:** X" in text and "counts as cancelled" not in text
    assert "requests the stop aborted count as cancelled" in text
    assert "> **Warning:** comparison invalid: Y" in text


def test_only_429_counts_as_a_shaper_throttle_and_unknown_states_log_once(capsys):
    assert demo._terminal_http_status({"state": "WEIRD"}) == 503
    assert demo._terminal_http_status({"state": "WEIRD"}) == 503
    assert capsys.readouterr().out.count("unexpected STATUS") == 1


def test_an_unwritable_recording_turns_itself_off(monkeypatch, tmp_path):
    monkeypatch.setattr(demo, "_event_path", str(tmp_path))  # a directory: open() fails
    demo.emit("sent", "shaper", 1)
    assert demo._event_path is None
    assert demo._run_log["warnings"] == ["live event recording stopped: IsADirectoryError"]


# ── WAF limit lookup ───────────────────────────────────────────────────────────


@pytest.fixture
def waf(monkeypatch):
    """_waf_limits with an empty cache and a fake config + wafv2 client."""
    monkeypatch.setattr(demo, "_waf_cache", {})
    monkeypatch.setattr(demo, "_waf_warned", set())
    arn = "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/acl/abc"
    monkeypatch.setattr(demo.config_loader, "load_config", lambda: {"WAF_WEB_ACL_ARN": arn})
    acl: dict = {"Rules": [{"Name": "Other"}]}
    calls = []

    class Waf:
        def get_web_acl(self, **kw):
            calls.append(kw)
            return {"WebACL": acl}

    monkeypatch.setattr(demo.boto3, "client", lambda *a, **k: Waf())
    return {"acl": acl, "calls": calls}


def test_waf_limits_read_the_live_rule_and_cache_it(waf):
    rule = {"RateBasedStatement": {"Limit": 10000, "EvaluationWindowSec": 60}}
    waf["acl"]["Rules"].append({"Name": "PerIpRateLimit", "Statement": rule})
    assert demo._waf_limits() == (10000, 60, "live web ACL")
    assert demo._waf_limits() == (10000, 60, "live web ACL")
    assert len(waf["calls"]) == 1


def test_waf_limits_fall_back_without_caching_when_the_rule_is_missing(waf, capsys):
    limit, window, source = demo._waf_limits()
    assert (limit, window) == (demo.DEMO_WAF_LIMIT, demo.DEMO_WAF_WINDOW_S)
    assert source == "default 200/300s: no PerIpRateLimit rule in the web ACL"
    demo._waf_limits()
    assert len(waf["calls"]) == 2  # not cached: read again
    assert capsys.readouterr().out.count("WARNING") == 1  # warned once


def test_waf_limits_name_an_aws_error(waf, monkeypatch):
    class Denied:
        def get_web_acl(self, **kw):
            raise _client_error("AccessDeniedException", "GetWebACL")

    monkeypatch.setattr(demo.boto3, "client", lambda *a, **k: Denied())
    assert demo._waf_limits()[2] == "default 200/300s: GetWebACL failed (AccessDeniedException)"


@pytest.mark.parametrize(
    "config,reason",
    [(SystemExit(1), "no config.env"), ({}, "unreadable WAF_WEB_ACL_ARN or web ACL (ValueError)")],
)
def test_waf_limits_without_usable_config(waf, monkeypatch, config, reason):
    def load():
        if isinstance(config, BaseException):
            raise config
        return config

    monkeypatch.setattr(demo.config_loader, "load_config", load)
    assert demo._waf_limits()[2] == f"default 200/300s: {reason}"


def test_waf_problem_makes_no_aws_call_when_limits_are_known(monkeypatch):
    def no_aws(*a, **k):
        raise AssertionError("no boto3 call expected")

    monkeypatch.setattr(demo.boto3, "client", no_aws)
    monkeypatch.setattr(demo.config_loader, "load_config", no_aws)
    problem = demo._waf_problem(demo.parse_load_profile("60:300"))
    assert "test default" in problem and "waf_ip_rate_limit=10000" in problem
