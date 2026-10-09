"""Offline contract tests for the real-run event recorder and shared replay feed."""

import json
import pathlib
import sys
import threading
from http.server import ThreadingHTTPServer
from urllib.request import urlopen

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))
import demo  # noqa: E402
import demo_ui  # noqa: E402


class FakeDynamo:
    """BatchGetItem stand-in. `states` is consumed one per read of each key; an
    exhausted list keeps returning its last state. None = item absent."""

    def __init__(self, states, table="t"):
        self.states, self.table, self.calls, self.reads = states, table, [], {}

    def batch_get_item(self, RequestItems):
        keys = RequestItems[self.table]["Keys"]
        self.calls.append([k["pk"] for k in keys])
        items = []
        for k in keys:
            n = self.reads.get(k["pk"], 0)
            self.reads[k["pk"]] = n + 1
            state = self.states[min(n, len(self.states) - 1)]
            if isinstance(state, tuple):
                items.append({"pk": k["pk"], "state": state[0], "reason": state[1]})
            elif state is not None:
                items.append({"pk": k["pk"], "state": state})
        return {"Responses": {self.table: items}, "UnprocessedKeys": {}}


def recording(monkeypatch, tmp_path):
    path = tmp_path / "run.jsonl"
    monkeypatch.setattr(demo, "_event_path", None)
    demo._start_events(str(path), 0.0)
    return path


def test_actual_sent_events_and_replay_use_same_file(monkeypatch, tmp_path):
    path = recording(monkeypatch, tmp_path)
    monkeypatch.setattr(demo, "_paced_indices", lambda start, profile: iter([22]))
    monkeypatch.setattr(
        demo, "_submit_sized", lambda *args: {"request_id": "r", "submit_status": 202}
    )
    monkeypatch.setattr(demo, "DEMO_STATUS_POLL_INTERVAL_S", 0.01)
    demo._run_shaper_arm(
        "http://unused", "model", "prompt", demo.time.time(), FakeDynamo(["SUCCEEDED"]), "t"
    )

    class NoBedrock:
        def converse(self, **kwargs):
            pytest.fail("rejected requests must not reach Bedrock")

    for arm in ("direct", "direct+retry"):
        demo._direct_call(
            NoBedrock(),
            22,
            "unused",
            "prompt",
            100,
            demo._new_virtual_budget(1.0, level=0),
            arm,
        )
    lines = demo_ui.load_events(str(path))
    sent = [line for line in lines if line["event"] == "sent"]
    assert [(line["arm"], line["req"]) for line in sent] == [
        ("shaper", 22),
        ("direct", 22),
        ("direct+retry", 22),
    ]
    assert all(line["est"] == demo.DEMO_TOKENS_PER_REQUEST_ESTIMATE for line in sent)


def test_direct_gate_rejections_emit_attempts_without_aws(monkeypatch, tmp_path):
    path = recording(monkeypatch, tmp_path)
    monkeypatch.setattr(demo, "_backoff", lambda attempt: None)

    class NoBedrock:
        def converse(self, **kwargs):
            pytest.fail("rejected requests must not reach Bedrock")

    budget = demo._new_virtual_budget(1.0, level=0)
    result = demo._direct_call(
        NoBedrock(), 22, "unused", "prompt", 100, budget, "direct+retry", max_retries=1
    )
    assert result["simulated_throttle"] and result["bedrock_calls"] == 0
    attempts = [line for line in demo_ui.load_events(str(path)) if line["event"] == "attempt"]
    assert [(line["attempt"], line["outcome"], line["simulated"]) for line in attempts] == [
        (1, "throttled", True),
        (2, "exhausted", True),
    ]


def test_success_is_recorded_only_after_actual_converse(monkeypatch, tmp_path):
    path = recording(monkeypatch, tmp_path)

    class Bedrock:
        def converse(self, **kwargs):
            assert not any(e["event"] == "attempt" for e in demo_ui.load_events(str(path)))
            return {"usage": {"inputTokens": 12, "outputTokens": 5}}

    budget = demo._new_virtual_budget(1.0)
    result = demo._direct_call(Bedrock(), 1, "unused", "prompt", 100, budget, "direct")
    assert result["ok"]
    attempt = demo_ui.load_events(str(path))[-1]
    assert (attempt["outcome"], attempt["in"], attempt["out"]) == ("ok", 12, 5)


def test_shaper_events_are_observed_not_predicted(monkeypatch, tmp_path):
    path = recording(monkeypatch, tmp_path)
    monkeypatch.setattr(demo, "_paced_indices", lambda start, profile: iter([1]))
    monkeypatch.setattr(
        demo, "_submit_sized", lambda *args: {"request_id": "r", "submit_status": 202}
    )
    monkeypatch.setattr(demo, "DEMO_STATUS_POLL_INTERVAL_S", 0.01)
    demo._run_shaper_arm(
        "http://unused", "model", "prompt", demo.time.time(), FakeDynamo(["SUCCEEDED"]), "t"
    )
    lines = demo_ui.load_events(str(path))
    assert [line["event"] for line in lines] == ["meta", "sent", "attempt", "done"]
    assert lines[-1]["status"] == 200
    assert "queued_s" not in lines[-1]  # no invented internal dispatch time


def _fake_submits(monkeypatch, indices):
    monkeypatch.setattr(demo, "_paced_indices", lambda start, profile: iter(indices))
    monkeypatch.setattr(
        demo,
        "_submit_sized",
        lambda api, arm, *rest: {"request_id": arm.split("req")[1], "submit_status": 202},
    )
    monkeypatch.setattr(demo, "DEMO_STATUS_POLL_INTERVAL_S", 0.01)


def test_poller_reads_only_outstanding_ids_and_drops_terminal(monkeypatch, tmp_path):
    path = recording(monkeypatch, tmp_path)
    _fake_submits(monkeypatch, [1, 2])
    fake = FakeDynamo([None, "QUEUED", "SUCCEEDED"])
    results = demo._run_shaper_arm("http://unused", "m", "p", demo.time.time(), fake, "t")
    assert [r["status"] for r in results] == [200, 200]
    assert all(r["ms"] is not None for r in results)
    # Each ID is read until terminal (3 reads), then never again.
    assert fake.reads == {"REQUEST#1": 3, "REQUEST#2": 3}
    assert all(call for call in fake.calls)  # never an empty BatchGetItem
    done = [line for line in demo_ui.load_events(str(path)) if line["event"] == "done"]
    assert sorted(line["req"] for line in done) == [1, 2]


def test_failed_reasons_use_result_fn_status_map():
    for reason, code in demo.DEMO_FAILED_REASON_TO_STATUS.items():
        assert demo._terminal_http_status({"state": "FAILED", "reason": reason}) == code
    assert demo._terminal_http_status({"state": "FAILED", "reason": "???"}) == 503
    assert demo._terminal_http_status({"state": "SUCCEEDED"}) == 200
    for state in (None, "PENDING", "QUEUED"):
        assert demo._terminal_http_status({"state": state}) is None


def test_rejected_submit_is_never_polled(monkeypatch, tmp_path):
    recording(monkeypatch, tmp_path)
    monkeypatch.setattr(demo, "_paced_indices", lambda start, profile: iter([1]))
    monkeypatch.setattr(demo, "_submit_sized", lambda *a: {"request_id": "x", "submit_status": 403})
    monkeypatch.setattr(demo, "DEMO_STATUS_POLL_INTERVAL_S", 0.01)
    fake = FakeDynamo(["SUCCEEDED"])
    results = demo._run_shaper_arm("http://unused", "m", "p", demo.time.time(), fake, "t")
    # An ingress rejection is a failed request, timed from its slot to the response.
    assert [(r["status"], r["outcome"]) for r in results] == [(403, "failed")]
    assert results[0]["ms"] is not None
    assert fake.calls == []


def test_batch_errors_are_logged_and_ids_stay_outstanding(capsys):
    from botocore.exceptions import ClientError

    class Failing:
        def batch_get_item(self, RequestItems):
            raise ClientError({"Error": {"Code": "AccessDeniedException"}}, "BatchGetItem")

    assert demo._read_status_batch(Failing(), "t", ["a"]) == {}
    assert "AccessDeniedException" in capsys.readouterr().out

    class Unprocessed:
        def batch_get_item(self, RequestItems):
            keys = RequestItems["t"]["Keys"]
            return {"Responses": {"t": []}, "UnprocessedKeys": {"t": {"Keys": keys}}}

    assert demo._read_status_batch(Unprocessed(), "t", ["a", "b"]) == {}
    assert "2 key(s) unprocessed" in capsys.readouterr().out


def test_batches_are_chunked_at_the_key_limit(monkeypatch):
    monkeypatch.setattr(demo, "DEMO_STATUS_BATCH_MAX_KEYS", 2)
    fake = FakeDynamo(["SUCCEEDED"])
    assert len(demo._read_status_batch(fake, "t", ["a", "b", "c"])) == 3
    assert [len(c) for c in fake.calls] == [2, 1]


def test_drain_timeout_leaves_unresolved_as_none(monkeypatch, tmp_path):
    recording(monkeypatch, tmp_path)
    _fake_submits(monkeypatch, [1])
    monkeypatch.setattr(demo, "DEMO_DRAIN_TIMEOUT_S", 0.05)
    results = demo._run_shaper_arm(
        "http://unused", "m", "p", demo.time.time(), FakeDynamo(["QUEUED"]), "t"
    )
    assert results == [{"status": None, "ms": None, "outcome": "unresolved"}]


def test_http_cursor_tails_same_file_in_replay_and_live_modes(monkeypatch, tmp_path):
    path = recording(monkeypatch, tmp_path)
    monkeypatch.setattr(demo_ui.Handler, "trace", str(path))
    monkeypatch.setattr(demo_ui.Handler, "process", None)
    server = ThreadingHTTPServer(("127.0.0.1", 0), demo_ui.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/events?since="
    try:
        with urlopen(url + "0") as response:  # nosec B310 -- fixed localhost HTTP URL
            first = json.load(response)
        assert first["live"] is False and first["cursor"] == 1
        with urlopen(url.split("/events")[0] + "/") as response:  # nosec B310 -- localhost
            assert b"Actual demo events" in response.read()
        demo.emit("sent", "shaper", 1, est=demo.DEMO_TOKENS_PER_REQUEST_ESTIMATE)
        with urlopen(url + "1") as response:  # nosec B310 -- fixed localhost HTTP URL
            tail = json.load(response)
        assert tail["cursor"] == 2 and tail["events"][0]["req"] == 1
        with open(path, "ab") as stream:
            stream.write(b'{"event":"sent"')  # incomplete append must not be published
        assert len(demo_ui.load_events(str(path))) == 2
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
