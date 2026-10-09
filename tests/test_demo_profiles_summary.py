"""Offline tests for --profile parsing, the WAF/drain preflight, the end-of-run
summary statistics, and the demo-ui launcher's request guards. No AWS.

Run: python -m pytest tests/test_demo_profiles_summary.py -q
"""

import http.client
import json
import pathlib
import sys
import threading
from http.server import ThreadingHTTPServer

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))
import demo  # noqa: E402
import demo_ui  # noqa: E402


@pytest.fixture(autouse=True)
def stack_default_waf(monkeypatch):
    """Pin the WAF limits to the stack defaults for every test here (and only
    here): these tests must not read a live web ACL."""
    limits = (demo.DEMO_WAF_LIMIT, demo.DEMO_WAF_WINDOW_S, "test default")
    monkeypatch.setitem(demo._waf_cache, "limits", limits)


# ── profile grammar ─────────────────────────────────────────────────────────────


def test_default_preset_is_the_original_profile():
    assert demo.parse_load_profile("default") == demo.DEMO_LOAD_PROFILE


def test_count_and_multiplier_phases():
    assert demo.parse_load_profile("25:70, 15:0", base_rpm=30) == [(25, 70), (15, 0)]
    # 15s at 6x of 30/min = 45 requests; 0x is an idle gap that keeps its duration.
    assert demo.parse_load_profile("15@6x,30@0x", base_rpm=30) == [(15, 45), (30, 0)]
    assert demo.parse_load_profile("10@1.5", base_rpm=60) == [(10, 15)]


@pytest.mark.parametrize("bad", ["", "25", "25:x", "0:5", "-3@2x", "10@-1x", "10:0", "abc"])
def test_bad_specs_are_rejected(bad):
    with pytest.raises(ValueError):
        demo.parse_load_profile(bad)


@pytest.mark.parametrize("name", list(demo.DEMO_PROFILE_PRESETS))
def test_every_preset_parses_fits_the_waf_and_drains_before_expiry(name):
    # Presets are sized for the demo stack's WAF (deployed with
    # -c waf_ip_rate_limit=10000 -c waf_ip_rate_window_sec=60); against the stack
    # default the launcher refuses the larger ones before touching CONFIG.
    profile = demo.parse_load_profile(name)
    assert demo._waf_problem(profile, (10_000, 60)) is None
    expected_drain_s, _, _ = demo._run_bounds(profile, demo.DEMO_TPM_OVERRIDE * 0.85)
    assert expected_drain_s < demo.DEMO_MAX_DRAIN_S


def test_profile_over_the_per_ip_waf_limit_is_flagged():
    # The 223-request shape from the PR thread puts 223 POSTs in one 300s window.
    problem = demo._waf_problem(demo.parse_load_profile("60:33,25:70,15:0,15:105,60:15"))
    assert problem and "223" in problem


def test_peak_window_counts_a_sliding_window():
    assert demo._peak_requests_in_window([0, 1, 2, 10, 11], 5) == 3
    assert demo._peak_requests_in_window([0, 5, 10], 5) == 1  # half-open window


def test_run_bounds_scale_with_the_profile():
    small = demo._run_bounds([(25, 70)], 85_000, est_tokens=2820)
    big = demo._run_bounds([(600, 300)], 85_000, est_tokens=2820)
    assert small[1] == demo.DEMO_DRAIN_TIMEOUT_S  # floor
    assert big[0] == pytest.approx(300 * 2820 / (85_000 / 60))
    assert big[1] == pytest.approx(2 * big[0])
    # one worker per request, inside [min, max]
    assert small[2] == 70 and big[2] == 300
    assert demo._run_bounds([(5, 2)], 85_000)[2] == demo.DEMO_DIRECT_MIN_WORKERS


# ── summary statistics ─────────────────────────────────────────────────────────


def test_nearest_rank_percentiles():
    vals = list(range(1, 101))
    assert demo._pctl(vals, 50) == 50
    assert demo._pctl(vals, 95) == 95
    assert demo._pctl([7], 95) == 7
    assert demo._pctl([], 95) is None
    stats = demo._latency_stats([None, 3.0, 1.0, 2.0])
    assert stats == {"n": 3, "avg": 2.0, "p50": 2.0, "p95": 3.0, "p99": 3.0, "max": 3.0}


def _direct(idx, ok, elapsed_ms, status=None):
    return {
        "idx": idx,
        "ok": ok,
        "elapsed_ms": elapsed_ms,
        "status": status or (200 if ok else 429),
    }


def test_summary_counts_latency_total_time_and_phases():
    profile = [(10, 2), (10, 2)]  # requests at t=0, 5, 10, 15
    per_arm = {
        "shaper": [
            {"status": 200, "ms": 20_000},
            {"status": 200, "ms": 30_000},
            {"status": 504, "ms": 60_000},
            {"status": None, "ms": None},  # never resolved
        ],
        "direct": [
            _direct(1, True, 4_000),
            _direct(2, False, 1.0),
            _direct(3, True, 6_000),
            _direct(4, False, 2.0),
        ],
    }
    rows = {"shaper": {"attempts": 4, "in": 10, "out": 5}, "direct": {"attempts": 4}}
    s = demo._build_summary(profile, per_arm, rows, {"model_id": "m"})

    sh = s["arms"]["shaper"]
    assert (sh["served"], sh["failed"], sh["unresolved"]) == (2, 1, 1)
    assert sh["latency_served"]["avg"] == 25_000 and sh["latency_served"]["p95"] == 30_000
    assert sh["total_s"] == pytest.approx(10 + 60)  # slot 10s + 60s to resolve
    assert sh["last_success_s"] == pytest.approx(5 + 30)
    assert sh["tokens"] == 15

    d = s["arms"]["direct"]
    assert (d["served"], d["failed"]) == (2, 2)
    # served latency excludes the fast rejections; latency_all includes them
    assert d["latency_served"]["avg"] == 5_000
    assert d["latency_all"]["n"] == 4
    assert [p["served"] for p in d["phases"]] == [1, 1]
    assert d["phases"][1]["latency_served"]["avg"] == 6_000
    assert d["served_pct"] == 50.0


def test_summary_markdown_and_files(tmp_path):
    profile = [(10, 1)]
    s = demo._build_summary(
        profile, {"shaper": [{"status": 200, "ms": 1500}]}, {}, {"model_id": "m"}
    )
    path = demo._write_summary(s, str(tmp_path / "run.jsonl"))
    assert path.endswith("run-summary.md")
    text = pathlib.Path(path).read_text()
    assert "| shaper | 1 | 1 (100%)" in text and "1.5s" in text
    assert json.loads((tmp_path / "run-summary.json").read_text())["arms"]["shaper"]["served"] == 1


# ── direct arm: done events, ordering, slot-based latency ──────────────────────


def _record(monkeypatch, tmp_path):
    path = tmp_path / "run.jsonl"
    monkeypatch.setattr(demo, "_event_path", None)
    demo._start_events(str(path), 0.0)
    return path


def _events(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_direct_call_emits_a_done_event_with_status_and_slot_latency(monkeypatch, tmp_path):
    path = _record(monkeypatch, tmp_path)
    budget = demo._new_virtual_budget(1.0, level=0)  # empty: rejected at once

    class NoBedrock:
        def converse(self, **kwargs):
            pytest.fail("rejected requests must not reach Bedrock")

    t_submit = demo.time.time() - 2.0  # waited 2s for a worker
    res = demo._direct_call(NoBedrock(), 7, "m", "p", 100, budget, "direct", t_submit=t_submit)
    assert res["idx"] == 7 and res["status"] == 429 and res["ms"] is None
    assert res["elapsed_ms"] >= 2000  # the wait counts against the arm
    done = [e for e in _events(path) if e["event"] == "done"]
    assert done == [
        {
            "event": "done",
            "t": done[0]["t"],
            "arm": "direct",
            "req": 7,
            "status": 429,
            "ms": done[0]["ms"],
            "simulated": True,
        }
    ]


def test_meta_event_carries_profile_offsets_and_run_fields(monkeypatch, tmp_path):
    path = tmp_path / "run.jsonl"
    monkeypatch.setattr(demo, "_event_path", None)
    demo._start_events(str(path), 0.0, [(10, 2)], model_id="m", profile_spec="10:2")
    meta = _events(path)[0]
    assert meta["offered"] == 2 and meta["offsets"] == [0.0, 5.0]
    assert meta["model_id"] == "m" and meta["profile_spec"] == "10:2"


def test_cli_parse_is_syntax_only_and_preflight_is_per_model():
    # The WAF/expiry decision waits for the model's burndown (main), not argv parsing.
    assert demo._parse_args(["haiku-4-5", "--profile", "300@0.8x"]).load_profile
    nova = demo.parse_load_profile("300@0.8x", demo.base_rpm_for(1.0))
    haiku = demo.parse_load_profile("300@0.8x", demo.base_rpm_for(5.0))
    assert "WAF" in demo._preflight_problem(nova, 1.0, 0.85)
    assert demo._preflight_problem(haiku, 5.0, 0.85) is None


def test_preflight_refuses_a_profile_that_outlives_queue_expiry():
    problem = demo._preflight_problem([(3000, 1500)], 5.0, 0.85)
    assert problem and "expire" in problem


@pytest.mark.parametrize("bad", ["nan:5", "inf:5", "10@infx", "10@nanx"])
def test_non_finite_phases_are_rejected(bad):
    with pytest.raises(ValueError):
        demo.parse_load_profile(bad)


def test_one_failed_post_fails_one_request_not_the_arm(monkeypatch, tmp_path):
    _record(monkeypatch, tmp_path)
    calls = iter([OSError("boom"), {"request_id": "r2", "submit_status": 202}])

    def submit(*args):
        r = next(calls)
        if isinstance(r, Exception):
            raise r
        return r

    class Done:
        def batch_get_item(self, RequestItems):
            keys = RequestItems["t"]["Keys"]
            return {"Responses": {"t": [{"pk": k["pk"], "state": "SUCCEEDED"} for k in keys]}}

    monkeypatch.setattr(demo, "_paced_indices", lambda start, profile: iter([1, 2]))
    monkeypatch.setattr(demo, "_submit_sized", submit)
    monkeypatch.setattr(demo, "DEMO_STATUS_POLL_INTERVAL_S", 0.01)
    results = demo._run_shaper_arm("http://unused", "m", "p", demo.time.time(), Done(), "t")
    assert results[0]["status"] is None and results[1]["status"] == 200


# ── demo-ui launcher guards ────────────────────────────────────────────────────


class FakeRunner:
    def __init__(self):
        self.started = []

    def models(self):
        return [
            {
                "model_id": "us.amazon.nova-2-lite-v1:0",
                "alias": "nova-2-lite",
                "burndown": 1.0,
                "queue_fraction": 0.85,
            },
            {
                "model_id": "us.anthropic.claude-haiku-4-5-20251001-v1:0",
                "alias": "haiku-4-5",
                "burndown": 5.0,
                "queue_fraction": 0.85,
            },
        ]

    def start(self, model, profile):
        self.started.append((model, profile))
        return True, "trace"

    def state(self):
        return {"trace": "", "run_id": len(self.started), "running": False, "exit_code": None}


@pytest.fixture
def launcher(monkeypatch):
    runner = FakeRunner()
    monkeypatch.setattr(demo_ui.Handler, "runner", runner)
    monkeypatch.setattr(demo_ui.Handler, "process", None)
    server = ThreadingHTTPServer(("127.0.0.1", 0), demo_ui.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield runner, server.server_port
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def _post(port, body, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    hdrs = {"Content-Type": "application/json"} if headers is None else headers
    conn.request("POST", "/start", body=json.dumps(body), headers=hdrs)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def test_start_uses_the_models_burndown_for_the_waf_check(launcher):
    runner, port = launcher
    body = {"model": "us.anthropic.claude-haiku-4-5-20251001-v1:0", "profile": "300@0.8x"}
    assert _post(port, body)[0] == 200  # 117 requests at haiku's 1x; 284 at nova's


def test_start_runs_an_allowed_model_and_profile(launcher):
    runner, port = launcher
    status, _ = _post(port, {"model": "us.amazon.nova-2-lite-v1:0", "profile": "steep"})
    assert status == 200 and runner.started == [("us.amazon.nova-2-lite-v1:0", "steep")]


@pytest.mark.parametrize(
    "body,expected",
    [
        ({"model": "not-configured", "profile": "default"}, 400),
        ({"model": "us.amazon.nova-2-lite-v1:0", "profile": "nonsense"}, 400),
        ({"model": "us.amazon.nova-2-lite-v1:0", "profile": "60:300"}, 400),  # WAF
        ({"model": "us.amazon.nova-2-lite-v1:0", "profile": "nan:5"}, 400),
        ({"model": "us.anthropic.claude-haiku-4-5-20251001-v1:0", "profile": "3000:1500"}, 400),
        ({"profile": "default"}, 400),
    ],
)
def test_start_rejects_unknown_models_bad_profiles_and_waf_busters(launcher, body, expected):
    runner, port = launcher
    assert _post(port, body)[0] == expected and runner.started == []


def test_start_requires_json_and_a_local_origin(launcher):
    runner, port = launcher
    body = {"model": "us.amazon.nova-2-lite-v1:0"}
    assert _post(port, body, {"Content-Type": "text/plain"})[0] == 415
    evil = {"Content-Type": "application/json", "Origin": "https://evil.example"}
    assert _post(port, body, evil)[0] == 403
    rebound = {"Content-Type": "application/json", "Host": "evil.example"}
    assert _post(port, body, rebound)[0] == 403
    assert runner.started == []


def test_events_report_controls_and_run_id(launcher):
    runner, port = launcher
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("GET", "/events?since=0")
    data = json.loads(conn.getresponse().read())
    conn.close()
    assert data["controls"] is True and data["run_id"] == 0 and data["events"] == []


def test_one_x_slows_with_output_burndown():
    # A 5x-burndown model charges ~2.3x the tokens per request, so its 1x is slower.
    assert demo.base_rpm_for(1.0) == pytest.approx(demo.DEMO_BASE_RPM)
    assert demo.base_rpm_for(5.0) < demo.base_rpm_for(1.0) / 2


def test_reconcile_settles_output_at_the_burndown_rate():
    """Bedrock settles quota as input + output x burndown, so a 5x model's credit
    back is est - (in + 5*out), not est - (in + out)."""
    budget = demo._new_virtual_budget(5.0)

    class Bedrock:
        def converse(self, **kwargs):
            return {"usage": {"inputTokens": 1000, "outputTokens": 200}}

    est = demo.estimate_request_tokens(
        prompt="p" * 400, max_tokens=1000, burndown_rate=5.0, bytes_per_token=5
    )
    demo._direct_call(Bedrock(), 1, "m", "p" * 400, 1000, budget, "direct")
    assert budget["limit"] - budget["level"] == pytest.approx(1000 + 5 * 200, abs=2)
    assert est > 1000 + 5 * 200


def test_waf_check_follows_the_deployed_limit():
    profile = demo.parse_load_profile("60:300")
    assert demo._waf_problem(profile, (200, 300))
    assert demo._waf_problem(profile, (10_000, 60)) is None
    burst = demo.parse_load_profile("15:5076")  # 10x a 6M quota at 11,820 est tokens
    assert demo._waf_problem(burst, (10_000, 60)) is None


# ── stop + cleanup ─────────────────────────────────────────────────────────────


@pytest.fixture
def stop_flag():
    demo._STOP.clear()
    yield demo._STOP
    demo._STOP.clear()


def test_a_stop_ends_the_paced_schedule(stop_flag):
    gen = demo._paced_indices(demo.time.time(), [(1, 100)])
    assert next(gen) == 1
    stop_flag.set()
    assert list(gen) == []


def test_a_stopped_direct_call_never_reaches_bedrock(stop_flag, monkeypatch, tmp_path):
    _record(monkeypatch, tmp_path)
    stop_flag.set()

    class NoBedrock:
        def converse(self, **kwargs):
            pytest.fail("a stopped request must not call Bedrock")

    res = demo._direct_call(NoBedrock(), 3, "m", "p", 100, demo._new_virtual_budget(1.0), "d")
    assert res["ok"] is False and res["status"] is None and res["attempts"] == 0


def test_cancel_deletes_only_this_runs_queue_items_then_stops_executions(monkeypatch):
    deleted, stopped = [], []

    class Batch:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def delete_item(self, Key):
            deleted.append(Key["sk"])

    class Table:
        def query(self, **kw):
            assert kw["ExpressionAttributeValues"][":p"] == "MODEL#m#QUEUE#ITEMS"
            if "ExclusiveStartKey" not in kw:
                return {
                    "Items": [{"pk": "q", "sk": "1", "request_id": "mine"}],
                    "LastEvaluatedKey": {"sk": "1"},
                }
            return {"Items": [{"pk": "q", "sk": "2", "request_id": "someone-else"}]}

        def batch_writer(self):
            return Batch()

    class Dynamo:
        def Table(self, name):
            return Table()

    class Sfn:
        def stop_execution(self, executionArn, **kw):
            stopped.append(executionArn)

    monkeypatch.setattr(demo.boto3, "client", lambda *a, **k: Sfn())
    counts = demo._cancel_outstanding(Dynamo(), "t", "m", ["mine"], ["arn:1", None], "us-east-1")
    assert counts == {"deleted": 1, "stopped": 1, "failed_stops": 0, "errors": []}
    assert deleted == ["1"] and stopped == ["arn:1"]


def test_stop_route_signals_the_runner(launcher):
    runner, port = launcher
    runner.stopped = 0
    runner.stop = lambda: (setattr(runner, "stopped", 1), (True, "stopping"))[1]
    assert _post_path(port, "/stop", {})[0] == 200 and runner.stopped == 1
    assert _post_path(port, "/stop", {}, {"Content-Type": "text/plain"})[0] == 415


def _post_path(port, path, body, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request(
        "POST", path, body=json.dumps(body), headers=headers or {"Content-Type": "application/json"}
    )
    resp = conn.getresponse()
    resp.read()
    conn.close()
    return resp.status, None


def test_direct_arm_survives_a_mid_run_stop_and_keeps_its_results(stop_flag, monkeypatch, tmp_path):
    """Regression: a stop used to crash _run_direct_arm (NameError), dropping the
    direct lanes from the summary and charts."""
    _record(monkeypatch, tmp_path)

    class Bedrock:
        def converse(self, **kwargs):
            return {"usage": {"inputTokens": 10, "outputTokens": 5}}

    monkeypatch.setattr(demo.boto3, "client", lambda *a, **k: Bedrock())
    real_paced = demo._paced_indices

    def paced(start, profile):
        for i in real_paced(start, profile):
            if i == 3:
                stop_flag.set()
            yield i

    monkeypatch.setattr(demo, "_paced_indices", paced)
    out = {}
    demo._guarded_direct_arm(
        "us-east-1", "m", "p", 100, [(1, 20)], demo._new_virtual_budget(1.0), out, "direct", 0
    )
    assert "error" not in out
    assert [r["idx"] for r in out["results"]] == [1, 2, 3]


def test_a_crashing_direct_arm_is_recorded_not_dropped(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("kaput")

    monkeypatch.setattr(demo, "_run_direct_arm", boom)
    out = {}
    demo._guarded_direct_arm(None, None, None, None, None, None, out, "direct", 0)
    assert out["error"] == "RuntimeError"  # type and error code only, no message text
