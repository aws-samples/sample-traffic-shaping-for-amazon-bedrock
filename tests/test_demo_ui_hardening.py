"""Offline tests for the demo-ui launcher's hardening (frame/CSRF headers, failure
reporting, keep-alive, corrupt recordings) and the stack's WAF context validation.
No AWS.

Run: python -m pytest tests/test_demo_ui_hardening.py -q
"""

import http.client
import json
import pathlib
import sys
import threading
import time
from http.server import ThreadingHTTPServer

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import demo  # noqa: E402
import demo_ui  # noqa: E402

NOVA = "us.amazon.nova-2-lite-v1:0"


@pytest.fixture(autouse=True)
def stack_default_waf(monkeypatch):
    """Preflight sees the stack's default WAF limit instead of reading a live web ACL."""
    monkeypatch.setattr(
        demo, "_waf_limits", lambda: (demo.DEMO_WAF_LIMIT, demo.DEMO_WAF_WINDOW_S, "test")
    )


class FakeRunner:
    def __init__(self, exit_code=None, tail=""):
        self.started, self.stops, self.exit_code, self.tail = [], 0, exit_code, tail

    def models(self):
        return [{"model_id": NOVA, "alias": "nova-2-lite", "burndown": 1.0, "queue_fraction": 0.85}]

    def start(self, model, profile):
        self.started.append((model, profile))
        return True, "trace"

    def stop(self):
        self.stops += 1
        return False, "no run in progress"

    def state(self):
        return {"trace": "", "run_id": 1, "running": False, "exit_code": self.exit_code}

    def log_tail(self):
        return self.tail


@pytest.fixture(autouse=True)
def restore_handler(monkeypatch):
    """main() assigns the Handler's class attributes; put them back after each test."""
    for name in ("runner", "trace", "process"):
        monkeypatch.setattr(demo_ui.Handler, name, getattr(demo_ui.Handler, name))


def _serve(monkeypatch, runner=None, trace=""):
    monkeypatch.setattr(demo_ui.Handler, "runner", runner)
    monkeypatch.setattr(demo_ui.Handler, "trace", trace)
    server = ThreadingHTTPServer(("127.0.0.1", 0), demo_ui.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


@pytest.fixture
def launcher(monkeypatch):
    runner = FakeRunner()
    server = _serve(monkeypatch, runner)
    yield runner, server.server_port
    server.shutdown()
    server.server_close()


def _request(port, method, path, body=None, headers=None, conn=None):
    conn = conn or http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    hdrs = {"Content-Type": "application/json"} if headers is None else headers
    conn.request(method, path, body=None if body is None else json.dumps(body), headers=hdrs)
    resp = conn.getresponse()
    return resp, resp.read()


# ── headers and cross-site requests ────────────────────────────────────────────


@pytest.mark.parametrize(
    "method,path",
    [("GET", "/"), ("GET", "/events"), ("GET", "/options"), ("GET", "/nope"), ("POST", "/stop")],
)
def test_every_response_refuses_framing(launcher, method, path):
    _, port = launcher
    resp, _ = _request(port, method, path, {} if method == "POST" else None)
    assert resp.getheader("X-Frame-Options") == "DENY"
    assert resp.getheader("Content-Security-Policy") == "frame-ancestors 'none'"


def test_error_responses_refuse_framing_too(launcher):
    _, port = launcher
    resp, _ = _request(port, "POST", "/start", {"model": NOVA}, {"Content-Type": "text/plain"})
    assert resp.status == 415 and resp.getheader("X-Frame-Options") == "DENY"


@pytest.mark.parametrize("site,expected", [("cross-site", 403), ("same-site", 403), ("none", 403)])
def test_posts_not_from_this_page_are_refused(launcher, site, expected):
    runner, port = launcher
    headers = {"Content-Type": "application/json", "Sec-Fetch-Site": site}
    assert _request(port, "POST", "/start", {"model": NOVA}, headers)[0].status == expected
    assert _request(port, "POST", "/stop", {}, headers)[0].status == expected
    assert runner.started == [] and runner.stops == 0


def test_same_origin_post_is_accepted(launcher):
    runner, port = launcher
    headers = {"Content-Type": "application/json", "Sec-Fetch-Site": "same-origin"}
    assert _request(port, "POST", "/start", {"model": NOVA}, headers)[0].status == 200
    assert runner.started == [(NOVA, "default")]


def test_stop_consumes_its_body_on_a_keepalive_connection(launcher):
    runner, port = launcher
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    resp, _ = _request(port, "POST", "/stop", {}, conn=conn)
    assert resp.status == 409 and runner.stops == 1
    # The same socket: an unread "{}" would turn this into a "{}GET" request line.
    resp, body = _request(port, "GET", "/events", headers={}, conn=conn)
    assert resp.status == 200 and json.loads(body)["controls"] is True
    conn.close()


def test_oversized_profile_is_refused_before_offsets_are_built(launcher):
    runner, port = launcher
    started = time.monotonic()
    resp, body = _request(port, "POST", "/start", {"model": NOVA, "profile": "1:100000000"})
    assert resp.status == 400 and "limit" in json.loads(body)["error"]
    assert time.monotonic() - started < 2 and runner.started == []


def test_options_report_each_models_own_1x(launcher, monkeypatch):
    runner, port = launcher
    haiku = {"model_id": "haiku", "alias": "haiku-4-5", "burndown": 5.0, "queue_fraction": 0.85}
    monkeypatch.setattr(runner, "models", lambda: FakeRunner().models() + [haiku])
    models = json.loads(_request(port, "GET", "/options")[1])["models"]
    assert [m["est_tokens"] for m in models] == [demo.est_tokens_for(1.0), demo.est_tokens_for(5.0)]
    assert models[1]["base_rpm"] == round(demo.base_rpm_for(5.0), 1) < models[0]["base_rpm"]


# ── recordings and failure reporting ───────────────────────────────────────────


def test_a_corrupt_line_is_skipped_and_counted(monkeypatch, tmp_path):
    path = tmp_path / "run.jsonl"
    path.write_bytes(b'{"event":"meta"}\n{"event":"sent","ar\n\xff\n{"event":"stopped"}\n{"ev')
    assert demo_ui.read_events(str(path)) == ([{"event": "meta"}, {"event": "stopped"}], 2)
    server = _serve(monkeypatch, trace=str(path))
    try:
        data = json.loads(_request(server.server_port, "GET", "/events?since=0")[1])
        assert data["cursor"] == 2 and data["bad_lines"] == 2
    finally:
        server.shutdown()
        server.server_close()


def test_events_return_the_log_tail_only_after_a_failed_exit(monkeypatch):
    for code, expected in ((None, ""), (0, ""), (2, "Refusing to run: WAF\n")):
        server = _serve(monkeypatch, FakeRunner(exit_code=code, tail="Refusing to run: WAF\n"))
        try:
            data = json.loads(_request(server.server_port, "GET", "/events")[1])
            assert (data["exit_code"], data["log_tail"]) == (code, expected)
        finally:
            server.shutdown()
            server.server_close()


def test_runner_writes_child_output_to_a_log_and_tails_it(monkeypatch, tmp_path):
    calls = []

    class FakePopen:
        def __init__(self, argv, stdout, **kwargs):
            calls.append(argv)
            stdout.write(b"".join(b"line %d\n" % i for i in range(100)))
            stdout.write(b"Refusing to run: too big\n")

        def poll(self):
            return 2

    monkeypatch.setattr(demo_ui.subprocess, "Popen", FakePopen)
    runner = demo_ui.Runner(str(tmp_path))
    ok, trace = runner.start("-model", "steep")
    assert ok and runner.log == trace.removesuffix(".jsonl") + ".log"
    # Options are =-joined and `--` ends them, so a model id cannot parse as a flag.
    assert calls[0][2:] == ["--profile=steep", f"--events={trace}", "--", "-model"]
    tail = runner.log_tail().splitlines()
    assert len(tail) == demo_ui.LOG_TAIL_LINES and tail[-1] == "Refusing to run: too big"
    assert runner.state()["exit_code"] == 2


def test_run_mode_refuses_a_profile_the_preflight_rejects(monkeypatch):
    monkeypatch.setattr(demo_ui.Runner, "models", lambda self: FakeRunner().models())
    monkeypatch.setattr(demo_ui.Runner, "start", lambda *a: pytest.fail("must not start"))
    with pytest.raises(SystemExit) as exit_info:
        demo_ui.main(["--run", "--profile", "60:300", "--port", "0", "--no-browser"])
    assert exit_info.value.code == 2


def test_viewer_ctrl_c_stops_the_running_demo(monkeypatch, capsys):
    stops = []
    monkeypatch.setattr(demo_ui.Runner, "stop", lambda self: stops.append(1) or (True, "stopping"))

    def interrupted(self, *args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(demo_ui.ThreadingHTTPServer, "serve_forever", interrupted)
    demo_ui.main(["--live", "--port", "0", "--no-browser"])
    assert stops == [1] and "SIGTERM" in capsys.readouterr().out


# ── stack: PerIpRateLimit context ──────────────────────────────────────────────

cdk = pytest.importorskip("aws_cdk")
assertions = pytest.importorskip("aws_cdk.assertions")


def _stack(context):
    sys.path.insert(0, str(ROOT / "infrastructure"))
    import semaphore_stack

    if not (ROOT / "infrastructure" / "lambda_layer").is_dir():
        pytest.skip("Lambda asset directories are not available")
    app = cdk.App(context=context)
    return semaphore_stack.SemaphoreRateLimiterStack(app, "WafContextTest")


def _per_ip_rule(stack):
    acl = assertions.Template.from_stack(stack).find_resources("AWS::WAFv2::WebACL")
    (props,) = [r["Properties"] for r in acl.values()]
    # demo._waf_limits looks the rule up by this name.
    (rule,) = [r for r in props["Rules"] if r["Name"] == "PerIpRateLimit"]
    return rule["Statement"]["RateBasedStatement"]


def test_stack_default_per_ip_limit_is_200_per_300s():
    stack = _stack({})
    rate = _per_ip_rule(stack)
    assert (rate["Limit"], rate["EvaluationWindowSec"]) == (200, 300)
    warnings = assertions.Annotations.from_stack(stack).find_warning(
        "*", assertions.Match.string_like_regexp("PerIpRateLimit")
    )
    assert warnings == []


def test_stack_honours_the_demo_override_and_warns():
    stack = _stack({"waf_ip_rate_limit": "10000", "waf_ip_rate_window_sec": "60"})
    rate = _per_ip_rule(stack)
    assert (rate["Limit"], rate["EvaluationWindowSec"]) == (10000, 60)
    assertions.Annotations.from_stack(stack).has_warning(
        "*", assertions.Match.string_like_regexp("PerIpRateLimit is 10000 requests / 60s")
    )


@pytest.mark.parametrize(
    "context",
    [
        {"waf_ip_rate_limit": "0"},
        {"waf_ip_rate_limit": "9"},
        {"waf_ip_rate_limit": "2000000001"},
        {"waf_ip_rate_limit": "lots"},
        {"waf_ip_rate_limit": "1e4"},
        {"waf_ip_rate_window_sec": "0"},
        {"waf_ip_rate_window_sec": "90"},
    ],
)
def test_stack_rejects_bad_waf_context_at_synth(context):
    with pytest.raises(ValueError, match="waf_ip_rate"):
        _stack(context)
