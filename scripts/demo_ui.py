#!/usr/bin/env python3
"""Local viewer for actual demo recordings. Live and replay use the same JSONL feed.

Live mode is also the launcher: the page lists every runtime model with a CONFIG
row and the load-profile presets, and POST /start runs demo.py with the chosen
pair. One run at a time -- each run temporarily rewrites its model's CONFIG."""

import argparse
import collections
import json
import os
import signal
import subprocess  # nosec B404 -- fixed local demo.py argv, no shell
import sys
import tempfile
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
PAGE = os.path.join(HERE, "demo_ui.html")
sys.path.insert(0, HERE)

# demo.py is importable offline (the tests rely on it); it owns the profile grammar.
import demo  # noqa: E402

MAX_START_BODY = 4096
LOG_TAIL_LINES = 40


def read_events(path):
    """(events, bad_lines). Only complete lines are visible while a run is appending
    to the file; a complete line that is not JSON (a truncated write from a crashed
    run) is skipped and counted instead of breaking the feed."""
    events, bad_lines = [], 0
    if not path or not os.path.exists(path):
        return events, bad_lines
    with open(path, "rb") as stream:
        for line in stream:
            if not line.endswith(b"\n") or not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except ValueError:
                bad_lines += 1
    return events, bad_lines


def load_events(path):
    return read_events(path)[0]


def start_problem(models, model, profile):
    """Why demo.py would refuse this run (no CONFIG row, bad profile, WAF limit or
    queue expiry), or None. Shared by POST /start and --run so a refused run is
    refused before anything starts."""
    entry = {m["model_id"]: m for m in models}.get(model)
    if entry is None:
        return f"no runtime CONFIG row for {model!r}"
    try:
        # The model's own 1x, as demo.py uses. The parse also enforces the profile
        # size caps, so an oversized profile never reaches the offset-building WAF check.
        parsed = demo.parse_load_profile(profile, demo.base_rpm_for(entry["burndown"]))
    except ValueError as e:
        return str(e)
    return demo._preflight_problem(parsed, entry["burndown"], entry["queue_fraction"])


def list_demo_models():
    """[{"model_id", "alias"}] for every runtime model with a CONFIG row -- the only
    models demo.py can run. Reads config.env + DynamoDB; raises on failure."""
    import boto3
    import config_loader
    from create_model_config import MODEL_MAP

    try:
        config = config_loader.load_config()
    except SystemExit:
        raise RuntimeError("config.env not found -- run make setup/deploy first") from None
    table = boto3.resource("dynamodb", region_name=config.get("AWS_REGION", "us-east-1")).Table(
        config.get("SINGLE_TABLE_NAME", "semaphore-single-table")
    )
    alias_of = {model_id: alias for alias, model_id in MODEL_MAP.items()}
    scan = {
        "FilterExpression": "sk = :c AND entity_type = :t",
        "ExpressionAttributeValues": {":c": "CONFIG", ":t": "model_config"},
        "ProjectionExpression": (
            "model_id, backend, output_token_burndown_rate, tpm_limit, tpm_queue_capacity"
        ),
    }
    models = []
    while True:
        resp = table.scan(**scan)
        for item in resp.get("Items", []):
            if item.get("backend", "runtime") == "runtime" and item.get("model_id"):
                tpm = float(item.get("tpm_limit") or 0)
                models.append(
                    {
                        "model_id": item["model_id"],
                        "alias": alias_of.get(item["model_id"]),
                        "burndown": float(item.get("output_token_burndown_rate", 1.0)),
                        # demo.py keeps the live queue/ceiling split when it scales down
                        "queue_fraction": (
                            float(item.get("tpm_queue_capacity") or 0) / tpm if tpm else 0.85
                        ),
                    }
                )
        if "LastEvaluatedKey" not in resp:
            break
        scan["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return sorted(models, key=lambda m: (m["alias"] is None, m["alias"] or m["model_id"]))


class Runner:
    """The one live run: its trace file, process, and a run counter the page uses
    to notice a new run and reset. Not used in replay mode."""

    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.lock = threading.Lock()
        self.process = None
        self.trace = ""
        self.log = ""
        self.run_id = 0
        self.model = None
        self.profile = None
        self.stopping = False
        self._models = None

    def models(self):
        with self.lock:
            cached = self._models
        if cached is None:
            # A DynamoDB scan: run it outside the lock that /events polls through.
            cached = list_demo_models()
            with self.lock:
                self._models = cached
        return cached

    def start(self, model, profile):
        """Start demo.py; returns (ok, message). Refuses while a run is in flight."""
        with self.lock:
            if self.process is not None and self.process.poll() is None:
                return False, "a run is already in progress"
            os.makedirs(self.output_dir, exist_ok=True)
            fd, trace = tempfile.mkstemp(prefix="demo-live-", suffix=".jsonl", dir=self.output_dir)
            os.close(fd)
            log = os.path.splitext(trace)[0] + ".log"
            # A separate process group keeps a viewer Ctrl-C from reaching demo.py
            # directly; main() sends it one SIGTERM instead. Its output goes to the
            # log, whose tail /events returns if the run fails.
            with open(log, "wb") as out:
                self.process = subprocess.Popen(  # nosec B603 -- fixed script, validated argv
                    [
                        sys.executable,
                        os.path.join(HERE, "demo.py"),
                        f"--profile={profile}",
                        f"--events={trace}",
                        "--",
                        model,
                    ],
                    stdout=out,
                    stderr=subprocess.STDOUT,
                    env={**os.environ, "PYTHONUNBUFFERED": "1"},
                    start_new_session=True,
                )
            self.trace, self.log, self.model, self.profile = trace, log, model, profile
            self.stopping = False
            self.run_id += 1
            return True, trace

    def stop(self):
        """Ask the running demo.py to stop: SIGTERM makes it stop offering requests,
        delete its still-queued shaper items, stop their executions, restore CONFIG
        and write a partial summary. Returns (ok, message)."""
        with self.lock:
            if self.process is None or self.process.poll() is not None:
                return False, "no run in progress"
            self.process.send_signal(signal.SIGTERM)
            self.stopping = True
            return True, "stopping"

    def state(self):
        with self.lock:
            code = self.process.poll() if self.process is not None else None
            return {
                "trace": self.trace,
                "run_id": self.run_id,
                "running": self.process is not None and code is None,
                "stopping": self.stopping and code is None,
                "exit_code": code,
                "model": self.model,
                "profile": self.profile,
            }

    def log_tail(self):
        """Last LOG_TAIL_LINES lines of the current run's demo.py output."""
        try:
            with open(self.log, "rb") as stream:
                lines = collections.deque(stream, maxlen=LOG_TAIL_LINES)
        except OSError:
            return ""
        return b"".join(lines).decode(errors="replace")


class Handler(BaseHTTPRequestHandler):
    trace = ""  # replay recording; unused when runner is set
    process = None  # kept for replay-mode compatibility (always None there)
    runner = None
    protocol_version = "HTTP/1.1"
    timeout = 10  # a client that stalls mid-request does not pin a thread

    def end_headers(self):
        # Never framed: a framed Start click would be a same-origin POST (clickjacking).
        # Here rather than in _send so send_error responses carry them too.
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Content-Security-Policy", "frame-ancestors 'none'")
        super().end_headers()

    def _send(self, body, ctype, code=200):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload, code=200):
        self._send(json.dumps(payload).encode(), "application/json", code)

    def _local_request(self):
        """Refuse requests whose Host is not this loopback server (DNS rebinding)
        and cross-origin POSTs (a page on another site driving /start)."""
        port = self.server.server_address[1]
        allowed = {f"127.0.0.1:{port}", f"localhost:{port}"}
        if self.headers.get("Host") not in allowed:
            return False
        # Browsers send Sec-Fetch-Site: a POST from anything but this page is refused.
        if self.command == "POST" and self.headers.get("Sec-Fetch-Site") not in (
            None,
            "same-origin",
        ):
            return False
        origin = self.headers.get("Origin")
        return origin is None or origin in {f"http://{h}" for h in allowed}

    def _read_body(self):
        """The request body, at most MAX_START_BODY bytes; ValueError on a bad length."""
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 <= length <= MAX_START_BODY:
            raise ValueError("bad length")
        return self.rfile.read(length)

    def do_GET(self):
        if not self._local_request():
            self.send_error(403, "loopback only")
            return
        route = urlparse(self.path)
        if route.path == "/":
            with open(PAGE, "rb") as stream:
                self._send(stream.read(), "text/html; charset=utf-8")
        elif route.path == "/events":
            try:
                since = int(parse_qs(route.query).get("since", ["0"])[0])
                if since < 0:
                    raise ValueError("negative cursor")
            except ValueError:
                self.send_error(400, "invalid cursor")
                return
            if self.runner is not None:
                state = self.runner.state()
                trace, live = state["trace"], True
            else:
                code = self.process.poll() if self.process is not None else None
                state = {
                    "run_id": 0,
                    "running": self.process is not None and code is None,
                    "exit_code": code,
                }
                trace, live = self.trace, self.process is not None
            events, bad_lines = read_events(trace)
            since = min(since, len(events))
            failed = self.runner is not None and state["exit_code"] not in (None, 0)
            self._json(
                {
                    "cursor": len(events),
                    "events": events[since:],
                    "bad_lines": bad_lines,
                    "live": live,
                    "controls": self.runner is not None,
                    "run_id": state["run_id"],
                    "running": state["running"],
                    "stopping": state.get("stopping", False),
                    "exit_code": state["exit_code"],
                    # demo.py's own words for why it failed (refusal, traceback, ...)
                    "log_tail": self.runner.log_tail() if failed else "",
                }
            )
        elif route.path == "/options":
            if self.runner is None:
                self._json({"models": [], "profiles": {}, "error": "replay mode"})
                return
            try:
                # Each model's own 1x and estimate, for the profile hint in the page.
                models = [
                    {
                        **m,
                        "base_rpm": round(demo.base_rpm_for(m["burndown"]), 1),
                        "est_tokens": demo.est_tokens_for(m["burndown"]),
                    }
                    for m in self.runner.models()
                ]
                error = None
            except Exception as e:  # surfaced in the page, never silently empty
                models, error = [], f"could not list models: {e}"
            self._json(
                {
                    "models": models,
                    "profiles": demo.DEMO_PROFILE_PRESETS,
                    "base_rpm": round(demo.DEMO_BASE_RPM, 1),
                    "error": error,
                }
            )
        else:
            self.send_error(404, "not here")

    def do_POST(self):
        if not self._local_request():
            self.send_error(403, "loopback only")
            return
        path = urlparse(self.path).path
        if path not in ("/start", "/stop") or self.runner is None:
            self.send_error(404, "not here")
            return
        # A JSON content type makes a cross-site form/fetch a preflighted request,
        # which this server never answers, on top of the Origin check above.
        if self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json":
            self.send_error(415, "application/json required")
            return
        if path == "/stop":
            # The body is unused, but left unread on this keep-alive connection it
            # would be parsed as the start of the next request.
            try:
                self._read_body()
            except ValueError:
                self.close_connection = True
            ok, message = self.runner.stop()
            self._json({"ok": ok, "error": None if ok else message}, 200 if ok else 409)
            return
        try:
            body = json.loads(self._read_body())
            model, profile = str(body["model"]), str(body.get("profile") or "default")
        except (ValueError, KeyError, TypeError):
            self.send_error(400, "expected {model, profile}")
            return
        try:
            models = self.runner.models()
        except Exception as e:
            self._json({"ok": False, "error": f"could not list models: {e}"}, 503)
            return
        problem = start_problem(models, model, profile)
        if problem:
            self._json({"ok": False, "error": problem}, 400)
            return
        ok, message = self.runner.start(model, profile)
        self._json({"ok": ok, "error": None if ok else message}, 200 if ok else 409)

    def log_message(self, *args):
        pass


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--run", action="store_true", help="live launcher; starts a run of --model at once"
    )
    source.add_argument("--live", action="store_true", help="live launcher; pick a run in the page")
    source.add_argument("--replay", help="view a recording without AWS")
    parser.add_argument("--model", default="nova-2-lite", help="model for --run")
    parser.add_argument("--profile", default="default", help="load profile for --run")
    parser.add_argument("--port", type=int, default=8700)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args(argv)

    if args.replay and not os.path.isfile(args.replay):
        parser.error(f"no recording at {args.replay}")

    Handler.trace = args.replay or ""
    Handler.process = None
    Handler.runner = None
    if args.run or args.live:
        Handler.runner = Runner(os.path.join(os.path.dirname(HERE), "tmp"))
    # Bind first: a busy port must fail BEFORE a run starts rewriting CONFIG.
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    if args.run:
        model = demo.MODEL_MAP.get(args.model, args.model)
        try:
            problem = start_problem(Handler.runner.models(), model, args.profile)
        except Exception as e:
            problem = f"could not list models: {e}"
        ok, trace = (False, problem) if problem else Handler.runner.start(model, args.profile)
        if not ok:
            server.server_close()
            parser.error(trace)
        print(f"live run: {trace} (demo.py output: {Handler.runner.log})")
    url = f"http://127.0.0.1:{args.port}/"
    print(
        f"{'replay: ' + args.replay if args.replay else 'live launcher'}\n"
        f"serving {url} (ctrl-c to stop)",
        flush=True,
    )
    if not args.no_browser:
        timer = threading.Timer(0.5, webbrowser.open, [url])
        timer.daemon = True
        timer.start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        # Same as the Stop button: demo.py stops sending, cleans up and restores
        # CONFIG on its own after the viewer is gone.
        if Handler.runner and Handler.runner.stop()[0]:
            print(
                "\nViewer stopped; sent the demo run SIGTERM. It is stopping and will restore"
                f" its CONFIG before it exits. Recording: {Handler.runner.trace}"
                f" (output: {Handler.runner.log})"
            )
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
