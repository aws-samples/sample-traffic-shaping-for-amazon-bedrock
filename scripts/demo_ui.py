#!/usr/bin/env python3
"""Local viewer for actual demo recordings. Live and replay use the same JSONL feed."""

import argparse
import json
import os
import subprocess  # nosec B404 -- fixed local demo.py argv, no shell
import sys
import tempfile
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
PAGE = os.path.join(HERE, "demo_ui.html")


def load_events(path):
    """Only complete lines are visible while a run is appending to the file."""
    if not os.path.exists(path):
        return []
    with open(path, "rb") as stream:
        return [json.loads(line) for line in stream if line.endswith(b"\n") and line.strip()]


class Handler(BaseHTTPRequestHandler):
    trace = ""
    process = None
    protocol_version = "HTTP/1.1"

    def _send(self, body, ctype):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
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
            events = load_events(self.trace)
            since = min(since, len(events))
            code = self.process.poll() if self.process is not None else None
            body = json.dumps(
                {
                    "cursor": len(events),
                    "events": events[since:],
                    "live": self.process is not None,
                    "running": self.process is not None and code is None,
                    "exit_code": code,
                }
            ).encode()
            self._send(body, "application/json")
        else:
            self.send_error(404, "not here")

    def log_message(self, *args):
        pass


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--run", action="store_true", help="run the real AWS demo and view it")
    source.add_argument("--replay", help="view a recording without AWS")
    parser.add_argument("--model", default="nova-2-lite", help="model for --run")
    parser.add_argument("--port", type=int, default=8700)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args(argv)

    if args.replay and not os.path.isfile(args.replay):
        parser.error(f"no recording at {args.replay}")
    if args.run:
        output_dir = os.path.join(os.path.dirname(HERE), "tmp")
        os.makedirs(output_dir, exist_ok=True)
        fd, trace = tempfile.mkstemp(prefix="demo-live-", suffix=".jsonl", dir=output_dir)
        os.close(fd)
    else:
        trace = args.replay

    Handler.trace = trace
    Handler.process = None
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    if args.run:
        # A separate process group protects demo.py's CONFIG restore from a
        # Ctrl-C intended only to stop the viewer. The recording remains on disk.
        Handler.process = subprocess.Popen(  # nosec B603 -- fixed script, no shell
            [sys.executable, os.path.join(HERE, "demo.py"), args.model, "--events", trace],
            start_new_session=True,
        )
    url = f"http://127.0.0.1:{args.port}/"
    print(
        f"{'live run' if args.run else 'replay'}: {trace}\nserving {url} (ctrl-c to stop)",
        flush=True,
    )
    if not args.no_browser:
        timer = threading.Timer(0.5, webbrowser.open, [url])
        timer.daemon = True
        timer.start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        if Handler.process is not None and Handler.process.poll() is None:
            print("\nViewer stopped; demo continues to restore its CONFIG. Recording:", trace)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
