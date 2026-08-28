"""End-to-end functional smoke of the honest-outcomes HTTP contract.

Fires ONE SigV4-signed request at the shaper `/invoke` ingress, then polls
GET /result/{request_id} until a terminal outcome resolves. This validates the
deployed honest-outcomes path — PENDING → terminal → /result — WITHOUT spending
overload-scale quota. Cheap (1 request), model defaults to Nova 2 Lite.

Note: the former `/baseline/{retry,jitter}` comparison arms were removed in a
re-architecture; only the shaper `/invoke` arm remains.

Usage:
  python scripts/smoke_honest_outcomes.py                    # nova, shaper /invoke
  python scripts/smoke_honest_outcomes.py --model us.anthropic.claude-sonnet-4-6

Reads API_GATEWAY_URL from config.env (or --api-url). SigV4 via the caller's
current credentials (execute-api). Read-only against AWS beyond the requests it
submits.
"""
import argparse
import json
import os
import re
import sys
import time
import uuid

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import urllib.request
import urllib.error

REGION = "us-east-1"
SERVICE = "execute-api"


def _load_api_url(explicit):
    if explicit:
        return explicit.rstrip("/")
    # config.env sits at repo root, one level up from scripts/
    cfg = os.path.join(os.path.dirname(__file__), "..", "config.env")
    try:
        with open(cfg) as fh:
            for line in fh:
                if line.startswith("API_GATEWAY_URL="):
                    return line.split("=", 1)[1].strip().rstrip("/")
    except FileNotFoundError:
        pass
    sys.exit("API_GATEWAY_URL not found — pass --api-url or generate config.env (make deploy).")


def _signed_request(method, url, body=None):
    """SigV4-sign and send one execute-api request; return (status, body_text)."""
    creds = boto3.Session().get_credentials().get_frozen_credentials()
    data = json.dumps(body).encode() if body is not None else None
    req = AWSRequest(method=method, url=url, data=data,
                     headers={"Content-Type": "application/json"} if data else {})
    SigV4Auth(creds, SERVICE, REGION).add_auth(req)
    urllib_req = urllib.request.Request(url, data=data, headers=dict(req.headers), method=method)
    try:
        resp = urllib.request.urlopen(urllib_req, timeout=30)  # nosec B310 — static execute-api host from our own deploy
        return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def _submit(api_url, arm, model_id, prompt):
    correlation_id = str(uuid.uuid4())
    path = "/invoke"
    # /invoke maps the body into StartExecution; request_id is client-supplied here.
    request_id = str(uuid.uuid4())
    body = {"request_id": request_id, "model_id": model_id,
            "prompt": prompt, "correlation_id": correlation_id, "max_tokens": 64}
    status, text = _signed_request("POST", f"{api_url}{path}", body)
    print(f"  [{arm}] POST {path} → {status}")
    try:
        parsed = json.loads(text)
    except ValueError:
        parsed = {}
    rid = parsed.get("request_id", request_id)
    if status not in (200, 202):
        print(f"    ⚠ unexpected submit status: {status} {text[:200]}")
    return {"arm": arm, "correlation_id": correlation_id, "request_id": rid,
            "submit_status": status}


def _poll_result(api_url, request_id, timeout_s=120, interval_s=3):
    """Poll GET /result/{request_id} until terminal (200/429/503/504/400) or timeout."""
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        status, text = _signed_request("GET", f"{api_url}/result/{request_id}")
        last = (status, text)
        # 202 = still PENDING/QUEUED; anything else is terminal per the HTTP map.
        if status != 202:
            return status, text
        time.sleep(interval_s)  # nosemgrep: arbitrary-sleep — bounded poll of our own async result endpoint
    return last if last else (None, "no response")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="us.amazon.nova-2-lite-v1:0")
    ap.add_argument("--api-url", default=None)
    ap.add_argument("--arms", default="invoke",
                    help="comma list of arms to smoke (only 'invoke' remains; "
                         "the baseline retry/jitter arms were removed)")
    ap.add_argument("--timeout", type=int, default=120)
    args = ap.parse_args()

    api_url = _load_api_url(args.api_url)
    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    print(f"Honest-outcomes smoke — model={args.model}  api={api_url}")
    print(f"Arms: {arms}\n")

    submissions = []
    for arm in arms:
        submissions.append(_submit(api_url, arm, args.model, f"Smoke test via {arm}. Reply OK."))

    print("\nPolling /result for each request...\n")
    results = []
    for sub in submissions:
        if not sub["request_id"]:
            print(f"  [{sub['arm']}] no request_id from submit — cannot poll")
            results.append({**sub, "terminal_status": None, "outcome": "no_request_id"})
            continue
        status, text = _poll_result(api_url, sub["request_id"], timeout_s=args.timeout)
        try:
            parsed = json.loads(text)
        except (ValueError, TypeError):
            parsed = {}
        outcome = parsed.get("state") or parsed.get("error") or f"http_{status}"
        has_output = bool(parsed.get("output_url") or parsed.get("output_ref"))
        print(f"  [{sub['arm']}] /result → {status}  state/outcome={outcome}"
              f"  output={'yes' if has_output else 'no'}")
        results.append({**sub, "terminal_status": status, "outcome": outcome,
                        "has_output": has_output})

    print("\n=== SMOKE SUMMARY ===")
    ok = 0
    for r in results:
        verdict = "✅" if r.get("terminal_status") == 200 else "⚠"
        if r.get("terminal_status") == 200:
            ok += 1
        print(f"  {verdict} {r['arm']:8s} submit={r['submit_status']} "
              f"terminal={r.get('terminal_status')} outcome={r.get('outcome')} "
              f"correlation_id={r['correlation_id']}")
    print(f"\n{ok}/{len(results)} arms reached a 200 SUCCEEDED terminal outcome.")
    # Non-200 terminals are still VALID honest outcomes (429/503/504) — the smoke's
    # job is to prove the contract resolves a terminal, not that Bedrock never throttles.
    unresolved = [r for r in results if r.get("terminal_status") in (None, 202)]
    if unresolved:
        print(f"⚠ {len(unresolved)} request(s) did NOT resolve a terminal outcome (still pending / no id).")
        sys.exit(1)
    print("All requests resolved a terminal outcome — honest-outcomes contract is live.")


if __name__ == "__main__":
    main()
