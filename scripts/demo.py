#!/usr/bin/env python3
"""
Demo harness: temporarily re-derive a model's config at a 200,000 TPM ceiling,
then offer the same paced load to three arms at once:

  shaper        -- SigV4-signed requests through the shaper's /invoke ingress
  direct        -- straight to bedrock-runtime converse(), no retry
  direct+retry  -- same, with exponential backoff + full jitter

Bedrock's real quota is far above this load, so both direct arms are held to
the same 200,000-token ceiling by a client-side SIMULATED quota (see
_virtual_budget_admit). Those rejections are simulated; all tokens, latencies
and Bedrock calls are real. The original CONFIG item is backed up under tmp/
before the override and restored even on error. The override applies to the
whole deployment, so do not run this against a stack serving real workloads.

Usage:
    python scripts/demo.py [MODEL] [-v|--verbose] [--profile SPEC] [--events PATH]
                           [--real-quota] [--force] [--list-profiles]

    MODEL defaults to nova-2-lite. --verbose prints every request/attempt;
    --profile picks a preset or inline load spec; --events records a JSONL trace
    for demo-ui; --real-quota runs the shaper arm alone against the model's real
    CONFIG; --force runs past the per-IP WAF check; --list-profiles prints presets.
"""

import argparse
import concurrent.futures as cf
import contextlib
import json
import math
import os
import random
import signal
import sys
import threading
import time
import uuid
from statistics import median

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# Lambda layer on sys.path so the direct arms' virtual quota is charged with the
# shaper's OWN estimate_request_tokens(), not a local copy of the formula.
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '..', 'infrastructure', 'lambda_layer', 'python'
    ),
)
import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.config import Config
import config_loader
from create_model_config import MODEL_MAP, calculate_config, create_model_config
from botocore.exceptions import BotoCoreError, ClientError
from smoke_honest_outcomes import _load_api_url, _signed_request
from shared_service import estimate_request_tokens

DEMO_TPM_OVERRIDE = 200_000

# Request profile: flat constants, deliberately not tuned toward any result.
# DEMO_BYTES_PER_TOKEN is written only into the demo's own override item.
DEMO_TARGET_INPUT_TOKENS = 2000
DEMO_CHARS_PER_TOKEN_ESTIMATE = 4
DEMO_BYTES_PER_TOKEN = 5
DEMO_MAX_OUTPUT_TOKENS = 1000

# Offered-load timeline: (duration_s, request_count) phases played back to back;
# requests are spread evenly within a phase. This is the default; --profile takes
# a preset name or an inline spec (see parse_load_profile), and the drain/direct
# timeouts are re-derived from whichever profile runs (see _run_bounds).
DEMO_LOAD_PROFILE = [(25, 140)]
DEMO_REQUEST_COUNT = sum(count for _, count in DEMO_LOAD_PROFILE)
DEMO_SUBMIT_WINDOW_S = sum(duration for duration, _ in DEMO_LOAD_PROFILE)

# Named profiles for --profile. `D@Mx` = D seconds at M times the 1x rate, where
# 1x is the ceiling's sustainable request rate (DEMO_TPM_OVERRIDE / est tokens
# per request, ~71/min); `D:N` = exactly N requests over D seconds. The presets
# assume a stack deployed with the demo WAF override (`-c waf_ip_rate_limit=10000
# -c waf_ip_rate_window_sec=60`, or WAF_IP_RATE_LIMIT / WAF_IP_RATE_WINDOW_SEC via
# deploy.sh); against the stack default (200/300s, DEMO_WAF_*) every preset but
# `default` and `steep` is refused at preflight.
DEMO_PROFILE_PRESETS = {
    "default": "25:140",
    "ramp": "60@1x,25@2x,15@0x,15@6x,60@0.5x",
    "steep": "30@0.5x,5@20x,90@0x",
    "multi-spike": "20@0.5x,10@6x,30@0.5x,10@6x,30@0.5x,10@6x,30@0.5x",
    "long": "90@0.4x,15@5x,185@0.4x,15@5x,185@0.4x,15@5x,95@0.4x",
}
# Upper bounds on any profile: the schedule is built in memory, and the CONFIG
# override is held for the whole run.
DEMO_MAX_PROFILE_REQUESTS = 50_000
DEMO_MAX_PROFILE_S = 3600

# The API stage's WAF blocks an IP past 200 requests per 300s by default
# (semaphore_stack.py PerIpRateLimit, context-overridable). Every shaper-arm POST
# counts, so a profile over the limit gets 403s at ingress that have nothing to do
# with the shaper. The live limit is read from the web ACL (_waf_limits); these are
# the fallback. Refused unless --force.
DEMO_WAF_LIMIT = 200
DEMO_WAF_WINDOW_S = 300
DEMO_WAF_HEADROOM = 10

# Floor for the shaper drain wait, counted from the last submission. The real
# bound is derived per profile in _run_bounds (2x the time the queue share needs
# to drain the whole offered load); the processor reschedules itself past its
# 13-minute per-invocation ceiling, so a long drain is not cut off server-side.
DEMO_DRAIN_TIMEOUT_S = 600
# Queue items expire after 60 minutes (enqueue_request expiry_hours=1) and the
# state machine times out at 65, so a profile that cannot drain well inside that
# would report expiries as shaper failures.
DEMO_MAX_DRAIN_S = 50 * 60

# Shaper completions are read straight from the REQUEST#{id}/STATUS items with
# BatchGetItem (off the measured API path -- no /result polling, no WAF load).
# Only still-outstanding IDs are read each pass; BatchGetItem takes <=100 keys.
DEMO_STATUS_POLL_INTERVAL_S = 1.0
DEMO_STATUS_BATCH_MAX_KEYS = 100
DEMO_STATUS_READS_PER_S = 1000
# Consecutive poll passes in which every BatchGetItem failed before the poller
# gives up and fails the run (~30s at the 1s poll interval).
DEMO_STATUS_MAX_FAILED_PASSES = 30
# Terminal STATUS item -> HTTP code, the same map result_fn.py serves on /result.
DEMO_FAILED_REASON_TO_STATUS = {
    "throttled": 429,
    "ingress_throttled": 429,
    "error": 503,
    "timed_out": 504,
    "queue_expired": 504,
    "validation_error": 400,
}

# EMF metrics lag behind the requests; poll for a bounded window.
DEMO_METRICS_WAIT_S = 120
DEMO_METRICS_POLL_INTERVAL_S = 15

# Direct arms: worker pool and wait bound. The pool gets one worker per request
# (min DEMO_DIRECT_MIN_WORKERS, max DEMO_DIRECT_MAX_WORKERS): a short pool would
# queue requests CLIENT-side during a spike, which shapes the direct arms and
# hides their throttles. DEMO_DIRECT_TIMEOUT_S is the tail allowed past the end
# of the submission window (worst case ~2 waves of 4 slow attempts + 7s backoff).
DEMO_DIRECT_MIN_WORKERS = 40
DEMO_DIRECT_MAX_WORKERS = 400
# Concurrent /invoke POSTs for the shaper arm (each POST is ~0.7s; 32 workers
# keep up with ~45 req/s, enough for the steepest preset).
DEMO_SHAPER_SUBMIT_WORKERS = 32
DEMO_DIRECT_TIMEOUT_S = 240
DEMO_DIRECT_THROTTLE_CODES = {
    "ThrottlingException",
    "TooManyRequestsException",
    "ServiceQuotaExceededException",
}

# Retry policy, same as scripts/test_direct_bedrock_retry.py.
DEMO_RETRY_MAX_RETRIES = 3
DEMO_RETRY_BASE_DELAY_S = 1.0
DEMO_RETRY_MAX_DELAY_S = 30.0

# Bedrock quotas are per-minute, so the simulated quota is a token bucket that
# regenerates its whole ceiling over one minute (cap / DEMO_QUOTA_REFILL_S per
# second, continuously). Separate from the PEAK REPORTING window below, which is
# a genuine rolling 60s measurement and has no bearing on what the gate admits.
DEMO_QUOTA_REFILL_S = 60
DEMO_PEAK_WINDOW_S = 60

VERBOSE = False

# Set by SIGTERM/SIGINT (demo-ui's Stop button sends SIGTERM), and by main() when
# the run fails. Every arm stops offering new requests, retries and waits end
# early, and the shaper arm's still-queued items are deleted and their executions
# stopped before CONFIG is restored.
_STOP = threading.Event()
# How long the direct arms wait for in-flight converse() calls after a stop.
DEMO_STOP_GRACE_S = 20
# Before shaper cleanup: every POST has returned by then, but an accepted request
# reaches the queue only once its execution's budget_manager step has run.
DEMO_STOP_ENQUEUE_SETTLE_S = 5

# CONFIG restore: attempts, and the backoff before the 2nd and 3rd (doubling).
DEMO_RESTORE_ATTEMPTS = 3
DEMO_RESTORE_BACKOFF_S = 2.0
# CONFIG backups and summaries without --events land here.
DEMO_TMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tmp")

# Per-run problems, reset by main(). Errors fail the run (exit 1) and land in the
# summary's meta.run_error; warnings land in meta.warnings and leave the exit code.
_run_log: dict[str, list[str]] = {"errors": [], "warnings": []}

# Live progress: every DEMO_PROGRESS_INTERVAL_S, one line of requests sent per
# arm, until every arm has sent its full load.
DEMO_PROGRESS_INTERVAL_S = 5
_progress = {"lock": threading.Lock(), "sent": {}}
_event_path = None
_event_start = 0.0
_event_lock = threading.Lock()


def emit(event, arm=None, req=0, **fields):
    """Append an observed live event. Disabled unless --events is specified. A
    write failure turns recording off (with a warning) rather than failing the
    request, poll or submit that emitted it."""
    global _event_path
    if _event_path is None:
        return
    with _event_lock:
        if _event_path is None:
            return
        line = {"event": event, "t": round(time.time() - _event_start, 6), "arm": arm, "req": req}
        line.update(fields)
        try:
            with open(_event_path, "a") as stream:
                stream.write(json.dumps(line) + "\n")
        except OSError as e:
            _event_path = None
            _warn(f"live event recording stopped: {type(e).__name__}")


def _warn(message):
    _run_log["warnings"].append(message)
    print(f"[demo] WARNING: {message}")


def _record_error(reason, message):
    """Fail the run: a `run_error` event (`reason` short, `message` <=200 chars) and
    an entry in meta.run_error. Messages carry exception types and AWS error codes
    only (see _brief_error), never boto text that can hold ARNs or account ids."""
    message = message[:200]
    _run_log["errors"].append(message)
    emit("run_error", reason=reason, message=message)
    print(f"[demo] RUN ERROR: {message}")


def _start_events(path, start, profile=None, **meta):
    global _event_path, _event_start
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w"):
        pass  # a new live run must never append to an old recording
    _event_start = start
    _event_path = path
    profile = DEMO_LOAD_PROFILE if profile is None else profile
    emit(
        "meta",
        profile=profile,
        offered=sum(count for _, count in profile),
        offsets=[round(o, 6) for o in _schedule_offsets(profile)],
        **meta,
    )


def _tick_sent(arm):
    with _progress["lock"]:
        _progress["sent"][arm] = _progress["sent"].get(arm, 0) + 1


def _progress_reporter(stop, start, total):
    while not stop.wait(DEMO_PROGRESS_INTERVAL_S):
        with _progress["lock"]:
            sent = dict(_progress["sent"])
        if all(n >= total for n in sent.values()):
            return
        print(
            f"  [t+{time.time() - start:3.0f}s] sent "
            + " | ".join(f"{arm} {n}/{total}" for arm, n in sent.items())
        )


def _vprint(*args):
    if VERBOSE:
        print(*args)


def _build_filler_prompt():
    """Fixed ~DEMO_TARGET_INPUT_TOKENS filler prompt plus a long-response instruction."""
    # No apostrophes: the /invoke integration runs the body through
    # $util.escapeJavaScript(), which turns ' into the invalid JSON escape \'.
    filler_sentence = (
        "The Bedrock traffic shaper paces inference requests to stay within a "
        "provisioned token-per-minute quota for the model, queueing bursts and "
        "draining them at a steady rate instead of letting them throttle. "
    )
    target_chars = DEMO_TARGET_INPUT_TOKENS * DEMO_CHARS_PER_TOKEN_ESTIMATE
    repeats = target_chars // len(filler_sentence) + 1
    filler = (filler_sentence * repeats)[:target_chars]
    instruction = (
        "\n\nWrite a long, detailed, thorough response of at least 800 words "
        "discussing the passage above: explain the concepts in your own "
        "words, give concrete examples, and keep elaborating with further "
        "detail and examples until you reach the maximum response length "
        "you are allowed."
    )
    return filler + instruction


# What the shaper's estimator charges per request under this config (reported,
# not used for sizing). The direct arms' virtual quota charges the same number.
DEMO_TOKENS_PER_REQUEST_ESTIMATE = estimate_request_tokens(
    prompt=_build_filler_prompt(),
    max_tokens=DEMO_MAX_OUTPUT_TOKENS,
    bytes_per_token=DEMO_BYTES_PER_TOKEN,
)

# The 1x rate the `D@Mx` profile phases scale: requests/min the ceiling sustains.
DEMO_BASE_RPM = DEMO_TPM_OVERRIDE / DEMO_TOKENS_PER_REQUEST_ESTIMATE


def est_tokens_for(burndown_rate):
    """The shaper's per-request estimate for the demo request at this burndown."""
    return estimate_request_tokens(
        prompt=_build_filler_prompt(),
        max_tokens=DEMO_MAX_OUTPUT_TOKENS,
        burndown_rate=burndown_rate,
        bytes_per_token=DEMO_BYTES_PER_TOKEN,
    )


def base_rpm_for(burndown_rate):
    """The 1x rate for a model with this output burndown: requests/min the ceiling
    sustains at that model's per-request estimate."""
    return DEMO_TPM_OVERRIDE / est_tokens_for(burndown_rate)


def _preflight_problems(profile, burndown_rate, queue_fraction, ceiling_tpm=DEMO_TPM_OVERRIDE):
    """Every reason this profile cannot run for this model, as [(kind, message)]
    with kind "waf" or "expiry". Both are checked, so --force (which waives "waf"
    only) cannot skip the expiry check."""
    problems = []
    waf = _waf_problem(profile)
    if waf:
        problems.append(("waf", waf))
    queue_tpm = ceiling_tpm * queue_fraction
    expected_drain_s, _, _ = _run_bounds(profile, queue_tpm, est_tokens_for(burndown_rate))
    if expected_drain_s > DEMO_MAX_DRAIN_S:
        problems.append(
            (
                "expiry",
                f"profile needs ~{expected_drain_s / 60:.0f} min to drain at the "
                f"{queue_tpm:,.0f} TPM queue share; queue items expire at 60 min",
            )
        )
    return problems


def _preflight_problem(profile, burndown_rate, queue_fraction, ceiling_tpm=DEMO_TPM_OVERRIDE):
    """Why this profile cannot run for this model, or None. Shared by demo.py's main
    and the demo-ui launcher so both refuse the same runs before any CONFIG is
    touched."""
    problems = _preflight_problems(profile, burndown_rate, queue_fraction, ceiling_tpm)
    return "; ".join(message for _, message in problems) or None


def parse_load_profile(spec, base_rpm=DEMO_BASE_RPM):
    """Preset name or inline spec -> [(duration_s, count), ...].

    Inline phases are comma-separated, each either `D:N` (N requests over D
    seconds) or `D@Mx` (D seconds at M times base_rpm, rounded to whole
    requests). `0x` / `:0` phases are idle gaps that still advance the clock. A
    profile may offer at most DEMO_MAX_PROFILE_REQUESTS over DEMO_MAX_PROFILE_S."""
    spec = DEMO_PROFILE_PRESETS.get(spec.strip(), spec)
    profile = []
    for raw in spec.split(","):
        phase = raw.strip().lower()
        try:
            if "@" in phase:
                duration, mult = phase.split("@")
                duration_s, mult = float(duration), float(mult.removesuffix("x"))
                count = round(mult * base_rpm * duration_s / 60)
            else:
                duration, count = phase.split(":")
                duration_s, count = float(duration), int(count)
        except (ValueError, OverflowError):
            raise ValueError(f"bad load phase {raw!r}: expected D:N or D@Mx") from None
        if not math.isfinite(duration_s) or duration_s <= 0 or count < 0:
            raise ValueError(f"load phase must have D > 0 and N >= 0: {raw!r}")
        profile.append((int(duration_s) if duration_s.is_integer() else duration_s, count))
    if not any(count for _, count in profile):
        raise ValueError(f"profile {spec!r} offers no requests")
    total = sum(count for _, count in profile)
    if total > DEMO_MAX_PROFILE_REQUESTS:
        raise ValueError(
            f"profile offers {total:,} requests; the limit is {DEMO_MAX_PROFILE_REQUESTS:,}"
        )
    duration = sum(d for d, _ in profile)
    if duration > DEMO_MAX_PROFILE_S:
        raise ValueError(f"profile runs {duration:,.0f}s; the limit is {DEMO_MAX_PROFILE_S:,}s")
    return profile


def _peak_requests_in_window(offsets, window_s):
    """Most scheduled sends in any window_s-long window (two-pointer sweep)."""
    peak = left = 0
    for right, t in enumerate(offsets):
        while offsets[left] <= t - window_s:
            left += 1
        peak = max(peak, right - left + 1)
    return peak


_waf_cache: dict[str, tuple[int, int, str]] = {}
_waf_warned: set[str] = set()


def _waf_limits():
    """(limit, window_s, source) of the deployed PerIpRateLimit rule, read from the
    web ACL in config.env's WAF_WEB_ACL_ARN; source is "live web ACL". A live read
    is cached for the process. On failure it falls back to the stack's defaults
    (DEMO_WAF_LIMIT / DEMO_WAF_WINDOW_S) with source "default 200/300s: <reason>",
    warns once per reason, and is not cached, so the next call reads again."""
    if "limits" in _waf_cache:
        return _waf_cache["limits"]
    try:
        arn = config_loader.load_config().get("WAF_WEB_ACL_ARN", "")
        name, acl_id = arn.split("/")[-2:]
        acl = boto3.client("wafv2", region_name=arn.split(":")[3]).get_web_acl(
            Name=name, Scope="REGIONAL", Id=acl_id
        )["WebACL"]
        rate = next(r for r in acl["Rules"] if r["Name"] == "PerIpRateLimit")["Statement"][
            "RateBasedStatement"
        ]
        limits = (int(rate["Limit"]), int(rate.get("EvaluationWindowSec", 300)), "live web ACL")
    except SystemExit:  # config_loader exits when config.env is missing
        reason = "no config.env"
    except StopIteration:
        reason = "no PerIpRateLimit rule in the web ACL"
    except (ClientError, BotoCoreError) as e:
        reason = f"GetWebACL failed ({_error_code(e)})"
    except (KeyError, ValueError, IndexError) as e:
        reason = f"unreadable WAF_WEB_ACL_ARN or web ACL ({type(e).__name__})"
    else:
        _waf_cache["limits"] = limits
        return limits
    if reason not in _waf_warned:
        _waf_warned.add(reason)
        print(f"WARNING: using the stack's default WAF limit for preflight: {reason}")
    return (
        DEMO_WAF_LIMIT,
        DEMO_WAF_WINDOW_S,
        f"default {DEMO_WAF_LIMIT}/{DEMO_WAF_WINDOW_S}s: {reason}",
    )


def _waf_problem(profile, limits=None):
    """Why this profile would trip the per-IP WAF rule, or None if it fits. `limits`
    is (limit, window_s[, source]); None reads the deployed rule (_waf_limits)."""
    limit, window_s, *source = limits or _waf_limits()
    headroom = max(DEMO_WAF_HEADROOM, limit // 20)
    peak = _peak_requests_in_window(_schedule_offsets(profile), window_s)
    if peak > limit - headroom:
        return (
            f"{peak} shaper submissions fall in one {window_s}s window; the API's "
            f"PerIpRateLimit WAF rule blocks past {limit} per {window_s}s "
            f"({source[0] if source else 'given limit'}; keeping {headroom} headroom), so the "
            "tail would be 403'd at ingress. Raise the limit by redeploying with "
            "`-c waf_ip_rate_limit=10000 -c waf_ip_rate_window_sec=60` (or "
            "WAF_IP_RATE_LIMIT / WAF_IP_RATE_WINDOW_SEC with deploy.sh), or pick a smaller profile"
        )
    return None


def _run_bounds(profile, queue_tpm, est_tokens=DEMO_TOKENS_PER_REQUEST_ESTIMATE):
    """Timeouts for one profile: (expected_drain_s, drain_timeout_s, direct_workers).

    expected_drain_s is how long the queue share needs for the whole offered load
    at its sustained rate, from run start. The drain wait (measured from the last
    submission) gets 2x that, never less than DEMO_DRAIN_TIMEOUT_S."""
    count = sum(c for _, c in profile)
    expected_drain_s = count * est_tokens / (queue_tpm / 60) if queue_tpm > 0 else 0.0
    drain_timeout_s = max(DEMO_DRAIN_TIMEOUT_S, 2 * expected_drain_s)
    workers = min(DEMO_DIRECT_MAX_WORKERS, max(DEMO_DIRECT_MIN_WORKERS, count))
    return expected_drain_s, drain_timeout_s, workers


def _new_virtual_budget(burndown_rate, level=None, now=time.time):
    """State for one direct arm's SIMULATED quota: a token bucket holding `level`
    of `limit` tokens. bpt matches the shaper's override item so both arms are
    charged identical estimates.

    The bucket starts FULL. `level` is the deliberate hook for evaluating an
    EMPTY starting bucket (bead 386) without touching the gate itself; `now` is
    injectable so tests can drive regeneration on a fake clock."""
    return {
        "limit": DEMO_TPM_OVERRIDE,
        "burndown": burndown_rate,
        "bpt": DEMO_BYTES_PER_TOKEN,
        "level": float(DEMO_TPM_OVERRIDE if level is None else level),
        "ts": now(),
        "lock": threading.Lock(),
    }


def _virtual_budget_admit(budget, prompt, max_tokens, now=time.time):
    """Charge the estimate up front (as Bedrock does) against a token bucket that
    regenerates its whole ceiling every DEMO_QUOTA_REFILL_S. Requests that do not
    fit in the CURRENT level are REJECTED, never queued -- failing fast is what
    the real service does, and waiting would turn this arm into a second shaper.

    Returns (charge, est, used); charge is None when rejected, and a rejected
    attempt costs nothing. `used` is how far the bucket is drawn down (limit
    minus the level) as of this admit, before this request's own debit. A
    successful call credits back est - actual via _virtual_budget_credit
    (mirroring the shaper's reconcile step); a failed call keeps the estimate."""
    est = estimate_request_tokens(
        prompt=prompt,
        max_tokens=max_tokens,
        burndown_rate=budget["burndown"],
        bytes_per_token=budget["bpt"],
    )
    with budget["lock"]:
        # Read the clock INSIDE the lock: taken outside it, a thread could be
        # descheduled between the read and the acquire, then apply a refill
        # measured from a `ts` another thread has since moved forward -- a
        # NEGATIVE refill that rewinds ts and debits the bucket for an attempt
        # that was rejected.
        t = now()
        cap = budget["limit"]
        budget["level"] = min(cap, budget["level"] + (t - budget["ts"]) * cap / DEMO_QUOTA_REFILL_S)
        budget["ts"] = t
        used = cap - budget["level"]
        if budget["level"] < est:
            return None, est, used
        budget["level"] -= est
        return {"est": est}, est, used


def _virtual_budget_credit(budget, tokens):
    """Return `tokens` to the bucket, clamped at its ceiling. Used to reconcile a
    successful call's up-front estimate to its actual usage, so a negative
    `tokens` (actual usage EXCEEDED the estimate) legitimately debits the
    overage. A named function (rather than an inline mutation) so the credit is
    testable without mocking converse()."""
    with budget["lock"]:
        budget["level"] = min(budget["limit"], budget["level"] + tokens)


def _schedule_offsets(profile):
    """Each request's scheduled offset (s) from run start for a load timeline.
    Pure, so it can be tested without sleeping."""
    offsets = []
    phase_start = 0.0
    for duration_s, count in profile:
        if duration_s < 0 or count < 0:
            raise ValueError(f"load phase must be non-negative: {(duration_s, count)}")
        if count:
            interval = duration_s / count
            offsets.extend(phase_start + k * interval for k in range(count))
        phase_start += duration_s
    return offsets


def _paced_indices(start, profile):
    """Yield 1..N, sleeping until each request's slot. Shared by every arm so
    all three are offered the identical submission profile."""
    for i, offset in enumerate(_schedule_offsets(profile), start=1):
        delay = start + offset - time.time()
        if _STOP.is_set() or (delay > 0 and _STOP.wait(delay)):
            return
        yield i


_http = {"pool": None, "creds": None, "lock": threading.Lock()}


def _fast_signed_post(url, body):
    """SigV4 POST over a keep-alive urllib3 pool with cached refreshable credentials.
    smoke_honest_outcomes._signed_request builds a boto3 Session and a fresh TLS
    connection per call, which caps one process far below the hundreds of POSTs/s
    a real-quota burst needs. Returns (status, body_text); raises on transport error."""
    import urllib3
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    with _http["lock"]:
        if _http["pool"] is None:
            _http["pool"] = urllib3.PoolManager(maxsize=DEMO_DIRECT_MAX_WORKERS, block=False)
            _http["creds"] = boto3.Session().get_credentials()
    data = json.dumps(body).encode()
    req = AWSRequest(
        method="POST", url=url, data=data, headers={"Content-Type": "application/json"}
    )
    region = url.split(".execute-api.")[1].split(".")[0]
    SigV4Auth(_http["creds"].get_frozen_credentials(), "execute-api", region).add_auth(req)
    resp = _http["pool"].request(
        "POST", url, body=data, headers=dict(req.headers), timeout=30.0, retries=False
    )
    return resp.status, resp.data.decode()


def _submit_sized(api_url, arm, model_id, prompt, max_tokens, fast=False, request_id=None):
    """Signed POST /invoke (smoke_honest_outcomes._submit with max_tokens exposed).
    Returns request_id, submit_status and the StartExecution executionArn (or None)."""
    request_id = request_id or str(uuid.uuid4())
    body = {
        "request_id": request_id,
        "model_id": model_id,
        "prompt": prompt,
        "correlation_id": str(uuid.uuid4()),
        "max_tokens": max_tokens,
    }
    if fast:
        status, text = _fast_signed_post(f"{api_url}/invoke", body)
    else:
        status, text = _signed_request("POST", f"{api_url}/invoke", body)
    _vprint(f"  [{arm}] POST /invoke -> {status}")
    if status not in (200, 202):
        print(f"  [{arm}] unexpected submit status: {status} {text[:200]}")
    execution_arn = None
    try:
        parsed = json.loads(text)
        request_id = parsed.get("request_id") or request_id
        execution_arn = parsed.get("executionArn")
    except (ValueError, AttributeError):
        pass
    return {"request_id": request_id, "submit_status": status, "execution_arn": execution_arn}


def _error_code(e):
    resp = getattr(e, "response", None)
    code = resp.get("Error", {}).get("Code", "") if isinstance(resp, dict) else ""
    return code or type(e).__name__


def _brief_error(e):
    """Exception type plus AWS error code: the only exception text that goes into
    recordings and summaries (boto messages can carry ARNs and account ids)."""
    name, code = type(e).__name__, _error_code(e)
    return name if code == name else f"{name} ({code})"


def _backoff(attempt):
    delay = min(DEMO_RETRY_MAX_DELAY_S, DEMO_RETRY_BASE_DELAY_S * (2**attempt))
    _STOP.wait(random.uniform(0, delay))  # nosec B311 -- backoff jitter; a stop cuts it short


def _direct_call(
    brt,
    idx,
    model_id,
    prompt,
    max_tokens,
    budget,
    arm,
    max_retries=0,
    t_submit=None,
    est_tokens=DEMO_TOKENS_PER_REQUEST_ESTIMATE,
):
    """One direct-Bedrock request, gated by the SIMULATED quota. Retries (with
    backoff, re-entering the gate each time) on simulated or real throttles up to
    max_retries; other errors are never retried, and their code is kept in
    `error_code`.

    Counts are PER ATTEMPT: attempts == bedrock_calls + sim_throttle_attempts.
    `ms` is total wall clock including backoff, measured from t_submit (the
    request's scheduled slot, so any wait for a pool worker counts against the
    arm); None for a no-retry rejection that never reached Bedrock. `elapsed_ms`
    is the same clock for EVERY outcome, rejections included, and None for a
    request a stop cancelled (outcome "cancelled"). est_tokens is the model's
    per-request estimate, reported in the `sent` event."""
    t0 = time.time() if t_submit is None else t_submit
    emit("sent", arm, idx, est=est_tokens)
    calls = sim = 0
    real_codes = []

    def result(
        ok,
        attempts,
        in_tok=0,
        out_tok=0,
        sim_final=False,
        ms="elapsed",
        throttled=False,
        stopped=False,
        error_code=None,
    ):
        now = time.time()
        elapsed_ms = None if stopped else (now - t0) * 1000
        status = None if stopped else 200 if ok else 429 if (sim_final or throttled) else 500
        emit(
            "done",
            arm,
            idx,
            status=status,
            ms=None if elapsed_ms is None else round(elapsed_ms, 1),
            simulated=sim_final,
        )
        return {
            "idx": idx,
            "ok": ok,
            "outcome": "cancelled" if stopped else "served" if ok else "failed",
            "simulated_throttle": sim_final,
            "in": in_tok,
            "out": out_tok,
            "ms": elapsed_ms if ms == "elapsed" else ms,
            "elapsed_ms": elapsed_ms,
            "status": status,
            "attempts": attempts,
            "bedrock_calls": calls,
            "sim_throttle_attempts": sim,
            "real_throttle_codes": real_codes,
            "error_code": error_code,
            "completed_ts": now,
        }

    for attempt in range(max_retries + 1):
        if _STOP.is_set():
            return result(False, attempt, ms=None, stopped=True)
        last = attempt == max_retries
        tag = f"  [{arm}] req{idx}" + (f" #{attempt + 1}" if max_retries else "")
        charge, est, used = _virtual_budget_admit(budget, prompt, max_tokens)
        if charge is None:
            sim += 1
            emit(
                "attempt",
                arm,
                idx,
                attempt=attempt + 1,
                outcome="exhausted" if last else "throttled",
                simulated=True,
            )
            _vprint(
                f"{tag} SIM-THROTTLE (level {budget['limit'] - used:.0f} < est {est})"
                f"{'' if last else ' -> retry'}"
            )
            if not last:
                _backoff(attempt)
                continue
            return result(
                False, attempt + 1, sim_final=True, ms=None if not max_retries else "elapsed"
            )

        calls += 1
        try:
            r = brt.converse(
                modelId=model_id,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": max_tokens},
            )
        except Exception as e:
            code = _error_code(e)
            real_throttle = code in DEMO_DIRECT_THROTTLE_CODES
            if real_throttle:
                real_codes.append(code)
            emit("attempt", arm, idx, attempt=attempt + 1, outcome="error", code=code)
            retry = real_throttle and not last
            print(
                f"{tag} converse() -> {'REAL THROTTLE' if real_throttle else 'error'} {code}"
                f"{' -> retry' if retry else ''}"
            )
            if retry:
                _backoff(attempt)
                continue
            return result(
                False,
                attempt + 1,
                throttled=real_throttle,
                error_code=None if real_throttle else code,
            )

        usage = r.get("usage", {})
        in_tok, out_tok = usage.get("inputTokens", 0), usage.get("outputTokens", 0)
        # est -> actual, settled the way Bedrock settles quota: output tokens count
        # at the model's burndown rate (docs: quotas-token-burndown).
        _virtual_budget_credit(budget, charge["est"] - (in_tok + budget["burndown"] * out_tok))
        res = result(True, attempt + 1, in_tok, out_tok)
        emit(
            "attempt", arm, idx, attempt=attempt + 1, outcome="ok", **{"in": in_tok, "out": out_tok}
        )
        _vprint(f"{tag} ok in={in_tok} out={out_tok} ({res['ms']:.0f}ms)")
        return res


def _run_direct_arm(
    region,
    model_id,
    prompt,
    max_tokens,
    profile,
    budget,
    out,
    arm,
    max_retries,
    start=None,
    workers=DEMO_DIRECT_MIN_WORKERS,
    est_tokens=DEMO_TOKENS_PER_REQUEST_ESTIMATE,
):
    """Background-thread body for one direct arm; writes results/start/end into `out`.
    `results` is in request order (idx 1..N) and is written even if the arm raises,
    so a crash keeps every row it got to. A request whose worker raised becomes a
    failed row with `error`, counted in out["crashed_requests"]."""
    client_cfg = Config(
        retries={"total_max_attempts": 1, "mode": "standard"},
        read_timeout=120,
        connect_timeout=10,
        max_pool_connections=workers,
    )
    brt = boto3.client("bedrock-runtime", region_name=region, config=client_cfg)
    # A shared start keeps every arm on the identical wall-clock schedule.
    start = time.time() if start is None else start
    out["start"] = start
    offsets = _schedule_offsets(profile)
    ex = cf.ThreadPoolExecutor(max_workers=workers)
    futs: list[tuple[int, cf.Future]] = []
    try:
        for i in _paced_indices(start, profile):
            fut = ex.submit(
                _direct_call,
                brt,
                i,
                model_id,
                prompt,
                max_tokens,
                budget,
                arm,
                max_retries,
                start + offsets[i - 1],
                est_tokens,
            )
            _tick_sent(arm)
            futs.append((i, fut))
        print(f"  [{arm}] submitted {len(futs)} requests in {time.time() - start:.1f}s")
        pending = {f for _, f in futs}
        deadline = time.time() + DEMO_DIRECT_TIMEOUT_S
        while pending and time.time() < deadline:
            _, pending = cf.wait(pending, timeout=1.0)
            if _STOP.is_set():
                deadline = min(deadline, time.time() + DEMO_STOP_GRACE_S)
    finally:
        # A stop does not wait here for in-flight converse() calls (read timeout
        # 120s): they finish in the background, and the interpreter joins their
        # threads at exit, so the process can outlive the summary by that long.
        ex.shutdown(wait=not _STOP.is_set(), cancel_futures=_STOP.is_set())
        results = [_direct_row(i, f) for i, f in futs]
        out["results"] = results
    end = time.time()
    crashed = [r for r in results if r.get("error")]
    if crashed:
        out["crashed_requests"] = len(crashed)
        print(f"  [{arm}] {len(crashed)} request(s) crashed, first: {crashed[0]['error']}")
    ok = sum(1 for r in results if r["ok"])
    sim = sum(1 for r in results if r["simulated_throttle"])
    unresolved = sum(1 for r in results if r["attempts"] is None and not r.get("error"))
    print(
        f"  [{arm}] done in {end - start:.1f}s -- {ok} ok, {sim} rejected by simulated quota, "
        f"{len(results) - ok - sim - unresolved} error"
        + (f", {unresolved} still pending at wait bound" if unresolved else "")
    )
    out["end"] = end


def _direct_row(idx, fut):
    """One direct request's result row. Unresolved at the wait bound (or cancelled
    by a stop): counts are unknowable, so None (excluded from the attempt sums and
    reported as unknown), not 0."""
    if fut.done() and not fut.cancelled():
        exc = fut.exception()
        if exc is None:
            return fut.result()
        print(f"  request {idx} crashed: {type(exc).__name__}: {exc}")
    else:
        exc = None
    return {
        "idx": idx,
        "ok": False,
        "outcome": "failed" if exc else "cancelled" if _STOP.is_set() else "unresolved",
        "simulated_throttle": False,
        "in": 0,
        "out": 0,
        "ms": None,
        "elapsed_ms": None,
        "status": None,
        "attempts": None,
        "bedrock_calls": None,
        "sim_throttle_attempts": None,
        "real_throttle_codes": None,
        "error": _brief_error(exc) if exc else None,
        "completed_ts": None,
    }


def _guarded_direct_arm(*args, **kwargs):
    """Thread target: a crash inside a direct arm is recorded on its `out` dict (and
    so in the run's summary as a run error), never silently dropped with the thread."""
    out = args[6]
    try:
        _run_direct_arm(*args, **kwargs)
    except Exception as e:
        out["error"] = _brief_error(e)
        print(f"  [{args[7]}] arm crashed: {type(e).__name__}: {e}")


def _peak_window_tpm(events, window_s=DEMO_PEAK_WINDOW_S):
    """Peak token sum over any trailing window_s window, from (completed_ts, tokens)
    events. The max always occurs at a window ending on an event, so a two-pointer
    sweep over sorted events is exact."""
    events = sorted(events)
    peak = running = 0.0
    left = 0
    for t, tok in events:
        running += tok
        while events[left][0] <= t - window_s:
            running -= events[left][1]
            left += 1
        peak = max(peak, running)
    return peak


def _aggregate_direct_results(results, offered, elapsed_s):
    """One direct arm's per-request results -> one table row. Timed-out requests
    (attempts=None) are excluded from the per-attempt sums and counted in
    unknown_attempts. error_codes counts the non-throttle errors Bedrock returned
    (AccessDenied, ValidationException, ...): any of them makes the comparison
    invalid, since they are not quota outcomes."""
    known = [r for r in results if r.get("attempts") is not None]
    error_codes: dict[str, int] = {}
    for r in results:
        if r.get("error_code"):
            error_codes[r["error_code"]] = error_codes.get(r["error_code"], 0) + 1
    attempts = sum(r["attempts"] for r in known)
    peak_events = [
        (r["completed_ts"], r["in"] + r["out"])
        for r in results
        if r["ok"] and r.get("completed_ts") is not None
    ]
    lat = [r["ms"] for r in results if r.get("ms") is not None]
    real_codes = [c for r in known for c in (r.get("real_throttle_codes") or [])]
    return {
        "offered": offered,
        "succ": sum(1 for r in results if r["ok"]),
        "err": sum(1 for r in results if not r["ok"] and not r.get("simulated_throttle")),
        "sim_thr": sum(r.get("sim_throttle_attempts", 0) for r in known),
        "real_thr": len(real_codes),
        "real_codes": sorted(set(real_codes)),
        "in": sum(r.get("in", 0) for r in results),
        "out": sum(r.get("out", 0) for r in results),
        "peak_tpm": _peak_window_tpm(peak_events),
        "p50": median(lat) if lat else 0.0,
        "attempts": attempts,
        "bedrock_calls": sum(r.get("bedrock_calls", 0) for r in known),
        "ratio": attempts / offered if offered else 0.0,
        "unknown_attempts": len(results) - len(known),
        "error_codes": error_codes,
        "run_time": elapsed_s,
    }


def _pctl(sorted_values, p):
    """Nearest-rank percentile of an already-sorted list (None when empty)."""
    if not sorted_values:
        return None
    rank = max(1, math.ceil(p / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def _latency_stats(ms_values):
    """avg / p50 / p95 / p99 / max (ms) over the non-None values."""
    vals = sorted(v for v in ms_values if v is not None)
    return {
        "n": len(vals),
        "avg": sum(vals) / len(vals) if vals else None,
        "p50": _pctl(vals, 50),
        "p95": _pctl(vals, 95),
        "p99": _pctl(vals, 99),
        "max": vals[-1] if vals else None,
    }


def _phase_of_each_request(profile):
    """Phase number (0-based) for request idx 1..N, in schedule order."""
    return [p for p, (_, count) in enumerate(profile) for _ in range(count)]


# Every offered request ends in exactly one of these, so per arm and per phase
# served + failed + unresolved + unsent + cancelled == offered.
DEMO_OUTCOMES = ("served", "failed", "unresolved", "unsent", "cancelled")


def _outcome(arm, r):
    """A result's outcome: its own `outcome` field, or (for results without one)
    served on success, failed if a status came back, otherwise unresolved."""
    if r.get("outcome"):
        return r["outcome"]
    if (r.get("status") == 200) if arm == "shaper" else r.get("ok", False):
        return "served"
    return "failed" if r.get("status") is not None else "unresolved"


def _arm_requests(arm, results, offsets, missing=None):
    """Normalize one arm's ordered results to the summary's per-request shape, one
    row per scheduled request: {"idx", "ok", "outcome", "status", "ms", "done_s"}
    (plus "error" when known). ms is end-to-end latency from the scheduled slot,
    set for served and failed requests only; done_s is completion time from run
    start. Shaper results are {"status", "ms", "outcome"}; direct results carry
    elapsed_ms. Scheduled requests past the end of `results` take `missing`
    (default: never sent)."""
    reqs = []
    for idx in range(1, len(offsets) + 1):
        r = results[idx - 1] if idx <= len(results) else (missing or {"outcome": "unsent"})
        outcome = _outcome(arm, r)
        ms = r.get("ms") if arm == "shaper" else r.get("elapsed_ms")
        if outcome not in ("served", "failed"):
            ms = None
        done_s = offsets[idx - 1] + ms / 1000 if ms is not None else None
        row = {
            "idx": idx,
            "ok": outcome == "served",
            "outcome": outcome,
            "status": r.get("status"),
            "ms": ms,
            "done_s": done_s,
        }
        if r.get("error"):
            row["error"] = r["error"]
        reqs.append(row)
    return reqs


def _outcome_counts(reqs):
    return {o: sum(1 for r in reqs if r["outcome"] == o) for o in DEMO_OUTCOMES}


def _build_summary(profile, per_arm_results, rows, meta):
    """The end-of-run statistics page as plain data (rendered by demo-ui, written
    to Markdown/JSON, printed to the terminal).

    Per arm: offered (the whole profile), one count per DEMO_OUTCOMES entry,
    total_s (first scheduled send to the last served or failed request),
    last_success_s, and latency stats over SERVED requests (the time a caller
    waited for a real answer) and over served + failed (fast rejections included).
    The same counts, per load phase. An arm whose row carries `error` crashed: its
    missing requests count as unresolved, not unsent."""
    offsets = _schedule_offsets(profile)
    phase_of = _phase_of_each_request(profile)
    arms = {}
    for arm, results in per_arm_results.items():
        row = rows.get(arm, {})
        missing = {"outcome": "unresolved", "error": row["error"]} if row.get("error") else None
        reqs = _arm_requests(arm, results, offsets, missing)
        served = [r for r in reqs if r["ok"]]
        resolved = [r for r in reqs if r["ms"] is not None]
        phases = []
        for p, (duration_s, count) in enumerate(profile):
            in_phase = [r for r in reqs if phase_of[r["idx"] - 1] == p]
            phases.append(
                {
                    "phase": p + 1,
                    "duration_s": duration_s,
                    "offered": count,
                    **_outcome_counts(in_phase),
                    "latency_served": _latency_stats(r["ms"] for r in in_phase if r["ok"]),
                }
            )
        arms[arm] = {
            "offered": len(reqs),
            **_outcome_counts(reqs),
            "served_pct": 100 * len(served) / len(reqs) if reqs else 0.0,
            "total_s": max((r["done_s"] for r in resolved), default=None),
            "last_success_s": max((r["done_s"] for r in served), default=None),
            "latency_served": _latency_stats(r["ms"] for r in served),
            "latency_all": _latency_stats(r["ms"] for r in resolved),
            "attempts": row.get("attempts"),
            "bedrock_calls": row.get("bedrock_calls"),
            "sim_throttles": row.get("sim_thr"),
            "real_throttles": row.get("real_thr"),
            # None = unknown (the shaper's EMF token metrics never landed), not 0.
            "tokens": (
                None
                if row.get("in") is None and row.get("out") is None
                else (row.get("in") or 0) + (row.get("out") or 0)
            ),
            "peak_tpm": row.get("peak_tpm"),
            "error_codes": row.get("error_codes") or {},
            "error": row.get("error"),
            "phases": phases,
            "requests": reqs,
        }
    return {"meta": meta, "profile": [list(p) for p in profile], "arms": arms}


def _fmt_s(ms):
    return "-" if ms is None else f"{ms / 1000:.1f}s"


def _summary_markdown(summary):
    """The summary as a Markdown report (the file written next to the recording)."""
    meta = summary["meta"]
    lines = [
        f"# Traffic shaper demo — {meta.get('model_id', '?')}",
        "",
        f"- Run started: {meta.get('started_at', '?')}",
        f"- Ceiling: {meta.get('tpm_ceiling', 0):,} TPM (queue share "
        f"{meta.get('queue_tpm', 0):,}); est. {meta.get('est_tokens', 0):,} tokens/request",
        f"- Profile: `{meta.get('profile_spec', '')}` = "
        + ", ".join(f"{d}s×{c}" for d, c in summary["profile"]),
        "",
        *(
            [f"> **Run error:** {meta['run_error']} -- figures for affected arms are partial.", ""]
            if meta.get("run_error")
            else []
        ),
        *(
            [
                "> **Stopped early** -- requests the stop aborted count as cancelled, "
                "requests it kept from being sent as unsent.",
                "",
            ]
            if meta.get("stopped")
            else []
        ),
        *[line for w in meta.get("warnings") or [] for line in (f"> **Warning:** {w}", "")],
        "Latency runs from each request's scheduled send to its final answer, backoff and "
        "queue wait included. Shaper latency carries up to 1s of status-poll lag. Failed "
        "includes ingress rejections; unresolved requests had no answer by the wait bound.",
        "",
        "| arm | offered | served | failed | unresolved | cancelled | unsent | total time "
        "| last success | avg | p50 | p95 | p99 | max | attempts | Bedrock calls "
        "| sim throttles | real throttles |",
        "|---|" + "---:|" * 17,
    ]
    for arm, a in summary["arms"].items():
        lat = a["latency_served"]
        lines.append(
            f"| {arm} | {a['offered']} | {a['served']} ({a['served_pct']:.0f}%) | "
            f"{a['failed']} | {a['unresolved']} | {a['cancelled']} | {a['unsent']} | "
            f"{_fmt_s(None if a['total_s'] is None else a['total_s'] * 1000)} | "
            f"{_fmt_s(None if a['last_success_s'] is None else a['last_success_s'] * 1000)} | "
            f"{_fmt_s(lat['avg'])} | {_fmt_s(lat['p50'])} | {_fmt_s(lat['p95'])} | "
            f"{_fmt_s(lat['p99'])} | {_fmt_s(lat['max'])} | {a['attempts']} | "
            f"{a['bedrock_calls']} | {a['sim_throttles']} | {a['real_throttles']} |"
        )
    lines += ["", "## Per phase (served requests)", ""]
    lines.append("| arm | phase | duration | offered | served | avg | p95 |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for arm, a in summary["arms"].items():
        for ph in a["phases"]:
            lat = ph["latency_served"]
            lines.append(
                f"| {arm} | {ph['phase']} | {ph['duration_s']}s | {ph['offered']} | "
                f"{ph['served']} | {_fmt_s(lat['avg'])} | {_fmt_s(lat['p95'])} |"
            )
    return "\n".join(lines) + "\n"


def _write_summary(summary, events_path=None):
    """Write summary JSON + Markdown beside the recording (or under tmp/).
    Returns the Markdown path."""
    if events_path:
        base = os.path.splitext(os.path.abspath(events_path))[0] + "-summary"
    else:
        os.makedirs(DEMO_TMP_DIR, exist_ok=True)
        base = os.path.join(DEMO_TMP_DIR, time.strftime("demo-summary-%Y%m%d-%H%M%S"))
    with open(base + ".json", "w") as stream:
        json.dump(summary, stream, indent=2, default=str)
    with open(base + ".md", "w") as stream:
        stream.write(_summary_markdown(summary))
    return base + ".md"


def _shaper_tokens(region, model_id, start_epoch, end_epoch):
    """(input_tokens, output_tokens, peak_60s_tpm) from the shaper's EMF metrics.
    Period=60 buckets are wall-clock aligned, so the peak approximates -- but is
    not computed identically to -- the direct arms' true sliding window."""
    cw = boto3.client("cloudwatch", region_name=region)
    dims = [
        {"Name": "ServiceName", "Value": "TrafficShaper"},
        {"Name": "model_id", "Value": model_id},
    ]
    q = [
        {
            "Id": f"m{i}",
            "ReturnData": True,
            "MetricStat": {
                "Metric": {"Namespace": "BedrockShaper", "MetricName": metric, "Dimensions": dims},
                "Period": 60,
                "Stat": "Sum",
            },
        }
        for i, metric in enumerate(("InputTokens", "OutputTokens"))
    ]
    r = cw.get_metric_data(MetricDataQueries=q, StartTime=start_epoch - 60, EndTime=end_epoch + 120)
    totals, buckets = {}, {}
    for res in r["MetricDataResults"]:
        totals[res["Id"]] = sum(res["Values"])
        for ts, val in zip(res["Timestamps"], res["Values"]):
            buckets[ts] = buckets.get(ts, 0.0) + val
    return totals.get("m0", 0.0), totals.get("m1", 0.0), max(buckets.values(), default=0.0)


def _build_ceiling_override_item(original_item, model_id, tpm_override=DEMO_TPM_OVERRIDE):
    """Re-derive the WHOLE config at tpm_override via create_model_config.

    Setting tpm_limit alone is inert: the queue processor gates on the fields
    calculate_config() derives from it (tpm_queue_capacity,
    tpm_queue_regeneration_rate). The capacity split is read off the live item so
    the deployed shape is scaled down, not invented. Merged onto original_item
    because calculate_config() omits api_style, backend and adaptive_* fields.
    bytes_per_token is the one demo-specific value (DEMO_BYTES_PER_TOKEN)."""
    live_tpm = int(original_item.get('tpm_limit') or 0)

    def _fraction(field, default):
        if live_tpm <= 0:
            return default
        return float(int(original_item.get(field) or 0)) / live_tpm

    derived = calculate_config(
        rpm=int(original_item['rpm_limit']) if original_item.get('rpm_limit') else None,
        tpm=tpm_override,
        burndown_rate=float(original_item.get('output_token_burndown_rate', 1.0)),
        bytes_per_token=DEMO_BYTES_PER_TOKEN,
        short_window_sec=int(original_item.get('short_window_sec', 2)),
        long_window_sec=int(original_item.get('long_window_sec', 15)),
        burst_fraction=_fraction('tpm_burst_capacity', 0.0),
        queue_fraction=_fraction('tpm_queue_capacity', 0.85),
        buffer_fraction=_fraction('tpm_buffer_capacity', 0.15),
    )
    # dry_run=True: item assembly only; the write uses main()'s own table handle.
    return {**original_item, **create_model_config(model_id, derived, dry_run=True)}


def _config_backup_path(run_id):
    return os.path.abspath(os.path.join(DEMO_TMP_DIR, f"{run_id}-config-backup.json"))


def _restore_command(region, table_name, backup_path):
    return (
        f"aws dynamodb put-item --region {region} --table-name {table_name} "
        f"--item file://{backup_path}"
    )


def _write_config_backup(original_item, run_id):
    """Save the original CONFIG item before it is overridden, in DynamoDB JSON
    (typed, so Decimals round-trip exactly) that `aws dynamodb put-item --item
    file://...` takes as is. Returns the path."""
    path = _config_backup_path(run_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as stream:
        json.dump(TypeSerializer().serialize(original_item)["M"], stream, indent=2)
    return path


def _restore_config(table, key, original_item, run_id, backup_path, region, table_name):
    """Put the original CONFIG back, but only over this run's own override (the
    `demo_override_run` marker), then read it back and compare. Retried
    DEMO_RESTORE_ATTEMPTS times with backoff. If another run's marker is there, it
    owns the item: warn and leave it. On failure the backup is kept and the run
    fails with the restore command; on success the backup is deleted."""
    last = None
    for attempt in range(DEMO_RESTORE_ATTEMPTS):
        if attempt:
            time.sleep(DEMO_RESTORE_BACKOFF_S * 2 ** (attempt - 1))
        try:
            try:
                table.put_item(
                    Item=original_item,
                    ConditionExpression="demo_override_run = :mine",
                    ExpressionAttributeValues={":mine": run_id},
                )
            except ClientError as e:
                # No marker of ours: an earlier attempt already restored it (its
                # read-back failed), the override was never written, or another
                # run has since taken the item over.
                if _error_code(e) != "ConditionalCheckFailedException":
                    raise
            current = table.get_item(Key=key, ConsistentRead=True).get("Item")
            if current == original_item:
                with contextlib.suppress(OSError):
                    os.remove(backup_path)
                print("[demo] Restored original CONFIG (verified by read-back)")
                return
            if current and current.get("demo_override_run") not in (None, run_id):
                _warn(
                    f"CONFIG carries another demo run's override "
                    f"({current['demo_override_run']}); left it for that run to restore"
                )
                return
            last = "read-back does not match the original"
        except (ClientError, BotoCoreError) as e:
            last = _brief_error(e)
            print(f"[demo] CONFIG restore attempt {attempt + 1} failed: {type(e).__name__}: {e}")
    _record_error(
        "config_restore_failed",
        f"CONFIG restore failed after {DEMO_RESTORE_ATTEMPTS} attempts ({last}); the "
        f"200k override is still live. Backup kept: {os.path.basename(backup_path)}",
    )
    print(
        "\n"
        + "!" * 72
        + f"\nCONFIG NOT RESTORED. The original item is saved at:\n  {backup_path}\n"
        f"Restore it with:\n  {_restore_command(region, table_name, backup_path)}\n" + "!" * 72
    )


_unknown_status_seen: set[tuple[str, str]] = set()


def _terminal_http_status(item):
    """STATUS item -> terminal HTTP code (result_fn.py's mapping), or None while the
    request is still PENDING/QUEUED. A state or failure reason outside the map is
    served as 503 (as result_fn.py does) and logged once."""
    state = item.get("state")
    if state in (None, "PENDING", "QUEUED"):
        return None
    if state == "SUCCEEDED":
        return 200
    reason = item.get("reason")
    if state != "FAILED" or reason not in DEMO_FAILED_REASON_TO_STATUS:
        seen = (str(state), str(reason))
        if seen not in _unknown_status_seen:
            _unknown_status_seen.add(seen)
            print(f"  [shaper] unexpected STATUS state={state!r} reason={reason!r}: counted as 503")
    return DEMO_FAILED_REASON_TO_STATUS.get(reason, 503)


def _read_status_batch(dynamodb, table_name, request_ids, errors=None):
    """One BatchGetItem pass over request_ids -> {request_id: http_status} for the
    ones now terminal. Eventually consistent: a lagging read is simply re-read on
    the next pass. Errors are logged (and their codes appended to `errors`), never
    raised -- the IDs stay outstanding."""
    terminal = {}
    ids = list(request_ids)
    for i in range(0, len(ids), DEMO_STATUS_BATCH_MAX_KEYS):
        chunk = ids[i : i + DEMO_STATUS_BATCH_MAX_KEYS]
        try:
            resp = dynamodb.batch_get_item(
                RequestItems={
                    table_name: {
                        "Keys": [{"pk": f"REQUEST#{rid}", "sk": "STATUS"} for rid in chunk],
                        "ProjectionExpression": "pk, #st, reason",
                        "ExpressionAttributeNames": {"#st": "state"},
                    }
                }
            )
        except (ClientError, BotoCoreError) as e:
            print(
                f"  [shaper] ERROR BatchGetItem failed ({_error_code(e)}) for "
                f"{len(chunk)} request(s): {e}"
            )
            if errors is not None:
                errors.append(_error_code(e))
            continue
        for item in resp.get("Responses", {}).get(table_name, []):
            rid = str(item.get("pk", "")).removeprefix("REQUEST#")
            if rid not in chunk:
                print(f"  [shaper] ERROR BatchGetItem returned an unexpected item: {item!r}")
                continue
            status = _terminal_http_status(item)
            if status is not None:
                terminal[rid] = status
        unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
        if unprocessed:
            print(
                f"  [shaper] ERROR BatchGetItem left {len(unprocessed)} key(s) unprocessed "
                "-- re-reading next pass"
            )
    return terminal


def _cancel_outstanding(dynamodb, table_name, model_id, request_ids, execution_arns, region):
    """Stop cleanup for the shaper arm: delete this run's still-queued items, then stop
    their Step Functions executions. Deletion comes first because bedrock_processor
    never checks whether an execution was aborted -- a dequeued item still calls
    Bedrock. Items the processor already pulled into its in-memory chunk (<=
    queue_batch_size) cannot be recalled.

    The two phases fail independently: a delete failure still lets the stops run.
    Returns {"deleted", "stopped", "failed_stops", "errors"}, where failed_stops
    counts StopExecution calls that failed (an execution already gone is not a
    failure) and errors lists what went wrong, as type and error code."""
    ids = set(request_ids)
    counts: dict = {"deleted": 0, "stopped": 0, "failed_stops": 0, "errors": []}
    if ids:
        try:
            table = dynamodb.Table(table_name)
            query = {
                "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": f"MODEL#{model_id}#QUEUE#ITEMS"},
                "ProjectionExpression": "pk, sk, request_id",
            }
            doomed = []
            while True:
                page = table.query(**query)
                doomed += [i for i in page.get("Items", []) if i.get("request_id") in ids]
                if "LastEvaluatedKey" not in page:
                    break
                query["ExclusiveStartKey"] = page["LastEvaluatedKey"]
            with table.batch_writer() as batch:
                for item in doomed:
                    batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})
            counts["deleted"] = len(doomed)
        except (ClientError, BotoCoreError) as e:
            counts["errors"].append(f"queue delete {_brief_error(e)}")
            print(f"  [shaper] queue item delete failed: {type(e).__name__}: {e}")
    arns = [a for a in execution_arns if a]
    if arns:
        sfn = boto3.client(
            "stepfunctions", region_name=region, config=Config(retries={"mode": "adaptive"})
        )

        def stop(arn):
            try:
                sfn.stop_execution(executionArn=arn, error="DemoStopped", cause="demo-ui Stop")
                return "stopped"
            except (ClientError, BotoCoreError) as e:  # throttled past retries, network
                if _error_code(e) == "ExecutionDoesNotExist":
                    return "gone"
                print(f"  [shaper] StopExecution failed for {arn}: {_error_code(e)}")
                return _brief_error(e)

        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            for outcome in ex.map(stop, arns):
                if outcome == "stopped":
                    counts["stopped"] += 1
                elif outcome != "gone":
                    counts["failed_stops"] += 1
                    if f"StopExecution {outcome}" not in counts["errors"]:
                        counts["errors"].append(f"StopExecution {outcome}")
    return counts


def _run_shaper_arm(
    api_url,
    model_id,
    prompt,
    run_start,
    dynamodb,
    table_name,
    profile=None,
    drain_timeout_s=None,
    submit_workers=DEMO_SHAPER_SUBMIT_WORKERS,
    fast=False,
    est_tokens=DEMO_TOKENS_PER_REQUEST_ESTIMATE,
    state=None,
):
    """Submit the paced load to /invoke while a background poller reads each
    accepted request's STATUS item until it is terminal. Results are in request
    order, {"status", "ms", "outcome"}; `ms` runs from the request's scheduled slot
    to the poll that saw the terminal state (so it carries up to
    DEMO_STATUS_POLL_INTERVAL_S of observation lag), or to the POST's response for
    a request rejected at ingress. Requests still open at the end are "cancelled"
    after a stop and "unresolved" otherwise; `state` (if given) receives the
    run's request tracking, for _cleanup_shaper."""
    profile = DEMO_LOAD_PROFILE if profile is None else profile
    drain_timeout_s = DEMO_DRAIN_TIMEOUT_S if drain_timeout_s is None else drain_timeout_s
    # request_id -> (idx, submit_ts). Starts empty; an ID is added once its POST
    # is accepted and removed the moment its STATUS item is terminal.
    outstanding = {}
    lock = threading.Lock()
    results = {}  # idx -> {"status", "ms", "outcome"}
    sent_ids = {}  # idx -> request_id, known before the POST, so a stop can find it
    arns = {}  # request_id -> executionArn, once the POST returns
    submit_failures: dict[str, int] = {}  # status or exception type -> count
    submits_done = threading.Event()
    drain = {"deadline": None}
    poll_error = {}
    if state is not None:
        state.update(lock=lock, results=results, sent_ids=sent_ids, arns=arns)

    def poll():
        resolved = failed_passes = 0
        while True:
            with lock:
                batch = dict(outstanding)
            if _STOP.is_set():
                return
            if submits_done.is_set():
                if not batch or time.time() >= drain["deadline"]:
                    return
            if batch:
                errors = []
                terminal = _read_status_batch(dynamodb, table_name, batch, errors)
                failed_passes = failed_passes + 1 if errors and not terminal else 0
                if failed_passes >= DEMO_STATUS_MAX_FAILED_PASSES:
                    poll_error.update(
                        reason="status_reads_failing",
                        message=f"shaper STATUS reads failed {failed_passes} passes in a row "
                        f"({errors[-1]}); stopped polling",
                    )
                    return
                for rid, status in terminal.items():
                    idx, submit_ts = batch[rid]
                    ms = (time.time() - submit_ts) * 1000
                    with lock:
                        outstanding.pop(rid, None)
                        results[idx] = {
                            "status": status,
                            "ms": ms,
                            "outcome": "served" if status == 200 else "failed",
                        }
                    emit("done", "shaper", idx, status=status, ms=round(ms, 1))
                    _vprint(f"  [shaper] req{idx} -> {status}")
                    resolved += 1
                    if resolved % 10 == 0:
                        print(f"  [shaper] {resolved} resolved")
            # One pass reads every outstanding id (<=100 per BatchGetItem); slow the pass
            # rate with the backlog so a 5k-request burst is not 5k reads/s. Costs up to
            # this much latency-observation lag.
            _STOP.wait(max(DEMO_STATUS_POLL_INTERVAL_S, len(batch) / DEMO_STATUS_READS_PER_S))

    def guarded_poll():
        # A dead poller would read as "the shaper lost"; record it as a run error.
        try:
            poll()
        except Exception as e:
            poll_error.update(
                reason="shaper_poller_crashed",
                message=f"shaper status poller crashed: {_brief_error(e)}",
            )
            print(f"  [shaper] status poller crashed: {type(e).__name__}: {e}")

    # Each signed POST takes longer than a pacing slot, so POSTs are dispatched to
    # a pool (like the direct arms) to keep the shaper on the identical schedule.
    offsets = _schedule_offsets(profile)

    def submit(i):
        if _STOP.is_set():  # queued behind a spike when the stop came: never POSTed
            with lock:
                results[i] = {"status": None, "ms": None, "outcome": "unsent"}
            return
        # Latency runs from the scheduled slot, the same clock the direct arms use.
        submit_ts = run_start + offsets[i - 1]
        emit("sent", "shaper", i, est=est_tokens)
        rid = str(uuid.uuid4())
        with lock:
            sent_ids[i] = rid
        try:
            sub = _submit_sized(
                api_url, f"shaper:req{i}", model_id, prompt, DEMO_MAX_OUTPUT_TOKENS, fast, rid
            )
        except Exception as e:  # one failed POST is one failed request, not the arm
            print(f"  [shaper] req{i} POST failed: {type(e).__name__}: {e}")
            sub = {"request_id": rid, "submit_status": None, "execution_arn": None}
            failure = type(e).__name__
        else:
            failure = str(sub["submit_status"])
        accepted = sub["submit_status"] in (200, 202)
        emit(
            "attempt",
            "shaper",
            i,
            attempt=1,
            outcome="admitted" if accepted else "error",
            status=sub["submit_status"],
        )
        ms = (time.time() - submit_ts) * 1000
        with lock:
            if sub.get("execution_arn"):
                arns[sub["request_id"]] = sub["execution_arn"]
            if accepted:
                outstanding[sub["request_id"]] = (i, submit_ts)
            else:
                # Rejected at ingress: terminal now, nothing to poll.
                results[i] = {"status": sub["submit_status"], "ms": ms, "outcome": "failed"}
                submit_failures[failure] = submit_failures.get(failure, 0) + 1
        if not accepted:
            emit("done", "shaper", i, status=sub["submit_status"], ms=round(ms, 1))

    poller = threading.Thread(target=guarded_poll, daemon=True)
    poller.start()
    indices = []
    with cf.ThreadPoolExecutor(max_workers=submit_workers) as ex:
        futs = []
        for i in _paced_indices(run_start, profile):
            futs.append(ex.submit(submit, i))
            _tick_sent("shaper")
            indices.append(i)
        dispatched_s = time.time() - run_start
    for f in futs:
        f.result()  # surface submit-thread exceptions
    drain["deadline"] = time.time() + drain_timeout_s
    submits_done.set()
    print(
        f"[shaper] submitted {len(futs)} requests (dispatched over {dispatched_s:.1f}s, "
        f"all accepted by {time.time() - run_start:.1f}s); draining (up to {drain_timeout_s:.0f}s)..."
    )
    poller.join()
    if poll_error:
        _record_error(poll_error["reason"], poll_error["message"])

    left_open = "cancelled" if _STOP.is_set() else "unresolved"
    with lock:
        ordered = [
            results.get(i, {"status": None, "ms": None, "outcome": left_open}) for i in indices
        ]
        pending = len(outstanding)
    if state is not None:
        state.update(pending=pending, submit_failures=submit_failures, sent=len(sent_ids))
    print(
        f"[shaper] {'drained' if not pending else 'drain TIMED OUT'} -- "
        f"{len(ordered) - pending}/{len(ordered)} complete"
    )
    return ordered


def _cleanup_shaper(state, dynamodb, table_name, model_id, region, why):
    """Cancel every sent-but-unresolved shaper request -- after a stop, a drain
    timeout or a run error -- so none of them drains against the real quota once
    CONFIG is restored. Two passes: the second catches items budget_manager
    enqueued after the first. Counts accumulate across passes; any delete or stop
    failure is a warning and a run error, since that work may still run."""
    with state["lock"]:
        any_open = any(idx not in state["results"] for idx in state["sent_ids"])
    if not any_open:
        return
    time.sleep(DEMO_STOP_ENQUEUE_SETTLE_S)  # nosemgrep: arbitrary-sleep -- see the constant
    with state["lock"]:
        open_ids = [rid for idx, rid in state["sent_ids"].items() if idx not in state["results"]]
        open_arns = [state["arns"].get(rid) for rid in open_ids]
    total = {"deleted": 0, "stopped": 0, "failed_stops": 0}
    errors = []
    for arns in (open_arns, []):
        counts = _cancel_outstanding(dynamodb, table_name, model_id, open_ids, arns, region)
        for k in total:
            total[k] += counts[k]
        errors += [e for e in counts["errors"] if e not in errors]
    unknown_arn = sum(1 for a in open_arns if not a)
    print(
        f"[shaper] {why.upper()} cleanup -- {len(open_ids)} open request(s): deleted "
        f"{total['deleted']} queued item(s), stopped {total['stopped']} execution(s), "
        f"{total['failed_stops']} stop(s) failed, {unknown_arn} without an execution ARN"
    )
    emit("cleanup", "shaper", open=len(open_ids), unknown_arn=unknown_arn, reason=why, **total)
    if errors or total["failed_stops"]:
        message = (
            f"shaper cleanup after {why} incomplete ({'; '.join(errors)}): queued items or "
            "executions may still run against the restored CONFIG"
        )
        _warn(message)
        _record_error("cleanup_failed", message)


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("model", nargs="?", default="nova-2-lite")
    parser.add_argument("-v", "--verbose", action="store_true", help="print every request/attempt")
    parser.add_argument("--events", help="record observed live events as JSONL for demo-ui/replay")
    parser.add_argument(
        "--profile",
        default="default",
        help="preset name or inline spec, e.g. '60@1x,15@6x,60@0.5x' or '25:70' "
        "(see --list-profiles)",
    )
    parser.add_argument("--list-profiles", action="store_true", help="print presets and exit")
    parser.add_argument(
        "--real-quota",
        action="store_true",
        help="shaper arm only, against the model's REAL CONFIG and Bedrock quota: no "
        "200k override, no direct arms, and `D@Mx` scales the real tpm_limit",
    )
    parser.add_argument(
        "--force", action="store_true", help="run a profile the per-IP WAF limit would block"
    )
    args = parser.parse_args(argv)
    if args.list_profiles:
        print(f"1x = {DEMO_BASE_RPM:.1f} requests/min ({DEMO_TPM_OVERRIDE:,} TPM ceiling)")
        for name, spec in DEMO_PROFILE_PRESETS.items():
            profile = parse_load_profile(spec)
            print(
                f"  {name:<12} {sum(c for _, c in profile):>4} req over "
                f"{sum(d for d, _ in profile):>4}s  {spec}"
            )
        sys.exit(0)
    # Syntax only here: whether the profile fits the WAF limit and the queue expiry
    # depends on the model's burndown, so main() checks that once CONFIG is read.
    try:
        args.load_profile = parse_load_profile(args.profile)
    except ValueError as e:
        parser.error(str(e))
    return args


def _request_stop(signum, _frame):
    if not _STOP.is_set():
        _STOP.set()  # first, so a failing print cannot skip the stop
        print(
            f"\n[demo] {signal.Signals(signum).name}: stopping -- cleaning up, then restoring CONFIG"
        )


def _col(value, width, fmt="d"):
    return f"{'-':>{width}s}" if value is None else f"{value:>{width}{fmt}}"


def main(argv=None):
    global VERBOSE
    args = _parse_args(argv)
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    VERBOSE = args.verbose
    _run_log["errors"].clear()
    _run_log["warnings"].clear()
    model_id = MODEL_MAP.get(args.model, args.model)

    config = config_loader.get_config_with_aws_check()
    region = config.get('AWS_REGION', 'us-east-1')
    table_name = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    key = {'pk': f'MODEL#{model_id}', 'sk': 'CONFIG'}

    original_item = table.get_item(Key=key).get('Item')
    if original_item is None:
        print(f"No CONFIG item found for {model_id} in {table_name} -- nothing to demo.")
        sys.exit(1)
    if original_item.get("backend", "runtime") != "runtime":
        # The direct arms call bedrock-runtime converse() and the override derives
        # combined-TPM fields only; a mantle (split iTPM/oTPM) config fits neither.
        print(f"{model_id} uses backend={original_item.get('backend')}; the demo is runtime-only.")
        sys.exit(1)
    if original_item.get("demo_override_run"):
        # What is in CONFIG now is another run's 200k override, not the real item:
        # snapshotting it as "original" would leave the override in place for good.
        marker = original_item["demo_override_run"]
        print(
            f"Refusing to run: {model_id} CONFIG carries demo override run {marker}. Another "
            "demo is running on this model, or one crashed before restoring CONFIG. If none "
            "is running, restore the original from that run's backup:\n  "
            + _restore_command(region, table_name, _config_backup_path(marker))
        )
        sys.exit(2)
    print(f"Model: {model_id} (table: {table_name}, region: {region})")

    # The per-request estimate, and so the 1x rate, depends on the model's output
    # burndown (5x-15x on recent Claude models): re-derive both, re-parse `D@Mx`
    # phases against this model's 1x, and re-check the WAF limit on the result.
    burndown_rate = float(original_item.get('output_token_burndown_rate', 1.0))
    est_tokens = est_tokens_for(burndown_rate)
    real = args.real_quota
    if real:
        # The model's own CONFIG is what runs: nothing is written, nothing restored.
        overridden_item = original_item
        ceiling = int(original_item.get('tpm_limit') or 0)
        if ceiling <= 0:
            print(f"Refusing to run: {model_id} CONFIG has no tpm_limit for --real-quota to use.")
            sys.exit(2)
    else:
        # Pure (no write): the override is built here so the preflight sees the
        # queue share before CONFIG is touched.
        overridden_item = _build_ceiling_override_item(original_item, model_id)
        ceiling = DEMO_TPM_OVERRIDE
    base_rpm = ceiling / est_tokens
    queue_tpm = int(overridden_item.get('tpm_queue_capacity') or 0)
    try:
        profile = parse_load_profile(args.profile, base_rpm)
        problems = _preflight_problems(profile, burndown_rate, queue_tpm / ceiling, ceiling)
    except ValueError as e:
        print(f"Refusing to run: {e} (this model's 1x = {base_rpm:.1f} req/min).")
        sys.exit(2)
    blocking = [message for kind, message in problems if not (args.force and kind == "waf")]
    if blocking:
        forceable = not args.force and any(kind == "waf" for kind, _ in problems)
        print(
            f"Refusing to run: {'; '.join(blocking)} (this model's 1x = {base_rpm:.1f} req/min)."
            + (" Pass --force to run past the WAF limit anyway." if forceable else "")
        )
        sys.exit(2)
    if problems:
        _warn("--force: the profile exceeds the per-IP WAF limit; expect shaper 403s at ingress")
    offered = sum(count for _, count in profile)
    submit_window_s = sum(duration for duration, _ in profile)

    # Two SEPARATE budgets with identical parameters: sharing one would make the
    # direct arms contend with each other.
    # --real-quota runs no direct arms: they would draw on the same real quota.
    arms = [] if real else [("direct", 0), ("direct+retry", DEMO_RETRY_MAX_RETRIES)]
    peak_1s = _peak_requests_in_window(_schedule_offsets(profile), 1.0)
    # ~0.7s per POST: enough workers to hold the schedule through the peak second.
    submit_workers = min(DEMO_DIRECT_MAX_WORKERS, max(DEMO_SHAPER_SUBMIT_WORKERS, 2 * peak_1s))
    arm_out: dict[str, dict] = {label: {} for label, _ in arms}
    threads = []
    stop_progress = threading.Event()
    shaper_results = []
    shaper_state: dict = {}
    itok = otok = shaper_peak_tpm = None
    run_start = run_end = time.time()
    summary = None
    meta = {}
    run_id = str(uuid.uuid4())
    backup_path = None
    aborted = False

    try:
        expected_drain_s, drain_timeout_s, workers = _run_bounds(profile, queue_tpm, est_tokens)
        if real:
            print(
                f"REAL QUOTA run: {model_id} CONFIG untouched (tpm_limit {ceiling:,}, queue "
                f"share {queue_tpm:,}); shaper arm only, {submit_workers} submit workers."
            )
        else:
            backup_path = _write_config_backup(original_item, run_id)
            print(f"Original CONFIG saved to {backup_path}")
            # The marker is one extra attribute: the Lambdas read CONFIG through
            # DynamoService.get_model_config() and pick fields by name, so they
            # ignore it. The condition catches a second run that read CONFIG
            # before this write.
            table.put_item(
                Item={**overridden_item, "demo_override_run": run_id},
                ConditionExpression="attribute_not_exists(demo_override_run)",
            )
        print(
            ""
            if real
            else f"Config re-derived at {DEMO_TPM_OVERRIDE:,} TPM (restored at end). All three arms are "
            f"held to this {DEMO_TPM_OVERRIDE:,} TPM ceiling: the shaper via this config, the "
            f"direct arms via a client-side simulated quota, modeled as {DEMO_TPM_OVERRIDE // 1000}k/60 tokens/s "
            f"regeneration:"
        )
        for field in (
            'tpm_limit',
            'tpm_queue_capacity',
            'tpm_queue_regeneration_rate',
            'tpm_burst_capacity',
            'tpm_buffer_capacity',
            'bytes_per_token',
        ):
            print(f"    {field:<30} {original_item.get(field)} -> {overridden_item.get(field)}")

        api_url = _load_api_url(None)
        prompt = _build_filler_prompt()
        print(
            f"Offering {offered} requests over ~{submit_window_s}s to each arm "
            f"(profile {args.profile!r}: "
            + ", ".join(f"{d}s×{c}" for d, c in profile)
            + f"; {est_tokens} est. tokens/request at burndown {burndown_rate:g}, "
            f"max_output_tokens={DEMO_MAX_OUTPUT_TOKENS}). Expected shaper drain "
            f"~{expected_drain_s:.0f}s from start; waiting up to {drain_timeout_s:.0f}s "
            f"after the last submission.\n"
        )

        run_start = time.time()
        meta = {
            "model": args.model,
            "model_id": model_id,
            "profile_spec": args.profile,
            "tpm_ceiling": ceiling,
            "real_quota": real,
            "queue_tpm": queue_tpm,
            "est_tokens": est_tokens,
            "burndown": burndown_rate,
            "base_rpm": round(base_rpm, 2),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(run_start)),
            "start_epoch": round(run_start, 3),
            "planned_s": submit_window_s,
        }
        if args.events:
            _start_events(args.events, run_start, profile, **meta)
        for arm in ["shaper"] + [label for label, _ in arms]:  # fixes the column order
            _progress["sent"][arm] = 0
        threading.Thread(
            target=_progress_reporter,
            daemon=True,
            args=(stop_progress, run_start, offered),
        ).start()
        for label, max_retries in arms:
            t = threading.Thread(
                target=_guarded_direct_arm,
                daemon=True,
                args=(
                    region,
                    model_id,
                    prompt,
                    DEMO_MAX_OUTPUT_TOKENS,
                    profile,
                    _new_virtual_budget(burndown_rate),
                    arm_out[label],
                    label,
                    max_retries,
                    run_start,
                    workers,
                    est_tokens,
                ),
            )
            t.start()
            threads.append((label, t))

        shaper_results = _run_shaper_arm(
            api_url,
            model_id,
            prompt,
            run_start,
            dynamodb,
            table_name,
            profile,
            drain_timeout_s,
            submit_workers,
            fast=real,
            est_tokens=est_tokens,
            state=shaper_state,
        )
        run_end = time.time()
        stop_progress.set()
        # Open shaper requests would drain against the real quota once CONFIG is
        # restored: cancel them before the finally below restores it.
        if _STOP.is_set() or shaper_state["pending"]:
            if not _STOP.is_set():
                _warn(
                    f"shaper drain timed out with {shaper_state['pending']} request(s) "
                    "unresolved; they were cancelled"
                )
            why = "stop" if _STOP.is_set() else "drain timeout"
            shaper_state["cleaned"] = True
            _cleanup_shaper(shaper_state, dynamodb, table_name, model_id, region, why)

        # EMF lands minute bucket by minute bucket: accept the totals once two reads
        # agree, so a first partial bucket is not reported as the whole run.
        print("[shaper] waiting for CloudWatch EMF token metrics...")
        metrics_deadline = time.time() + (0 if _STOP.is_set() else DEMO_METRICS_WAIT_S)
        last = None
        while True:
            try:
                read = _shaper_tokens(region, model_id, run_start, run_end)
            except (ClientError, BotoCoreError) as e:
                _warn(f"shaper token metrics unreadable ({_brief_error(e)}); tokens unknown")
                break
            if (read[0] or read[1]) and read == last:
                itok, otok, shaper_peak_tpm = read
                break
            last = read
            if time.time() >= metrics_deadline:
                if read[0] or read[1]:
                    itok, otok, shaper_peak_tpm = read
                    _warn("shaper token metrics had not settled; shaper tokens may be partial")
                else:
                    _warn(
                        f"shaper EMF metrics did not land within {DEMO_METRICS_WAIT_S}s; "
                        "shaper tokens unknown (re-query CloudWatch later)"
                    )
                break
            _STOP.wait(DEMO_METRICS_POLL_INTERVAL_S)
        if itok is not None:
            print(
                f"[shaper] {itok + otok:,.0f} tokens in {run_end - run_start:.0f}s, peak 60s = "
                f"{shaper_peak_tpm:,.0f} TPM (queue share "
                f"{int(overridden_item['tpm_queue_capacity']):,} / ceiling {ceiling:,})"
            )
    except Exception as e:
        print(f"[demo] run failed: {type(e).__name__}: {e}")
        _record_error("run_failed", f"demo run failed: {_brief_error(e)}")
        aborted = True
        _STOP.set()  # the direct arms stop offering load too
        if shaper_state.get("sent_ids") and not shaper_state.get("cleaned"):
            _cleanup_shaper(shaper_state, dynamodb, table_name, model_id, region, "run error")
    finally:
        stop_progress.set()
        if backup_path:
            # Restore the ENTIRE item -- the override rewrote every derived field.
            _restore_config(table, key, original_item, run_id, backup_path, region, table_name)

    for label, t in threads:
        t.join(timeout=DEMO_DIRECT_TIMEOUT_S + submit_window_s + 60)
        out = arm_out[label]
        if t.is_alive():
            out["error"] = "did not finish within the join bound"
            _record_error("direct_arm_unfinished", f"{label} arm did not finish in time")
        elif out.get("error"):
            _record_error("direct_arm_crashed", f"{label} arm crashed: {out['error']}")
        if out.get("crashed_requests"):
            first = next(r["error"] for r in out["results"] if r.get("error"))
            _record_error(
                "direct_request_crashed",
                f"{label}: {out['crashed_requests']} request(s) crashed ({first})",
            )

    n_sent = sum(1 for r in shaper_results if r.get("outcome") != "unsent")
    rows = {
        "shaper": {
            "offered": offered,
            "attempts": n_sent,
            "bedrock_calls": n_sent,
            # 429 is the shaper's throttle status (throttled / ingress_throttled);
            # 503 (backend error) and 504 (timeout / queue expiry) are not throttles.
            "real_thr": sum(1 for r in shaper_results if r["status"] == 429),
            "real_codes": ["429"],
            "sim_thr": 0,
            "in": itok,
            "out": otok,
            "peak_tpm": shaper_peak_tpm,
            "unknown_attempts": 0,
            # The shaper arm raised before returning: its rows are missing, not unsent.
            "error": _run_log["errors"][0] if aborted and not shaper_results else None,
        }
    }
    for label, _ in arms:
        out = arm_out[label]
        elapsed_s = max(out["end"] - out["start"], 1e-6) if out.get("end") else submit_window_s
        rows[label] = {
            **_aggregate_direct_results(out.get("results", []), offered, elapsed_s),
            "error": out.get("error"),
        }

    # Failures that are not quota outcomes make the arms incomparable.
    failures = shaper_state.get("submit_failures") or {}
    if failures:
        _warn(
            f"comparison invalid: {sum(failures.values())}/{shaper_state.get('sent', 0)} shaper "
            "POSTs failed at ingress ("
            + ", ".join(f"{k} x{n}" for k, n in sorted(failures.items()))
            + ")"
        )
    for label, _ in arms:
        if rows[label]["error_codes"]:
            _warn(
                f"comparison invalid: {label} got non-throttle Bedrock errors ("
                + ", ".join(f"{k} x{n}" for k, n in sorted(rows[label]["error_codes"].items()))
                + ")"
            )

    per_arm = {"shaper": shaper_results}
    per_arm.update({label: arm_out[label].get("results", []) for label, _ in arms})
    if meta:
        summary = _build_summary(
            profile,
            per_arm,
            rows,
            {
                **meta,
                "run_error": "; ".join(_run_log["errors"]) or None,
                "warnings": list(_run_log["warnings"]),
                "stopped": _STOP.is_set() and not aborted,
            },
        )

    if summary:
        print()
        hdr = (
            f"{'arm':13s}{'offered':>8s}{'served':>7s}{'failed':>7s}{'unres':>6s}{'cancel':>7s}"
            f"{'unsent':>7s}{'attempts':>9s}{'calls':>7s}{'sim_thr':>8s}{'total_tok':>10s}"
            f"{'peak_tpm':>10s}{'total_s':>8s}{'avg_s':>7s}{'p50_s':>7s}{'p95_s':>7s}"
        )
        print(hdr)
        print("-" * len(hdr))
        for arm, a in summary["arms"].items():
            lat = a["latency_served"]

            def sec(v):
                return _col(None if v is None else v / 1000, 7, ".1f")

            print(
                f"{arm:13s}{a['offered']:>8d}{a['served']:>7d}{a['failed']:>7d}"
                f"{a['unresolved']:>6d}{a['cancelled']:>7d}{a['unsent']:>7d}"
                + _col(a["attempts"], 9)
                + _col(a["bedrock_calls"], 7)
                + _col(a["sim_throttles"], 8)
                + _col(a["tokens"], 10, ".0f")
                + _col(a["peak_tpm"], 10, ".0f")
                + _col(a["total_s"], 8, ".1f")
                + f"{sec(lat['avg'])}{sec(lat['p50'])}{sec(lat['p95'])}"
            )
        print(
            "total_s = first send to last request served or failed; avg/p50/p95 = end-to-end "
            "latency of SERVED requests from their scheduled send."
        )

    for arm, r in rows.items():
        if r["unknown_attempts"]:
            print(
                f"NOTE: {arm} had {r['unknown_attempts']} request(s) unresolved at the "
                f"{DEMO_DIRECT_TIMEOUT_S}s bound, excluded from attempts/calls/sim_thr."
            )
    throttled = [
        f"{arm}={r['real_thr']} ({', '.join(r['real_codes'])})"
        for arm, r in rows.items()
        if r["real_thr"]
    ]
    if throttled:
        print(f"⚠ REAL throttles occurred (not simulated): {'; '.join(throttled)}")

    if summary:
        path = _write_summary(summary, args.events)
        emit("summary", summary=summary, path=os.path.basename(path))
        print(f"\nSummary written to {path}")
    if _STOP.is_set() and not aborted:
        emit("stopped")
    if _run_log["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
