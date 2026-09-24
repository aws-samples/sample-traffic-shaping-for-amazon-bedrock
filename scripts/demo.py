#!/usr/bin/env python3
"""
Demo harness: re-derive a model's whole config at a 100,000 TPM ceiling
(via create_model_config.calculate_config(), so the queue-capacity and
regeneration-rate fields the queue processor actually gates on move with the
ceiling -- setting tpm_limit alone is inert), fire enough
SigV4-signed ~2k-input/~1.2k-output-token requests through the shaper's
/invoke ingress to overshoot that ceiling over a 20-30s window, wait for the
queue to drain, report the real CloudWatch-measured token throughput, then
restore the original config item -- even on error.

The request profile is deliberately NOT predicted or tuned: flat constants
(DEMO_BYTES_PER_TOKEN, DEMO_MAX_OUTPUT_TOKENS, DEMO_REQUEST_COUNT), enough
offered load that the queue cannot starve, and whatever throughput that
actually produces is the measurement. The demo does not forecast the drain.

The same offered load is sent simultaneously straight to bedrock-runtime,
bypassing the shaper. That direct arm is held to the SAME 100,000-token
ceiling by a client-side SIMULATED quota (see _virtual_budget_admit) and
rejects whatever does not fit, so the shaper's protection is observable.
Those rejections are simulated and labelled as such everywhere they appear;
Bedrock itself does not throttle at this volume.

Usage:
    python scripts/demo.py [MODEL]

    MODEL defaults to nova-2-lite.
"""

import concurrent.futures as cf
import json
import os
import random
import sys
import threading
import time
import uuid
from statistics import median

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# Same lambda-layer sys.path insert 8+ scripts already use (see
# scripts/test_budget_manager.py:19). The direct arm's virtual quota is charged
# with the shaper's OWN accounting function -- never a local copy of the
# formula, so the two arms cannot be judged by different arithmetic.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                '..', 'infrastructure', 'lambda_layer', 'python'))
import boto3
from botocore.config import Config
import config_loader
from create_model_config import MODEL_MAP, calculate_config, create_model_config
from smoke_honest_outcomes import _load_api_url, _signed_request, _poll_result
from burst_benchmark import shaper_tokens
from shared_service import estimate_request_tokens

DEMO_TPM_OVERRIDE = 100_000

# Rough chars-per-token heuristic (English text averages ~4 chars/token);
# exact tokenization isn't required, just a prompt big enough to approach
# the target input-token size.
DEMO_CHARS_PER_TOKEN_ESTIMATE = 4
DEMO_TARGET_INPUT_TOKENS = 2000

# FLAT, deliberately. Bead a7q.9 derived these three from a hardcoded per-prompt
# tokenizer measurement so the charge landed on exactly one of the queue
# processor's 2-second token slots; that whole chain is gone (bead a7q.10). The
# demo sets the config, offers more load than the ceiling can drain, and reports
# what came out -- it does not predict the drain and nothing here is tuned
# toward a target number.
#
# bytes_per_token = 5 is a one-reading owner choice, written into the demo's own
# override item only (see _build_ceiling_override_item) and never into
# create_model_config's per-model defaults. The live Nova config carries 3.0,
# which charges this prompt roughly 1.9x what it really costs; whether that
# generalises beyond this one repetitive filler prompt is a separate
# investigation and explicitly not settled here.
#
# DEMO_REQUEST_COUNT only has to be comfortably more load than the ceiling can
# drain so the queue never starves. It is not computed from the slot size or the
# ceiling and does not need to be precise.
DEMO_BYTES_PER_TOKEN = 5
DEMO_MAX_OUTPUT_TOKENS = 1000

# The offered-load TIMELINE every arm is paced by: an ordered list of
# (duration_s, request_count) phases, played back to back. Within a phase the
# requests are spread evenly across its duration; a phase with request_count 0
# is an idle gap that still advances the clock. The default is the single
# phase "70 requests over 25s" -- the middle of the 20-30s window the epic
# asks for. A multi-phase profile, e.g. [(60, 35), (120, 210), (30, 9), (60, 0)],
# is expressible here, but this script's timeouts are still sized for the
# default single ~25s burst -- widen them before running a long profile.
DEMO_LOAD_PROFILE = [(25, 70)]

# Derived from the timeline, never set independently, so the report's
# "offered" count and the submission window can never disagree with what was
# actually paced.
DEMO_REQUEST_COUNT = sum(count for _, count in DEMO_LOAD_PROFILE)
DEMO_SUBMIT_WINDOW_S = sum(duration for duration, _ in DEMO_LOAD_PROFILE)

# queue_processor.py's dispatch pool is a single worker (DISPATCH_POOL_WORKERS
# = 1) -- invokes run serialized on one thread, not in parallel. ~70 requests
# each generating up to ~1,200 output tokens on nova-2-lite could plausibly
# take several seconds apiece serialized, so 120s (right for 5 tiny "Reply
# OK." requests) is not a safe bound here. 600s stays comfortably under the
# processor's own 13-minute per-invocation ceiling while giving real headroom.
#
# Left unchanged across the doubled offered load (bead 5) and the return to flat
# constants (bead a7q.10). 70 requests against a 100,000-TPM ceiling is ~2x the
# ceiling's worth of offered load, so the drain is expected to run for minutes
# rather than seconds, and 600s stays comfortably under the processor's own
# 13-minute per-invocation ceiling -- raising the bound would only push it past
# that point, where the timeout would stop being the thing that fires first.
# How long the drain ACTUALLY takes is one of the things this demo measures; it
# is not forecast here.
DEMO_DRAIN_TIMEOUT_S = 600

# EMF metrics can lag after the underlying requests finish; give them a
# bounded window to land before reporting rather than printing a false zero.
DEMO_METRICS_WAIT_S = 120
DEMO_METRICS_POLL_INTERVAL_S = 15

# Direct-Bedrock arm (bead 4): same request count and submission window as
# the shaper arm above, its own bounded worker pool and wait timeout. Bedrock's
# real account-level quota for this model is 8,000,000 TPM -- two orders of
# magnitude above what this demo offers -- so the real service never throttles
# here and the unshaped arm's cost of overshooting stays invisible. Bead 5
# therefore holds this arm to the SAME DEMO_TPM_OVERRIDE ceiling with a
# client-side SIMULATED quota (see _virtual_budget_admit).
DEMO_DIRECT_MAX_WORKERS = 40

# Re-checked for bead 6's retry+jitter arm, which now shares this same bound
# (both direct arms run through _run_direct_arm's one cf.wait call). Measured
# raw converse() latency for this model/profile (beads 4-5, real data): p50
# ~5.5s. The retry arm's added backoff is bounded at min(30, 1.0*2^attempt)
# summed over the 3 possible retries = 1+2+4 = 7s worst case (bead's own
# arithmetic). Giving latency 3x headroom over the measured p50 for a worst-
# case slow call (~16.5s) and 4 attempts (1 initial + 3 retries) per request:
# 4 * 16.5s + 7s backoff = ~73s worst case for one request to resolve. With
# DEMO_DIRECT_MAX_WORKERS=40 against DEMO_REQUEST_COUNT (~70) requests paced
# over DEMO_SUBMIT_WINDOW_S=25s, the pool can need up to 2 waves (70/40
# rounds up to 2) if every request needed its full worst-case retry run:
# ~25s submission spread + ~73s wave 1 + ~73s wave 2 = ~171s. 180s left almost
# no margin against that pathological case, so this bound is raised to 240s
# (still well under DEMO_DRAIN_TIMEOUT_S=600s and the direct_join_timeout
# margin added below).
DEMO_DIRECT_TIMEOUT_S = 240
DEMO_DIRECT_THROTTLE_CODES = {"ThrottlingException", "TooManyRequestsException",
                              "ServiceQuotaExceededException"}

# Retry+jitter policy for the direct+retry arm (bead 6), lifted verbatim from
# scripts/test_direct_bedrock_retry.py:46-49 -- do not invent a new policy
# here, this IS that policy, applied inside the same client-side simulated
# gate the no-retry direct arm uses.
DEMO_RETRY_MAX_RETRIES = 3
DEMO_RETRY_BASE_DELAY_S = 1.0
DEMO_RETRY_MAX_DELAY_S = 30.0

# Bedrock's quota buckets are per-MINUTE, so the simulated quota uses an
# absolute rolling 60-second window -- deliberately NOT the 25s submission
# window and NOT a pro-rated rate limit, either of which would shed a different
# amount than the real service would.
DEMO_VIRTUAL_WINDOW_S = 60


def _build_filler_prompt():
    """Fixed filler prompt sized to ~DEMO_TARGET_INPUT_TOKENS tokens, plus an
    explicit long-response instruction so max_tokens has a real chance of
    being approached."""
    # No apostrophes: the /invoke API Gateway integration maps this body into
    # a Step Functions StartExecution call via $util.escapeJavaScript(), which
    # escapes ' as the invalid JSON sequence \' and gets rejected upstream
    # with a SerializationException -- a pre-existing infra quirk, worked
    # around here rather than fixed (out of this bead's scope).
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


# REPORTED, not tuned: what the shaper's own estimator makes of this prompt under
# the flat constants above, so the run can print the charge each request will
# actually incur (budget_manager reads bytes_per_token off the CONFIG item:
# budget_manager.py:278). Nothing is sized from this -- neither the output cap nor
# the request count -- it is the charge the config produces, printed so the run
# says what it is doing. Both the shaper's admission path and the direct arms'
# virtual quota charge this same number, so the "one budget, one accounting
# method" claim in the final report holds literally. burndown_rate is left at the
# default 1.0 here (it is 1.0 for every non-Claude-3.7+ model, including the
# nova-2-lite default); the direct arm's gate re-reads the live per-model rate off
# the CONFIG item.
DEMO_TOKENS_PER_REQUEST_ESTIMATE = estimate_request_tokens(
    prompt=_build_filler_prompt(), max_tokens=DEMO_MAX_OUTPUT_TOKENS,
    bytes_per_token=DEMO_BYTES_PER_TOKEN)


def _new_virtual_budget(burndown_rate):
    """Shared state for the direct arm's client-side SIMULATED quota: the same
    DEMO_TPM_OVERRIDE ceiling the shaper's tpm_limit is overridden to, a list of
    timestamped token charges, and a lock (the arm's converse() calls run across
    a thread pool). A plain dict, deliberately -- no class, matching this
    script's existing style.

    bpt carries DEMO_BYTES_PER_TOKEN, the same flat ratio written into the
    shaper's override item, because the gate below must charge per request what
    the shaper charges. Before bead a7q.9 this was left unset, so the gate fell
    through to the library's 4.0 default (3,275 tokens) while the live config's
    3.0 made the shaper charge 4,033 for the identical request -- the two arms
    were being judged by different arithmetic, which is exactly what the final
    report's "one budget under one accounting method" line says cannot happen."""
    return {"limit": DEMO_TPM_OVERRIDE, "burndown": burndown_rate,
            "bpt": DEMO_BYTES_PER_TOKEN,
            "charges": [], "lock": threading.Lock()}


def _virtual_budget_admit(budget, prompt, max_tokens):
    """Admission half of the simulated quota. Charges the estimate UP FRONT,
    mirroring what estimate_request_tokens()'s docstring describes Bedrock as
    doing: "At request start, Bedrock deducts: input_tokens + (max_tokens *
    burndown_rate)".

    Over-budget requests are REJECTED, never delayed, queued, retried or paced --
    a gate that waited for budget would turn this arm into a second shaper and
    erase the contrast the demo exists to show. Failing fast is also what the
    real service does.

    Returns (charge, est, used): charge is None when rejected, otherwise the
    mutable charge record to hand to _virtual_budget_reconcile()."""
    est = estimate_request_tokens(prompt=prompt, max_tokens=max_tokens,
                                  burndown_rate=budget["burndown"],
                                  bytes_per_token=budget["bpt"])
    now = time.time()
    with budget["lock"]:
        cutoff = now - DEMO_VIRTUAL_WINDOW_S
        budget["charges"] = [c for c in budget["charges"] if c["ts"] > cutoff]
        used = sum(c["tokens"] for c in budget["charges"])
        if used + est > budget["limit"]:
            return None, est, used
        charge = {"ts": now, "tokens": est}
        budget["charges"].append(charge)
        return charge, est, used


def _virtual_budget_reconcile(budget, charge, actual_tokens):
    """Reconciliation half, mirroring bedrock_processor.reconcile_consumption():
    overwrite the over-counted admission estimate with the real usage Bedrock
    returned, so the next window read sees actual consumption.

    Without this the direct arm would keep its admission charge for the whole
    60s window even after the response came back with real usage -- and any
    overcharge that outlives the call rigs the comparison in the shaper's
    favour, since the shaper reconciles and the direct arm would not.
    Only successes reconcile; a failed call returns no usage, so its pre-charge
    stands (and ages out of the window on its own)."""
    with budget["lock"]:
        charge["tokens"] = actual_tokens


def _schedule_offsets(profile):
    """Pure half of the pacing: turns a load timeline -- (duration_s,
    request_count) phases, see DEMO_LOAD_PROFILE -- into each request's
    scheduled offset in seconds from the run's start, in submission order.

    Within a phase the requests are spread evenly, the first landing on the
    phase's start; a zero-count phase emits nothing but still advances the
    clock. Kept free of sleeping and wall-clock reads so the schedule can be
    tested, and reused offline, without running anything."""
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
    """Yields 1..N, sleeping until each index's scheduled slot on the load
    timeline `profile` (see _schedule_offsets), measured from `start`.

    Single-sourced deliberately: both arms must be offered the SAME submission
    profile for the comparison to mean anything, and this arithmetic previously
    existed as two hand-maintained copies (one per arm) that could drift apart
    and silently rig the result. A generator rather than a callback so each
    caller keeps its own per-iteration work inline and readable."""
    for i, offset in enumerate(_schedule_offsets(profile), start=1):
        target = start + offset
        now = time.time()
        if target > now:
            time.sleep(target - now)
        yield i


def _submit_sized(api_url, arm, model_id, prompt, max_tokens):
    """Same signed-POST /invoke submission as bead 2's _submit
    (smoke_honest_outcomes._submit), with max_tokens parameterized so it
    isn't stuck at that helper's hardcoded 64."""
    correlation_id = str(uuid.uuid4())
    request_id = str(uuid.uuid4())
    body = {"request_id": request_id, "model_id": model_id, "prompt": prompt,
            "correlation_id": correlation_id, "max_tokens": max_tokens}
    status, text = _signed_request("POST", f"{api_url}/invoke", body)
    print(f"  [{arm}] POST /invoke -> {status}")
    try:
        parsed = json.loads(text)
    except ValueError:
        parsed = {}
    rid = parsed.get("request_id", request_id)
    if status not in (200, 202):
        print(f"    unexpected submit status: {status} {text[:200]}")
    return {"arm": arm, "correlation_id": correlation_id, "request_id": rid,
            "submit_status": status}


def _direct_call(brt, idx, model_id, prompt, max_tokens, budget, arm):
    """One synchronous bedrock-runtime converse() call for a direct-Bedrock
    arm (no retry), gated by the client-side SIMULATED quota; usage tokens
    come straight from the response, no CloudWatch needed. `arm` is just the
    print-prefix label ("direct") -- parameterized so _run_direct_arm can
    drive either direct arm through the same code path.

    A request that does not fit the virtual budget is rejected BEFORE converse()
    is called, so it reaches Bedrock never and costs nothing.

    Returns attempts=1 and bedrock_calls=1 iff converse() was actually reached,
    so this arm's result dicts share the exact shape _direct_call_with_retry's
    do and can be aggregated by the same helper (_aggregate_direct_results).
    That shared shape is why the per-attempt sim_throttle_attempts/
    real_throttle_codes fields (bead 8yk) are returned here too even though this
    arm never retries, so one attempt is always the whole story -- see
    _direct_call_with_retry's docstring for what those fields are for.

    Every returned dict also carries "completed_ts" (bead a7q.7): an absolute
    time.time() taken at the moment this request reached a terminal outcome,
    used to bucket real token consumption into the peak rolling-60s-window
    calculation in _aggregate_direct_results -- the result dicts previously
    carried only a duration ("ms"), which cannot place a request on an
    absolute timeline the way a completion timestamp can."""
    charge, est, used = _virtual_budget_admit(budget, prompt, max_tokens)
    if charge is None:
        # Same code and same membership test the real-error path below uses, so
        # the rejection is counted by the existing throttle path unchanged.
        code = "ThrottlingException"
        throttled = code in DEMO_DIRECT_THROTTLE_CODES
        print(f"  [{arm}] req{idx} SIMULATED {code} -- rejected BEFORE converse(), no Bedrock "
              f"call made (client-side virtual quota: {used} used + {est} est > "
              f"{budget['limit']} per {DEMO_VIRTUAL_WINDOW_S}s)")
        return {"ok": False, "simulated_throttle": throttled, "real_throttle_code": None,
                "error": None, "in": 0, "out": 0, "ms": None, "attempts": 1, "bedrock_calls": 0,
                "sim_throttle_attempts": 1 if throttled else 0, "real_throttle_codes": [],
                "completed_ts": time.time()}

    t0 = time.time()
    try:
        r = brt.converse(
            modelId=model_id,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": max_tokens},
        )
        completed_ts = time.time()
        dt_ms = (completed_ts - t0) * 1000
        usage = r.get("usage", {})
        in_tok = usage.get("inputTokens", 0)
        out_tok = usage.get("outputTokens", 0)
        _virtual_budget_reconcile(budget, charge, in_tok + out_tok)
        print(f"  [{arm}] req{idx} converse() -> ok in={in_tok} out={out_tok} ({dt_ms:.0f}ms) "
              f"[virtual charge reconciled {est} est -> {in_tok + out_tok} actual]")
        return {"ok": True, "simulated_throttle": False, "real_throttle_code": None,
                "error": None, "in": in_tok, "out": out_tok, "ms": dt_ms,
                "attempts": 1, "bedrock_calls": 1, "sim_throttle_attempts": 0,
                "real_throttle_codes": [], "completed_ts": completed_ts}
    except Exception as e:
        completed_ts = time.time()
        dt_ms = (completed_ts - t0) * 1000
        resp = getattr(e, "response", None)
        code = resp.get("Error", {}).get("Code", "") if isinstance(resp, dict) else ""
        if not code:
            code = type(e).__name__
        # A genuine throttle from the service is an ANOMALY here, not a statistic:
        # it stays out of the simulated_throttle column and instead trips the
        # real-throttle warning line, on top of counting as an error.
        real_throttle = code in DEMO_DIRECT_THROTTLE_CODES
        print(f"  [{arm}] req{idx} converse() -> "
              f"{'REAL THROTTLE FROM BEDROCK' if real_throttle else 'error'} "
              f"{code} ({dt_ms:.0f}ms)")
        return {"ok": False, "simulated_throttle": False,
                "real_throttle_code": code if real_throttle else None,
                "error": code[:60], "in": 0, "out": 0, "ms": dt_ms,
                "attempts": 1, "bedrock_calls": 1, "sim_throttle_attempts": 0,
                "real_throttle_codes": [code] if real_throttle else [],
                "completed_ts": completed_ts}


def _direct_call_with_retry(brt, idx, model_id, prompt, max_tokens, budget, arm):
    """The direct+retry arm's per-request call (bead 6): identical converse()
    call and identical client-side SIMULATED gate as _direct_call, but wrapped
    in the exponential-backoff-with-full-jitter retry loop lifted from
    scripts/test_direct_bedrock_retry.py:136-208 (that file is read-only --
    this is the same arithmetic, not a new policy).

    RETRIES on a simulated-quota rejection (charge is None) and on a real
    throttle code from converse(); each retry RE-ENTERS
    _virtual_budget_admit() -- that re-entry is the whole point, a retry that
    skipped the gate would prove nothing about amplification against a shared
    budget. Non-throttle errors are NOT retried (break out immediately, same
    as the lifted code). Same signature as _direct_call so _run_direct_arm can
    submit either call through one code path.

    Returns attempts (every try, including the initial one) and bedrock_calls
    (the subset of those attempts that actually reached converse() -- gate
    rejections never do). Recorded `ms` is total wall clock from the first
    attempt to the terminal outcome, INCLUDING time spent sleeping in
    backoff -- that latency cost is part of what this arm exists to show.

    sim_throttle_attempts/real_throttle_codes (bead 8yk) record throttles
    PER ATTEMPT, so a rejection that was retried is still counted. The terminal
    `simulated_throttle`/`real_throttle_code` fields cannot serve that purpose:
    they describe only how the request ENDED, so on this arm they see one
    throttle per give-up and none at all for a request that was throttled
    several times and then succeeded -- which undercounted the retry arm's
    rejections roughly 4x and broke the table's
    attempts == bedrock_calls + sim_thr invariant.

    Also carries "completed_ts" (bead a7q.7), an absolute time.time() at the
    terminal outcome -- see _direct_call's docstring for why."""
    t0 = time.time()
    attempts = 0
    bedrock_calls = 0
    sim_throttle_attempts = 0
    real_throttle_codes = []
    for attempt in range(DEMO_RETRY_MAX_RETRIES + 1):  # +1 for the initial try
        attempts += 1
        charge, est, used = _virtual_budget_admit(budget, prompt, max_tokens)
        if charge is None:
            # Rejected before converse() is ever called -- a SIMULATED
            # throttle, same code/membership test as the no-retry arm.
            code = "ThrottlingException"
            throttled = code in DEMO_DIRECT_THROTTLE_CODES
            # Counted HERE, before the retryable branch below, so the rejection is
            # recorded whether this attempt goes on to retry or gives up.
            if throttled:
                sim_throttle_attempts += 1
            retryable = attempt < DEMO_RETRY_MAX_RETRIES
            print(f"  [{arm}] req{idx} attempt{attempts} SIMULATED {code} -- rejected BEFORE "
                  f"converse(), no Bedrock call made (client-side virtual quota: {used} used + "
                  f"{est} est > {budget['limit']} per {DEMO_VIRTUAL_WINDOW_S}s)"
                  f"{' -- retrying' if retryable else ' -- retries exhausted'}")
            if retryable:
                computed_delay = min(DEMO_RETRY_MAX_DELAY_S,
                                      DEMO_RETRY_BASE_DELAY_S * (2 ** attempt))
                actual_delay = random.uniform(0, computed_delay)  # nosec B311  # non-crypto: backoff jitter
                time.sleep(actual_delay)  # nosemgrep: arbitrary-sleep -- deliberate exponential backoff on simulated throttle
                continue
            completed_ts = time.time()
            dt_ms = (completed_ts - t0) * 1000
            return {"ok": False, "simulated_throttle": throttled, "real_throttle_code": None,
                    "error": None, "in": 0, "out": 0, "ms": dt_ms,
                    "attempts": attempts, "bedrock_calls": bedrock_calls,
                    "sim_throttle_attempts": sim_throttle_attempts,
                    "real_throttle_codes": real_throttle_codes,
                    "completed_ts": completed_ts}

        bedrock_calls += 1
        try:
            r = brt.converse(
                modelId=model_id,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": max_tokens},
            )
            completed_ts = time.time()
            dt_ms = (completed_ts - t0) * 1000
            usage = r.get("usage", {})
            in_tok = usage.get("inputTokens", 0)
            out_tok = usage.get("outputTokens", 0)
            _virtual_budget_reconcile(budget, charge, in_tok + out_tok)
            print(f"  [{arm}] req{idx} attempt{attempts} converse() -> ok in={in_tok} "
                  f"out={out_tok} ({dt_ms:.0f}ms) [virtual charge reconciled {est} est -> "
                  f"{in_tok + out_tok} actual]")
            return {"ok": True, "simulated_throttle": False, "real_throttle_code": None,
                    "error": None, "in": in_tok, "out": out_tok, "ms": dt_ms,
                    "attempts": attempts, "bedrock_calls": bedrock_calls,
                    "sim_throttle_attempts": sim_throttle_attempts,
                    "real_throttle_codes": real_throttle_codes,
                    "completed_ts": completed_ts}
        except Exception as e:
            completed_ts = time.time()
            dt_ms = (completed_ts - t0) * 1000
            resp = getattr(e, "response", None)
            code = resp.get("Error", {}).get("Code", "") if isinstance(resp, dict) else ""
            if not code:
                code = type(e).__name__
            real_throttle = code in DEMO_DIRECT_THROTTLE_CODES
            # Same reason as the simulated counter above: record the throttle on the
            # attempt that saw it, so a retried-then-succeeded throttle stays visible.
            # Recorded as the code itself rather than a bare tally so the warning
            # line's real_thr count and its code(s) list come from ONE field and
            # cannot disagree about a throttle that was retried away.
            if real_throttle:
                real_throttle_codes.append(code)
            retryable = real_throttle and attempt < DEMO_RETRY_MAX_RETRIES
            print(f"  [{arm}] req{idx} attempt{attempts} converse() -> "
                  f"{'REAL THROTTLE FROM BEDROCK' if real_throttle else 'error'} "
                  f"{code} ({dt_ms:.0f}ms){' -- retrying' if retryable else ''}")
            if retryable:
                computed_delay = min(DEMO_RETRY_MAX_DELAY_S,
                                      DEMO_RETRY_BASE_DELAY_S * (2 ** attempt))
                actual_delay = random.uniform(0, computed_delay)  # nosec B311  # non-crypto: backoff jitter
                time.sleep(actual_delay)  # nosemgrep: arbitrary-sleep -- deliberate exponential backoff on Bedrock throttle
                continue
            # Non-throttle error, or throttle with retries exhausted: break out
            # (mirrors the lifted code -- non-throttle errors are never retried).
            completed_ts = time.time()
            dt_ms = (completed_ts - t0) * 1000
            return {"ok": False, "simulated_throttle": False,
                    "real_throttle_code": code if real_throttle else None,
                    "error": code[:60], "in": 0, "out": 0, "ms": dt_ms,
                    "attempts": attempts, "bedrock_calls": bedrock_calls,
                    "sim_throttle_attempts": sim_throttle_attempts,
                    "real_throttle_codes": real_throttle_codes,
                    "completed_ts": completed_ts}


def _run_direct_arm(region, model_id, prompt, max_tokens, profile, budget, out, call_fn, arm):
    """Runs in a background thread so its request window overlaps the other
    arms' own submission+drain in real wall-clock time. Writes results into
    `out` (a plain dict) since a Thread's return value isn't otherwise
    retrievable. Bounded by client-level timeouts plus DEMO_DIRECT_TIMEOUT_S --
    no infinite loops.

    `call_fn` (either _direct_call or _direct_call_with_retry) and `arm` (the
    print-prefix label, "direct" or "direct+retry") are parameterized so this
    one function drives both direct arms -- one function, two callers,
    instead of a second copy-pasted arm runner."""
    client_cfg = Config(retries={"total_max_attempts": 1, "mode": "standard"},
                         read_timeout=120, connect_timeout=10)
    brt = boto3.client("bedrock-runtime", region_name=region, config=client_cfg)
    n = sum(count for _, count in profile)
    start = time.time()
    with cf.ThreadPoolExecutor(max_workers=DEMO_DIRECT_MAX_WORKERS) as ex:
        futs = []
        for i in _paced_indices(start, profile):
            futs.append(ex.submit(call_fn, brt, i, model_id, prompt, max_tokens, budget, arm))
        submit_end = time.time()
        print(f"  [{arm}] submitted {n} requests in {submit_end - start:.1f}s")
        done, not_done = cf.wait(futs, timeout=DEMO_DIRECT_TIMEOUT_S)
    results = [f.result() for f in done]
    for _ in not_done:
        # A timeout is a genuine failure, not a simulated throttle -- it stays in
        # the error bucket. Its true attempt count is unknowable (the thread may
        # still be mid-backoff), so attempts is None rather than a guess; the
        # aggregation helper excludes None from the attempts/bedrock_calls totals
        # and reports how many were excluded, rather than silently undercounting.
        # Its throttle counts are unknowable for the same reason, so they are None
        # (not 0) and are excluded by the same attempts-is-None test -- a 0 here
        # would be a guess that reads as "this request was never throttled".
        results.append({"ok": False, "simulated_throttle": False,
                         "real_throttle_code": None, "error": "timeout",
                         "in": 0, "out": 0, "ms": None, "attempts": None, "bedrock_calls": None,
                         "sim_throttle_attempts": None, "real_throttle_codes": None,
                         "completed_ts": None})
    end = time.time()
    if not_done:
        print(f"  [{arm}] wait bound hit -- {len(done)}/{n} complete, {len(not_done)} still "
              "pending (reporting partial results honestly)")
    else:
        print(f"  [{arm}] all {n} requests complete")
    out["results"] = results
    out["start"] = start
    out["end"] = end


def _peak_window_tpm(events, window_s=DEMO_VIRTUAL_WINDOW_S):
    """Bead a7q.7: the direct arms' half of "achieved TPM measured as peak
    rolling 60-second token consumption" -- a TRUE sliding window over real
    per-request completion timestamps, not an approximation.

    `events` is a list of (completed_ts, tokens) pairs, one per successful
    request. For a set of discrete point events, the sum inside a trailing
    window [t - window_s, t] only changes at event boundaries, so its maximum
    over all real-valued t is achieved at some window whose right edge sits
    exactly on an event's timestamp -- checking a window ending at every event
    (not e.g. dividing total tokens by total elapsed time, and not reusing
    budget["charges"], which _virtual_budget_admit already prunes to the last
    60s and would have lost most of this history by run end) finds the true
    peak with no approximation. Sorted-and-two-pointer, so this is O(n log n)
    rather than the O(n^2) a naive per-event rescan would be."""
    if not events:
        return 0.0
    ts_sorted = sorted(events, key=lambda e: e[0])
    peak = 0.0
    running = 0.0
    left = 0
    for t, tok in ts_sorted:
        running += tok
        while ts_sorted[left][0] <= t - window_s:
            running -= ts_sorted[left][1]
            left += 1
        peak = max(peak, running)
    return peak


def _aggregate_direct_results(results, offered, elapsed_min):
    """Turns one direct arm's list of per-request result dicts (from either
    _direct_call or _direct_call_with_retry -- both share the same dict shape)
    into the row of stats the final table prints. Shared by both direct arms
    (bead 6) so their arithmetic can never drift apart into two hand-
    maintained copies -- the same reason _paced_indices above is single-
    sourced.

    attempts/bedrock_calls/sim_thr/real_thr: results from requests that hit the
    DEMO_DIRECT_TIMEOUT_S wait bound carry attempts=None (their true attempt
    count is unknowable, not zero -- see _run_direct_arm), so they are
    excluded from all four sums rather than guessed at; `unknown_attempts`
    reports how many were excluded so that exclusion is never silent.

    sim_thr/real_thr (bead 8yk) are ATTEMPT counts, not request counts, which is
    what makes the table's rows internally consistent: whenever
    unknown_attempts == 0, attempts == bedrock_calls + sim_thr holds, because
    every attempt either got past the simulated gate and reached converse() or
    was rejected by it. Deriving them from the terminal `simulated_throttle` /
    `real_throttle_code` fields instead (as this helper originally did) silently
    dropped every retried rejection on the direct+retry arm and broke that
    identity -- a run of 134 attempts against 37 bedrock_calls reported 24
    simulated rejections instead of the 97 that actually happened.

    peak_tpm (bead a7q.7): the arm's achieved rate against the ceiling, measured
    as the peak of a TRUE sliding 60-second window over each successful
    request's real completion timestamp (see _peak_window_tpm) -- NOT `tpm`
    below, which is total tokens divided by this arm's own wall-clock elapsed
    time and can read as a ceiling violation for an arm that was actually
    compliant every second of the run (the whole reason this bead exists)."""
    known = [r for r in results if r.get("attempts") is not None]
    succ = sum(1 for r in results if r["ok"])
    # sim_thr/real_thr are PER-ATTEMPT sums over `known` (bead 8yk), not counts of
    # requests carrying a terminal throttle flag, and not sums over `results`:
    # timeout requests carry None for these, same as attempts, and are excluded
    # from all three sums together so the invariant below stays coherent.
    sim_thr = sum(r.get("sim_throttle_attempts", 0) for r in known)
    err = sum(1 for r in results if not r["ok"] and not r.get("simulated_throttle"))
    in_tok = sum(r.get("in", 0) for r in results)
    out_tok = sum(r.get("out", 0) for r in results)
    tpm = (in_tok + out_tok) / elapsed_min if elapsed_min > 0 else 0.0
    peak_events = [(r["completed_ts"], r["in"] + r["out"])
                   for r in results if r["ok"] and r.get("completed_ts") is not None]
    peak_tpm = _peak_window_tpm(peak_events)
    lat = [r["ms"] for r in results if r.get("ms") is not None]
    p50 = median(lat) if lat else 0.0
    real_codes = sorted({c for r in known for c in (r.get("real_throttle_codes") or [])})
    real_thr = sum(len(r.get("real_throttle_codes") or []) for r in known)
    attempts = sum(r["attempts"] for r in known)
    bedrock_calls = sum(r.get("bedrock_calls", 0) for r in known)
    unknown_attempts = len(results) - len(known)
    ratio = attempts / offered if offered else 0.0
    return {"succ": succ, "err": err, "sim_thr": sim_thr, "in": in_tok, "out": out_tok,
            "tpm": tpm, "peak_tpm": peak_tpm, "p50": p50, "real_codes": real_codes,
            "real_thr": real_thr, "attempts": attempts, "bedrock_calls": bedrock_calls,
            "ratio": ratio, "unknown_attempts": unknown_attempts}


def _shaper_peak_tpm_60s(region, model_id, start_epoch, end_epoch):
    """Bead a7q.7: the shaper arm's half of "achieved TPM measured as peak
    rolling 60-second token consumption". Modelled on
    burst_benchmark.shaper_tokens()'s query shape (same namespace, same
    ServiceName/model_id dimensions, same Stat: Sum) -- but that function is
    READ-ONLY here and hardcodes Period: 300 (5-minute buckets) summed
    together, which cannot yield the per-minute resolution this needs. This is
    a NEW, separate helper local to this script; shaper_tokens() itself is
    left untouched and is still used for the totals reported elsewhere.

    IMPORTANT ASYMMETRY WITH THE DIRECT ARMS: CloudWatch's Period: 60 buckets
    are wall-clock-aligned (e.g. 12:00:00-12:00:59), NOT aligned to this run's
    t0 and NOT a true sliding window the way _peak_window_tpm() is for the
    direct arms above. Taking the maximum wall-clock-aligned bucket is an
    acceptable approximation of the peak minute, but it is NOT computed
    identically to the direct arms' figure -- do not read the two peak_tpm
    numbers as bit-for-bit comparable."""
    cw = boto3.client("cloudwatch", region_name=region)
    dims = [{"Name": "ServiceName", "Value": "TrafficShaper"}, {"Name": "model_id", "Value": model_id}]
    q = []
    for i, metric in enumerate(("InputTokens", "OutputTokens")):
        q.append({"Id": f"m{i}", "MetricStat": {
            "Metric": {"Namespace": "BedrockShaper", "MetricName": metric, "Dimensions": dims},
            "Period": 60, "Stat": "Sum"}, "ReturnData": True})
    r = cw.get_metric_data(MetricDataQueries=q,
                           StartTime=start_epoch - 60, EndTime=end_epoch + 120)
    # Keep each Period:60 datapoint (value AND timestamp) rather than summing
    # them the way shaper_tokens() does -- combine the two metrics (input,
    # output) into one total per wall-clock minute bucket, then take the max.
    buckets = {}
    for res in r["MetricDataResults"]:
        for ts, val in zip(res["Timestamps"], res["Values"]):
            buckets[ts] = buckets.get(ts, 0.0) + val
    return max(buckets.values()) if buckets else 0.0


def _build_ceiling_override_item(original_item, model_id, tpm_override=DEMO_TPM_OVERRIDE):
    """Re-derive the WHOLE config at tpm_override using create_model_config's own functions.

    Setting 'tpm_limit' alone does nothing. tpm_limit is a BUILD-TIME INPUT to
    create_model_config.calculate_config(); no deployed code ever reads it back.
    queue_processor.py gates on the fields calculate_config() DERIVES from it --
    tpm_queue_capacity and tpm_queue_regeneration_rate (queue_processor.py:197-198,
    used by the Gate 2/Gate 4 token windows) -- so a config whose tpm_limit says
    100,000 while its queue capacity still says 6,800,000 is paced at 6,800,000.
    That is exactly what the earlier single-field override produced: a run where
    queue_processor logged 'tpm_2s_cap=226666, tpm_queue_capacity=6800000' and not
    one gate ever fired. intents/spec.md T-08 called this out up front: "Overriding
    TPM alone is insufficient."

    So call the real derivation instead of re-deriving the arithmetic here. The
    capacity SPLIT is read off the live item rather than hardcoded, so the demo
    scales the deployed shape down to tpm_override instead of inventing a shape
    that was never deployed.

    The write itself stays with the caller's own table handle. create_model_config()
    is used only for item ASSEMBLY (dry_run=True): its non-dry-run path builds a
    fresh boto3 resource from module globals that only its own main() refreshes,
    which could target a different region/table than demo.py resolved.

    Merging onto original_item is load-bearing -- calculate_config() does not emit
    api_style, backend, or the adaptive_* fields, and dropping them would break the run.

    bytes_per_token is the ONE field taken from the demo rather than scaled off the
    live item: DEMO_BYTES_PER_TOKEN, a flat 5 (bead a7q.10), against the live item's
    3.0. It is DEMO-ONLY -- it lives in the item this function builds, which main()
    restores wholesale afterwards, and never in create_model_config's per-model
    defaults. It does NOT feed any capacity arithmetic -- calculate_config() passes
    it straight through to the item (create_model_config.py:317) -- so the 0.85
    burst/queue/buffer split and tpm_queue_capacity=85000 are untouched by it.
    """
    live_tpm = int(original_item.get('tpm_limit') or 0)

    def _fraction(field, default):
        # Guarded: a missing/zero live tpm_limit must fall back, not divide by zero.
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
    return {
        **original_item,
        **create_model_config(model_id, derived, dry_run=True),
    }


def main():
    model_short = sys.argv[1] if len(sys.argv) > 1 else 'nova-2-lite'
    model_id = MODEL_MAP.get(model_short, model_short)

    config = config_loader.get_config_with_aws_check()
    region = config.get('AWS_REGION', 'us-east-1')
    table_name = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')

    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    key = {'pk': f'MODEL#{model_id}', 'sk': 'CONFIG'}

    response = table.get_item(Key=key)
    original_item = response.get('Item')
    if original_item is None:
        print(f"No CONFIG item found for {model_id} in {table_name} -- nothing to demo.")
        sys.exit(1)

    print(f"Model: {model_id} (table: {table_name}, region: {region})")
    print(f"Current tpm_limit: {original_item.get('tpm_limit')}")

    # The direct arms' virtual quota charges tokens with the shaper's own
    # estimator, so they need the shaper's own per-model burndown rate -- read off
    # the same CONFIG item ONCE, exactly as scripts/test_budget_manager.py:197
    # does, and passed to BOTH _new_virtual_budget() calls below. Two SEPARATE
    # budget instances (bead 6) built from the same ceiling and the same
    # burndown_rate -- if the two direct arms shared one budget they would
    # contend with each other and the comparison would be meaningless; if they
    # were built from different parameters, the retry arm's extra successes
    # could be an artifact of a looser gate rather than of retrying.
    burndown_rate = float(original_item.get('output_token_burndown_rate', 1.0))
    direct_budget = _new_virtual_budget(burndown_rate)
    retry_budget = _new_virtual_budget(burndown_rate)

    # Populated by the direct-Bedrock arms' background threads; read after the
    # shaper's own try/finally below so a failure or hang on any arm can
    # never block another arm's reporting or the shaper's unconditional ceiling
    # restore (bead 1) -- each arm's error handling is scoped to itself only.
    direct_thread_out = {}
    retry_thread_out = {}
    direct_thread = None
    retry_thread = None
    itok = otok = approx_tpm = 0.0
    shaper_peak_tpm = 0.0
    run_start = run_end = time.time()
    shaper_results = []

    try:
        overridden_item = _build_ceiling_override_item(original_item, model_id)
        table.put_item(Item=overridden_item)
        print(f"Re-derived the whole config at {DEMO_TPM_OVERRIDE} TPM via "
              f"create_model_config.calculate_config() -- the gating fields the queue "
              f"processor actually reads now move with the ceiling:")
        for field in ('tpm_limit', 'tpm_queue_capacity', 'tpm_queue_regeneration_rate',
                      'tpm_burst_capacity', 'tpm_buffer_capacity', 'bytes_per_token'):
            print(f"    {field:<30} {original_item.get(field)} -> {overridden_item.get(field)}")

        # tpm_queue_capacity is the share the queue processor actually paces on, so
        # it is the number the measured drain below should be read against. Printed
        # as the configured share and nothing more -- the demo deliberately does not
        # predict what the drain will come out to (bead a7q.10).
        print(f"    {'configured queue share':<30} {overridden_item['tpm_queue_capacity']} TPM "
              f"-- this is what the queue processor paces on, and what the MEASURED drain "
              f"reported after the run should be read against")
        print(f"Overrode tpm_limit to {DEMO_TPM_OVERRIDE} -- this ceiling override affects ONLY "
              "the shaper's own internal budget accounting below. Bedrock's real account-level "
              f"quota for {model_id} is orders of magnitude higher, so the direct-Bedrock arm "
              f"below is instead held to the SAME {DEMO_TPM_OVERRIDE}-token ceiling by a "
              "client-side SIMULATED quota -- both arms are judged by one budget under one "
              "accounting method.")
        print(f"[direct] SIMULATED quota active: {DEMO_TPM_OVERRIDE} tokens per rolling "
              f"{DEMO_VIRTUAL_WINDOW_S}s, charged with the shaper's own "
              f"estimate_request_tokens() (burndown_rate={burndown_rate}, "
              f"bytes_per_token={DEMO_BYTES_PER_TOKEN} -- the same flat ratio written "
              f"into the shaper's own config item above) and reconciled to each "
              "response's real usage.inputTokens+outputTokens. Over-budget requests are REJECTED "
              "before converse() is called -- never delayed or queued. These rejections are "
              "SIMULATED: Bedrock is not throttling this run.")

        api_url = _load_api_url(None)
        prompt = _build_filler_prompt()
        est_input_tokens = len(prompt) // DEMO_CHARS_PER_TOKEN_ESTIMATE
        print(f"\nFiller prompt: {len(prompt)} chars (~{est_input_tokens} est. input tokens "
              f"at ~{DEMO_CHARS_PER_TOKEN_ESTIMATE} chars/token), max_output_tokens="
              f"{DEMO_MAX_OUTPUT_TOKENS} -- a flat cap, not sized against the queue's pacing. "
              f"Identical prompt/token profile used by both arms.")
        print(f"[shaper] Submitting {DEMO_REQUEST_COUNT} requests to {api_url}/invoke over "
              f"~{DEMO_SUBMIT_WINDOW_S}s ({DEMO_TOKENS_PER_REQUEST_ESTIMATE} tokens charged per "
              f"request by this config, vs a {DEMO_TPM_OVERRIDE} TPM ceiling)...")

        run_start = time.time()

        # Kick off both direct-Bedrock arms in their own background threads right
        # alongside the shaper's own submission loop below, so all three arms'
        # submission and completion windows genuinely overlap in wall-clock time
        # rather than running one after another.
        direct_thread = threading.Thread(
            target=_run_direct_arm,
            args=(region, model_id, prompt, DEMO_MAX_OUTPUT_TOKENS, DEMO_LOAD_PROFILE,
                  direct_budget, direct_thread_out,
                  _direct_call, "direct"),
            daemon=True,
        )
        direct_thread.start()
        print(f"[direct] launched {DEMO_REQUEST_COUNT} direct-Bedrock converse() requests to "
              f"{model_id} over ~{DEMO_SUBMIT_WINDOW_S}s, running simultaneously with the "
              "shaper arm above, under the simulated quota described above.")

        retry_thread = threading.Thread(
            target=_run_direct_arm,
            args=(region, model_id, prompt, DEMO_MAX_OUTPUT_TOKENS, DEMO_LOAD_PROFILE,
                  retry_budget, retry_thread_out,
                  _direct_call_with_retry, "direct+retry"),
            daemon=True,
        )
        retry_thread.start()
        print(f"[direct+retry] launched {DEMO_REQUEST_COUNT} direct-Bedrock converse() requests "
              f"to {model_id} over ~{DEMO_SUBMIT_WINDOW_S}s, running simultaneously with the "
              "shaper and no-retry direct arms above, under its OWN instance of the same "
              f"simulated quota (same {DEMO_TPM_OVERRIDE}-token ceiling, same burndown_rate="
              f"{burndown_rate}), each rejection retried with exponential backoff + full jitter "
              f"(max {DEMO_RETRY_MAX_RETRIES} retries, base {DEMO_RETRY_BASE_DELAY_S}s, cap "
              f"{DEMO_RETRY_MAX_DELAY_S}s) before giving up.")

        submissions = []
        for i in _paced_indices(run_start, DEMO_LOAD_PROFILE):
            submit_ts = time.time()
            sub = _submit_sized(api_url, f"shaper:req{i}", model_id, prompt, DEMO_MAX_OUTPUT_TOKENS)
            sub["submit_ts"] = submit_ts
            submissions.append(sub)
        submit_end = time.time()
        print(f"[shaper] Submitted {len(submissions)} requests in {submit_end - run_start:.1f}s")

        print(f"\n[shaper] Waiting up to {DEMO_DRAIN_TIMEOUT_S}s for the queue to drain "
              "(polling /result for each request)...")
        deadline = time.time() + DEMO_DRAIN_TIMEOUT_S
        completed = 0
        for i, sub in enumerate(submissions, start=1):
            if not sub["request_id"]:
                print(f"  [shaper] Request {i}: no request_id from submit -- cannot poll")
                shaper_results.append({"status": None, "ms": None})
                continue
            remaining = max(0, deadline - time.time())
            status, _ = _poll_result(api_url, sub["request_id"], timeout_s=remaining, interval_s=3)
            terminal = status is not None and status != 202
            if terminal:
                completed += 1
            latency_ms = (time.time() - sub["submit_ts"]) * 1000 if terminal else None
            shaper_results.append({"status": status, "ms": latency_ms})
            print(f"  [shaper] Request {i}: request_id={sub['request_id']} -> "
                  f"status={status} ({'complete' if terminal else 'not complete'})")
        run_end = time.time()

        if completed == DEMO_REQUEST_COUNT:
            print(f"\n[shaper] queue drained -- {completed}/{DEMO_REQUEST_COUNT} complete")
        else:
            print(f"\n[shaper] queue drain timed out -- {completed}/{DEMO_REQUEST_COUNT} complete")

        print(f"\n[shaper] Querying shaper CloudWatch metrics for measured token usage "
              f"(namespace BedrockShaper, model={model_id})...")
        metrics_deadline = time.time() + DEMO_METRICS_WAIT_S
        while True:
            itok, otok = shaper_tokens(model_id, run_start, run_end)
            if itok or otok or time.time() >= metrics_deadline:
                break
            time.sleep(DEMO_METRICS_POLL_INTERVAL_S)

        elapsed_min = max(run_end - run_start, 1e-6) / 60.0
        if itok == 0 and otok == 0:
            print(f"[shaper] CloudWatch EMF metrics have not landed within {DEMO_METRICS_WAIT_S}s of "
                  "drain -- not reporting a zero as if it were a real measurement. Re-run "
                  "shaper_tokens() against this window later to see the real totals.")
        else:
            total_tok = itok + otok
            approx_tpm = total_tok / elapsed_min
            # Peak rolling-60s TPM (bead a7q.7) -- see _shaper_peak_tpm_60s's
            # docstring for why this is a wall-clock-aligned CloudWatch bucket
            # maximum, an approximation of the peak, and NOT computed the same
            # way as the direct arms' true sliding-window peak below.
            shaper_peak_tpm = _shaper_peak_tpm_60s(region, model_id, run_start, run_end)
            print(f"[shaper] Measured (real, CloudWatch EMF): input_tokens={itok:.0f} "
                  f"output_tokens={otok:.0f} total_tokens={total_tok:.0f} over "
                  f"{elapsed_min * 60:.0f}s -> avg {approx_tpm:.0f} TPM, peak 60s-bucket "
                  f"{shaper_peak_tpm:.0f} TPM against a configured queue share of "
                  f"{overridden_item['tpm_queue_capacity']} TPM (ceiling "
                  f"{DEMO_TPM_OVERRIDE}). These are MEASURED; whatever they come out to is the "
                  f"result -- nothing in this demo is tuned toward a target.")
    except Exception as e:
        print(f"[shaper] Demo run failed: {e}")
    finally:
        # Restores the ENTIRE original item, not just tpm_limit -- the override now
        # rewrites every derived capacity/regen field, so a field-by-field restore
        # would leave the demo's 100k-derived pacing live on the deployed config.
        table.put_item(Item=original_item)
        print(f"[shaper] Restored the original CONFIG item in full "
              f"(tpm_limit={original_item.get('tpm_limit')}, "
              f"tpm_queue_capacity={original_item.get('tpm_queue_capacity')}, "
              f"tpm_queue_regeneration_rate={original_item.get('tpm_queue_regeneration_rate')})")

    # --- Direct-Bedrock arms' reporting (outside the shaper's try/finally above,
    # so no arm's completion/reporting can block another's) ---
    for thread, label in ((direct_thread, "direct"), (retry_thread, "direct+retry")):
        join_timeout = DEMO_DIRECT_TIMEOUT_S + 60
        if thread is not None:
            print(f"\n[{label}] waiting up to {join_timeout}s for the direct-Bedrock arm's "
                  "background thread to finish...")
            thread.join(timeout=join_timeout)
            if thread.is_alive():
                print(f"[{label}] background thread did not finish within the join bound -- "
                      "reporting partial results honestly; it will keep running in the "
                      "background until its own request-level timeouts resolve it.")

    # Both direct arms' result dicts share one shape (see _aggregate_direct_results),
    # so one loop aggregates both rows instead of a second hand-written copy of
    # this arithmetic.
    direct_agg = {}
    for label, thread_out in (("direct", direct_thread_out), ("direct+retry", retry_thread_out)):
        results = thread_out.get("results", [])
        start_ = thread_out.get("start")
        end_ = thread_out.get("end")
        elapsed_s = (max(end_ - start_, 1e-6) if start_ and end_ else DEMO_SUBMIT_WINDOW_S)
        agg = _aggregate_direct_results(results, DEMO_REQUEST_COUNT, elapsed_s / 60.0)
        agg["elapsed_s"] = elapsed_s
        direct_agg[label] = agg

    # Named shortcuts for the no-retry direct arm, kept for the pre-existing
    # REAL RATE EXCURSION section below which discusses that arm specifically.
    direct_succ = direct_agg["direct"]["succ"]
    direct_err = direct_agg["direct"]["err"]
    direct_sim_thr = direct_agg["direct"]["sim_thr"]
    direct_in = direct_agg["direct"]["in"]
    direct_out_tok = direct_agg["direct"]["out"]
    direct_tpm = direct_agg["direct"]["tpm"]
    direct_elapsed_s = direct_agg["direct"]["elapsed_s"]
    direct_elapsed_min = direct_elapsed_s / 60.0

    retry_in = direct_agg["direct+retry"]["in"]
    retry_out_tok = direct_agg["direct+retry"]["out"]

    # 429/503/504 are the shaper's own honest-outcomes throttle-family terminal
    # statuses (see smoke_honest_outcomes.py); 200 is success, anything else
    # (400, or never-terminal/None from a timed-out poll) counts as an error.
    # These are REAL throttles, so they are never put in the simulated_throttle
    # column -- they count as errors and additionally trip the warning below.
    shaper_succ = sum(1 for r in shaper_results if r["status"] == 200)
    shaper_real_thr = sum(1 for r in shaper_results if r["status"] in (429, 503, 504))
    shaper_err = sum(1 for r in shaper_results if r["status"] != 200)
    shaper_lat = [r["ms"] for r in shaper_results if r["ms"] is not None]
    shaper_p50 = median(shaper_lat) if shaper_lat else 0.0
    shaper_total_tok = itok + otok

    # One row per arm, all three built the same shape, so the print loop below
    # is not a third copy-paste of the same column arithmetic. The shaper and
    # no-retry direct arms never retry client-side, so their attempts is by
    # construction equal to offered and their ratio is 1.0x -- only the
    # direct+retry row's attempts/bedrock_calls/ratio come from real retries.
    # run_time (bead a7q.7): total wall-clock seconds each arm was active.
    # Shaper: from its own submission start to its queue-drain end (already
    # tracked as run_start/run_end above). Direct arms: the elapsed_s already
    # computed per-arm in _aggregate_direct_results, from that arm's own
    # background-thread start/end (see the direct_agg loop above).
    shaper_run_time = run_end - run_start

    rows = [
        {"arm": "shaper", "offered": DEMO_REQUEST_COUNT, "attempts": DEMO_REQUEST_COUNT,
         "bedrock_calls": DEMO_REQUEST_COUNT, "ratio": 1.0, "succ": shaper_succ,
         "err": shaper_err, "sim_thr": 0, "in": itok, "out": otok, "tpm": approx_tpm,
         "peak_tpm": shaper_peak_tpm, "run_time": shaper_run_time, "p50": shaper_p50},
        {"arm": "direct", "offered": DEMO_REQUEST_COUNT, "attempts": direct_agg["direct"]["attempts"],
         "bedrock_calls": direct_agg["direct"]["bedrock_calls"], "ratio": direct_agg["direct"]["ratio"],
         "succ": direct_succ, "err": direct_err, "sim_thr": direct_sim_thr, "in": direct_in,
         "out": direct_out_tok, "tpm": direct_tpm, "peak_tpm": direct_agg["direct"]["peak_tpm"],
         "run_time": direct_elapsed_s, "p50": direct_agg["direct"]["p50"]},
        {"arm": "direct+retry", "offered": DEMO_REQUEST_COUNT,
         "attempts": direct_agg["direct+retry"]["attempts"],
         "bedrock_calls": direct_agg["direct+retry"]["bedrock_calls"],
         "ratio": direct_agg["direct+retry"]["ratio"], "succ": direct_agg["direct+retry"]["succ"],
         "err": direct_agg["direct+retry"]["err"], "sim_thr": direct_agg["direct+retry"]["sim_thr"],
         "in": retry_in, "out": retry_out_tok, "tpm": direct_agg["direct+retry"]["tpm"],
         "peak_tpm": direct_agg["direct+retry"]["peak_tpm"],
         "run_time": direct_agg["direct+retry"]["elapsed_s"],
         "p50": direct_agg["direct+retry"]["p50"]},
    ]

    print(f"\n{'=' * 138}")
    print("FINAL COMPARISON -- shaper (/invoke, TPM-shaped) vs direct-Bedrock, no retry, vs "
          "direct-Bedrock+retry/jitter (all simulated-quota-gated)")
    print(f"{'=' * 138}")
    # avg_tpm (renamed from the old "~TPM", bead a7q.7): total tokens divided by
    # THIS ARM'S OWN wall-clock elapsed time -- an average rate, not a
    # ceiling-compliance measure. A direct arm that finishes in ~25s naturally
    # divides down to a much higher number here than the shaper, which stays
    # busy for minutes -- that is an artifact of averaging over different
    # elapsed windows, NOT evidence the direct arm exceeded the ceiling. Read
    # peak_tpm (a true peak rolling-60s-window measurement) for that question.
    hdr = (f"{'arm':13s}{'offered':>8s}{'attempts':>9s}{'bedrock_calls':>14s}{'ratio':>7s}"
           f"{'succ':>6s}{'err':>6s}{'sim_thr':>8s}{'in_tok':>9s}{'out_tok':>9s}"
           f"{'total_tok':>10s}{'avg_tpm':>9s}{'peak_tpm':>10s}{'run_time_s':>11s}{'p50_ms':>9s}")
    print(hdr)
    print("-" * len(hdr))
    for row in rows:
        print(f"{row['arm']:13s}{row['offered']:>8d}{row['attempts']:>9d}"
              f"{row['bedrock_calls']:>14d}{row['ratio']:>7.2f}{row['succ']:>6d}{row['err']:>6d}"
              f"{row['sim_thr']:>8d}{row['in']:>9.0f}{row['out']:>9.0f}"
              f"{row['in'] + row['out']:>10.0f}{row['tpm']:>9.0f}{row['peak_tpm']:>10.0f}"
              f"{row['run_time']:>11.1f}{row['p50']:>9.0f}")
    print(f"{'=' * 138}")

    for label in ("direct", "direct+retry"):
        unknown = direct_agg[label]["unknown_attempts"]
        if unknown:
            print(f"  NOTE: {label} arm had {unknown} request(s) still unresolved at the "
                  f"{DEMO_DIRECT_TIMEOUT_S}s wait bound -- their attempt counts are unknown (not "
                  "zero, the thread may still be mid-backoff) and are excluded from the "
                  "attempts/bedrock_calls/ratio above.")

    # THE HEADLINE FINDING this bead exists to show: every attempt is counted,
    # including retries, so the retry arm's attempts visibly outruns what it
    # actually serves. This is the ACTUAL ratio this run produced -- nothing
    # about the retry policy, the offered load, or the ceiling was tuned to
    # land in any particular range; report whatever it really came out to.
    print(f"\nRETRY AMPLIFICATION (genuine measurement, no simulation involved): the direct+retry "
          f"arm was offered {DEMO_REQUEST_COUNT}")
    print(f"requests but made {direct_agg['direct+retry']['attempts']} total attempts (initial + "
          f"every retry), of which {direct_agg['direct+retry']['bedrock_calls']} actually reached "
          f"converse() ->")
    print(f"{direct_agg['direct+retry']['ratio']:.2f}x amplification (attempts/offered). The "
          f"no-retry direct arm made {direct_agg['direct']['attempts']} attempts for the same "
          f"{DEMO_REQUEST_COUNT} offered")
    print(f"({direct_agg['direct']['ratio']:.2f}x) and the shaper made {DEMO_REQUEST_COUNT} "
          f"({1.0:.2f}x) -- neither of those arms retries, so their attempts equal offered by "
          "construction.")

    # Real throttles are an ANOMALY, not a statistic: one prominent warning line,
    # printed only if one actually happened. No reassuring "0 real throttles"
    # line competing with the simulated_throttle column for attention.
    direct_real_thr = direct_agg["direct"]["real_thr"]
    direct_real_codes = direct_agg["direct"]["real_codes"]
    retry_real_thr = direct_agg["direct+retry"]["real_thr"]
    retry_real_codes = direct_agg["direct+retry"]["real_codes"]
    if direct_real_thr or retry_real_thr or shaper_real_thr:
        print("\n" + "!" * 108)
        print("WARNING: during test there were real application throttles -- NOT simulated. "
              "These are excluded from")
        print("the simulated_throttle column above and are counted as errors:")
        if direct_real_thr:
            print(f"  direct arm: {direct_real_thr} real throttle(s) from converse(), "
                  f"code(s): {', '.join(direct_real_codes)}")
        if retry_real_thr:
            print(f"  direct+retry arm: {retry_real_thr} real throttle(s) from converse() "
                  f"(counted per attempt, so one retried away and one that exhausted retries "
                  f"are both here), code(s): {', '.join(retry_real_codes)}")
        if shaper_real_thr:
            print(f"  shaper arm: {shaper_real_thr} real throttle(s) -- terminal 429/503/504 "
                  "status from /result")
        print("A real throttle is not expected at this volume against the account's real quota. "
              "Treat this as a")
        print("finding about the account or the shaper, not as a number to tune away.")
        print("!" * 108)

    # The genuine, unsimulated half of the story: the rate the unshaped arm was
    # actually driving, measured from real converse() usage, against the same
    # declared budget.
    direct_admitted = direct_succ + direct_err
    mean_actual = (direct_in + direct_out_tok) / direct_succ if direct_succ else 0.0
    offered_tpm = (mean_actual * DEMO_REQUEST_COUNT / direct_elapsed_min
                   if direct_elapsed_min > 0 else 0.0)
    print(f"\nREAL RATE EXCURSION (genuine measurement, no simulation involved): the "
          f"{direct_admitted} direct requests that")
    print(f"the gate admitted really ran and really consumed {direct_in + direct_out_tok:.0f} "
          f"tokens in {direct_elapsed_s:.0f}s -> {direct_tpm:.0f} TPM, at a measured mean of")
    print(f"{mean_actual:.0f} actual tokens per request. At that measured per-request cost the "
          f"full offered load of")
    print(f"{DEMO_REQUEST_COUNT} unshaped requests works out to ~{offered_tpm:.0f} TPM against a "
          f"declared {DEMO_TPM_OVERRIDE}-token budget")
    print(f"({offered_tpm / DEMO_TPM_OVERRIDE:.1f}x over). Bead a7q.4 measured exactly this "
          "excursion directly, with no gate in place at all,")
    print("at 174,615 TPM. That number is real; only the rejections are modelled.")

    print("\nNOTE -- which part of this table is MEASURED and which is MODELLED:")
    print(f"  * The shaper row is entirely real. Its input/output token figures are real "
          "CloudWatch EMF")
    print(f"    measurements emitted by the deployed shaper, and its throttle count is a real "
          "outcome -- the")
    print("    shaper either shed requests or it did not.")
    print(f"  * The direct row's {direct_succ} successful requests are real converse() calls "
          "reporting real")
    print(f"    usage.inputTokens/usage.outputTokens. Its {direct_sim_thr} simulated_throttle "
          "rejections are SIMULATED:")
    print(f"    Bedrock did NOT throttle these requests and never saw them. They were rejected "
          "client-side,")
    print(f"    before converse() was called, by a virtual {DEMO_TPM_OVERRIDE}-token budget over "
          f"a rolling {DEMO_VIRTUAL_WINDOW_S}s")
    print(f"    window -- the same ceiling the shaper's tpm_limit was overridden to, charged with "
          "the shaper's")
    print(f"    own estimate_request_tokens() and reconciled to each response's actual usage. The "
          "account's real")
    print(f"    Bedrock quota for this model is far higher and was never approached.")
    print(f"  * The direct+retry row backs off against that SAME client-side SIMULATED ceiling, "
          "via its own SEPARATE")
    print(f"    budget instance built from the identical {DEMO_TPM_OVERRIDE}-token ceiling and "
          f"burndown_rate={burndown_rate} -- Bedrock did")
    print(f"    NOT throttle this arm either; every one of its "
          f"{direct_agg['direct+retry']['sim_thr']} simulated_throttle rejections is client-side.")
    # That count is PER ATTEMPT, so on this arm alone it can exceed the offered
    # request count -- one request rejected on all four of its tries contributes
    # four. Spelled out because a rejection count larger than the offered load
    # reads like an error otherwise.
    print(f"    That is a count of REJECTED ATTEMPTS, not of requests: it is what makes "
          f"attempts ({direct_agg['direct+retry']['attempts']}) ==")
    print(f"    bedrock_calls ({direct_agg['direct+retry']['bedrock_calls']}) + "
          f"simulated_throttle ({direct_agg['direct+retry']['sim_thr']}) hold, and it can exceed "
          f"the {DEMO_REQUEST_COUNT} requests offered")
    print("    because a single request that is rejected on every one of its tries is counted "
          "once per try.")
    print(f"    Its {direct_agg['direct+retry']['bedrock_calls']} bedrock_calls are real "
          "converse() calls that really cost money -- a retried attempt")
    print(f"    that eventually passes the gate is a genuine additional Bedrock call, not a free "
          "retry, so retrying only ever")
    print(f"    adds real spend on top of the no-retry arm's, never removes it. Whether that adds "
          "up to strictly MORE total")
    print(f"    real tokens in a given run depends on how many extra admissions retrying happens "
          "to win in that run's")
    # Honesty over the AC's blanket wording: printing "MORE" unconditionally next to numbers that
    # could tie or run the other way would contradict the script's own MEASURED-vs-MODELLED
    # discipline, so the comparison word is chosen from what this run's real numbers actually show.
    retry_total_tok = retry_in + retry_out_tok
    direct_total_tok = direct_in + direct_out_tok
    if retry_total_tok > direct_total_tok:
        cmp_word = "MORE"
    elif retry_total_tok < direct_total_tok:
        cmp_word = "LESS (see note below)"
    else:
        cmp_word = "the SAME amount of (see note below)"
    print(f"    particular timing: this run measured {cmp_word} real tokens for direct+retry -- "
          f"{retry_total_tok:.0f} tokens across")
    print(f"    {direct_agg['direct+retry']['bedrock_calls']} real converse() calls here, versus "
          f"{direct_total_tok:.0f} tokens across "
          f"{direct_agg['direct']['bedrock_calls']} calls for the no-retry arm.")
    print(f"  * The shaper's ceiling override affects ONLY the shaper's own internal budget "
          "accounting. Holding")
    print(f"    both direct arms to the same number is what makes all three rows comparable: same "
          "offered load, same")
    print(f"    ceiling, same accounting function -- the difference in outcome is each approach's "
          "own contribution.")


if __name__ == "__main__":
    main()
