#!/usr/bin/env python3
"""
Demo harness: temporarily re-derive a model's config at a 100,000 TPM ceiling,
then offer the same paced load to three arms at once:

  shaper        -- SigV4-signed requests through the shaper's /invoke ingress
  direct        -- straight to bedrock-runtime converse(), no retry
  direct+retry  -- same, with exponential backoff + full jitter

Bedrock's real quota is far above this load, so both direct arms are held to
the same 100,000-token ceiling by a client-side SIMULATED quota (see
_virtual_budget_admit). Those rejections are simulated; all tokens, latencies
and Bedrock calls are real. The original CONFIG item is restored even on error.

Usage:
    python scripts/demo.py [MODEL] [-v|--verbose]

    MODEL defaults to nova-2-lite. --verbose prints every request/attempt.
"""

import argparse
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
# Lambda layer on sys.path so the direct arms' virtual quota is charged with the
# shaper's OWN estimate_request_tokens(), not a local copy of the formula.
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '..', 'infrastructure', 'lambda_layer', 'python'
    ),
)
import boto3
from botocore.config import Config
import config_loader
from create_model_config import MODEL_MAP, calculate_config, create_model_config
from smoke_honest_outcomes import _load_api_url, _signed_request, _poll_result
from shared_service import estimate_request_tokens

DEMO_TPM_OVERRIDE = 100_000

# Request profile: flat constants, deliberately not tuned toward any result.
# DEMO_BYTES_PER_TOKEN is written only into the demo's own override item.
DEMO_TARGET_INPUT_TOKENS = 2000
DEMO_CHARS_PER_TOKEN_ESTIMATE = 4
DEMO_BYTES_PER_TOKEN = 5
DEMO_MAX_OUTPUT_TOKENS = 1000

# Offered-load timeline: (duration_s, request_count) phases played back to back;
# requests are spread evenly within a phase. Timeouts below are sized for the
# default single ~25s burst -- widen them before running a longer profile.
DEMO_LOAD_PROFILE = [(25, 70)]
DEMO_REQUEST_COUNT = sum(count for _, count in DEMO_LOAD_PROFILE)
DEMO_SUBMIT_WINDOW_S = sum(duration for duration, _ in DEMO_LOAD_PROFILE)

# The queue processor dispatches serially, so the drain takes minutes. 600s stays
# under the processor's own 13-minute per-invocation ceiling.
DEMO_DRAIN_TIMEOUT_S = 600

# EMF metrics lag behind the requests; poll for a bounded window.
DEMO_METRICS_WAIT_S = 120
DEMO_METRICS_POLL_INTERVAL_S = 15

# Direct arms: worker pool and wait bound (worst case ~2 waves of 4 slow
# attempts + 7s backoff each, plus the 25s submission window, is ~171s).
DEMO_DIRECT_MAX_WORKERS = 40
# Concurrent /invoke POSTs for the shaper arm (each POST is ~0.7s vs a ~0.36s slot).
DEMO_SHAPER_SUBMIT_WORKERS = 16
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

# Bedrock quotas are per-minute, so the simulated quota is a rolling 60s window.
DEMO_VIRTUAL_WINDOW_S = 60

VERBOSE = False

# Live progress: every DEMO_PROGRESS_INTERVAL_S, one line of requests sent per
# arm, until every arm has sent its full load.
DEMO_PROGRESS_INTERVAL_S = 5
_progress = {"lock": threading.Lock(), "sent": {}}


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


def _new_virtual_budget(burndown_rate):
    """State for one direct arm's SIMULATED quota. bpt matches the shaper's
    override item so both arms are charged identical estimates."""
    return {
        "limit": DEMO_TPM_OVERRIDE,
        "burndown": burndown_rate,
        "bpt": DEMO_BYTES_PER_TOKEN,
        "charges": [],
        "lock": threading.Lock(),
    }


def _virtual_budget_admit(budget, prompt, max_tokens):
    """Charge the estimate up front (as Bedrock does) against a rolling 60s window.
    Over-budget requests are REJECTED, never queued -- failing fast is what the
    real service does, and waiting would turn this arm into a second shaper.

    Returns (charge, est, used); charge is None when rejected. Successful calls
    overwrite charge["tokens"] with actual usage (mirroring the shaper's
    reconcile step); failed calls keep the estimate until it ages out."""
    est = estimate_request_tokens(
        prompt=prompt,
        max_tokens=max_tokens,
        burndown_rate=budget["burndown"],
        bytes_per_token=budget["bpt"],
    )
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
        if delay > 0:
            time.sleep(delay)
        yield i


def _submit_sized(api_url, arm, model_id, prompt, max_tokens):
    """Signed POST /invoke (smoke_honest_outcomes._submit with max_tokens exposed)."""
    request_id = str(uuid.uuid4())
    body = {
        "request_id": request_id,
        "model_id": model_id,
        "prompt": prompt,
        "correlation_id": str(uuid.uuid4()),
        "max_tokens": max_tokens,
    }
    status, text = _signed_request("POST", f"{api_url}/invoke", body)
    _vprint(f"  [{arm}] POST /invoke -> {status}")
    if status not in (200, 202):
        print(f"  [{arm}] unexpected submit status: {status} {text[:200]}")
    try:
        request_id = json.loads(text).get("request_id") or request_id
    except (ValueError, AttributeError):
        pass
    return {"request_id": request_id, "submit_status": status}


def _error_code(e):
    resp = getattr(e, "response", None)
    code = resp.get("Error", {}).get("Code", "") if isinstance(resp, dict) else ""
    return code or type(e).__name__


def _backoff(attempt):
    delay = min(DEMO_RETRY_MAX_DELAY_S, DEMO_RETRY_BASE_DELAY_S * (2**attempt))
    time.sleep(random.uniform(0, delay))  # nosec B311 nosemgrep: arbitrary-sleep -- backoff jitter


def _direct_call(brt, idx, model_id, prompt, max_tokens, budget, arm, max_retries=0):
    """One direct-Bedrock request, gated by the SIMULATED quota. Retries (with
    backoff, re-entering the gate each time) on simulated or real throttles up to
    max_retries; other errors are never retried.

    Counts are PER ATTEMPT: attempts == bedrock_calls + sim_throttle_attempts.
    `ms` is total wall clock including backoff; None for a no-retry rejection
    that never reached Bedrock."""
    t0 = time.time()
    calls = sim = 0
    real_codes = []

    def result(ok, attempts, in_tok=0, out_tok=0, sim_final=False, ms="elapsed"):
        now = time.time()
        return {
            "ok": ok,
            "simulated_throttle": sim_final,
            "in": in_tok,
            "out": out_tok,
            "ms": (now - t0) * 1000 if ms == "elapsed" else ms,
            "attempts": attempts,
            "bedrock_calls": calls,
            "sim_throttle_attempts": sim,
            "real_throttle_codes": real_codes,
            "completed_ts": now,
        }

    for attempt in range(max_retries + 1):
        last = attempt == max_retries
        tag = f"  [{arm}] req{idx}" + (f" #{attempt + 1}" if max_retries else "")
        charge, est, used = _virtual_budget_admit(budget, prompt, max_tokens)
        if charge is None:
            sim += 1
            _vprint(
                f"{tag} SIM-THROTTLE ({used:.0f} used + {est} est > {budget['limit']})"
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
            retry = real_throttle and not last
            print(
                f"{tag} converse() -> {'REAL THROTTLE' if real_throttle else 'error'} {code}"
                f"{' -> retry' if retry else ''}"
            )
            if retry:
                _backoff(attempt)
                continue
            return result(False, attempt + 1)

        usage = r.get("usage", {})
        in_tok, out_tok = usage.get("inputTokens", 0), usage.get("outputTokens", 0)
        with budget["lock"]:
            charge["tokens"] = in_tok + out_tok  # reconcile estimate -> actual
        res = result(True, attempt + 1, in_tok, out_tok)
        _vprint(f"{tag} ok in={in_tok} out={out_tok} ({res['ms']:.0f}ms)")
        return res


def _run_direct_arm(region, model_id, prompt, max_tokens, profile, budget, out, arm, max_retries):
    """Background-thread body for one direct arm; writes results/start/end into `out`."""
    client_cfg = Config(
        retries={"total_max_attempts": 1, "mode": "standard"}, read_timeout=120, connect_timeout=10
    )
    brt = boto3.client("bedrock-runtime", region_name=region, config=client_cfg)
    start = time.time()
    with cf.ThreadPoolExecutor(max_workers=DEMO_DIRECT_MAX_WORKERS) as ex:
        futs = []
        for i in _paced_indices(start, profile):
            fut = ex.submit(
                _direct_call, brt, i, model_id, prompt, max_tokens, budget, arm, max_retries
            )
            _tick_sent(arm)
            futs.append(fut)
        print(f"  [{arm}] submitted {len(futs)} requests in {time.time() - start:.1f}s")
        done, not_done = cf.wait(futs, timeout=DEMO_DIRECT_TIMEOUT_S)
    results = [f.result() for f in done]
    # Unresolved at the wait bound: counts are unknowable, so None (excluded from
    # the attempt sums and reported as unknown), not 0.
    results += [
        {
            "ok": False,
            "simulated_throttle": False,
            "in": 0,
            "out": 0,
            "ms": None,
            "attempts": None,
            "bedrock_calls": None,
            "sim_throttle_attempts": None,
            "real_throttle_codes": None,
            "completed_ts": None,
        }
        for _ in not_done
    ]
    end = time.time()
    ok = sum(1 for r in results if r["ok"])
    sim = sum(1 for r in results if r["simulated_throttle"])
    print(
        f"  [{arm}] done in {end - start:.1f}s -- {ok} ok, {sim} rejected by simulated quota, "
        f"{len(results) - ok - sim} error"
        + (f", {len(not_done)} still pending at wait bound" if not_done else "")
    )
    out.update(results=results, start=start, end=end)


def _peak_window_tpm(events, window_s=DEMO_VIRTUAL_WINDOW_S):
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
    unknown_attempts."""
    known = [r for r in results if r.get("attempts") is not None]
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
        "run_time": elapsed_s,
    }


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


def _run_shaper_arm(api_url, model_id, prompt, run_start):
    """Submit the paced load to /invoke, then poll each request to a terminal status."""

    # Each signed POST takes longer than a pacing slot, so POSTs are dispatched to
    # a pool (like the direct arms) to keep the shaper on the identical schedule.
    def submit(i):
        submit_ts = time.time()
        sub = _submit_sized(api_url, f"shaper:req{i}", model_id, prompt, DEMO_MAX_OUTPUT_TOKENS)
        sub["submit_ts"] = submit_ts
        return sub

    with cf.ThreadPoolExecutor(max_workers=DEMO_SHAPER_SUBMIT_WORKERS) as ex:
        futs = []
        for i in _paced_indices(run_start, DEMO_LOAD_PROFILE):
            futs.append(ex.submit(submit, i))
            _tick_sent("shaper")
        dispatched_s = time.time() - run_start
    submissions = [f.result() for f in futs]
    print(
        f"[shaper] submitted {len(submissions)} requests (dispatched over {dispatched_s:.1f}s, "
        f"all accepted by {time.time() - run_start:.1f}s); draining (up to {DEMO_DRAIN_TIMEOUT_S}s)..."
    )

    deadline = time.time() + DEMO_DRAIN_TIMEOUT_S
    results = []
    for i, sub in enumerate(submissions, start=1):
        remaining = max(0, deadline - time.time())
        status, _ = _poll_result(api_url, sub["request_id"], timeout_s=remaining, interval_s=3)
        terminal = status is not None and status != 202
        results.append(
            {"status": status, "ms": (time.time() - sub["submit_ts"]) * 1000 if terminal else None}
        )
        _vprint(f"  [shaper] req{i} -> {status}")
        if i % 10 == 0:
            print(f"  [shaper] {i}/{len(submissions)} resolved")
    done = sum(1 for r in results if r["ms"] is not None)
    print(
        f"[shaper] {'drained' if done == len(results) else 'drain TIMED OUT'} -- "
        f"{done}/{len(results)} complete"
    )
    return results


def main():
    global VERBOSE
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("model", nargs="?", default="nova-2-lite")
    parser.add_argument("-v", "--verbose", action="store_true", help="print every request/attempt")
    args = parser.parse_args()
    VERBOSE = args.verbose
    model_id = MODEL_MAP.get(args.model, args.model)

    config = config_loader.get_config_with_aws_check()
    region = config.get('AWS_REGION', 'us-east-1')
    table_name = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')
    table = boto3.resource('dynamodb', region_name=region).Table(table_name)
    key = {'pk': f'MODEL#{model_id}', 'sk': 'CONFIG'}

    original_item = table.get_item(Key=key).get('Item')
    if original_item is None:
        print(f"No CONFIG item found for {model_id} in {table_name} -- nothing to demo.")
        sys.exit(1)
    print(f"Model: {model_id} (table: {table_name}, region: {region})")

    # Two SEPARATE budgets with identical parameters: sharing one would make the
    # direct arms contend with each other.
    burndown_rate = float(original_item.get('output_token_burndown_rate', 1.0))
    arms = [("direct", 0), ("direct+retry", DEMO_RETRY_MAX_RETRIES)]
    arm_out = {label: {} for label, _ in arms}
    threads = []
    stop_progress = threading.Event()
    shaper_results = []
    itok = otok = shaper_peak_tpm = 0.0
    run_start = run_end = time.time()

    try:
        overridden_item = _build_ceiling_override_item(original_item, model_id)
        table.put_item(Item=overridden_item)
        print(
            f"Config re-derived at {DEMO_TPM_OVERRIDE:,} TPM (restored at end). All three arms are "
            f"held to this {DEMO_TPM_OVERRIDE:,} TPM ceiling: the shaper via this config, the "
            f"direct arms via a client-side simulated quota:"
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
            f"Offering {DEMO_REQUEST_COUNT} requests over ~{DEMO_SUBMIT_WINDOW_S}s to each arm "
            f"({DEMO_TOKENS_PER_REQUEST_ESTIMATE} est. tokens/request, "
            f"max_output_tokens={DEMO_MAX_OUTPUT_TOKENS}).\n"
        )

        run_start = time.time()
        for arm in ("shaper", "direct", "direct+retry"):  # fixes the column order
            _progress["sent"][arm] = 0
        threading.Thread(
            target=_progress_reporter,
            daemon=True,
            args=(stop_progress, run_start, DEMO_REQUEST_COUNT),
        ).start()
        for label, max_retries in arms:
            t = threading.Thread(
                target=_run_direct_arm,
                daemon=True,
                args=(
                    region,
                    model_id,
                    prompt,
                    DEMO_MAX_OUTPUT_TOKENS,
                    DEMO_LOAD_PROFILE,
                    _new_virtual_budget(burndown_rate),
                    arm_out[label],
                    label,
                    max_retries,
                ),
            )
            t.start()
            threads.append((label, t))

        shaper_results = _run_shaper_arm(api_url, model_id, prompt, run_start)
        run_end = time.time()
        stop_progress.set()

        print("[shaper] waiting for CloudWatch EMF token metrics...")
        metrics_deadline = time.time() + DEMO_METRICS_WAIT_S
        while True:
            itok, otok, shaper_peak_tpm = _shaper_tokens(region, model_id, run_start, run_end)
            if itok or otok or time.time() >= metrics_deadline:
                break
            time.sleep(DEMO_METRICS_POLL_INTERVAL_S)
        if itok or otok:
            print(
                f"[shaper] {itok + otok:,.0f} tokens in {run_end - run_start:.0f}s, peak 60s = "
                f"{shaper_peak_tpm:,.0f} TPM (queue share "
                f"{overridden_item['tpm_queue_capacity']:,} / ceiling {DEMO_TPM_OVERRIDE:,})"
            )
        else:
            print(
                f"[shaper] EMF metrics did not land within {DEMO_METRICS_WAIT_S}s -- token "
                "columns for the shaper will read 0; re-query CloudWatch later."
            )
    except Exception as e:
        print(f"[shaper] Demo run failed: {e}")
    finally:
        stop_progress.set()
        # Restore the ENTIRE item -- the override rewrote every derived field.
        table.put_item(Item=original_item)
        print("[shaper] Restored original CONFIG")

    for label, t in threads:
        t.join(timeout=DEMO_DIRECT_TIMEOUT_S + 60)
        if t.is_alive():
            print(f"[{label}] background thread still running -- reporting partial results")

    shaper_lat = [r["ms"] for r in shaper_results if r["ms"] is not None]
    n_shaper = len(shaper_results)
    rows = {
        "shaper": {
            "offered": DEMO_REQUEST_COUNT,
            "attempts": n_shaper,
            "bedrock_calls": n_shaper,
            "succ": sum(1 for r in shaper_results if r["status"] == 200),
            "err": sum(1 for r in shaper_results if r["status"] != 200),
            # 429/503/504 are the shaper's throttle-family terminal statuses: REAL throttles.
            "real_thr": sum(1 for r in shaper_results if r["status"] in (429, 503, 504)),
            "real_codes": ["429/503/504"],
            "sim_thr": 0,
            "in": itok,
            "out": otok,
            "peak_tpm": shaper_peak_tpm,
            "run_time": run_end - run_start,
            "p50": median(shaper_lat) if shaper_lat else 0.0,
            "unknown_attempts": 0,
        }
    }
    for label, _ in arms:
        out = arm_out[label]
        elapsed_s = (
            max(out["end"] - out["start"], 1e-6) if out.get("start") else DEMO_SUBMIT_WINDOW_S
        )
        rows[label] = _aggregate_direct_results(
            out.get("results", []), DEMO_REQUEST_COUNT, elapsed_s
        )

    print()
    hdr = (
        f"{'arm':13s}{'offered':>8s}{'attempts':>9s}{'calls':>7s}{'succ':>6s}{'err':>5s}"
        f"{'sim_thr':>8s}{'total_tok':>10s}{'peak_tpm':>10s}{'run_s':>7s}{'p50_ms':>8s}"
    )
    print(hdr)
    print("-" * len(hdr))
    for arm, r in rows.items():
        print(
            f"{arm:13s}{r['offered']:>8d}{r['attempts']:>9d}{r['bedrock_calls']:>7d}"
            f"{r['succ']:>6d}{r['err']:>5d}{r['sim_thr']:>8d}{r['in'] + r['out']:>10.0f}"
            f"{r['peak_tpm']:>10.0f}{r['run_time']:>7.1f}{r['p50']:>8.0f}"
        )

    for arm, r in rows.items():
        if r["unknown_attempts"]:
            print(
                f"NOTE: {arm} had {r['unknown_attempts']} request(s) unresolved at the "
                f"{DEMO_DIRECT_TIMEOUT_S}s bound, excluded from attempts/calls/sim_thr."
            )
    real = [
        f"{arm}={r['real_thr']} ({', '.join(r['real_codes'])})"
        for arm, r in rows.items()
        if r["real_thr"]
    ]
    if real:
        print(f"⚠ REAL throttles occurred (not simulated): {'; '.join(real)}")


if __name__ == "__main__":
    main()
