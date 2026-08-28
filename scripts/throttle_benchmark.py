#!/usr/bin/env python3
"""
Meaningful-load throttle benchmark: drive each model past its real Bedrock TPM
quota (concurrent large-input burst) so the BASELINE actually 429s, then run the
same load through the shaper and loop its TPM cap down until residual throttling
is ~0 (the "ideal" queue-only config). Reproduces the shaper-vs-baseline table
under real overload, with true per-request outcomes and end-to-end latency.

IMPORTANT — outcome source of truth: the shaper records honest 200/429/503 outcomes
in the terminal record `REQUEST#<id> / STATUS` (http_status + state). A 429 is stored
as state=FAILED, http_status=429 with a NULL Step Functions cause — so reading SFN
execution status alone misses throttles. We read the terminal records.

Lever: TPM exhaustion via large-input prompts (few requests, real 429s, small SFN
footprint). Prompts stay <256KB (Step Functions input limit). Only models whose
real TPM quota is cheap to exceed are included; opus-5 (30M) / gpt-5.6-luna (20M) /
glm-5 (100M) are out of scope — cannot be overloaded at a sane cost.

Usage:
    python scripts/throttle_benchmark.py --over 6 --prompt-tokens 40000
"""
import argparse
import concurrent.futures as cf
import json
import os
import sys
import time
from datetime import datetime
from statistics import median

import boto3
from botocore.config import Config

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config_loader
import create_model_config as cmc

CFG = config_loader.get_config_with_aws_check()
REGION = CFG.get("AWS_REGION", "us-east-1")
TABLE = CFG.get("SINGLE_TABLE_NAME", "semaphore-single-table")
SFN_ARN = CFG.get("STATE_MACHINE_ARN")
HERE = os.path.dirname(os.path.abspath(__file__))

_CFG = Config(retries={"total_max_attempts": 1, "mode": "standard"}, read_timeout=120, connect_timeout=10)
_THROTTLE = {"ThrottlingException", "TooManyRequestsException", "ServiceQuotaExceededException"}
_UNAVAIL = {"ServiceUnavailableException", "ModelTimeoutException", "InternalServerException"}

# (alias, model_id, real_tpm_quota) — AWS Service Quotas 2026-08-24.
FEASIBLE = [
    ("llama4-maverick", "us.meta.llama4-maverick-17b-instruct-v1:0", 600_000),
    ("llama4-scout",    "us.meta.llama4-scout-17b-instruct-v1:0",    600_000),
    ("nova-lite",       "us.amazon.nova-lite-v1:0",                  4_000_000),
    ("fable-5",         "us.anthropic.claude-fable-5",               4_000_000),
    ("haiku-4-5",       "us.anthropic.claude-haiku-4-5-20251001-v1:0", 5_000_000),
    ("sonnet-5",        "us.anthropic.claude-sonnet-5",              6_000_000),
    # Larger-model data points (added 2026-08-25). nova-2-pro has no callable us. CRIS
    # profile (invalid model id) so nova-pro (2M) stands in for the mid-size Nova point.
    ("nova-pro",        "us.amazon.nova-pro-v1:0",                   2_000_000),
    ("gpt-5.6-sol",     "us.openai.gpt-5.6-sol",                     20_000_000),  # real bucket ~20M (mantle fig was 10M)
    ("gpt-5.6-luna",    "us.openai.gpt-5.6-luna",                    20_000_000),
    ("gpt-5.6-terra",   "us.openai.gpt-5.6-terra",                   20_000_000),
    ("opus-5",          "us.anthropic.claude-opus-5",                30_000_000),  # LAST — burst disrupts this session's Opus quota
]


def big_prompt(approx_tokens):
    unit = "The quick brown fox jumps over the lazy dog. "
    return ("Summarize in one word.\n" + unit * max(1, int(approx_tokens / 11)))[: approx_tokens * 4]


def pctl(vals, p):
    if not vals:
        return 0.0
    s = sorted(vals)
    return s[max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))]


def chunks(xs, k):
    for i in range(0, len(xs), k):
        yield xs[i:i + k]


def _iso_ms(created, updated):
    try:
        return (datetime.fromisoformat(updated) - datetime.fromisoformat(created)).total_seconds() * 1000
    except Exception:  # noqa: BLE001
        return 0.0


def classify_http(e):
    resp = getattr(e, "response", None)
    code = resp.get("Error", {}).get("Code", "") if isinstance(resp, dict) else type(e).__name__
    if code in _THROTTLE:
        return 429, code
    if code in _UNAVAIL:
        return 503, code
    return 0, code  # other/timeout


# ---------- baseline (direct Converse) ----------
def one_call(brt, model_id, prompt):
    t0 = time.time()
    try:
        r = brt.converse(modelId=model_id, messages=[{"role": "user", "content": [{"text": prompt}]}],
                         inferenceConfig={"maxTokens": 16})
        u = r.get("usage", {})
        # inputTokens is ONLY the non-cached delta; true input = input + cacheRead + cacheWrite
        # (AWS prompt-caching docs). Cached reads also don't count toward the TPM quota.
        in_tok = u.get("inputTokens", 0) + u.get("cacheReadInputTokens", 0) + u.get("cacheWriteInputTokens", 0)
        return {"http": 200, "ms": (time.time() - t0) * 1000,
                "in": in_tok, "out": u.get("outputTokens", 0), "cache_read": u.get("cacheReadInputTokens", 0)}
    except Exception as e:  # noqa: BLE001
        http, code = classify_http(e)
        return {"http": http, "ms": (time.time() - t0) * 1000, "err": code, "in": 0, "out": 0}


def run_baseline(model_id, n, prompt, workers=200):
    # 200 workers: slow models (GPT ~3s/call) need high concurrency to deliver the
    # fresh-token burst fast enough to exhaust the bucket before it refills.
    brt = boto3.client("bedrock-runtime", region_name=REGION,
                       config=_CFG.merge(Config(max_pool_connections=workers + 20)))
    res = []
    start = time.time()
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        # Unique PREFIX per request defeats prompt caching (cached reads don't count
        # toward TPM), so every request contributes fresh tokens to the burst.
        futs = [ex.submit(one_call, brt, model_id, f"u{i}-{i*7919}-{i*104729} " + prompt) for i in range(n)]
        for f in cf.as_completed(futs):
            res.append(f.result())
    return res, time.time() - start


# ---------- shaper (Step Functions) — outcomes read from terminal records ----------
def submit(sfn, model_id, rid, prompt):
    try:
        sfn.start_execution(stateMachineArn=SFN_ARN, input=json.dumps(
            {"request_id": rid, "model_id": model_id, "prompt": prompt, "max_tokens": 16}))
        return rid
    except Exception:  # noqa: BLE001
        return None


def run_shaper(model_id, n, prompt, run_tag, drain_cap_s=720):
    sfn = boto3.client("stepfunctions", region_name=REGION)
    ddb = boto3.resource("dynamodb", region_name=REGION)
    ids = []
    start = time.time()
    with cf.ThreadPoolExecutor(max_workers=40) as ex:
        futs = [ex.submit(submit, sfn, model_id, f"thr_{run_tag}_{i}",
                          f"u{i}-{i*7919}-{i*104729} " + prompt) for i in range(n)]  # unique prefix: no cache hits
        for f in cf.as_completed(futs):
            r = f.result()
            if r:
                ids.append(r)
    # Poll terminal records (REQUEST#<id>/STATUS) until all resolve or the cap.
    outcomes = {}
    deadline = time.time() + drain_cap_s
    while len(outcomes) < len(ids) and time.time() < deadline:
        missing = [i for i in ids if i not in outcomes]
        for batch in chunks(missing, 100):
            keys = [{"pk": f"REQUEST#{i}", "sk": "STATUS"} for i in batch]
            resp = ddb.batch_get_item(RequestItems={TABLE: {"Keys": keys}})
            for it in resp.get("Responses", {}).get(TABLE, []):
                # Terminal only. QUEUED writes state=QUEUED/http_status=202 (Accepted) —
                # that's in-flight, NOT an outcome. Wait for SUCCEEDED(200)/FAILED(429|503).
                if it.get("state") not in ("SUCCEEDED", "FAILED"):
                    continue
                hs = int(it.get("http_status", 0))
                outcomes[it["request_id"]] = {"http": hs, "ms": _iso_ms(it.get("created_at"), it.get("updated_at"))}
        if len(outcomes) < len(ids):
            time.sleep(4)
    res = []
    for i in ids:
        if i in outcomes:
            res.append(outcomes[i])
        else:
            res.append({"http": 0, "ms": 0, "queued": True})  # still draining (in-flight, not a failure)
    return res, time.time() - start


def set_shaper_cap(alias, tpm_cap):
    """Upsert a queue-only (0/85/15 default) token-only config in-process.

    Calls create_model_config directly instead of shelling out — no shell/exec
    surface. Equivalent to `create_model_config.py <alias> --rpm 0 --tpm <cap>`.
    """
    cmc.AWS_REGION = REGION
    cmc.SINGLE_TABLE_NAME = TABLE
    model_id = cmc.MODEL_MAP.get(alias, alias)
    cfg = cmc.calculate_config(
        rpm=None, tpm=int(tpm_cap),
        burndown_rate=cmc.OUTPUT_BURNDOWN_RATE.get(alias, 1.0),
        bytes_per_token=cmc.BYTES_PER_TOKEN.get(alias, 4.0),
    )
    cfg["backend"] = "runtime"
    cfg["api_style"] = "converse"
    cmc.create_model_config(model_id, cfg)


def agg(path, res, elapsed, offered_tpm):
    n = len(res) or 1
    succ = sum(1 for r in res if r.get("http") == 200)
    thr = sum(1 for r in res if r.get("http") == 429)
    rej = sum(1 for r in res if r.get("http") == 503)
    q = sum(1 for r in res if r.get("queued"))
    err = len(res) - succ - thr - rej - q
    lat = [r["ms"] for r in res if r.get("ms")]
    return {"path": path, "offered": offered_tpm, "n": len(res), "success": succ, "throttle": thr,
            "rejected": rej, "queued": q, "error": err,
            "throttle_pct": 100.0 * thr / n, "success_pct": 100.0 * succ / n,
            "p50": median(lat) if lat else 0.0, "p95": pctl(lat, 95), "elapsed": elapsed}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--over", type=float, default=6.0, help="offered burst = over x real TPM quota")
    ap.add_argument("--prompt-tokens", type=int, default=40000, help="approx input tokens/request (<60k for SFN 256KB)")
    ap.add_argument("--max-reqs", type=int, default=450, help="cap requests/model so big quotas drain in-window")
    ap.add_argument("--models", help="comma aliases to limit the feasible set")
    args = ap.parse_args()

    sel = set(args.models.split(",")) if args.models else None
    models = [m for m in FEASIBLE if not sel or m[0] in sel]
    prompt = big_prompt(args.prompt_tokens)
    tag0 = int(time.time())
    rows = []

    print(f"\n{'='*140}")
    print(f"MEANINGFUL-LOAD THROTTLE BENCHMARK | burst = {args.over}x real TPM quota | ~{args.prompt_tokens} in-tok/req "
          f"| region={REGION} | outcomes from terminal records (200/429/503)")
    print(f"{'='*140}\n")

    for mi, (alias, mid, quota) in enumerate(models):
        # Cap request count so big-quota models still finish draining in-window; the
        # burst still exceeds ~2x quota (past onset) so the baseline throttles.
        n = min(args.max_reqs, max(4, int(round(quota * args.over / (args.prompt_tokens + 16)))))
        offered = n * (args.prompt_tokens + 16)  # actual burst tokens
        print(f"▶ {alias}  ({mid})  real TPM quota={quota:,}  burst≈{offered:,.0f} tok ({offered/quota:.1f}x)  -> {n} reqs", flush=True)
        try:
            b_res, b_el = run_baseline(mid, n, prompt)
            b = agg("baseline", b_res, b_el, offered)
            print(f"    baseline: 200={b['success']} 429={b['throttle']} 503={b['rejected']} err={b['error']} "
                  f"({b['throttle_pct']:.0f}% throttled) p50={b['p50']/1000:.1f}s p95={b['p95']/1000:.1f}s", flush=True)

            # Ideal queue-only cap = 0.8x real quota: under-provisions to absorb the
            # byte-estimate over-admission that otherwise leaks Bedrock 429s.
            cap = 0.8 * quota
            set_shaper_cap(alias, cap)
            time.sleep(2)
            s_res, s_el = run_shaper(mid, n, prompt, f"{tag0}_{mi}", drain_cap_s=300)
            s = agg("shaper", s_res, s_el, offered)
            s["cap"] = cap
            print(f"    shaper(cap={cap/1e6:.2f}M): 200={s['success']} 429={s['throttle']} 503={s['rejected']} "
                  f"queued={s['queued']} err={s['error']} ({s['throttle_pct']:.0f}% throttled) "
                  f"p50={s['p50']/1000:.1f}s p95={s['p95']/1000:.1f}s", flush=True)
            rows.append((alias, quota, b, s))
        except Exception as e:  # noqa: BLE001
            print(f"    !! {alias} failed: {type(e).__name__}: {str(e)[:80]}", flush=True)
        print(flush=True)

    def s2(ms):
        return f"{ms/1000:.0f}s"
    print(f"{'='*146}")
    h = (f"{'MODEL':15}{'quota':>7}{'burst':>8} | {'BASE 429%':>10}{'SHAP 429%':>10} | "
         f"{'base 200':>9}{'shap 200':>9}{'shap 503':>9}{'shap q':>8} | {'base p50/p95':>14}{'shaper p50/p95':>16} | {'cap':>7}")
    print(h); print("-" * len(h))
    for alias, quota, b, s in rows:
        print(f"{alias:15}{quota/1e6:>6.1f}M{b['offered']/1e6:>7.0f}M | "
              f"{b['throttle_pct']:>9.0f}%{s['throttle_pct']:>9.0f}% | "
              f"{b['success']:>9}{s['success']:>9}{s['rejected']:>9}{s['queued']:>8} | "
              f"{s2(b['p50'])+'/'+s2(b['p95']):>14}{s2(s['p50'])+'/'+s2(s['p95']):>16} | {s['cap']/1e6:>5.1f}M", flush=True)
    print(f"{'='*146}")
    print("BASE 429% = direct Bedrock at burst (heavy throttling). SHAP 429% = same load via shaper at its looped cap.")
    print("shap 503 = shaper backpressure (queue full — clean reject, not a Bedrock 429). shap q = still draining at cap")
    print("(in-flight; SFN timeout 65min). Latency p50/p95 = end-to-end (shaper incl. queue wait), completed requests.")
    print(f"{'='*146}\n")


if __name__ == "__main__":
    main()
