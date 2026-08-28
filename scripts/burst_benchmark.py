#!/usr/bin/env python3
"""
2x burst benchmark: shaper (Step Functions) vs baseline (direct Bedrock), side by side.

For each configured model it offers the SAME load on both paths — a burst at
`multiplier` x the model's configured RPM, sustained for `duration` seconds — and
reports, per path: requests offered, success / throttle / error, offered vs
effective RPM, effective input/output TPM (from real usage), and latency p50/p95.

The shaper should convert would-be throttles into queued successes (paced to the
configured rate); the baseline runs unshaped and throttles only once the offered
rate exceeds the model's real Bedrock quota.

Usage:
    python scripts/burst_benchmark.py                       # all configured models
    python scripts/burst_benchmark.py --models nova-2-lite,sonnet-5,opus-5
    python scripts/burst_benchmark.py --duration 30 --multiplier 2 --max-tokens 48
"""
import argparse
import concurrent.futures as cf
import json
import os
import sys
import time
from statistics import median

import boto3
from botocore.config import Config

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config_loader
from create_model_config import MODEL_MAP  # shared alias source

CFG = config_loader.get_config_with_aws_check()
REGION = CFG.get("AWS_REGION", "us-east-1")
TABLE = CFG.get("SINGLE_TABLE_NAME", "semaphore-single-table")
SFN_ARN = CFG.get("STATE_MACHINE_ARN")

# One attempt, no boto retries — throttles must surface, not be silently absorbed.
# Bounded read timeout so a slow model (e.g. grok under burst) fails fast as an
# error instead of hanging the pool for the default 60s.
_NO_RETRY = Config(retries={"total_max_attempts": 1, "mode": "standard"},
                   read_timeout=45, connect_timeout=10)
_THROTTLE_CODES = {"ThrottlingException", "TooManyRequestsException", "ServiceQuotaExceededException"}


def pctl(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def load_configs(dynamodb):
    """Return {model_id: config_dict} for every model_config CONFIG row."""
    table = dynamodb.Table(TABLE)
    resp = table.scan(
        FilterExpression="sk = :c AND entity_type = :t",
        ExpressionAttributeValues={":c": "CONFIG", ":t": "model_config"},
    )
    out = {}
    for it in resp.get("Items", []):
        out[it["model_id"]] = it
    return out


def resolve(alias_or_id):
    return MODEL_MAP.get(alias_or_id, alias_or_id)


def build_prompt():
    return "Reply with a single short sentence."


# ---------- baseline path (direct Converse) ----------
def baseline_call(brt, model_id, max_tokens):
    t0 = time.time()
    try:
        r = brt.converse(
            modelId=model_id,
            messages=[{"role": "user", "content": [{"text": build_prompt()}]}],
            inferenceConfig={"maxTokens": max_tokens},
        )
        dt = (time.time() - t0) * 1000
        u = r.get("usage", {})
        return {"ok": True, "throttled": False, "error": None,
                "in": u.get("inputTokens", 0), "out": u.get("outputTokens", 0), "ms": dt}
    except Exception as e:  # noqa: BLE001
        dt = (time.time() - t0) * 1000
        resp = getattr(e, "response", None)
        code = resp.get("Error", {}).get("Code", "") if isinstance(resp, dict) else ""
        if not code:
            code = type(e).__name__  # e.g. ReadTimeoutError, ConnectTimeoutError
        throttled = code in _THROTTLE_CODES
        return {"ok": False, "throttled": throttled, "error": code[:60],
                "in": 0, "out": 0, "ms": dt}


def run_baseline(model_id, n, interval, max_tokens):
    brt = boto3.client("bedrock-runtime", region_name=REGION, config=_NO_RETRY)
    results = []
    start = time.time()
    with cf.ThreadPoolExecutor(max_workers=40) as ex:
        futs = []
        for i in range(n):
            target = start + i * interval
            now = time.time()
            if target > now:
                time.sleep(target - now)
            futs.append(ex.submit(baseline_call, brt, model_id, max_tokens))
        for f in cf.as_completed(futs):
            results.append(f.result())
    elapsed = time.time() - start
    return results, elapsed


# ---------- shaper path (Step Functions) ----------
def shaper_submit(sfn, model_id, req_id, max_tokens):
    payload = {"request_id": f"burst_{req_id}", "model_id": model_id,
               "prompt": build_prompt(), "max_tokens": max_tokens}
    try:
        arn = sfn.start_execution(stateMachineArn=SFN_ARN, input=json.dumps(payload))["executionArn"]
        return arn
    except Exception:  # noqa: BLE001
        return None


def run_shaper(model_id, n, interval, max_tokens):
    sfn = boto3.client("stepfunctions", region_name=REGION)
    arns = []
    start = time.time()
    with cf.ThreadPoolExecutor(max_workers=40) as ex:
        futs = []
        for i in range(n):
            target = start + i * interval
            now = time.time()
            if target > now:
                time.sleep(target - now)
            futs.append(ex.submit(shaper_submit, sfn, model_id, i, max_tokens))
        for f in cf.as_completed(futs):
            a = f.result()
            if a:
                arns.append(a)

    # Poll to terminal, capture status + end-to-end (incl queue) latency.
    results = []
    pending = set(arns)
    while pending:
        done = set()
        for arn in list(pending):
            d = sfn.describe_execution(executionArn=arn)
            st = d["status"]
            if st in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
                ms = (d["stopDate"] - d["startDate"]).total_seconds() * 1000
                if st == "SUCCEEDED":
                    results.append({"ok": True, "throttled": False, "error": None, "ms": ms})
                else:
                    cause = (d.get("cause") or "") + (d.get("error") or "")
                    throttled = any(c in cause for c in _THROTTLE_CODES) or "429" in cause or "503" in cause
                    results.append({"ok": False, "throttled": throttled,
                                    "error": (d.get("error") or "FAILED")[:60], "ms": ms})
                done.add(arn)
        pending -= done
        if pending:
            time.sleep(2)
    elapsed = time.time() - start
    return results, elapsed


def shaper_tokens(model_id, start_epoch, end_epoch):
    """Sum real input/output tokens the shaper reconciled, from its EMF metrics."""
    cw = boto3.client("cloudwatch", region_name=REGION)
    dims = [{"Name": "ServiceName", "Value": "TrafficShaper"}, {"Name": "model_id", "Value": model_id}]
    q = []
    for i, metric in enumerate(("InputTokens", "OutputTokens")):
        q.append({"Id": f"m{i}", "MetricStat": {
            "Metric": {"Namespace": "BedrockShaper", "MetricName": metric, "Dimensions": dims},
            "Period": 300, "Stat": "Sum"}, "ReturnData": True})
    r = cw.get_metric_data(MetricDataQueries=q,
                           StartTime=start_epoch - 60, EndTime=end_epoch + 120)
    vals = {res["Id"]: sum(res["Values"]) for res in r["MetricDataResults"]}
    return vals.get("m0", 0.0), vals.get("m1", 0.0)


def summarize(path, results, elapsed, offered, in_tok=None, out_tok=None):
    n = len(results)
    succ = sum(1 for r in results if r["ok"])
    thr = sum(1 for r in results if not r["ok"] and r["throttled"])
    err = sum(1 for r in results if not r["ok"] and not r["throttled"])
    lat = [r["ms"] for r in results]
    mins = max(elapsed, 1e-6) / 60.0
    if in_tok is None:
        in_tok = sum(r.get("in", 0) for r in results)
        out_tok = sum(r.get("out", 0) for r in results)
    return {
        "path": path, "offered": offered, "completed": n,
        "success": succ, "throttle": thr, "error": err,
        "success_pct": 100.0 * succ / n if n else 0.0,
        "throttle_pct": 100.0 * thr / n if n else 0.0,
        "eff_rpm": succ / mins,
        "eff_itpm": in_tok / mins, "eff_otpm": out_tok / mins,
        "p50_ms": median(lat) if lat else 0.0, "p95_ms": pctl(lat, 95),
        "elapsed_s": elapsed,
    }


def main():
    ap = argparse.ArgumentParser(description="2x burst: shaper vs baseline")
    ap.add_argument("--models", help="comma list of aliases/ids; default = all configured")
    ap.add_argument("--duration", type=int, default=30, help="submission window seconds (default 30)")
    ap.add_argument("--multiplier", type=float, default=2.0, help="offered rate = multiplier x configured RPM")
    ap.add_argument("--base-rpm", type=int, default=60, help="fallback base RPM if config has no rpm_limit")
    ap.add_argument("--max-tokens", type=int, default=48)
    args = ap.parse_args()

    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    configs = load_configs(dynamodb)

    if args.models:
        targets = [(m, resolve(m)) for m in args.models.split(",")]
    else:
        targets = [(mid, mid) for mid in sorted(configs)]

    print(f"\n{'='*118}")
    print(f"2x BURST BENCHMARK — shaper vs baseline | duration={args.duration}s "
          f"multiplier={args.multiplier}x max_tokens={args.max_tokens} region={REGION}")
    print(f"{'='*118}\n")

    rows = []
    for alias, model_id in targets:
        cfg = configs.get(model_id)
        if not cfg:
            print(f"  ! {alias} ({model_id}): no CONFIG row — skipping", flush=True)
            continue
        base_rpm = int(cfg.get("rpm_limit") or args.base_rpm)
        offered_rpm = base_rpm * args.multiplier
        n = max(1, int(round(offered_rpm * args.duration / 60.0)))
        interval = args.duration / n
        print(f"▶ {alias}  ({model_id})", flush=True)
        print(f"    config rpm={base_rpm}  offered={offered_rpm:.0f} rpm  -> {n} requests over {args.duration}s", flush=True)

        try:
            b_res, b_el = run_baseline(model_id, n, interval, args.max_tokens)
            b = summarize("baseline", b_res, b_el, n)
            s_start = time.time()
            s_res, s_el = run_shaper(model_id, n, interval, args.max_tokens)
            s_end = time.time()
            itok, otok = shaper_tokens(model_id, s_start, s_end)
            s = summarize("shaper", s_res, s_el, n, in_tok=itok, out_tok=otok)
        except Exception as e:  # noqa: BLE001 — never let one model abort the sweep
            print(f"    !! {alias} failed: {type(e).__name__}: {str(e)[:80]} — skipping\n", flush=True)
            continue

        for r in (b, s):
            r["model"] = alias
            rows.append(r)
        print(f"    baseline: succ={b['success']}/{n} thr={b['throttle']} err={b['error']} "
              f"eff_rpm={b['eff_rpm']:.0f} eff_oTPM={b['eff_otpm']:.0f} p50={b['p50_ms']:.0f}ms p95={b['p95_ms']:.0f}ms", flush=True)
        print(f"    shaper:   succ={s['success']}/{n} thr={s['throttle']} err={s['error']} "
              f"eff_rpm={s['eff_rpm']:.0f} eff_oTPM={s['eff_otpm']:.0f} p50={s['p50_ms']:.0f}ms p95={s['p95_ms']:.0f}ms\n", flush=True)

    # Comparison table
    print(f"{'='*118}")
    hdr = f"{'model':16s}{'path':9s}{'off':>5s}{'succ':>6s}{'thr':>5s}{'err':>5s}{'succ%':>7s}{'effRPM':>8s}{'iTPM':>8s}{'oTPM':>8s}{'p50ms':>8s}{'p95ms':>8s}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(f"{r['model']:16s}{r['path']:9s}{r['offered']:>5d}{r['success']:>6d}{r['throttle']:>5d}"
              f"{r['error']:>5d}{r['success_pct']:>6.0f}%{r['eff_rpm']:>8.0f}{r['eff_itpm']:>8.0f}"
              f"{r['eff_otpm']:>8.0f}{r['p50_ms']:>8.0f}{r['p95_ms']:>8.0f}")
    print(f"{'='*118}\n")


if __name__ == "__main__":
    main()
