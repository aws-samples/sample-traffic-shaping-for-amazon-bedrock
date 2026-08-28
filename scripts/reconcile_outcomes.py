#!/usr/bin/env python3
"""
Reconcile Outcomes — honest success accounting for a load-test run (OBJ2).

Honest-outcomes model: docs/solution/architecture.md §8.

WHY THIS EXISTS
  An HTTP 200 from POST /invoke means "the state machine started", NOT "the request
  succeeded" — the AwsIntegration maps StartExecution to 200 unconditionally and every
  real terminal outcome commits AFTER that 200. Scoring on the HTTP column flatters the
  shaper (a run that was 88.6% end-to-end once reported ~100%). This tool scores on the
  TERMINAL outcome instead, reconciled by the client-minted correlation_id.

THE DENOMINATOR IS WHAT THE HARNESS SENT
  True success = succeeded / N, where N includes ingress_lost + still-pending. A
  correlation_id the harness sent that has NO 202 and NO terminal record is
  `ingress_lost` — a failure bucket, not an omission. This closes the
  denominator-omission that would otherwise let the shaper silently drop its own
  front-door failures (StartExecution throttle, WAF 403) out of the denominator.

INPUT — the run manifest (what was SENT)
  A JSON file the load run emits: a list of {correlation_id, request_id, submit_ts}
  (or {"requests": [ ... ]}). correlation_id is required; request_id/submit_ts optional.
  When request_id is absent, this tool resolves it from the RequestOutcome EMF (which
  carries correlation_id as a property) before querying the terminal-status items.

TERMINAL OUTCOMES — two independent sources, either or both
  1. DynamoDB status items: pk=REQUEST#{request_id}, sk=STATUS on semaphore-single-table
     (state + reason + http_status written by the OBJ3 terminal layer).
  2. RequestOutcome EMF via CloudWatch (namespace BedrockShaper) — the `outcome`
     dimension, correlated by the correlation_id property.

READ-ONLY. GetItem / Query / CloudWatch GetMetricData + Logs Insights only. No writes,
no deletes — safe against production (see rules/amazon-production-safety).

USAGE
  python scripts/reconcile_outcomes.py manifest.json
  python scripts/reconcile_outcomes.py manifest.json --source ddb
  python scripts/reconcile_outcomes.py manifest.json --source emf --hours 2
  python scripts/reconcile_outcomes.py manifest.json --region us-east-1 --table semaphore-single-table
"""

import argparse
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import boto3

# The honest outcome enum (design §OBJ3 HTTP mapping + §OBJ1 excluded buckets + Cato C-1).
# Order = display order. `succeeded` is the ONLY success bucket.
OUTCOMES = [
    "succeeded",
    "throttled",          # Bedrock 429
    "error",              # generic 503 (incl. Lambda.TooManyRequestsException — NOT throttled)
    "timed_out",          # SM TimedOut → 504
    "queue_expired",      # queue-TTL expiry → 504
    "deadline",           # client-side wall-clock abort (excluded bucket, OBJ1)
    "edge_throttled",     # WAF 403/429 at the edge (excluded bucket, OBJ1)
    "ingress_throttled",  # StartExecution throttle → 429 (Cato C-1)
    "ingress_lost",       # sent, but no 202 and no terminal record — closes the denominator
    "pending",            # still PENDING/QUEUED at reconcile time (counts against N)
]

# CloudWatch reason -> outcome normalisation for the DDB `reason`/`state` fields.
DDB_STATE_TO_OUTCOME = {
    "SUCCEEDED": "succeeded",
    "FAILED": "error",       # refined by `reason` below
    "PENDING": "pending",
    "QUEUED": "pending",
}
DDB_REASON_TO_OUTCOME = {
    "throttled": "throttled",
    "error": "error",
    "timed_out": "timed_out",
    "queue_expired": "queue_expired",
    "validation_error": "error",
}


def load_manifest(path):
    """Load {correlation_id, request_id, submit_ts} records. Accepts a bare list or {"requests": [...]}."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    records = data["requests"] if isinstance(data, dict) and "requests" in data else data
    if not isinstance(records, list):
        sys.exit(f"❌ manifest must be a JSON list (or {{'requests': [...]}}); got {type(records).__name__}")
    out = []
    for i, r in enumerate(records):
        if not isinstance(r, dict) or "correlation_id" not in r:
            print(f"  ⚠ manifest entry {i} missing correlation_id — treated as malformed")
            out.append({"correlation_id": None, "request_id": None, "submit_ts": None, "_malformed": True})
            continue
        out.append({
            "correlation_id": r.get("correlation_id"),
            "request_id": r.get("request_id"),
            "submit_ts": r.get("submit_ts"),
            "_malformed": False,
        })
    return out


def dedup_manifest(records, abort_threshold_pct=1.0):
    """
    Fail-safe dedup: drop duplicate + malformed correlation_ids, report the dropped
    rate, abort ONLY if it exceeds ~1% (below that it is noise; above it the id-minting
    is broken and the run is void — see README §C).
    """
    total = len(records)
    seen = set()
    kept, dropped_dup, dropped_malformed = [], 0, 0
    for r in records:
        cid = r["correlation_id"]
        if r["_malformed"] or cid is None:
            dropped_malformed += 1
            continue
        if cid in seen:
            dropped_dup += 1
            continue
        seen.add(cid)
        kept.append(r)

    dropped = dropped_dup + dropped_malformed
    dropped_pct = (dropped / total * 100.0) if total else 0.0
    print(f"\n--- Dedup (fail-safe) ---")
    print(f"  Manifest entries: {total}")
    print(f"  Dropped duplicates: {dropped_dup}")
    print(f"  Dropped malformed:  {dropped_malformed}")
    print(f"  Dropped rate:       {dropped_pct:.3f}%")
    if dropped_pct > abort_threshold_pct:
        sys.exit(
            f"❌ ABORT: dropped rate {dropped_pct:.3f}% exceeds {abort_threshold_pct}% — "
            f"correlation_id minting is broken (UUID-salt the JMX, see README §C). Run is void."
        )
    print(f"  Verdict: OK (dropped rate ≤ {abort_threshold_pct}%)")
    return kept


def outcome_from_ddb_item(item):
    """Map a status item (state + reason) to an honest outcome bucket."""
    state = item.get("state")
    reason = item.get("reason")
    if state == "FAILED" and reason in DDB_REASON_TO_OUTCOME:
        return DDB_REASON_TO_OUTCOME[reason]
    return DDB_STATE_TO_OUTCOME.get(state, "error")


def reconcile_via_ddb(records, table_name, region):
    """
    Query the terminal-status item for each record (pk=REQUEST#{request_id}, sk=STATUS).
    Read-only GetItem. Records with no request_id or no item → ingress_lost.
    Returns {correlation_id: outcome}.
    """
    ddb = boto3.client("dynamodb", region_name=region)
    result = {}
    missing_rid = 0
    for r in records:
        cid, rid = r["correlation_id"], r.get("request_id")
        if not rid:
            missing_rid += 1
            result[cid] = "ingress_lost"
            continue
        try:
            resp = ddb.get_item(
                TableName=table_name,
                Key={"pk": {"S": f"REQUEST#{rid}"}, "sk": {"S": "STATUS"}},
                ConsistentRead=True,
            )
        except Exception as e:  # noqa: BLE001 — surface AWS errors per-item, keep going
            print(f"  ⚠ GetItem failed for {rid}: {e}")
            result[cid] = "error"
            continue
        item = resp.get("Item")
        if not item:
            result[cid] = "ingress_lost"
            continue
        plain = {k: list(v.values())[0] for k, v in item.items()}
        result[cid] = outcome_from_ddb_item(plain)
    if missing_rid:
        print(f"  ⚠ {missing_rid} records had no request_id (no 202 recorded) → ingress_lost")
    return result


def reconcile_via_emf(records, region, hours, log_group):
    """
    Reconcile via the RequestOutcome EMF using CloudWatch Logs Insights, correlating on
    the correlation_id property. Read-only start_query / get_query_results.
    Returns {correlation_id: outcome}. Any sent correlation_id not found → ingress_lost.
    """
    logs = boto3.client("logs", region_name=region)
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)
    # Pull every RequestOutcome emission in-window; match to the sent set locally.
    query = (
        "fields correlation_id, outcome, @timestamp "
        "| filter ispresent(correlation_id) and ispresent(outcome) "
        "| sort @timestamp desc "
        "| limit 10000"
    )
    try:
        started = logs.start_query(
            logGroupName=log_group,
            startTime=int(start.timestamp()),
            endTime=int(end.timestamp()),
            queryString=query,
        )
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠ EMF query could not start on log group {log_group!r}: {e}")
        return {r["correlation_id"]: "ingress_lost" for r in records}

    qid = started["queryId"]
    rows = []
    for _ in range(60):
        res = logs.get_query_results(queryId=qid)
        if res["status"] == "Complete":
            rows = res.get("results", [])
            break
        time.sleep(1)

    # Latest outcome wins (query is sorted desc, so first seen == latest).
    emf = {}
    for row in rows:
        fields = {f["field"]: f["value"] for f in row}
        cid = fields.get("correlation_id")
        if cid and cid not in emf:
            emf[cid] = fields.get("outcome", "error")

    result = {}
    for r in records:
        cid = r["correlation_id"]
        result[cid] = emf.get(cid, "ingress_lost")
    return result


def merge_sources(ddb_map, emf_map, records):
    """
    Prefer a terminal DDB outcome; fall back to EMF; else ingress_lost. DDB is the
    authoritative record (exactly-once conditional write); EMF backfills anything the
    item query missed.
    """
    merged = {}
    for r in records:
        cid = r["correlation_id"]
        d = ddb_map.get(cid) if ddb_map else None
        e = emf_map.get(cid) if emf_map else None
        if d and d not in ("ingress_lost", "pending"):
            merged[cid] = d
        elif e and e != "ingress_lost":
            merged[cid] = e
        elif d:
            merged[cid] = d
        elif e:
            merged[cid] = e
        else:
            merged[cid] = "ingress_lost"
    return merged


def report(outcomes_by_cid, n_sent):
    """Per-outcome breakdown with counts + percentages and the honest success rate."""
    counts = Counter(outcomes_by_cid.values())
    print(f"\n{'=' * 70}")
    print(f"HONEST OUTCOME RECONCILIATION")
    print(f"{'=' * 70}")
    print(f"  N (sent, denominator): {n_sent}")
    print(f"\n  {'outcome':<20s} {'count':>8s} {'pct':>8s}")
    print(f"  {'─' * 38}")
    for oc in OUTCOMES:
        c = counts.get(oc, 0)
        pct = (c / n_sent * 100.0) if n_sent else 0.0
        print(f"  {oc:<20s} {c:>8d} {pct:>7.3f}%")

    # Any outcome not in the known enum (defensive — new EMF value).
    unknown = {k: v for k, v in counts.items() if k not in OUTCOMES}
    for oc, c in unknown.items():
        pct = (c / n_sent * 100.0) if n_sent else 0.0
        print(f"  {oc + ' (unknown)':<20s} {c:>8d} {pct:>7.3f}%")

    succeeded = counts.get("succeeded", 0)
    honest_rate = (succeeded / n_sent * 100.0) if n_sent else 0.0
    print(f"\n  {'─' * 38}")
    print(f"  HONEST success rate = succeeded / N = {succeeded} / {n_sent} = {honest_rate:.3f}%")
    print(f"  (N includes ingress_lost + pending — the shaper cannot drop its own front-door failures)")
    print(f"{'=' * 70}\n")
    return honest_rate


def main():
    ap = argparse.ArgumentParser(description="Honest terminal-outcome reconciliation for a load-test run")
    ap.add_argument("manifest", help="Run manifest JSON: [{correlation_id, request_id, submit_ts}, ...]")
    ap.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    ap.add_argument("--table", default="semaphore-single-table", help="Status table (default: semaphore-single-table)")
    ap.add_argument("--source", choices=["ddb", "emf", "both"], default="both",
                    help="Terminal-outcome source (default: both — DDB authoritative, EMF backfill)")
    ap.add_argument("--log-group", default="/bedrock-shaper/request-outcome",
                    help="CloudWatch log group carrying RequestOutcome EMF (for --source emf/both)")
    ap.add_argument("--hours", type=int, default=2, help="EMF lookback window in hours (default: 2)")
    ap.add_argument("--dedup-abort-pct", type=float, default=1.0,
                    help="Abort if dropped correlation_id rate exceeds this pct (default: 1.0)")
    args = ap.parse_args()

    if not os.path.exists(args.manifest):
        sys.exit(f"❌ manifest not found: {args.manifest}")

    print(f"Reconciling {args.manifest} | region={args.region} | table={args.table} | source={args.source}")
    raw = load_manifest(args.manifest)
    records = dedup_manifest(raw, abort_threshold_pct=args.dedup_abort_pct)
    n_sent = len(records)

    ddb_map = emf_map = None
    if args.source in ("ddb", "both"):
        print(f"\n--- Querying DynamoDB status items (read-only GetItem) ---")
        ddb_map = reconcile_via_ddb(records, args.table, args.region)
    if args.source in ("emf", "both"):
        print(f"\n--- Querying RequestOutcome EMF (read-only Logs Insights, {args.hours}h) ---")
        emf_map = reconcile_via_emf(records, args.region, args.hours, args.log_group)

    merged = merge_sources(ddb_map, emf_map, records)
    report(merged, n_sent)


if __name__ == "__main__":
    main()
