#!/usr/bin/env python3
"""
Analyze admission-gate TransactionConflict attribution.

The budget manager's put_allocation() emits a structured `transaction_cancellation`
log line for every cancelled TransactWriteItems, attributing the cancellation to the
specific transact item(s) that conflicted (TransactionConflict) or failed their
capacity condition (ConditionalCheckFailed). See _log_transaction_cancellation() in
shared_service/dynamo.py.

This script runs a CloudWatch Logs Insights query over the Budget Manager log group
and prints two histograms that settle the council's open question:

  1. shed_class breakdown — how many sheds were `contention` (retry exhaustion, the
     request SHOULD have been admitted) vs `cap_breach` (a genuine quota rejection,
     correctly queued). If most sheds are cap_breach, 3.58 req/s is the real quota and
     there is no contention bug. If most are contention, the hot-item serialization is
     the throttle.

  2. conflict-item histogram — which transact item (rate2s / tok2s / tpm_window /
     tpm_global / rpm_window / rpm_global / ...) is named as the conflicting item, and
     how often. This tells you WHETHER sharding RATE2S alone would help, or whether an
     unsharded TPM singleton is the true serializer (in which case sharding RATE2S is
     inert — the red team's objection).

Usage:
  make analyze-conflicts                       # last 30 min, final sheds only
  make analyze-conflicts ARGS="--minutes 120"  # wider window
  make analyze-conflicts ARGS="--all-attempts" # include non-final (retried) conflicts
"""

import sys
import os
import time
import argparse

import boto3
import config_loader

config = config_loader.get_config_with_aws_check()
AWS_REGION = config.get('AWS_REGION', 'us-east-1')
LOG_GROUP = config.get('BUDGET_MANAGER_LOG_GROUP')


def _run_insights_query(logs, log_group, query, start, end):
    """Start a Logs Insights query, poll to completion, return result rows."""
    start_resp = logs.start_query(
        logGroupName=log_group,
        startTime=int(start),
        endTime=int(end),
        queryString=query,
    )
    query_id = start_resp['queryId']
    while True:
        resp = logs.get_query_results(queryId=query_id)
        status = resp['status']
        if status in ('Complete', 'Failed', 'Cancelled', 'Timeout'):
            if status != 'Complete':
                raise RuntimeError(f"Logs Insights query {status}")
            return resp['results']
        time.sleep(1)


def _rows_to_dicts(results):
    """Convert Insights result rows ([{field,value}, ...]) to plain dicts."""
    out = []
    for row in results:
        out.append({cell['field']: cell['value'] for cell in row})
    return out


def _print_histogram(title, pairs, total):
    """pairs: list of (label, count) already sorted; total for pct."""
    print(f"\n{title}")
    print("-" * len(title))
    if not pairs:
        print("  (no matching log lines in window)")
        return
    width = max(len(label) for label, _ in pairs)
    for label, count in pairs:
        pct = (count / total * 100) if total else 0.0
        bar = "█" * int(pct / 2)  # 50 chars = 100%
        print(f"  {label:<{width}}  {count:>7,}  {pct:5.1f}%  {bar}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--minutes', type=int, default=30,
                        help='Look back this many minutes (default 30)')
    parser.add_argument('--all-attempts', action='store_true',
                        help='Include retried (non-final) conflicts, not just final sheds')
    parser.add_argument('--log-group', default=LOG_GROUP,
                        help='Override the Budget Manager log group name')
    args = parser.parse_args()

    log_group = args.log_group
    if not log_group:
        print("❌ No log group. Set BUDGET_MANAGER_LOG_GROUP in config.env or pass --log-group.")
        sys.exit(1)

    end = time.time()
    start = end - args.minutes * 60
    logs = boto3.client('logs', region_name=AWS_REGION)

    final_filter = "" if args.all_attempts else "| filter is_final = 1"
    scope = "all attempts (incl. retried)" if args.all_attempts else "final sheds only"

    print(f"Log group : {log_group}")
    print(f"Window    : last {args.minutes} min")
    print(f"Scope     : {scope}")

    # 1) shed_class breakdown: contention vs cap_breach vs unknown.
    class_query = f"""
fields shed_class
| filter log_type = "transaction_cancellation"
{final_filter}
| stats count(*) as n by shed_class
| sort n desc
"""
    class_rows = _rows_to_dicts(_run_insights_query(logs, log_group, class_query, start, end))
    class_pairs = [(r['shed_class'], int(r['n'])) for r in class_rows]
    class_total = sum(n for _, n in class_pairs)
    _print_histogram("shed_class breakdown (contention = should've admitted; "
                     "cap_breach = real quota)", class_pairs, class_total)

    # 2) conflict-item histogram: which transact item is named as conflicting.
    #    conflict_items is a JSON array on the log record; unnest via the parsed field.
    item_query = f"""
fields log_type, is_final
| filter log_type = "transaction_cancellation"
{final_filter}
| parse conflict_items "*" as conflict_items_raw
| stats count(*) as n by conflict_items_raw
| sort n desc
| limit 50
"""
    item_rows = _rows_to_dicts(_run_insights_query(logs, log_group, item_query, start, end))
    item_pairs = [(r.get('conflict_items_raw', '(none)') or '(none)', int(r['n']))
                  for r in item_rows]
    item_total = sum(n for _, n in item_pairs)
    _print_histogram("conflicting transact item(s) — the serializer attribution",
                     item_pairs, item_total)

    print("\nInterpretation:")
    print("  • Mostly cap_breach  → 3.58 req/s is the configured quota, not a bug.")
    print("  • Mostly contention  → hot-item serialization is the throttle.")
    print("  • conflict item = rate2s only        → sharding RATE2S recovers throughput.")
    print("  • conflict item includes tpm_window/ → unsharded TPM singleton is the")
    print("    tpm_global/tok2s                      serializer; sharding RATE2S is inert.")
    print("\n(Raw JSON lines: filter the log group on log_type=\"transaction_cancellation\".)")


if __name__ == '__main__':
    main()
