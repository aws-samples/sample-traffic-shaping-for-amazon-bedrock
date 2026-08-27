#!/usr/bin/env python3
"""
Post-Soak Analysis — Extract key metrics from soak test results JSON.

Focuses on the 5 blind-spot metrics from PRODUCTION-HARDENING-PLAN.md:
1. Counter drift: reconciliation sweep corrections trend
2. TTL cleanup: DDB item count trend
3. DDB partition behavior: consumed capacity trend
4. Memory: Lambda max memory used
5. Queue drain consistency: processing rate stability

Usage:
  python scripts/analyze_soak_results.py soak_results_72hr_*.json
  python scripts/analyze_soak_results.py --cloudwatch --hours 72   # Pull CW metrics directly
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3

# Add lambda layer
layer_path = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'lambda_layer', 'python')
sys.path.insert(0, layer_path)

import config_loader


def analyze_results_file(filepath):
    """Analyze a soak test results JSON file."""
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    print(f"\n{'=' * 70}")
    print(f"SOAK TEST ANALYSIS: {filepath}")
    print(f"{'=' * 70}")

    # Basic stats
    print(f"\n--- Summary ---")
    print(f"  Duration:       {data['duration_hours']:.2f} hours")
    print(f"  Total sent:     {data['total_sent']}")
    print(f"  Valid requests: {data['total_valid']}")
    print(f"  Succeeded:      {data['succeeded']}")
    print(f"  Queued:         {data['queued']}")
    print(f"  Failed:         {data['failed']}")
    print(f"  Rejected:       {data['rejected']}")
    print(f"  Success rate:   {data['success_rate']:.2f}%")
    print(f"  Effective RPM:  {data['effective_rpm']:.1f}")

    # Latency
    print(f"\n--- Latency ---")
    print(f"  p50: {data['latency_p50']:.0f} ms")
    print(f"  p95: {data['latency_p95']:.0f} ms")
    print(f"  p99: {data['latency_p99']:.0f} ms")

    # Queue and DLQ
    print(f"\n--- Queue/DLQ ---")
    print(f"  Max queue depth: {data['max_queue_depth']}")
    print(f"  Max DLQ depth:   {data['max_dlq_depth']}")

    # Errors
    if data.get('errors'):
        print(f"\n--- Errors ---")
        for error_type, count in data['errors'].items():
            print(f"  {error_type}: {count}")

    # Checkpoint progression
    checkpoints = data.get('checkpoints', [])
    if checkpoints:
        print(f"\n--- Checkpoint Progression ({len(checkpoints)} checkpoints) ---")
        print(f"  {'Time':>8s}  {'Sent':>7s}  {'OK':>7s}  {'Fail':>5s}  {'Queue':>5s}  {'DLQ':>4s}  {'Rate%':>6s}  {'p50ms':>6s}")
        print(f"  {'─' * 60}")
        for cp in checkpoints:
            elapsed_h = cp['elapsed_min'] / 60
            ok = cp['succeeded'] + cp['queued']
            print(f"  {elapsed_h:7.1f}h  {cp['total_sent']:7d}  {ok:7d}  {cp['failed']:5d}  "
                  f"{cp['queue_depth']:5d}  {cp['dlq_depth']:4d}  {cp['success_rate']:5.1f}%  {cp['latency_p50']:6.0f}")

        # Stability analysis: is success rate consistent across checkpoints?
        rates = [cp['success_rate'] for cp in checkpoints if cp['total_sent'] > 100]
        if len(rates) >= 3:
            first_third = rates[:len(rates)//3]
            last_third = rates[-len(rates)//3:]
            avg_first = sum(first_third) / len(first_third)
            avg_last = sum(last_third) / len(last_third)
            drift = avg_last - avg_first
            print(f"\n  Stability: first-third avg={avg_first:.2f}%, last-third avg={avg_last:.2f}%, drift={drift:+.2f}%")
            if abs(drift) < 0.5:
                print(f"  Verdict: STABLE (drift < 0.5%)")
            elif drift < 0:
                print(f"  Verdict: DEGRADING (success rate declining)")
            else:
                print(f"  Verdict: IMPROVING (success rate increasing)")

    # Verdict
    print(f"\n--- Verdict ---")
    if data['success_rate'] >= 99.9 and data['max_dlq_depth'] == 0:
        print(f"  PASS: {data['success_rate']:.2f}% success, 0 DLQ")
    elif data['max_dlq_depth'] > 0 and data['success_rate'] >= 99.0:
        print(f"  QUALIFIED PASS: {data['success_rate']:.2f}% success, {data['max_dlq_depth']} DLQ (check if Bedrock transient)")
    else:
        print(f"  FAIL: {data['success_rate']:.2f}% success, {data['max_dlq_depth']} DLQ")

    print(f"{'=' * 70}\n")


def analyze_cloudwatch(hours, region='us-east-1'):
    """Pull CloudWatch metrics for the soak test period."""
    config = config_loader.get_config_with_aws_check()
    cw = boto3.client('cloudwatch', region_name=region)
    logs_client = boto3.client('logs', region_name=region)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)

    print(f"\n{'=' * 70}")
    print(f"CLOUDWATCH ANALYSIS ({hours}h lookback)")
    print(f"{'=' * 70}")
    print(f"  Period: {start_time.isoformat()} to {end_time.isoformat()}")

    # 1. DDB consumed capacity
    print(f"\n--- 1. DynamoDB Consumed Capacity ---")
    for metric_name in ['ConsumedWriteCapacityUnits', 'ConsumedReadCapacityUnits']:
        try:
            response = cw.get_metric_statistics(
                Namespace='AWS/DynamoDB',
                MetricName=metric_name,
                Dimensions=[{'Name': 'TableName', 'Value': 'semaphore-single-table'}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1-hour buckets
                Statistics=['Sum', 'Maximum'],
            )
            datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
            if datapoints:
                first_hour = datapoints[0]['Sum']
                last_hour = datapoints[-1]['Sum']
                max_val = max(dp['Maximum'] for dp in datapoints)
                print(f"  {metric_name}:")
                print(f"    First hour sum: {first_hour:.0f}")
                print(f"    Last hour sum:  {last_hour:.0f}")
                print(f"    Max in any hour: {max_val:.0f}")
                print(f"    Trend: {'STABLE' if abs(last_hour - first_hour) / max(1, first_hour) < 0.2 else 'CHANGING'}")
            else:
                print(f"  {metric_name}: No data")
        except Exception as e:
            print(f"  {metric_name}: Error: {e}")

    # 3. DDB throttles
    print(f"\n--- 3. DynamoDB Throttles ---")
    for metric_name in ['WriteThrottleEvents', 'ReadThrottleEvents']:
        try:
            response = cw.get_metric_statistics(
                Namespace='AWS/DynamoDB',
                MetricName=metric_name,
                Dimensions=[{'Name': 'TableName', 'Value': 'semaphore-single-table'}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum'],
            )
            total = sum(dp['Sum'] for dp in response['Datapoints'])
            print(f"  {metric_name}: {total:.0f} total")
        except Exception as e:
            print(f"  {metric_name}: Error: {e}")

    # 4. Lambda memory
    print(f"\n--- 4. Lambda Memory Usage ---")
    for func_name, log_group_key in [
        ('Budget Manager', 'BUDGET_MANAGER_LOG_GROUP'),
        ('Queue Processor', 'QUEUE_PROCESSOR_LOG_GROUP'),
        ('Bedrock Processor', 'BEDROCK_PROCESSOR_LOG_GROUP'),
    ]:
        log_group = config.get(log_group_key, '')
        if not log_group:
            continue
        try:
            query = """
                filter @type = "REPORT"
                | stats max(@maxMemoryUsed / 1048576) as max_mb,
                        avg(@maxMemoryUsed / 1048576) as avg_mb,
                        count(*) as invocations
            """
            response = logs_client.start_query(
                logGroupName=log_group,
                startTime=int(start_time.timestamp()),
                endTime=int(end_time.timestamp()),
                queryString=query,
            )
            import time
            for _ in range(30):
                result = logs_client.get_query_results(queryId=response['queryId'])
                if result['status'] == 'Complete':
                    break
                time.sleep(1)  # nosemgrep: arbitrary-sleep -- poll interval for CloudWatch Logs Insights query completion

            if result.get('results') and result['results'][0]:
                fields = {f['field']: f['value'] for f in result['results'][0]}
                print(f"  {func_name}: max={fields.get('max_mb', '?')}MB, avg={fields.get('avg_mb', '?')}MB, invocations={fields.get('invocations', '?')}")
            else:
                print(f"  {func_name}: No REPORT data")
        except Exception as e:
            print(f"  {func_name}: Error: {e}")

    # 5. DDB item count trend
    print(f"\n--- 5. DynamoDB Item Count ---")
    try:
        response = cw.get_metric_statistics(
            Namespace='AWS/DynamoDB',
            MetricName='ItemCount',
            Dimensions=[{'Name': 'TableName', 'Value': 'semaphore-single-table'}],
            StartTime=start_time,
            EndTime=end_time,
            Period=21600,  # 6-hour buckets
            Statistics=['Maximum'],
        )
        datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
        if datapoints:
            for dp in datapoints:
                print(f"  {dp['Timestamp'].strftime('%Y-%m-%d %H:%M')}: {dp['Maximum']:.0f} items")
            first = datapoints[0]['Maximum']
            last = datapoints[-1]['Maximum']
            if last > first * 1.5:
                print(f"  WARNING: Item count growing ({first:.0f} -> {last:.0f}). TTL may not be cleaning up fast enough.")
            else:
                print(f"  TTL cleanup: OK (items not accumulating)")
        else:
            print(f"  No ItemCount data (metric updates every ~6 hours)")
    except Exception as e:
        print(f"  Error: {e}")

    # 6. BedrockShaper custom metrics
    print(f"\n--- 6. BedrockShaper Custom Metrics ---")
    for metric_name in ['CircuitBreakerTripped', 'OrphanedRecordsSwept', 'BurstCapacityExceeded']:
        try:
            response = cw.get_metric_statistics(
                Namespace='BedrockShaper',
                MetricName=metric_name,
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum'],
            )
            total = sum(dp['Sum'] for dp in response['Datapoints'])
            print(f"  {metric_name}: {total:.0f} total")
        except Exception as e:
            print(f"  {metric_name}: Error: {e}")

    print(f"\n{'=' * 70}\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze soak test results')
    parser.add_argument('results_file', nargs='?', help='Soak results JSON file')
    parser.add_argument('--cloudwatch', action='store_true', help='Pull CloudWatch metrics')
    parser.add_argument('--hours', type=int, default=72, help='CloudWatch lookback hours (default: 72)')
    args = parser.parse_args()

    if args.results_file:
        analyze_results_file(args.results_file)

    if args.cloudwatch or not args.results_file:
        analyze_cloudwatch(args.hours)

    if not args.results_file and not args.cloudwatch:
        print("Usage:")
        print("  python scripts/analyze_soak_results.py soak_results_72hr_*.json")
        print("  python scripts/analyze_soak_results.py --cloudwatch --hours 72")
        print("  python scripts/analyze_soak_results.py soak_results.json --cloudwatch")
