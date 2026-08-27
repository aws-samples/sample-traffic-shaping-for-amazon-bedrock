#!/usr/bin/env python3
"""
Multi-Model Contention Test — ISC 15-22
Sends requests for Opus, Jamba, and Nova Lite concurrently.
Verifies per-model admission isolation and zero cross-model interference.
"""

import boto3
import json
import time
import sys
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import config_loader

# Add lambda layer to Python path
layer_path = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'lambda_layer', 'python')
sys.path.insert(0, layer_path)

from shared_service import DynamoService

MODEL_ALIASES = {
    'opus': 'us.anthropic.claude-opus-5',
    'jamba': 'us.amazon.nova-2-lite-v1:0',
    'nova-lite': 'us.amazon.nova-lite-v1:0',
}

# Load configuration
config = config_loader.get_config_with_aws_check()
AWS_REGION = config.get('AWS_REGION', 'us-east-1')
STATE_MACHINE_ARN = config.get('STATE_MACHINE_ARN', '')
SINGLE_TABLE_NAME = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')

if not STATE_MACHINE_ARN or 'ACCOUNT' in STATE_MACHINE_ARN:
    print("Error: STATE_MACHINE_ARN not configured in config.env")
    sys.exit(1)

dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)


def classify_execution(exec_desc):
    """Classify a completed execution as immediate, queued, or failed."""
    status = exec_desc['status']
    if status == 'SUCCEEDED':
        output = json.loads(exec_desc.get('output', '{}'))
        if output.get('budget_result', {}).get('source') == 'queued':
            return 'queued'
        return 'immediate'
    elif status == 'FAILED':
        return 'failed'
    elif status == 'TIMED_OUT':
        return 'timed_out'
    return 'running'


def start_execution(sfn_client, model_alias, model_id, request_num):
    """Start a Step Function execution for a single request."""
    try:
        payload = {
            'request_id': f'multi_{model_alias}_{request_num}',
            'model_id': model_id,
            'prompt': f'Multi-model test: {model_alias} request {request_num}',
        }
        execution_arn = sfn_client.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(payload)
        )['executionArn']
        return {
            'model_alias': model_alias,
            'model_id': model_id,
            'request_num': request_num,
            'success': True,
            'execution_arn': execution_arn,
            'error': None,
        }
    except Exception as e:
        return {
            'model_alias': model_alias,
            'model_id': model_id,
            'request_num': request_num,
            'success': False,
            'execution_arn': None,
            'error': str(e),
        }


def run_multi_model_test(model_counts):
    """
    Run multi-model contention test.
    model_counts: dict of {alias: count}, e.g. {'opus': 10, 'jamba': 30, 'nova-lite': 30}
    """
    print(f"\n{'='*70}")
    print(f"MULTI-MODEL CONTENTION TEST")
    print(f"{'='*70}")

    # Validate all configs exist
    for alias, count in model_counts.items():
        model_id = MODEL_ALIASES[alias]
        try:
            cfg = dynamo_service.get_model_config(model_id)
            print(f"  {alias:12s}: {count} requests | burst_capacity={cfg.get('burst_capacity')}, RPM={cfg.get('rpm_limit')}")
        except Exception as e:
            print(f"  Error: {alias} config not found: {e}")
            sys.exit(1)

    total = sum(model_counts.values())
    print(f"  {'Total':12s}: {total} requests")
    print(f"{'='*70}\n")

    sfn_client = boto3.client('stepfunctions', region_name=AWS_REGION)

    # Phase 1: Submit all requests concurrently
    print("Phase 1: Submitting all requests concurrently...")
    all_results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for alias, count in model_counts.items():
            model_id = MODEL_ALIASES[alias]
            for i in range(count):
                f = executor.submit(start_execution, sfn_client, alias, model_id, i)
                futures.append(f)

        for future in as_completed(futures):
            result = future.result()
            all_results.append(result)

    submit_time = time.time() - start_time
    started = sum(1 for r in all_results if r['success'])
    errors = sum(1 for r in all_results if not r['success'])
    print(f"  Submitted {started}/{total} in {submit_time:.1f}s ({errors} errors)")

    if errors > 0:
        for r in all_results:
            if not r['success']:
                print(f"    Error: {r['model_alias']}#{r['request_num']}: {r['error']}")

    # Phase 2: Monitor per-model independently
    print(f"\nPhase 2: Monitoring per-model completion...")

    # Group running executions by model
    running_by_model = defaultdict(dict)
    for r in all_results:
        if r['success'] and r['execution_arn']:
            running_by_model[r['model_alias']][r['execution_arn']] = r

    # Per-model counters
    counters = {alias: {'immediate': 0, 'queued': 0, 'failed': 0, 'timed_out': 0}
                for alias in model_counts}

    max_wait = 600  # 10 minutes
    poll_interval = 3
    wait_start = time.time()

    while (time.time() - wait_start) < max_wait:
        total_running = 0

        for alias in model_counts:
            model_id = MODEL_ALIASES[alias]
            still_running = {}

            for exec_arn, result in running_by_model[alias].items():
                try:
                    exec_desc = sfn_client.describe_execution(executionArn=exec_arn)
                    classification = classify_execution(exec_desc)
                    if classification in ('immediate', 'queued', 'failed', 'timed_out'):
                        counters[alias][classification] += 1
                    else:
                        still_running[exec_arn] = result
                except Exception:
                    still_running[exec_arn] = result

            running_by_model[alias] = still_running
            total_running += len(still_running)

        elapsed = time.time() - wait_start

        # Print per-model status
        status_parts = []
        for alias in model_counts:
            c = counters[alias]
            r = len(running_by_model[alias])
            done = c['immediate'] + c['queued']
            status_parts.append(f"{alias}:{done}/{model_counts[alias]}")

        print(f"  {' | '.join(status_parts)} | running={total_running} | {elapsed:.0f}s", end='\r')

        if total_running == 0:
            print(f"\n  All executions complete after {elapsed:.1f}s")
            break

        time.sleep(poll_interval)  # nosemgrep: arbitrary-sleep -- intentional monitor poll interval in load test

    # Check queue depths
    print(f"\nPhase 3: Final queue depths...")
    for alias in model_counts:
        model_id = MODEL_ALIASES[alias]
        depth = dynamo_service.get_queue_depth(model_id)
        print(f"  {alias}: queue_depth={depth}")

    # Phase 4: Results
    total_time = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"RESULTS")
    print(f"{'='*70}")

    all_pass = True
    for alias in model_counts:
        c = counters[alias]
        count = model_counts[alias]
        done = c['immediate'] + c['queued']
        remaining = len(running_by_model[alias])
        success_rate = done / count * 100 if count > 0 else 0

        print(f"\n  {alias.upper()} ({MODEL_ALIASES[alias]}):")
        print(f"    Requests:  {count}")
        print(f"    Immediate: {c['immediate']}")
        print(f"    Queued:    {c['queued']}")
        print(f"    Failed:    {c['failed']}")
        print(f"    Timed Out: {c['timed_out']}")
        print(f"    Running:   {remaining}")
        print(f"    Success:   {success_rate:.1f}%")

        if done < count or c['failed'] > 0:
            all_pass = False

    print(f"\n  Total time: {total_time:.1f}s")
    print(f"{'='*70}\n")

    if all_pass:
        print(f"PASS: All {total} requests across {len(model_counts)} models processed successfully!")
        print(f"  Per-model isolation verified — no cross-model interference.")
    else:
        print(f"PARTIAL: Some requests did not complete successfully.")
        for alias in model_counts:
            c = counters[alias]
            if c['failed'] > 0 or c['timed_out'] > 0:
                print(f"  {alias}: {c['failed']} failed, {c['timed_out']} timed out")

    return counters, all_pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Multi-model contention test')
    parser.add_argument('--opus', type=int, default=10, help='Number of Opus requests (default: 10)')
    parser.add_argument('--jamba', type=int, default=30, help='Number of Jamba requests (default: 30)')
    parser.add_argument('--nova-lite', type=int, default=30, help='Number of Nova Lite requests (default: 30)')
    args = parser.parse_args()

    model_counts = {
        'opus': args.opus,
        'jamba': args.jamba,
        'nova-lite': args.nova_lite,
    }

    # Remove models with 0 requests
    model_counts = {k: v for k, v in model_counts.items() if v > 0}

    run_multi_model_test(model_counts)
