#!/usr/bin/env python3
"""
Load Test - Direct Bedrock Calls WITH Exponential Backoff + Jitter

This is the "customer status quo" baseline — what most teams implement today.
Compare results against the Traffic Shaper (test_budget_manager.py) to show:
  - Retries waste quota (each retry consumes a request slot)
  - Thundering herd: jittered retries still cluster, causing cascading throttles
  - Higher tail latency (p99) due to retry delays
  - Still not 100% success under sustained spikes

Usage:
  python scripts/test_direct_bedrock_retry.py --model nova-2-lite --num-requests 50
  python scripts/test_direct_bedrock_retry.py --model nova-lite --num-requests 200 --max-workers 20
"""

import boto3
import json
import time
import sys
import os
import random
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from botocore.config import Config
import config_loader

# Add lambda layer to Python path to import shared_service
layer_path = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'lambda_layer', 'python')
sys.path.insert(0, layer_path)

from shared_service import DynamoService

# Model ID aliases
MODEL_ALIASES = {
    'opus-5': 'us.anthropic.claude-opus-5',
    'sonnet-5': 'us.anthropic.claude-sonnet-5',
    'nova-2-lite': 'us.amazon.nova-2-lite-v1:0',
    'nova-lite': 'us.amazon.nova-lite-v1:0',
    'nova-lite-sr': 'amazon.nova-lite-v1:0',  # single-region: enforces per-region quotas
    'nova-pro': 'us.amazon.nova-pro-v1:0',
}

# Retry configuration (typical customer implementation)
MAX_RETRIES = 3
BASE_DELAY = 1.0       # seconds
MAX_DELAY = 30.0        # cap for exponential growth
JITTER_RANGE = 1.0      # full jitter: uniform(0, computed_delay)


def resolve_model_id(model_input: str) -> str:
    """Resolve model alias to full model ID."""
    return MODEL_ALIASES.get(model_input.lower(), model_input)


def validate_model_config(dynamo_service: DynamoService, model_id: str) -> None:
    """Validate that model config exists in DynamoDB."""
    try:
        config = dynamo_service.get_model_config(model_id)
        print(f"  Model config found: {model_id}")
        print(f"  Burst capacity: {config.get('burst_capacity')}")
        print(f"  RPM limit: {config.get('rpm_limit')}")
    except KeyError:
        print(f"  Model config not found: {model_id}")
        print(f"\nCreate config first:")
        print(f"  make create-config MODEL={model_id}")
        sys.exit(1)
    except Exception as e:
        print(f"  Error validating model config: {e}")
        sys.exit(1)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Load test direct Bedrock calls WITH retry+jitter (customer baseline)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This test simulates the typical customer retry pattern:
  - Exponential backoff: delay = base * 2^attempt
  - Full jitter: actual_delay = random(0, computed_delay)
  - Max 3 retries per request

Compare results against test_budget_manager.py (Traffic Shaper) to show
that retry+jitter still loses requests under load spikes, while the
Traffic Shaper achieves 100%% success by queuing instead of retrying.

Examples:
  python scripts/test_direct_bedrock_retry.py --model nova-2-lite --num-requests 50
  python scripts/test_direct_bedrock_retry.py --model nova-lite --num-requests 200 --max-workers 20
        """
    )

    parser.add_argument('--model', type=str, help='Model ID or alias (nova-2-lite, sonnet-5, opus-5)')
    parser.add_argument('--num-requests', type=int, help='Number of requests (default: config.env)')
    parser.add_argument('--max-workers', type=int, help='Concurrent threads (default: config.env)')
    parser.add_argument('--submission-duration', type=int, help='Submission spread in seconds (default: config.env)')
    parser.add_argument('--max-retries', type=int, default=MAX_RETRIES, help=f'Max retries per request (default: {MAX_RETRIES})')
    parser.add_argument('--prompt-size', type=int, default=None, help='Prompt size in chars (for TPM testing)')
    parser.add_argument('--max-tokens', type=int, default=20, help='max_tokens per request (default: 20)')

    return parser.parse_args()


# Load configuration and verify AWS access
config = config_loader.get_config_with_aws_check()
args = parse_args()

# Apply CLI overrides or use config.env defaults
AWS_REGION = config.get('AWS_REGION', 'us-east-1')
BEDROCK_MODEL_ID = resolve_model_id(args.model) if args.model else config.get('BEDROCK_MODEL_ID', 'us.amazon.nova-2-lite-v1:0')
SINGLE_TABLE_NAME = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')
NUM_REQUESTS = args.num_requests if args.num_requests else int(config.get('NUM_REQUESTS', '125'))
MAX_WORKERS = args.max_workers if args.max_workers else int(config.get('MAX_WORKERS', '10'))
SUBMISSION_DURATION = args.submission_duration if args.submission_duration is not None else int(config.get('SUBMISSION_DURATION', '10'))
MAX_RETRIES_ACTUAL = args.max_retries
PROMPT_SIZE = args.prompt_size
MAX_TOKENS = args.max_tokens

# Validate model config exists
dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)
validate_model_config(dynamo_service, BEDROCK_MODEL_ID)


def build_prompt(request_num, prompt_size=None):
    """Build a prompt of the requested size."""
    base = f'Say "Request {request_num} completed"'
    if prompt_size and prompt_size > len(base):
        padding = ' This is padding text for TPM validation testing.'
        reps = (prompt_size - len(base)) // len(padding) + 1
        base = (base + padding * reps)[:prompt_size]
    return base


def make_bedrock_call_with_retry(bedrock, request_num):
    """
    Make a single Bedrock call with exponential backoff + full jitter.

    This is the standard pattern customers implement:
      delay = min(MAX_DELAY, BASE_DELAY * 2^attempt)
      actual_delay = random.uniform(0, delay)  # full jitter

    Returns dict with request_num, success, error, attempts, total_time, retry_delays
    """
    start_time = time.time()
    attempts = 0
    retry_delays = []
    last_error = None

    prompt = build_prompt(request_num, PROMPT_SIZE)

    for attempt in range(MAX_RETRIES_ACTUAL + 1):  # +1 for initial attempt
        attempts += 1
        try:
            bedrock.converse(
                modelId=BEDROCK_MODEL_ID,
                messages=[
                    {
                        'role': 'user',
                        'content': [{'text': prompt}]
                    }
                ],
                inferenceConfig={
                    'maxTokens': MAX_TOKENS
                }
            )

            total_time = time.time() - start_time
            return {
                'request_num': request_num,
                'success': True,
                'error': None,
                'attempts': attempts,
                'total_time': total_time,
                'retry_delays': retry_delays
            }

        except ClientError as e:
            error_code = e.response['Error']['Code']
            is_throttle = error_code in ['ThrottlingException', 'TooManyRequestsException',
                                         'ServiceQuotaExceededException']
            last_error = '429' if is_throttle else error_code

            if is_throttle and attempt < MAX_RETRIES_ACTUAL:
                # Exponential backoff with full jitter
                computed_delay = min(MAX_DELAY, BASE_DELAY * (2 ** attempt))
                actual_delay = random.uniform(0, computed_delay)  # nosec B311  # non-crypto: backoff jitter
                retry_delays.append(actual_delay)
                time.sleep(actual_delay)  # nosemgrep: arbitrary-sleep -- deliberate exponential backoff on Bedrock throttle
            else:
                # Non-throttle error or max retries exceeded
                break

        except Exception as e:
            last_error = str(e)
            break

    total_time = time.time() - start_time
    return {
        'request_num': request_num,
        'success': False,
        'error': last_error,
        'attempts': attempts,
        'total_time': total_time,
        'retry_delays': retry_delays
    }


def test_direct_bedrock_with_retry():
    """
    Make direct Bedrock calls with exponential backoff + jitter.
    This is the customer baseline — compare against Traffic Shaper results.
    """
    print(f"\n{'='*60}")
    print(f"DIRECT BEDROCK TEST (Retry + Jitter Baseline)")
    print(f"{'='*60}")
    print(f"Model: {BEDROCK_MODEL_ID}")
    print(f"Total requests: {NUM_REQUESTS}")
    print(f"Concurrency: {MAX_WORKERS} threads")
    print(f"Max retries: {MAX_RETRIES_ACTUAL} (backoff: {BASE_DELAY}s * 2^attempt, jitter: full)")
    print(f"Prompt size: {PROMPT_SIZE or '~30'} chars, max_tokens: {MAX_TOKENS}")

    if SUBMISSION_DURATION > 0:
        submission_rate = NUM_REQUESTS / SUBMISSION_DURATION
        print(f"Submission: {SUBMISSION_DURATION}s duration ({submission_rate:.1f} req/s)")
    else:
        print(f"Submission: Instant spike (all at once)")

    print(f"{'='*60}\n")

    # Initialize client — disable SDK built-in retries (we handle them)
    config_obj = Config(retries={'max_attempts': 1, 'mode': 'standard'})
    bedrock = boto3.client('bedrock-runtime', region_name=AWS_REGION, config=config_obj)

    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        delay_between_requests = SUBMISSION_DURATION / NUM_REQUESTS if SUBMISSION_DURATION > 0 else 0

        for i in range(NUM_REQUESTS):
            future = executor.submit(make_bedrock_call_with_retry, bedrock, i)
            futures.append(future)

            if delay_between_requests > 0:
                time.sleep(delay_between_requests)
                print(f"  Submitted {i+1}/{NUM_REQUESTS} ({(i+1)/NUM_REQUESTS*100:.0f}%)", end='\r')

        if SUBMISSION_DURATION > 0:
            print(f"\n\nAll requests submitted. Waiting for completion...")

        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1

            status = f"ok ({result['attempts']} attempts)" if result['success'] else f"FAIL {result['error']} ({result['attempts']} attempts)"
            print(f"  Completed {completed}/{NUM_REQUESTS}: {status}", end='\r')

    total_time = time.time() - start_time

    # Calculate statistics
    success_count = sum(1 for r in results if r['success'])
    fail_count = NUM_REQUESTS - success_count
    throttle_fail_count = sum(1 for r in results if not r['success'] and r['error'] == '429')
    other_fail_count = fail_count - throttle_fail_count

    total_attempts = sum(r['attempts'] for r in results)
    total_retries = total_attempts - NUM_REQUESTS
    wasted_retries = sum(r['attempts'] - 1 for r in results if not r['success'])
    successful_retries = sum(r['attempts'] - 1 for r in results if r['success'] and r['attempts'] > 1)

    # Latency stats
    times = [r['total_time'] for r in results]
    times.sort()
    p50 = times[len(times) // 2] if times else 0
    p95 = times[int(len(times) * 0.95)] if times else 0
    p99 = times[int(len(times) * 0.99)] if times else 0

    success_times = sorted([r['total_time'] for r in results if r['success']])
    success_p50 = success_times[len(success_times) // 2] if success_times else 0
    success_p99 = success_times[int(len(success_times) * 0.99)] if success_times else 0

    retry_times = [d for r in results for d in r['retry_delays']]

    print(f"\n\n{'='*60}")
    print(f"RESULTS: Direct Bedrock with Retry + Jitter")
    print(f"{'='*60}")
    print(f"")
    print(f"  Success Rate")
    print(f"  {'─'*40}")
    print(f"  Total requests:       {NUM_REQUESTS}")
    print(f"  Successful:           {success_count} ({success_count/NUM_REQUESTS*100:.1f}%)")
    print(f"  Failed (throttled):   {throttle_fail_count} ({throttle_fail_count/NUM_REQUESTS*100:.1f}%)")
    print(f"  Failed (other):       {other_fail_count}")
    print(f"")
    print(f"  Retry Overhead")
    print(f"  {'─'*40}")
    print(f"  Total API calls:      {total_attempts} ({total_attempts/NUM_REQUESTS:.1f}x amplification)")
    print(f"  Total retries:        {total_retries}")
    print(f"  Retries that helped:  {successful_retries} (led to success)")
    print(f"  Retries wasted:       {wasted_retries} (still failed)")
    if retry_times:
        print(f"  Avg retry delay:      {sum(retry_times)/len(retry_times):.2f}s")
        print(f"  Total time in backoff:{sum(retry_times):.1f}s (across all threads)")
    print(f"")
    print(f"  Latency (all requests)")
    print(f"  {'─'*40}")
    print(f"  p50:                  {p50*1000:.0f}ms")
    print(f"  p95:                  {p95*1000:.0f}ms")
    print(f"  p99:                  {p99*1000:.0f}ms")
    if success_times:
        print(f"  p50 (success only):   {success_p50*1000:.0f}ms")
        print(f"  p99 (success only):   {success_p99*1000:.0f}ms")
    print(f"")
    print(f"  Timing")
    print(f"  {'─'*40}")
    print(f"  Total wall time:      {total_time:.1f}s")
    print(f"{'='*60}")

    # Summary comparison hint
    print(f"\n  Compare against Traffic Shaper:")
    print(f"  make test-budget-manager ARGS=\"--model {args.model or 'jamba'} --num-requests {NUM_REQUESTS} --max-workers {MAX_WORKERS}\"")

    if throttle_fail_count > 0:
        print(f"\n  {throttle_fail_count} requests exhausted all {MAX_RETRIES_ACTUAL} retries and still failed.")
        print(f"  The Traffic Shaper would queue these instead of dropping them.")
    elif success_count == NUM_REQUESTS and total_retries > 0:
        print(f"\n  All succeeded but required {total_retries} retries ({total_attempts/NUM_REQUESTS:.1f}x API call amplification).")
        print(f"  Each retry consumes quota, making throttling worse for other callers.")

    print()


if __name__ == "__main__":
    test_direct_bedrock_with_retry()
