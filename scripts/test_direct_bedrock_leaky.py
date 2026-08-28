#!/usr/bin/env python3
"""
Load Test - Direct Bedrock Calls with CLIENT-SIDE LEAKY BUCKET (token-bucket pacing)

This is the third baseline in the comparison matrix:
  1. No retry            (test_direct_bedrock.py)       — fire-and-drop
  2. Retry + jitter      (test_direct_bedrock_retry.py) — customer status quo
  3. Leaky bucket        (this script)                  — client-side pacing
  4. Traffic Shaper      (test_budget_manager.py)       — distributed semaphore

The leaky bucket paces outgoing requests to stay under the TPM (or RPM) quota
using a token bucket that refills at quota/60 per second. A request that cannot
be admitted immediately WAITS (leaks out) instead of being rejected — up to a
max-wait cap, after which it is counted as a failure.

Why it still falls short of the Traffic Shaper (the point of the comparison):
  - In-process only: no durability. If the client dies, in-flight/queued work is lost.
  - No cross-client coordination: N independent clients each think they own the
    full quota, so aggregate load still overshoots and throttles.
  - Local clock, not Bedrock's: the bucket estimates tokens; it does not observe
    Bedrock's actual regenerating quota, so it must run conservatively (leaving
    headroom on the table) or risk drift-induced throttles at window edges.
  - The wait cap converts sustained overload back into dropped requests.

Usage:
  python scripts/test_direct_bedrock_leaky.py --model nova-lite --num-requests 1000 \
    --prompt-size 60000 --tpm-limit 4000000 --max-wait 300
  python scripts/test_direct_bedrock_leaky.py --model jamba --num-requests 150 \
    --rpm-limit 100 --max-wait 120
"""

import boto3
import time
import sys
import os
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from botocore.config import Config
import config_loader

# Add lambda layer to Python path to import shared_service
layer_path = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'lambda_layer', 'python')
sys.path.insert(0, layer_path)

from shared_service import DynamoService

# Model ID aliases (kept in sync with test_direct_bedrock.py)
MODEL_ALIASES = {
    'opus-5': 'us.anthropic.claude-opus-5',
    'sonnet-5': 'us.anthropic.claude-sonnet-5',
    'nova-2-lite': 'us.amazon.nova-2-lite-v1:0',
    'nova-lite': 'us.amazon.nova-lite-v1:0',
    'nova-lite-sr': 'amazon.nova-lite-v1:0',
    'nova-pro': 'us.amazon.nova-pro-v1:0',
}

# Rough token estimate: ~4 chars/token for prompt, plus max_tokens for output.
CHARS_PER_TOKEN = 4.0


def resolve_model_id(model_input: str) -> str:
    return MODEL_ALIASES.get(model_input.lower(), model_input)


def validate_model_config(dynamo_service: DynamoService, model_id: str) -> None:
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


class LeakyBucket:
    """Thread-safe token bucket used as a leaky-bucket pacer.

    Capacity == the per-minute quota (TPM or RPM). Refills continuously at
    capacity/60 units per second. acquire(cost) blocks until `cost` units are
    available or `max_wait` elapses. Returns the waited time, or None on timeout.
    """

    def __init__(self, capacity_per_min: float, start_full: bool = False):
        self.capacity = float(capacity_per_min)
        self.refill_per_sec = self.capacity / 60.0
        # Start empty by default so the first minute cannot exceed one window's
        # worth of quota — matches how a fresh window behaves under a spike.
        self.tokens = self.capacity if start_full else 0.0
        self.last = time.time()
        self.lock = threading.Lock()

    def _refill_locked(self):
        now = time.time()
        elapsed = now - self.last
        self.last = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_sec)

    def acquire(self, cost: float, max_wait: float):
        """Block until `cost` tokens are available. Return waited seconds, or None if it exceeds max_wait."""
        cost = min(cost, self.capacity)  # a single request can never exceed one window
        start = time.time()
        while True:
            with self.lock:
                self._refill_locked()
                if self.tokens >= cost:
                    self.tokens -= cost
                    return time.time() - start
                deficit = cost - self.tokens
                wait = deficit / self.refill_per_sec
            if (time.time() - start) + wait > max_wait:
                return None
            time.sleep(min(wait, 0.5))


def parse_args():
    parser = argparse.ArgumentParser(
        description='Load test direct Bedrock calls with a client-side leaky bucket (paced)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This test paces requests client-side to stay under quota, waiting (leaking)
instead of retrying. Compare against test_direct_bedrock.py (no retry),
test_direct_bedrock_retry.py (retry+jitter), and test_budget_manager.py
(Traffic Shaper) for the four-way comparison table.

Examples:
  python scripts/test_direct_bedrock_leaky.py --model nova-lite --num-requests 1000 --prompt-size 60000 --tpm-limit 4000000
  python scripts/test_direct_bedrock_leaky.py --model jamba --num-requests 150 --rpm-limit 100
        """
    )
    parser.add_argument('--model', type=str, help='Model ID or alias (nova-2-lite, sonnet-5, opus-5)')
    parser.add_argument('--num-requests', type=int, help='Number of requests (default: config.env)')
    parser.add_argument('--max-workers', type=int, help='Concurrent threads (default: config.env)')
    parser.add_argument('--submission-duration', type=int, help='Submission spread in seconds (default: config.env)')
    parser.add_argument('--prompt-size', type=int, default=None, help='Prompt size in chars (for TPM testing)')
    parser.add_argument('--max-tokens', type=int, default=20, help='max_tokens per request (default: 20)')
    parser.add_argument('--tpm-limit', type=int, default=None,
                        help='TPM quota to pace against (token-bucket capacity). Use for TPM-bound models.')
    parser.add_argument('--rpm-limit', type=int, default=None,
                        help='RPM quota to pace against. Use for RPM-bound models. One of --tpm-limit/--rpm-limit required.')
    parser.add_argument('--max-wait', type=float, default=300.0,
                        help='Max seconds a request waits in the bucket before counting as failed (default: 300)')
    parser.add_argument('--headroom', type=float, default=1.0,
                        help='Fraction of quota the pacer targets (e.g. 0.9 = pace to 90%% of quota). Default 1.0.')
    return parser.parse_args()


# Load configuration and verify AWS access
config = config_loader.get_config_with_aws_check()
args = parse_args()

AWS_REGION = config.get('AWS_REGION', 'us-east-1')
BEDROCK_MODEL_ID = resolve_model_id(args.model) if args.model else config.get('BEDROCK_MODEL_ID', 'us.amazon.nova-2-lite-v1:0')
SINGLE_TABLE_NAME = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')
NUM_REQUESTS = args.num_requests if args.num_requests else int(config.get('NUM_REQUESTS', '125'))
MAX_WORKERS = args.max_workers if args.max_workers else int(config.get('MAX_WORKERS', '10'))
SUBMISSION_DURATION = args.submission_duration if args.submission_duration is not None else int(config.get('SUBMISSION_DURATION', '10'))
PROMPT_SIZE = args.prompt_size
MAX_TOKENS = args.max_tokens
MAX_WAIT = args.max_wait
HEADROOM = args.headroom

if args.tpm_limit is None and args.rpm_limit is None:
    print("ERROR: one of --tpm-limit or --rpm-limit is required (the quota to pace against).")
    sys.exit(1)

PACE_MODE = 'TPM' if args.tpm_limit is not None else 'RPM'
QUOTA_PER_MIN = (args.tpm_limit if PACE_MODE == 'TPM' else args.rpm_limit) * HEADROOM

# Validate model config exists
dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)
validate_model_config(dynamo_service, BEDROCK_MODEL_ID)

bucket = LeakyBucket(QUOTA_PER_MIN)


def build_prompt(request_num, prompt_size=None):
    base = f'Say "Request {request_num} completed"'
    if prompt_size and prompt_size > len(base):
        padding = ' This is padding text for TPM validation testing.'
        reps = (prompt_size - len(base)) // len(padding) + 1
        base = (base + padding * reps)[:prompt_size]
    return base


def estimate_cost(prompt: str) -> float:
    """Estimate the pacing cost of a request in the active mode."""
    if PACE_MODE == 'RPM':
        return 1.0
    # TPM: input tokens (prompt) + output tokens (max_tokens ceiling).
    return (len(prompt) / CHARS_PER_TOKEN) + MAX_TOKENS


def make_bedrock_call_leaky(bedrock, request_num):
    """Acquire from the leaky bucket, then make one Bedrock call (no retry)."""
    start_time = time.time()
    prompt = build_prompt(request_num, PROMPT_SIZE)
    cost = estimate_cost(prompt)

    waited = bucket.acquire(cost, MAX_WAIT)
    if waited is None:
        # Could not be admitted within max_wait — leaky bucket "drops" it.
        return {
            'request_num': request_num, 'success': False, 'error': 'bucket_timeout',
            'wait_time': MAX_WAIT, 'total_time': time.time() - start_time,
        }

    try:
        bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=[{'role': 'user', 'content': [{'text': prompt}]}],
            inferenceConfig={'maxTokens': MAX_TOKENS},
        )
        return {
            'request_num': request_num, 'success': True, 'error': None,
            'wait_time': waited, 'total_time': time.time() - start_time,
        }
    except ClientError as e:
        error_code = e.response['Error']['Code']
        is_throttle = error_code in ['ThrottlingException', 'TooManyRequestsException',
                                     'ServiceQuotaExceededException']
        return {
            'request_num': request_num, 'success': False,
            'error': '429' if is_throttle else error_code,
            'wait_time': waited, 'total_time': time.time() - start_time,
        }
    except Exception as e:
        return {
            'request_num': request_num, 'success': False, 'error': str(e),
            'wait_time': waited, 'total_time': time.time() - start_time,
        }


def test_direct_bedrock_leaky():
    print(f"\n{'='*60}")
    print(f"DIRECT BEDROCK TEST (Client-Side Leaky Bucket)")
    print(f"{'='*60}")
    print(f"Model: {BEDROCK_MODEL_ID}")
    print(f"Total requests: {NUM_REQUESTS}")
    print(f"Concurrency: {MAX_WORKERS} threads")
    print(f"Pacing: {PACE_MODE} bucket, capacity {QUOTA_PER_MIN:,.0f}/min (headroom {HEADROOM:.0%})")
    print(f"Prompt size: {PROMPT_SIZE or '~30'} chars, max_tokens: {MAX_TOKENS}")
    print(f"Per-request cost estimate: {estimate_cost(build_prompt(0, PROMPT_SIZE)):,.0f} {PACE_MODE} units")
    print(f"Max wait per request: {MAX_WAIT:.0f}s")

    if SUBMISSION_DURATION > 0:
        print(f"Submission: {SUBMISSION_DURATION}s duration ({NUM_REQUESTS/SUBMISSION_DURATION:.1f} req/s)")
    else:
        print(f"Submission: Instant spike (all at once)")
    print(f"{'='*60}\n")

    # No SDK retries — the bucket is the only rate control.
    config_obj = Config(retries={'max_attempts': 1, 'mode': 'standard'})
    bedrock = boto3.client('bedrock-runtime', region_name=AWS_REGION, config=config_obj)

    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        delay_between_requests = SUBMISSION_DURATION / NUM_REQUESTS if SUBMISSION_DURATION > 0 else 0

        for i in range(NUM_REQUESTS):
            futures.append(executor.submit(make_bedrock_call_leaky, bedrock, i))
            if delay_between_requests > 0:
                time.sleep(delay_between_requests)
                print(f"  Submitted {i+1}/{NUM_REQUESTS} ({(i+1)/NUM_REQUESTS*100:.0f}%)", end='\r')

        if SUBMISSION_DURATION > 0:
            print(f"\n\nAll requests submitted. Waiting for bucket drain + completion...")

        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            status = "ok" if result['success'] else f"FAIL {result['error']}"
            print(f"  Completed {completed}/{NUM_REQUESTS}: {status}", end='\r')

    total_time = time.time() - start_time

    # Statistics
    success_count = sum(1 for r in results if r['success'])
    throttle_count = sum(1 for r in results if r['error'] == '429')
    timeout_count = sum(1 for r in results if r['error'] == 'bucket_timeout')
    other_errors = sum(1 for r in results if r['error'] and r['error'] not in ('429', 'bucket_timeout'))

    times = sorted(r['total_time'] for r in results)
    waits = sorted(r['wait_time'] for r in results)

    def pct(sorted_list, p):
        return sorted_list[min(len(sorted_list) - 1, int(len(sorted_list) * p))] if sorted_list else 0

    print(f"\n\n{'='*60}")
    print(f"RESULTS: Direct Bedrock with Client-Side Leaky Bucket")
    print(f"{'='*60}")
    print(f"")
    print(f"  Success Rate")
    print(f"  {'-'*40}")
    print(f"  Total requests:       {NUM_REQUESTS}")
    print(f"  Successful:           {success_count} ({success_count/NUM_REQUESTS*100:.1f}%)")
    print(f"  Throttled (429):      {throttle_count} ({throttle_count/NUM_REQUESTS*100:.1f}%)")
    print(f"  Dropped (bucket wait):{timeout_count} ({timeout_count/NUM_REQUESTS*100:.1f}%)")
    print(f"  Other errors:         {other_errors}")
    print(f"")
    print(f"  API call amplification: 1x (no retries — same as no-retry and shaper)")
    print(f"")
    print(f"  Latency (all requests, incl. bucket wait)")
    print(f"  {'-'*40}")
    print(f"  p50:                  {pct(times, 0.50)*1000:.0f}ms")
    print(f"  p95:                  {pct(times, 0.95)*1000:.0f}ms")
    print(f"  p99:                  {pct(times, 0.99)*1000:.0f}ms")
    print(f"")
    print(f"  Bucket wait time (queue-drain analog)")
    print(f"  {'-'*40}")
    print(f"  p50 wait:             {pct(waits, 0.50):.1f}s")
    print(f"  p99 wait:             {pct(waits, 0.99):.1f}s")
    print(f"  max wait:             {pct(waits, 1.0):.1f}s")
    print(f"")
    print(f"  Timing")
    print(f"  {'-'*40}")
    print(f"  Total wall time:      {total_time:.1f}s")
    print(f"{'='*60}")
    print(f"\n  Compare against Traffic Shaper:")
    print(f"  make test-budget-manager ARGS=\"--model {args.model or 'jamba'} --num-requests {NUM_REQUESTS} --max-workers {MAX_WORKERS}\"")
    if throttle_count > 0:
        print(f"\n  {throttle_count} requests STILL throttled despite pacing — client-side buckets")
        print(f"  drift from Bedrock's real quota clock and can't coordinate across clients.")
    if timeout_count > 0:
        print(f"  {timeout_count} requests dropped after waiting {MAX_WAIT:.0f}s — no durable queue, unlike the shaper.")
    print()


if __name__ == "__main__":
    test_direct_bedrock_leaky()
