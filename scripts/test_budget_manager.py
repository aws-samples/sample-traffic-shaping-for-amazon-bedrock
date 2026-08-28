#!/usr/bin/env python3
"""
Minimal Load Test - Budget Manager + Queue Processor via Step Functions
Goal: Compare with direct Bedrock calls to show no throttling
"""

import boto3
import json
import time
import sys
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import config_loader
from create_model_config import MODEL_MAP  # single source of truth for model aliases

# Add lambda layer to Python path to import shared_service
layer_path = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'lambda_layer', 'python')
sys.path.insert(0, layer_path)

from shared_service import DynamoService

# Model ID aliases: reuse create_model_config's canonical MODEL_MAP (was a local
# copy here that drifted out of sync — the two must never diverge again).
MODEL_ALIASES = MODEL_MAP

def resolve_model_id(model_input: str) -> str:
    """Resolve model alias to full model ID."""
    return MODEL_ALIASES.get(model_input.lower(), model_input)

def validate_model_config(dynamo_service: DynamoService, model_id: str) -> None:
    """Validate that model config exists in DynamoDB."""
    try:
        config = dynamo_service.get_model_config(model_id)
        print(f"✓ Model config found: {model_id}")
        print(f"  Burst capacity: {config.get('burst_capacity')}")
        print(f"  RPM limit: {config.get('rpm_limit')}")
    except KeyError:
        print(f"❌ Model config not found: {model_id}")
        print(f"\nCreate config first:")
        print(f"  make create-config MODEL={model_id}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error validating model config: {e}")
        sys.exit(1)

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Load test Budget Manager via Step Functions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use defaults from config.env
  python scripts/test_budget_manager.py
  
  # Override model (supports aliases: opus, jamba)
  python scripts/test_budget_manager.py --model nova-2-lite
  python scripts/test_budget_manager.py --model opus-5
  
  # Override test parameters
  python scripts/test_budget_manager.py --num-requests 50 --max-workers 5
  
  # Override everything
  python scripts/test_budget_manager.py --model nova-2-lite --num-requests 200 --max-workers 20 --submission-duration 30
        """
    )
    
    parser.add_argument(
        '--model',
        type=str,
        help='Bedrock model ID (or alias: nova-2-lite, sonnet-5, opus-5). Defaults to config.env BEDROCK_MODEL_ID'
    )
    parser.add_argument(
        '--num-requests',
        type=int,
        help='Number of requests to send. Defaults to config.env NUM_REQUESTS (125)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        help='Concurrent thread count. Defaults to config.env MAX_WORKERS (10)'
    )
    parser.add_argument(
        '--submission-duration',
        type=int,
        help='Duration to spread submissions in seconds. Defaults to config.env SUBMISSION_DURATION (10)'
    )
    parser.add_argument(
        '--prompt-size',
        type=int,
        default=None,
        help='Prompt size in characters (for TPM validation). Default: ~10 chars'
    )
    parser.add_argument(
        '--max-tokens',
        type=int,
        default=None,
        help='max_tokens per request (for TPM validation). Default: 100'
    )

    return parser.parse_args()

# Load configuration and verify AWS access
config = config_loader.get_config_with_aws_check()
args = parse_args()

# Apply CLI overrides or use config.env defaults
AWS_REGION = config.get('AWS_REGION', 'us-east-1')
BEDROCK_MODEL_ID = resolve_model_id(args.model) if args.model else config.get('BEDROCK_MODEL_ID', 'us.amazon.nova-2-lite-v1:0')
STATE_MACHINE_ARN = config.get('STATE_MACHINE_ARN', '')
SINGLE_TABLE_NAME = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')
NUM_REQUESTS = args.num_requests if args.num_requests else int(config.get('NUM_REQUESTS', '125'))
MAX_WORKERS = args.max_workers if args.max_workers else int(config.get('MAX_WORKERS', '10'))
SUBMISSION_DURATION = args.submission_duration if args.submission_duration is not None else int(config.get('SUBMISSION_DURATION', '10'))
PROMPT_SIZE = args.prompt_size
MAX_TOKENS = args.max_tokens

if not STATE_MACHINE_ARN or 'ACCOUNT' in STATE_MACHINE_ARN:
    print("❌ STATE_MACHINE_ARN not configured in config.env")
    sys.exit(1)

# Validate model config exists
dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)
validate_model_config(dynamo_service, BEDROCK_MODEL_ID)

def build_prompt(request_num, prompt_size=None):
    """Build a prompt of the requested size."""
    base = f'Request {request_num}'
    if prompt_size and prompt_size > len(base):
        padding = ' This is padding text for TPM validation testing.'
        reps = (prompt_size - len(base)) // len(padding) + 1
        base = (base + padding * reps)[:prompt_size]
    return base


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
    return 'running'


def start_step_function(sfn_client, request_num):
    """Start a Step Function execution for a single request."""
    try:
        payload = {
            'request_id': f'load_test_{request_num}',
            'model_id': BEDROCK_MODEL_ID,
            'prompt': build_prompt(request_num, PROMPT_SIZE)
        }
        if MAX_TOKENS is not None:
            payload['max_tokens'] = MAX_TOKENS

        execution_arn = sfn_client.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(payload)
        )['executionArn']
        
        return {
            'request_num': request_num,
            'success': True,
            'execution_arn': execution_arn,
            'error': None
        }
        
    except Exception as e:
        return {
            'request_num': request_num,
            'success': False,
            'execution_arn': None,
            'error': str(e)
        }

def test_step_functions():
    """
    Start Step Function executions concurrently and check results.
    """
    print(f"\n{'='*60}")
    print(f"STEP FUNCTIONS TEST (Semaphore Queue)")
    print(f"{'='*60}")
    print(f"Model: {BEDROCK_MODEL_ID}")
    print(f"Total requests: {NUM_REQUESTS}")
    print(f"Concurrency: {MAX_WORKERS} threads")
    
    if PROMPT_SIZE or MAX_TOKENS is not None:
        print(f"Prompt size: {PROMPT_SIZE or '~10'} chars, max_tokens: {MAX_TOKENS or 100}")
        # Calculate estimated TPM per request for display
        from shared_service import estimate_request_tokens
        model_config = dynamo_service.get_model_config(BEDROCK_MODEL_ID)
        burndown = float(model_config.get('output_token_burndown_rate', 1.0))
        tpm_burst = int(model_config.get('tpm_burst_capacity', 0))
        est = estimate_request_tokens(
            prompt='x' * (PROMPT_SIZE or 10),
            max_tokens=MAX_TOKENS or 100,
            burndown_rate=burndown
        )
        print(f"Estimated TPM/request: {est} tokens (burndown={burndown}x)")
        if tpm_burst > 0:
            tpm_exhaust = tpm_burst / est if est > 0 else float('inf')
            rpm_burst = int(model_config.get('burst_capacity', 0))
            print(f"TPM exhaustion: ~request #{int(tpm_exhaust)} | RPM exhaustion: ~request #{rpm_burst + 1}")
            if tpm_exhaust < rpm_burst + 1:
                print(f">>> TPM will gate BEFORE RPM (crossover exceeded)")
            else:
                print(f">>> RPM will gate before TPM")

    if SUBMISSION_DURATION > 0:
        submission_rate = NUM_REQUESTS / SUBMISSION_DURATION
        print(f"Submission: {SUBMISSION_DURATION}s duration ({submission_rate:.1f} req/s)")
    else:
        print(f"Submission: Instant spike (all at once)")

    print(f"{'='*60}\n")
    
    # Initialize clients
    sfn_client = boto3.client('stepfunctions', region_name=AWS_REGION)
    
    results = []
    start_time = time.time()
    
    # Use ThreadPoolExecutor for concurrent executions
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit requests with optional rate limiting
        futures = []
        delay_between_requests = SUBMISSION_DURATION / NUM_REQUESTS if SUBMISSION_DURATION > 0 else 0
        
        for i in range(NUM_REQUESTS):
            future = executor.submit(start_step_function, sfn_client, i)
            futures.append(future)
            
            if delay_between_requests > 0:
                time.sleep(delay_between_requests)  # nosemgrep: arbitrary-sleep -- intentional request-submission pacing in load test
                print(f"  Submitted {i+1}/{NUM_REQUESTS} ({(i+1)/NUM_REQUESTS*100:.0f}%)", end='\r')
        
        if SUBMISSION_DURATION > 0:
            print(f"\n\nAll requests submitted. Waiting for completion...")
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            
            status = "✓ Started" if result['success'] else f"✗ {result['error']}"
            print(f"  Started {completed}/{NUM_REQUESTS}: {status}", end='\r')
    
    start_duration = time.time() - start_time
    
    print(f"\n\nMonitoring executions until all complete...")
    
    # Monitor both queue depth and execution status with polling
    poll_interval = 2  # seconds
    max_wait_time = 300  # 5 minutes max wait
    wait_start = time.time()
    queue_depth = -1
    queue_drained_at = None  # Track when queue first hits zero
    
    # Track which executions are still running (not terminal)
    running_executions = {r['execution_arn']: r for r in results if r['success'] and r['execution_arn']}

    # Accumulate final counts as executions complete
    final_succeeded = 0
    final_queued = 0
    final_failed = 0
    # Initialize before the loop so a throttle on the FIRST iteration can never
    # leave `running` unbound (was an UnboundLocalError at campaign scale).
    running = len(running_executions)
    monitor_throttles = 0  # transient DynamoDB/SFN throttles tolerated during polling

    while (time.time() - wait_start) < max_wait_time:
        try:
            # Check queue depth using single table
            queue_depth = dynamo_service.get_queue_depth(BEDROCK_MODEL_ID)
            
            # Record when queue first drains
            if queue_depth == 0 and queue_drained_at is None:
                queue_drained_at = time.time() - wait_start
                print(f"\n✓ Queue drained after {queue_drained_at:.1f}s")
            
            # Only check executions that haven't reached terminal state
            still_running = {}
            
            for exec_arn, result in running_executions.items():
                try:
                    exec_desc = sfn_client.describe_execution(executionArn=exec_arn)
                    classification = classify_execution(exec_desc)
                    if classification == 'immediate':
                        final_succeeded += 1
                    elif classification == 'queued':
                        final_queued += 1
                    elif classification == 'failed':
                        final_failed += 1
                    else:
                        still_running[exec_arn] = result
                except Exception:
                    pass  # nosec B110  # Skip errors during polling, best-effort telemetry
            
            # Update running set to only include still-running executions
            running_executions = still_running
            running = len(running_executions)
            
            elapsed = time.time() - wait_start
            print(f"  Queue: {queue_depth}, Running: {running}, Succeeded: {final_succeeded + final_queued}, Failed: {final_failed}, Elapsed: {elapsed:.1f}s", end='\r')
            
            # Exit when queue is empty AND no executions are running
            if queue_depth == 0 and running == 0:
                print(f"\n✓ All executions complete after {elapsed:.1f}s")
                break

            time.sleep(poll_interval)  # nosemgrep: arbitrary-sleep -- intentional monitor poll interval in load test
        except Exception as e:
            # A transient throttle on a monitoring query must NOT abort the whole
            # run — the shaper keeps draining server-side regardless. Tolerate a
            # bounded number of monitor throttles with backoff before giving up.
            monitor_throttles += 1
            if monitor_throttles <= 30:
                print(f"\n⏳ Monitor query throttled ({monitor_throttles}), backing off and retrying...")
                time.sleep(min(30, poll_interval * (2 ** min(monitor_throttles, 4))))
                continue
            print(f"\nError during monitoring (gave up after {monitor_throttles} throttles): {e}")
            print(f"   Shaper is still draining server-side — verify via SFN list-executions + CloudWatch.")
            break
    
    if running > 0 or queue_depth > 0:
        print(f"\n⚠️  Timeout after {max_wait_time}s: Queue={queue_depth}, Running={running}")
    
    # Final status check (only check remaining running executions if any)
    print(f"\nFinal execution statuses...")
    
    # If we have running executions left, check them one final time
    if running_executions:
        for exec_arn in running_executions.keys():
            try:
                exec_desc = sfn_client.describe_execution(executionArn=exec_arn)
                classification = classify_execution(exec_desc)
                if classification == 'immediate':
                    final_succeeded += 1
                elif classification == 'queued':
                    final_queued += 1
                elif classification == 'failed':
                    final_failed += 1
            except Exception as e:
                print(f"\nError checking execution: {e}")
    
    total_time = time.time() - start_time
    running = len(running_executions)
    
    # Print results
    print(f"\n{'='*60}")
    print(f"RESULTS:")
    print(f"{'='*60}")
    
    success_count = sum(1 for r in results if r['success'])
    error_count = sum(1 for r in results if not r['success'])
    
    print(f"  Total requests:       {NUM_REQUESTS}")
    print(f"  Started successfully: {success_count} ({success_count/NUM_REQUESTS*100:.1f}%)")
    print(f"  Start errors:         {error_count}")
    print(f"  Processed immediate:  {final_succeeded}")
    print(f"  Queued:               {final_queued}")
    print(f"  Failed:               {final_failed}")
    print(f"  Still running:        {running}")
    print(f"  Final queue depth:    {queue_depth}")
    print(f"  Start time:           {start_duration:.1f}s")
    if queue_drained_at:
        print(f"  Queue drain time:     {queue_drained_at:.1f}s")
    print(f"  Total time:           {total_time:.1f}s")
    print(f"{'='*60}\n")
    
    if queue_depth == 0 and final_failed == 0 and running == 0:
        print(f"✅ SUCCESS: All {NUM_REQUESTS} requests processed!")
        print(f"   {final_succeeded} processed immediately, {final_queued} queued and processed")
    elif queue_depth > 0:
        print(f"⚠️  Queue still has {queue_depth} items after timeout.")
        print(f"   Wait longer or check Queue Processor logs.")
    elif running > 0:
        print(f"⚠️  {running} executions still running.")
        print(f"   Wait longer for them to complete.")
    else:
        print(f"❌ {final_failed} executions failed. Check CloudWatch logs.")
    
    print()

if __name__ == "__main__":
    test_step_functions()
