#!/usr/bin/env python3
"""
Minimal Load Test - Direct Bedrock Calls (No Retry)
Goal: Get throttled by exceeding 50 RPM quota
"""

import boto3
import json
import time
import sys
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from botocore.config import Config
import config_loader

# Add lambda layer to Python path to import shared_service
layer_path = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'lambda_layer', 'python')
sys.path.insert(0, layer_path)

from shared_service import DynamoService

# Model ID aliases for common testing models
MODEL_ALIASES = {
    'opus-5': 'us.anthropic.claude-opus-5',
    'sonnet-5': 'us.anthropic.claude-sonnet-5',
    'nova-2-lite': 'us.amazon.nova-2-lite-v1:0',
    'nova-lite': 'us.amazon.nova-lite-v1:0',
    'nova-lite-sr': 'amazon.nova-lite-v1:0',  # single-region: enforces per-region quotas
    'nova-pro': 'us.amazon.nova-pro-v1:0',
}

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
        description='Load test direct Bedrock calls (baseline throttling test)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use defaults from config.env
  python scripts/test_direct_bedrock.py
  
  # Override model (supports aliases: opus, jamba)
  python scripts/test_direct_bedrock.py --model nova-2-lite
  python scripts/test_direct_bedrock.py --model opus-5
  
  # Override test parameters
  python scripts/test_direct_bedrock.py --num-requests 50 --max-workers 5
  
  # Override everything
  python scripts/test_direct_bedrock.py --model nova-2-lite --num-requests 200 --max-workers 20 --submission-duration 30
        """
    )
    
    parser.add_argument(
        '--model',
        type=str,
        help='Bedrock model ID (or alias: opus, jamba). Defaults to config.env BEDROCK_MODEL_ID'
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
        help='Prompt size in chars (for TPM exhaustion testing). Default: tiny prompt (~20 tokens)'
    )
    parser.add_argument(
        '--max-tokens',
        type=int,
        default=20,
        help='max_tokens per request (default: 20)'
    )

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


def make_bedrock_call(bedrock, request_num):
    """Make a single Bedrock call using Converse API and return result."""
    try:
        response = bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=[
                {
                    'role': 'user',
                    'content': [
                        {
                            'text': build_prompt(request_num, PROMPT_SIZE)
                        }
                    ]
                }
            ],
            inferenceConfig={
                'maxTokens': MAX_TOKENS
            }
        )
        
        return {'request_num': request_num, 'success': True, 'error': None}
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        is_throttle = error_code == 'ThrottlingException'
        return {
            'request_num': request_num,
            'success': False,
            'error': '429' if is_throttle else error_code
        }

def test_direct_bedrock():
    """
    Make direct Bedrock calls with no retry using concurrent threads.
    Count how many return 429 ThrottlingException.
    """
    print(f"\n{'='*60}")
    print(f"DIRECT BEDROCK TEST (No Retry, Concurrent)")
    print(f"{'='*60}")
    print(f"Model: {BEDROCK_MODEL_ID}")
    print(f"Total requests: {NUM_REQUESTS}")
    print(f"Concurrency: {MAX_WORKERS} threads")
    print(f"Prompt size: {PROMPT_SIZE or '~30'} chars, max_tokens: {MAX_TOKENS}")

    if SUBMISSION_DURATION > 0:
        submission_rate = NUM_REQUESTS / SUBMISSION_DURATION
        print(f"Submission: {SUBMISSION_DURATION}s duration ({submission_rate:.1f} req/s)")
    else:
        print(f"Submission: Instant spike (all at once)")
    
    print(f"{'='*60}\n")
    
    # Initialize client with no retries
    config_obj = Config(retries={'max_attempts': 1, 'mode': 'standard'})
    bedrock = boto3.client('bedrock-runtime', region_name=AWS_REGION, config=config_obj)
    
    results = []
    start_time = time.time()
    
    # Use ThreadPoolExecutor for concurrent calls
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit requests with optional rate limiting
        futures = []
        delay_between_requests = SUBMISSION_DURATION / NUM_REQUESTS if SUBMISSION_DURATION > 0 else 0
        
        for i in range(NUM_REQUESTS):
            future = executor.submit(make_bedrock_call, bedrock, i)
            futures.append(future)
            
            if delay_between_requests > 0:
                time.sleep(delay_between_requests)
                print(f"  Submitted {i+1}/{NUM_REQUESTS} ({(i+1)/NUM_REQUESTS*100:.0f}%)", end='\r')
        
        if SUBMISSION_DURATION > 0:
            print(f"\n\nAll requests submitted. Waiting for completion...")
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            
            status = "✓ Success" if result['success'] else f"✗ {result['error']}"
            print(f"  Completed {completed}/{NUM_REQUESTS}: {status}", end='\r')
    
    total_time = time.time() - start_time
    
    # Print results
    print(f"\n\n{'='*60}")
    print(f"RESULTS:")
    print(f"{'='*60}")
    
    success_count = sum(1 for r in results if r['success'])
    throttle_count = sum(1 for r in results if r['error'] == '429')
    other_errors = sum(1 for r in results if r['error'] and r['error'] != '429')
    
    print(f"  Total requests:    {NUM_REQUESTS}")
    print(f"  Successful:        {success_count} ({success_count/NUM_REQUESTS*100:.1f}%)")
    print(f"  Throttled (429):   {throttle_count} ({throttle_count/NUM_REQUESTS*100:.1f}%)")
    print(f"  Other errors:      {other_errors}")
    print(f"  Total time:        {total_time:.1f}s")
    print(f"{'='*60}\n")
    
    if throttle_count > 0:
        print(f"✅ SUCCESS: Got {throttle_count} throttles! Bedrock is rate limiting.")
    else:
        print(f"⚠️  No throttles detected. Try increasing NUM_REQUESTS or running faster.")
    
    print()

if __name__ == "__main__":
    test_direct_bedrock()
