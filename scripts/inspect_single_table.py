#!/usr/bin/env python3
"""
Inspect single table - view items across different partitions using targeted queries
"""

import sys
import os
import argparse
import boto3
from boto3.dynamodb.conditions import Key
import config_loader

# Add lambda layer to Python path to import shared_service
layer_path = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'lambda_layer', 'python')
sys.path.insert(0, layer_path)

from shared_service import DynamoService

# Load configuration and verify AWS access
config = config_loader.get_config_with_aws_check()
AWS_REGION = config.get('AWS_REGION', 'us-east-1')
SINGLE_TABLE_NAME = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')


# Common model IDs
MODEL_OPUS = 'us.anthropic.claude-opus-5'
MODEL_JAMBA = 'us.amazon.nova-2-lite-v1:0'
MODEL_NOVA_LITE = 'us.amazon.nova-lite-v1:0'


def inspect_model_config(model_id: str):
    """
    Inspect model configuration using targeted query (no scan).
    
    Args:
        model_id: Model ID to inspect (e.g., 'opus', 'jamba', or full model ID)
    """
    # Map short names to full model IDs
    model_map = {
        'opus': MODEL_OPUS,
        'jamba': MODEL_JAMBA,
        'nova-lite': MODEL_NOVA_LITE,
    }
    
    # Use mapped name if available, otherwise use as-is
    full_model_id = model_map.get(model_id.lower(), model_id)
    
    print(f"\n{'='*70}")
    print(f"MODEL CONFIGURATION")
    print(f"{'='*70}\n")
    print(f"Table: {SINGLE_TABLE_NAME}")
    print(f"Model ID: {full_model_id}\n")

    try:
        # Initialize DynamoService
        dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)

        # Query model config using DynamoService
        config = dynamo_service.get_model_config(full_model_id)

        print("Configuration found:")
        print(f"{'-'*70}")
        print(f"  PK: {config.get('pk', 'N/A')}")
        print(f"  SK: {config.get('sk', 'N/A')}")
        print(f"\n  Capacity Settings:")
        print(f"    burst_capacity: {config.get('burst_capacity', 'N/A')}")
        print(f"    queue_capacity: {config.get('queue_capacity', 'N/A')}")
        print(f"    buffer_capacity: {config.get('buffer_capacity', 'N/A')}")
        print(f"\n  RPM Rate Limits:")
        print(f"    rpm_limit: {config.get('rpm_limit', 'N/A')}")
        print(f"    burst_regeneration_rate: {config.get('burst_regeneration_rate', 'N/A')}")
        print(f"    queue_regeneration_rate: {config.get('queue_regeneration_rate', 'N/A')}")
        print(f"\n  TPM Rate Limits:")
        print(f"    tpm_limit: {config.get('tpm_limit', 'N/A')}")
        print(f"    tpm_burst_capacity: {config.get('tpm_burst_capacity', 'N/A')}")
        print(f"    tpm_burst_regeneration_rate: {config.get('tpm_burst_regeneration_rate', 'N/A')}")
        print(f"    tpm_queue_capacity: {config.get('tpm_queue_capacity', 'N/A')}")
        print(f"    tpm_queue_regeneration_rate: {config.get('tpm_queue_regeneration_rate', 'N/A')}")
        print(f"    tpm_buffer_capacity: {config.get('tpm_buffer_capacity', 'N/A')}")
        print(f"    output_token_burndown_rate: {config.get('output_token_burndown_rate', 'N/A')}")
        print(f"\n  Queue Settings:")
        print(f"    queue_batch_size: {config.get('queue_batch_size', 'N/A')}")

        # Show any other attributes
        skip_keys = {'pk', 'sk', 'burst_capacity', 'queue_capacity', 'buffer_capacity',
                     'rpm_limit', 'burst_regeneration_rate', 'queue_regeneration_rate',
                     'queue_batch_size', 'entity_type',
                     'tpm_limit', 'tpm_burst_capacity', 'tpm_burst_regeneration_rate',
                     'tpm_queue_capacity', 'tpm_queue_regeneration_rate', 'tpm_buffer_capacity',
                     'output_token_burndown_rate'}
        other_attrs = {k: v for k, v in config.items() if k not in skip_keys}
        if other_attrs:
            print(f"\n  Other Attributes:")
            for key, value in other_attrs.items():
                print(f"    {key}: {value}")

        print(f"\n{'='*70}\n")

    except KeyError as e:
        print(f"❌ Model config not found: {e}")
        print(f"\nTip: Available models might be 'opus' or 'jamba'")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error inspecting config: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def inspect_consumption_records(model_id: str, capacity_mode='BURST', limit=10, show_tpm=False):
    """
    Inspect consumption records using DynamoService (1:1 with Lambda code).

    Args:
        model_id: Model ID to query (e.g., 'opus', 'jamba', or full model ID)
        capacity_mode: 'BURST' or 'QUEUE'
        limit: Max records to show
        show_tpm: If True, show TPM token estimates and totals
    """
    # Map short names to full model IDs
    model_map = {
        'opus': MODEL_OPUS,
        'jamba': MODEL_JAMBA,
        'nova-lite': MODEL_NOVA_LITE,
    }
    
    # Use mapped name if available, otherwise use as-is
    full_model_id = model_map.get(model_id.lower(), model_id)
    
    print(f"\n{'='*70}")
    print(f"CONSUMPTION RECORDS INSPECTION")
    print(f"{'='*70}\n")
    print(f"Table: {SINGLE_TABLE_NAME}")
    print(f"Model ID: {full_model_id}")
    print(f"Capacity Mode: {capacity_mode}\n")

    try:
        # Initialize DynamoService (same as Lambda)
        dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)

        # Query consumption records using DynamoService method (same as Lambda)
        records = dynamo_service.query_consumption_records(
            model_id=full_model_id,
            capacity_mode=capacity_mode,
            window_seconds=60,
            consistent_read=True
        )

        print(f"Found {len(records)} consumption record(s) in last 60 seconds")

        if len(records) == 0:
            print("✅ No consumption records")
        else:
            total_tpm = 0
            print(f"\nShowing {min(len(records), limit)} record(s):")
            print(f"{'-'*70}")

            for i, record in enumerate(records[:limit], 1):
                print(f"\n{i}. pk: {record.get('pk', 'N/A')}")
                print(f"   sk: {record.get('sk', 'N/A')}")
                print(f"   request_id: {record.get('request_id', 'N/A')}")
                print(f"   consumed_at: {record.get('consumed_at', 'N/A')}")
                print(f"   count: {record.get('count', 'N/A')}")
                if show_tpm or record.get('estimated_tokens'):
                    est = record.get('estimated_tokens', 0)
                    print(f"   estimated_tokens: {est}")
                    total_tpm += int(est) if est else 0

            if show_tpm:
                total_rpm = len(records)
                print(f"\n--- TPM Summary ---")
                print(f"  Total RPM consumed (60s window): {total_rpm}")
                print(f"  Total TPM consumed (60s window): {total_tpm}")

        print(f"\n{'='*70}\n")

    except Exception as e:
        print(f"❌ Error inspecting consumption records: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def inspect_queue_items(model_id: str, limit=10):
    """
    Inspect queue items using targeted query (no scan).
    
    Args:
        model_id: Model ID to query (e.g., 'opus', 'jamba', or full model ID)
        limit: Max items to show
    """
    # Map short names to full model IDs
    model_map = {
        'opus': MODEL_OPUS,
        'jamba': MODEL_JAMBA,
        'nova-lite': MODEL_NOVA_LITE,
    }
    
    # Use mapped name if available, otherwise use as-is
    full_model_id = model_map.get(model_id.lower(), model_id)
    
    print(f"\n{'='*70}")
    print(f"QUEUE ITEMS INSPECTION")
    print(f"{'='*70}\n")
    print(f"Table: {SINGLE_TABLE_NAME}")
    print(f"Model ID: {full_model_id}\n")

    try:
        # Initialize DynamoService
        dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)

        # Get queue depth using DynamoService
        queue_depth = dynamo_service.get_queue_depth(full_model_id)
        
        print(f"Queue depth: {queue_depth}")

        if queue_depth == 0:
            print("✅ Queue is empty")
        else:
            # Query items directly for display
            dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
            table = dynamodb.Table(SINGLE_TABLE_NAME)
            
            response = table.query(
                KeyConditionExpression=Key('pk').eq(f'MODEL#{full_model_id}#QUEUE#ITEMS'),
                Limit=limit,
                ScanIndexForward=True  # Oldest first (FIFO order)
            )

            items = response.get('Items', [])
            
            print(f"\nShowing {min(len(items), limit)} item(s) (FIFO order):")
            print(f"{'-'*70}")

            for i, item in enumerate(items, 1):
                print(f"\n{i}. pk: {item.get('pk', 'N/A')}")
                print(f"   sk: {item.get('sk', 'N/A')}")
                print(f"   request_id: {item.get('request_id', 'N/A')}")
                print(f"   priority: {item.get('priority', 'N/A')}")
                print(f"   queued_at: {item.get('queued_at', 'N/A')}")
                if 'task_token' in item:
                    # Task tokens are bearer credentials — never print the value.
                    print(f"   task_token: [present]")

        print(f"\n{'='*70}\n")

    except Exception as e:
        print(f"❌ Error inspecting queue items: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Inspect single table - view items using targeted queries (no table scans)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # View model configuration
  python scripts/inspect_single_table.py --config --model opus
  python scripts/inspect_single_table.py --config --model jamba

  # View queue items
  python scripts/inspect_single_table.py --queue --model opus

  # View burst consumption
  python scripts/inspect_single_table.py --consumption --model opus --capacity-mode BURST

  # View queue consumption
  python scripts/inspect_single_table.py --consumption --model opus --capacity-mode QUEUE

Supported models:
  - opus: Claude Opus (us.anthropic.claude-opus-5)
  - jamba: Jamba Mini (us.amazon.nova-2-lite-v1:0)
  - Or use full model ID directly
        """
    )

    parser.add_argument(
        '--model',
        default='opus',
        help='Model ID (short name like "opus"/"jamba" or full model ID)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Max items to show (default: 10)'
    )
    
    # Action flags (mutually exclusive)
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument(
        '--config',
        action='store_true',
        help='Inspect model configuration'
    )
    action_group.add_argument(
        '--queue',
        action='store_true',
        help='Inspect queue items'
    )
    action_group.add_argument(
        '--consumption',
        action='store_true',
        help='Inspect consumption records'
    )
    
    parser.add_argument(
        '--capacity-mode',
        choices=['BURST', 'QUEUE'],
        default='BURST',
        help='Capacity mode for consumption inspection (default: BURST)'
    )
    parser.add_argument(
        '--show-tpm',
        action='store_true',
        help='Show TPM (estimated_tokens) in consumption records'
    )

    args = parser.parse_args()

    if args.config:
        inspect_model_config(args.model)
    elif args.queue:
        inspect_queue_items(args.model, args.limit)
    elif args.consumption:
        inspect_consumption_records(args.model, args.capacity_mode, args.limit, show_tpm=args.show_tpm)
    else:
        parser.print_help()
        sys.exit(1)
