#!/usr/bin/env python3
"""
Check queue depth - show current queue state and recent messages

NOTE: The reported "Queue depth" is only ONE PAGE of results. DynamoDB's
query(Select='COUNT') counts at most a single ~1 MB page, so for a deep queue
(hundreds+ of items) this UNDER-REPORTS the true depth and can appear "stuck"
at a page-sized number (e.g. ~885) even while the processor is actively
draining. For an accurate count, paginate on LastEvaluatedKey and sum the
per-page Counts, or read the CloudWatch BedrockShaper 'QueueDepth' metric.
"""

import sys
import os
import argparse
import boto3
import config_loader

# Add lambda layer to Python path to import shared_service
layer_path = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'lambda_layer', 'python')
sys.path.insert(0, layer_path)

from shared_service import DynamoService

# Load configuration and verify AWS access
config = config_loader.get_config_with_aws_check()
AWS_REGION = config.get('AWS_REGION', 'us-east-1')
SINGLE_TABLE_NAME = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')


def check_queue_depth(model_id, limit=5):
    """Check queue depth using DynamoService (1:1 with Lambda code).

    Args:
        model_id: Model ID to check queue for
        limit: Number of recent items to display (default: 5)
    """
    print(f"\n{'='*60}")
    print(f"QUEUE DEPTH CHECK")
    print(f"{'='*60}\n")
    print(f"Table:    {SINGLE_TABLE_NAME}")
    print(f"Model ID: {model_id}")
    print()

    try:
        # Initialize DynamoService (same as Lambda)
        dynamo_service = DynamoService(single_table_name=SINGLE_TABLE_NAME)

        # Query queue items using DynamoService method
        partition_key = f'MODEL#{model_id}#QUEUE#ITEMS'
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        table = dynamodb.Table(SINGLE_TABLE_NAME)

        # Count items
        response = table.query(
            KeyConditionExpression='pk = :pk',
            ExpressionAttributeValues={':pk': partition_key},
            Select='COUNT'
        )
        queue_depth = response['Count']

        print(f"Queue depth: {queue_depth}")

        if queue_depth == 0:
            print("✅ Queue is empty")
        else:
            # Get the most recent messages
            response = table.query(
                KeyConditionExpression='pk = :pk',
                ExpressionAttributeValues={':pk': partition_key},
                ScanIndexForward=False,  # Sort descending (most recent first)
                Limit=limit
            )

            items = response.get('Items', [])

            print(f"\nMost recent {len(items)} queue item(s):")
            print(f"{'-'*60}")

            for i, item in enumerate(items, 1):
                print(f"\n{i}. pk: {item.get('pk', 'N/A')}")
                print(f"   sk: {item.get('sk', 'N/A')}")
                print(f"   request_id: {item.get('request_id', 'N/A')}")
                print(f"   priority: {item.get('priority', 'N/A')}")
                print(f"   queued_at: {item.get('queued_at', 'N/A')}")
                if 'task_token' in item:
                    # Task tokens are bearer credentials — never print the value.
                    print(f"   task_token: [present]")

        print(f"\n{'='*60}\n")

    except Exception as e:
        print(f"❌ Error checking queue: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Check queue depth and display recent messages',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check queue for default model (opus)
  python scripts/check_queue_depth.py

  # Check queue for specific model
  python scripts/check_queue_depth.py --model-id us.amazon.nova-2-lite-v1:0

  # Show more items
  python scripts/check_queue_depth.py --limit 10
        """
    )

    parser.add_argument(
        '--model-id',
        default='us.anthropic.claude-opus-5',
        help='Model ID to check queue for (default: opus)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=5,
        help='Number of recent items to display (default: 5)'
    )

    args = parser.parse_args()
    check_queue_depth(args.model_id, args.limit)
