#!/usr/bin/env python3
"""
Update BURST_CAPACITY environment variable for Budget Manager Lambda without redeployment.

This allows you to adjust the token bucket capacity for testing different
scenarios without needing to redeploy the CDK stack.
"""

import sys
import argparse
import boto3
import config_loader

# Load configuration
config = config_loader.get_config_with_aws_check()
AWS_REGION = config.get('AWS_REGION', 'us-east-1')


def get_lambda_name_from_arn(arn):
    """Extract Lambda function name from ARN."""
    # ARN format: arn:aws:lambda:region:account:function:function-name
    return arn.split(':')[-1]


def update_burst_capacity(capacity):
    """Update BURST_CAPACITY environment variable for Budget Manager Lambda.

    Args:
        capacity: New burst capacity value (max tokens in bucket)
    """
    budget_manager_arn = config.get('BUDGET_MANAGER_ARN')
    if not budget_manager_arn:
        print("❌ Error: BUDGET_MANAGER_ARN not found in config.env")
        sys.exit(1)

    lambda_name = get_lambda_name_from_arn(budget_manager_arn)
    lambda_client = boto3.client('lambda', region_name=AWS_REGION)

    print(f"\n{'='*60}")
    print(f"Updating BURST_CAPACITY for Budget Manager")
    print(f"{'='*60}\n")
    print(f"Lambda: {lambda_name}")
    print(f"New BURST_CAPACITY: {capacity} tokens")
    print()

    try:
        # Get current configuration
        response = lambda_client.get_function_configuration(FunctionName=lambda_name)
        current_env = response.get('Environment', {}).get('Variables', {})
        current_capacity = current_env.get('BURST_CAPACITY', 'not set')

        print(f"Current BURST_CAPACITY: {current_capacity}")

        # Update environment variables
        current_env['BURST_CAPACITY'] = str(capacity)

        lambda_client.update_function_configuration(
            FunctionName=lambda_name,
            Environment={'Variables': current_env}
        )

        print(f"\n✅ Successfully updated BURST_CAPACITY to {capacity}")
        print(f"\n{'='*60}")
        print("Next steps:")
        print("  1. Wait ~10 seconds for Lambda to update")
        print("  2. Run 'make clean' to clear DynamoDB state")
        print("  3. Run 'make test' to test with new capacity")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"\n❌ Error updating Lambda configuration: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Update BURST_CAPACITY environment variable for Budget Manager Lambda',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Set capacity to 2 tokens (test end-to-end with make test)
  python scripts/update_burst_capacity.py 2

  # Set capacity to 50 tokens (production capacity)
  python scripts/update_burst_capacity.py 50

  # Or use the Makefile shortcut:
  make set-capacity CAPACITY=2

Note: After changing the capacity, you should:
  1. Wait ~10 seconds for Lambda to update
  2. Run 'make clean' to reset DynamoDB state
  3. Run 'make test' (4 requests) to verify queueing works with CAPACITY=2
        """
    )

    parser.add_argument(
        'capacity',
        type=int,
        help='New burst capacity (max tokens in bucket, e.g., 2 for testing, 50 for production)'
    )

    args = parser.parse_args()

    if args.capacity <= 0:
        print("❌ Error: capacity must be greater than 0")
        sys.exit(1)

    update_burst_capacity(args.capacity)
