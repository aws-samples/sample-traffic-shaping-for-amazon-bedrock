#!/usr/bin/env python3
"""
Cleanup script for DynamoDB single table.
Deletes all items except CONFIG records (model configuration).
"""

import boto3
import sys
from botocore.exceptions import ClientError
import config_loader

# Load configuration
config = config_loader.load_config()
SINGLE_TABLE = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')


def cleanup_table(table_name, key_schema, preserve_filter=None):
    """Delete all items from a DynamoDB table.

    Args:
        table_name: Name of the DynamoDB table
        key_schema: List of key attribute names
        preserve_filter: Optional function that returns True for items to preserve
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    print(f"\n🧹 Cleaning up table: {table_name}")

    try:
        # Scan all items
        response = table.scan()
        items = response.get('Items', [])

        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))

        if not items:
            print(f"   ✅ Table is already empty (0 items)")
            return

        # Filter items to delete (preserve some if filter provided)
        items_to_delete = items
        preserved_count = 0
        if preserve_filter:
            items_to_delete = [item for item in items if not preserve_filter(item)]
            preserved_count = len(items) - len(items_to_delete)

        if not items_to_delete:
            print(f"   ✅ No items to delete ({preserved_count} preserved)")
            return

        print(f"   Found {len(items_to_delete)} items to delete" +
              (f" ({preserved_count} preserved)" if preserved_count else ""))

        # Delete each item
        deleted_count = 0
        for item in items_to_delete:
            # Build key from key schema
            key = {k: item[k] for k in key_schema}
            table.delete_item(Key=key)
            deleted_count += 1

        print(f"   ✅ Deleted {deleted_count} items")

    except ClientError as e:
        print(f"   ❌ Error: {e.response['Error']['Message']}")
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")


def is_config_item(item):
    """Check if item is a CONFIG item that should be preserved."""
    return item.get('sk') == 'CONFIG'


def main():
    print("=" * 60)
    print("DynamoDB Table Cleanup Script")
    print("=" * 60)

    # Clean up single table, preserving CONFIG items (model configuration)
    cleanup_table(SINGLE_TABLE, ['pk', 'sk'], preserve_filter=is_config_item)

    print("\n" + "=" * 60)
    print("✅ Cleanup complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
