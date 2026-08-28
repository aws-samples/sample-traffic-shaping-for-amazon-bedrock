#!/bin/bash

# Test Stale Lock Recovery
# Scenario:
# 1. Create a stale lock in the single table with expired TTL
# 2. Enqueue a test request
# 3. Trigger Budget Manager (which should detect stale lock and overwrite it)
# 4. Verify Queue Processor acquires lock and processes the request
# 5. Verify lock is now held by new processor (different processor_id)
#
# Usage: ./test_stale_lock_recovery.sh [MODEL_ID]
# Example: ./test_stale_lock_recovery.sh us.anthropic.claude-opus-5

# Load configuration
if [ ! -f config.env ]; then
    echo "config.env not found. Run 'make setup' first."
    exit 1
fi
source config.env

# Default to Claude Opus, or use command line argument
MODEL_ID="${1:-us.anthropic.claude-opus-5}"

echo "=========================================="
echo "Test: Stale Lock Recovery"
echo "Model: $MODEL_ID"
echo "=========================================="
echo ""

# Step 1: Cleanup from previous runs
echo "Step 1: Cleaning up queue and locks from previous runs..."
python3 scripts/cleanup_tables.py
echo "Cleanup complete"
echo ""

# Step 1b: Set burst capacity to 0 to force queueing
echo "Step 1b: Setting burst_capacity=0 to force queueing..."
python3 scripts/create_model_config.py opus --burst-capacity 0
echo ""

# Step 2: Create a stale lock directly in DynamoDB single table
echo "Step 2: Creating stale lock in single table..."

# Calculate stale TTL (10 minutes in the past)
STALE_TTL=$(($(date +%s) - 600))  # nosemgrep: unquoted-command-substitution-in-command -- numeric arithmetic context, quoting not applicable
STALE_PROCESSOR_ID="stale-processor-$(date +%s)"

# Build JSON item as single line to avoid bash parsing issues
LOCK_ITEM="{\"pk\":{\"S\":\"MODEL#${MODEL_ID}#LOCK\"},\"sk\":{\"S\":\"PROCESSOR#0\"},\"entity_type\":{\"S\":\"processor_lock\"},\"processor_id\":{\"S\":\"${STALE_PROCESSOR_ID}\"},\"locked_at\":{\"S\":\"2024-01-01T00:00:00Z\"},\"ttl\":{\"N\":\"${STALE_TTL}\"}}"

if ! aws dynamodb put-item --table-name "$SINGLE_TABLE_NAME" --item "$LOCK_ITEM"; then
  echo "FAILED: Could not create stale lock"
  exit 1
fi

echo "Created stale lock:"
echo "  processor_id: $STALE_PROCESSOR_ID"
echo "  ttl: $STALE_TTL ($(date -r "$STALE_TTL"))"
echo ""

# Step 3: Verify stale lock exists
echo "Step 3: Verifying stale lock exists..."
LOCK_BEFORE=$(aws dynamodb get-item \
  --table-name "$SINGLE_TABLE_NAME" \
  --key "{\"pk\":{\"S\":\"MODEL#${MODEL_ID}#LOCK\"},\"sk\":{\"S\":\"PROCESSOR#0\"}}" \
  --query 'Item' --output json)

if [ "$LOCK_BEFORE" == "null" ] || [ -z "$LOCK_BEFORE" ]; then
  echo "FAILED: Stale lock was not created"
  exit 1
else
  echo "Stale lock exists:"
  echo "$LOCK_BEFORE" | jq '{processor_id: .processor_id.S, ttl: .ttl.N, locked_at: .locked_at.S}'
fi
echo ""

# Step 4: Fire off a test request via Step Functions
echo "Step 4: Sending test request via Step Functions..."
EXECUTION=$(aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --input "{\"request_id\":\"stale-lock-test-$(date +%s)\",\"model_id\":\"$MODEL_ID\",\"prompt\":\"Test stale lock recovery\"}" \
  --query 'executionArn' --output text)
echo "Execution started: $EXECUTION"
echo ""

# Step 5: Wait for processing
echo "Step 5: Waiting for Queue Processor to acquire lock and process request..."
sleep 10

# Step 6: Check the lock state after processing
echo ""
echo "Step 6: Checking lock state after processing..."
LOCK_AFTER=$(aws dynamodb get-item \
  --table-name "$SINGLE_TABLE_NAME" \
  --key "{\"pk\":{\"S\":\"MODEL#${MODEL_ID}#LOCK\"},\"sk\":{\"S\":\"PROCESSOR#0\"}}" \
  --query 'Item' --output json)

if [ "$LOCK_AFTER" == "null" ] || [ -z "$LOCK_AFTER" ]; then
  echo "Lock was released (deleted) - processor completed and cleaned up"
  LOCK_OVERWRITTEN=true
else
  NEW_PROCESSOR_ID=$(echo "$LOCK_AFTER" | jq -r '.processor_id.S')

  echo "Lock state after processing:"
  echo "$LOCK_AFTER" | jq '{processor_id: .processor_id.S, ttl: .ttl.N, locked_at: .locked_at.S}'

  # Check if processor_id changed (stale lock was overwritten)
  if [ "$NEW_PROCESSOR_ID" != "$STALE_PROCESSOR_ID" ]; then
    echo ""
    echo "SUCCESS: Stale lock was overwritten!"
    echo "  Old processor_id: $STALE_PROCESSOR_ID"
    echo "  New processor_id: $NEW_PROCESSOR_ID"
    LOCK_OVERWRITTEN=true
  else
    echo ""
    echo "WARNING: Lock still has same processor_id (may not have been overwritten)"
    LOCK_OVERWRITTEN=false
  fi
fi
echo ""

# Step 7: Check execution status
echo "Step 7: Checking execution status..."
STATUS=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION" --query 'status' --output text)
OUTPUT=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION" --query 'output' --output text 2>/dev/null || echo "{}")
QUEUED=$(echo "$OUTPUT" | jq -r '.budget_result.queued // "unknown"')

echo "Execution status: $STATUS"
echo "Request queued: $QUEUED"
echo ""

# Step 8: Check queue is empty (request was processed)
echo "Step 8: Verifying queue is empty..."
QUEUE_ITEMS=$(aws dynamodb query \
  --table-name "$SINGLE_TABLE_NAME" \
  --key-condition-expression "pk = :pk" \
  --expression-attribute-values "{\":pk\":{\"S\":\"MODEL#${MODEL_ID}#QUEUE#ITEMS\"}}" \
  --query 'Items' --output json)

QUEUE_COUNT=$(echo "$QUEUE_ITEMS" | jq 'length')
echo "Queue depth: $QUEUE_COUNT"
echo ""

# Step 9: Summary
echo "=========================================="
echo "Test Summary:"
echo "=========================================="

PASS_COUNT=0
FAIL_COUNT=0

# Check 1: Lock was overwritten
if [ "$LOCK_OVERWRITTEN" = true ]; then
  echo "PASS - Stale lock was overwritten by new processor"
  ((PASS_COUNT++))
else
  echo "FAIL - Stale lock was NOT overwritten"
  ((FAIL_COUNT++))
fi

# Check 2: Execution completed successfully
if [ "$STATUS" == "SUCCEEDED" ]; then
  echo "PASS - Step Function execution succeeded"
  ((PASS_COUNT++))
else
  echo "FAIL - Step Function execution status: $STATUS (expected: SUCCEEDED)"
  ((FAIL_COUNT++))
fi

# Check 3: Queue is empty (request was processed)
if [ "$QUEUE_COUNT" -eq 0 ]; then
  echo "PASS - Queue is empty (request was processed)"
  ((PASS_COUNT++))
else
  echo "FAIL - Queue depth: $QUEUE_COUNT (expected: 0)"
  ((FAIL_COUNT++))
fi

# Step 9: Restore burst capacity to default
echo "Step 9: Restoring burst capacity to default..."
python3 scripts/create_model_config.py opus
echo ""

echo ""
echo "Results: $PASS_COUNT passed, $FAIL_COUNT failed"

if [ "$FAIL_COUNT" -eq 0 ]; then
  echo ""
  echo "SUCCESS: Stale lock recovery is working correctly!"
  exit 0
else
  echo ""
  echo "FAILURE: Some checks failed. Check logs for details:"
  echo "  make logs-budget-recent"
  echo "  make logs-queue-recent"
  exit 1
fi
