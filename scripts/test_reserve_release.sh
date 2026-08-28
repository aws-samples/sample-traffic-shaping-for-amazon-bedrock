#!/bin/bash

# Test Immediate vs Queued Execution
# Scenario:
# 1. Request 1: Should be processed immediately
# 2. Requests 2-4: Submitted in parallel - may be immediate or queued depending on burst_capacity
# 3. Request 5: Should complete successfully
#
# Usage: ./test_reserve_release.sh [MODEL_ID]
# Example: ./test_reserve_release.sh us.amazon.nova-2-lite-v1:0

# Load configuration
if [ ! -f config.env ]; then
    echo "❌ config.env not found. Run 'make setup' first."
    exit 1
fi
source config.env

# Set AWS_DEFAULT_REGION so all aws CLI calls use the correct region
export AWS_DEFAULT_REGION="${AWS_REGION:-us-east-1}"

# Default to Opus, or use command line argument
MODEL_ID="${1:-us.anthropic.claude-opus-5}"

# Read burst_capacity from DynamoDB config
BURST_CAPACITY=$(aws dynamodb get-item \
  --table-name "$SINGLE_TABLE_NAME" \
  --key "{\"pk\":{\"S\":\"MODEL#${MODEL_ID}\"},\"sk\":{\"S\":\"CONFIG\"}}" \
  --query 'Item.burst_capacity.N' --output text 2>/dev/null)

if [ "$BURST_CAPACITY" == "None" ] || [ -z "$BURST_CAPACITY" ]; then
    echo "❌ No config found for model: $MODEL_ID"
    echo "   Run: make create-config MODEL=nova-2-lite"
    exit 1
fi

echo "=========================================="
echo "Test: Immediate vs Queued Execution (burst_capacity=$BURST_CAPACITY)"
echo "Model: $MODEL_ID"
echo "=========================================="
echo ""

# Cleanup function - purge queue and lock before test
echo "Cleaning up queue and lock from previous runs..."
python3 scripts/cleanup_tables.py
echo "✅ Cleanup complete"
echo ""

# Fire off Request 1
echo "Starting Request 1 (should get allocation)..."
EXECUTION_1=$(aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --input "{\"request_id\":\"test-req-1\",\"model_id\":\"$MODEL_ID\",\"prompt\":\"Hello 1\"}" \
  --query 'executionArn' --output text)
echo "Request 1: $EXECUTION_1"

# Fire off Requests 2-4 in parallel (should all be queued - tests lock idempotency)
echo ""
echo "Starting Requests 2-4 in parallel (testing lock idempotency and key condition)..."

# Start all three requests in parallel using background processes
aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --input "{\"request_id\":\"test-req-2\",\"model_id\":\"$MODEL_ID\",\"prompt\":\"Hello 2\"}" \
  --query 'executionArn' --output text > /tmp/exec_2.txt &
PID_2=$!

aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --input "{\"request_id\":\"test-req-3\",\"model_id\":\"$MODEL_ID\",\"prompt\":\"Hello 3\"}" \
  --query 'executionArn' --output text > /tmp/exec_3.txt &
PID_3=$!

aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --input "{\"request_id\":\"test-req-4\",\"model_id\":\"$MODEL_ID\",\"prompt\":\"Hello 4\"}" \
  --query 'executionArn' --output text > /tmp/exec_4.txt &
PID_4=$!

# Wait for all parallel requests to complete
wait "$PID_2" "$PID_3" "$PID_4"

# Read the execution ARNs from temp files
EXECUTION_2=$(cat /tmp/exec_2.txt)
EXECUTION_3=$(cat /tmp/exec_3.txt)
EXECUTION_4=$(cat /tmp/exec_4.txt)

echo "Request 2: Started"
echo "Request 3: Started"
echo "Request 4: Started"

# Wait for earlier requests to complete
echo ""
echo "Waiting for requests to complete..."
sleep 5

# Fire off Request 5
echo "Starting Request 5 (should succeed)..."
EXECUTION_5=$(aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --input "{\"request_id\":\"test-req-5\",\"model_id\":\"$MODEL_ID\",\"prompt\":\"Hello 5\"}" \
  --query 'executionArn' --output text)
echo "Request 5: $EXECUTION_5"

echo ""
echo "Waiting for executions to complete..."
sleep 3

echo ""
echo "=========================================="
echo "Results:"
echo "=========================================="

# Check all requests
STATUS_1=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_1" --query 'status' --output text)
OUTPUT_1=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_1" --query 'output' --output text)
SOURCE_1=$(echo "$OUTPUT_1" | jq -r '.budget_result.source // "unknown"')
echo "Request 1: $STATUS_1 | source=$SOURCE_1"

STATUS_2=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_2" --query 'status' --output text)
OUTPUT_2=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_2" --query 'output' --output text)
SOURCE_2=$(echo "$OUTPUT_2" | jq -r '.budget_result.source // "unknown"')
echo "Request 2: $STATUS_2 | source=$SOURCE_2"

STATUS_3=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_3" --query 'status' --output text)
OUTPUT_3=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_3" --query 'output' --output text)
SOURCE_3=$(echo "$OUTPUT_3" | jq -r '.budget_result.source // "unknown"')
echo "Request 3: $STATUS_3 | source=$SOURCE_3"

STATUS_4=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_4" --query 'status' --output text)
OUTPUT_4=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_4" --query 'output' --output text)
SOURCE_4=$(echo "$OUTPUT_4" | jq -r '.budget_result.source // "unknown"')
echo "Request 4: $STATUS_4 | source=$SOURCE_4"

STATUS_5=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_5" --query 'status' --output text)
OUTPUT_5=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_5" --query 'output' --output text)
SOURCE_5=$(echo "$OUTPUT_5" | jq -r '.budget_result.source // "unknown"')
echo "Request 5: $STATUS_5 | source=$SOURCE_5"

# Count immediate vs queued
IMMEDIATE_COUNT=0
QUEUED_COUNT=0
for SOURCE in "$SOURCE_1" "$SOURCE_2" "$SOURCE_3" "$SOURCE_4" "$SOURCE_5"; do
  if [ "$SOURCE" == "immediate" ]; then
    ((IMMEDIATE_COUNT++))
  elif [ "$SOURCE" == "queued" ]; then
    ((QUEUED_COUNT++))
  fi
done

echo ""
echo "Summary: $IMMEDIATE_COUNT immediate, $QUEUED_COUNT queued (burst_capacity=$BURST_CAPACITY)"

echo ""
echo "=========================================="
echo "Verify Queue State:"
echo "=========================================="

# Query single table for queued requests (model-based partition)
echo "Checking queue contents..."
QUEUE_ITEMS=$(aws dynamodb query \
  --table-name "$SINGLE_TABLE_NAME" \
  --key-condition-expression "pk = :pk" \
  --expression-attribute-values "{\":pk\":{\"S\":\"MODEL#${MODEL_ID}#QUEUE#ITEMS\"}}" \
  --query 'Items' --output json)

QUEUE_COUNT=$(echo "$QUEUE_ITEMS" | jq 'length')
echo "Queue depth: $QUEUE_COUNT (expected: 0 - all requests processed by Queue Processor)"

if [ "$QUEUE_COUNT" -eq 0 ]; then
  echo "✅ Queue empty - all requests processed successfully"
else
  echo "⚠️  Unexpected queue depth: $QUEUE_COUNT"
  echo "$QUEUE_ITEMS" | jq '.[] | {request_id: .request_id.S, sk: .sk.S, priority: .priority.N}'
fi

echo ""
echo "=========================================="
echo "Verify Lock Release:"
echo "=========================================="

# Check if processor lock exists in single table (should be released/deleted after processing)
LOCK=$(aws dynamodb get-item \
  --table-name "$SINGLE_TABLE_NAME" \
  --key "{\"pk\":{\"S\":\"MODEL#${MODEL_ID}#LOCK\"},\"sk\":{\"S\":\"PROCESSOR#0\"}}" \
  --query 'Item' --output json)

if [ "$LOCK" == "null" ] || [ -z "$LOCK" ]; then
  echo "✅ Processor lock properly released (deleted from table)"
else
  echo "⚠️  Processor lock still exists (may not have been released):"
  echo "$LOCK" | jq '{processor_id: .processor_id.S, locked_at: .locked_at.S, ttl: .ttl.N}'
fi

echo ""
echo "=========================================="
echo "View detailed logs:"
echo "=========================================="
echo "Budget Manager logs:"
echo "aws logs tail $BUDGET_MANAGER_LOG_GROUP --since 5m --format short"
echo ""
echo "Queue Processor logs:"
echo "aws logs tail $QUEUE_PROCESSOR_LOG_GROUP --since 5m --format short"

