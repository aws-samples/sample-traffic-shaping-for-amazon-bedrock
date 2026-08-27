#!/bin/bash
# Deploy script - Deploy CDK stack and generate config.env
# Usage:
#   ./deploy.sh --init    # First time: create venv, install deps, deploy
#   ./deploy.sh           # Redeploy: just deploy and update config

set -e  # Exit on error

# Parse arguments
INIT_MODE=false
if [ "$1" == "--init" ]; then
    INIT_MODE=true
fi

if [ "$INIT_MODE" = true ]; then
    echo "============================================================"
    echo "Semaphore Rate Limiter - Initial Setup"
    echo "============================================================"
else
    echo "============================================================"
    echo "Semaphore Rate Limiter - Redeploy"
    echo "============================================================"
fi
echo ""

# Initialize virtual environment if --init flag is set
if [ "$INIT_MODE" = true ]; then
    echo "Creating virtual environment..."
    if [ -d ".venv" ]; then
        echo "⚠️  Virtual environment already exists, skipping creation"
    else
        python3 -m venv .venv
        echo "✅ Virtual environment created"
    fi
    echo ""
    
    echo "Installing dependencies..."
    source .venv/bin/activate
    pip install -r requirements.txt
    echo "✅ Dependencies installed"
    echo ""
else
    # For redeploy, just activate existing venv
    if [ ! -d ".venv" ]; then
        echo "❌ Virtual environment not found. Please run with --init flag first:"
        echo "   ./deploy.sh --init"
        echo "   OR"
        echo "   make setup"
        exit 1
    fi
    source .venv/bin/activate
fi

# Check if CDK is installed and meets minimum version
MIN_CDK_VERSION="2.1033.0"
if ! command -v cdk &> /dev/null; then
    echo "❌ AWS CDK not found. Please install it first:"
    echo "   npm install -g aws-cdk@2.1033.0"
    exit 1
fi

CDK_VERSION=$(cdk --version | awk '{print $1}')
if [ "$(printf '%s\n' "$MIN_CDK_VERSION" "$CDK_VERSION" | sort -V | head -n1)" != "$MIN_CDK_VERSION" ]; then
    echo "❌ AWS CDK CLI version $CDK_VERSION is too old."
    echo "   Minimum required: $MIN_CDK_VERSION"
    echo "   Please update:  npm install -g aws-cdk@2.1033.0"
    exit 1
fi

# Check if AWS credentials are configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS credentials not configured. Please run:"
    echo "   aws configure"
    exit 1
fi

echo "✅ Prerequisites check passed"
echo ""

# Deploy CDK stack
echo "Deploying CDK stack..."
echo "This may take a few minutes..."
echo ""

# The Mantle (dual-backend) IAM grant is gated behind the CDK context flag
# `enable_mantle` (default OFF in code, so the public/default synth is Mantle-free).
# This deployed stack was provisioned with Mantle ENABLED — deploying WITHOUT the flag
# would tear out the live `bedrock-mantle:CreateInference` IAM grant. To keep routine
# `make deploy` safe, pass the flag by default here; override with ENABLE_MANTLE=false
# to intentionally deploy a Mantle-free stack.
ENABLE_MANTLE="${ENABLE_MANTLE:-true}"
echo "  enable_mantle=${ENABLE_MANTLE} (set ENABLE_MANTLE=false to deploy without the Mantle backend)"

cdk deploy --require-approval never \
    --context enable_mantle="${ENABLE_MANTLE}" \
    --outputs-file cdk-outputs.json

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ CDK deployment failed. Please check the error above."
    exit 1
fi

echo ""
echo "✅ CDK deployment successful"
echo ""

# Remove old config.env if it exists
rm -f config.env

# Parse CDK outputs and generate config.env
echo "Generating config.env from deployment outputs..."

# Extract values from cdk-outputs.json
STACK_NAME=$(jq -r 'keys[0]' cdk-outputs.json)
STATE_MACHINE_ARN=$(jq -r ".[\"$STACK_NAME\"].StateMachineArn" cdk-outputs.json)
BUDGET_MANAGER_ARN=$(jq -r ".[\"$STACK_NAME\"].BudgetManagerFunctionArn" cdk-outputs.json)
QUEUE_PROCESSOR_ARN=$(jq -r ".[\"$STACK_NAME\"].QueueProcessorFunctionArn" cdk-outputs.json)
FOUNDATION_MODEL_ARN=$(jq -r ".[\"$STACK_NAME\"].FoundationModelFunctionArn" cdk-outputs.json)
SINGLE_TABLE_NAME=$(jq -r ".[\"$STACK_NAME\"].SingleTableName" cdk-outputs.json)
API_GATEWAY_URL=$(jq -r ".[\"$STACK_NAME\"].ApiGatewayUrl // empty" cdk-outputs.json)
CLOUDFRONT_URL=$(jq -r ".[\"$STACK_NAME\"].CloudFrontUrl // empty" cdk-outputs.json)
CLOUDFRONT_DISTRIBUTION_ID=$(jq -r ".[\"$STACK_NAME\"].CloudFrontDistributionId // empty" cdk-outputs.json)
WAF_WEB_ACL_ARN=$(jq -r ".[\"$STACK_NAME\"].WafWebAclArn // empty" cdk-outputs.json)
DLQ_URL=$(jq -r ".[\"$STACK_NAME\"].DlqUrl // empty" cdk-outputs.json)
DLQ_ARN=$(jq -r ".[\"$STACK_NAME\"].DlqArn // empty" cdk-outputs.json)
DASHBOARD_URL=$(jq -r ".[\"$STACK_NAME\"].DashboardUrl // empty" cdk-outputs.json)
# Extract region from a deployed resource ARN (arn:aws:service:REGION:account:...)
# This ensures config.env matches the actual CDK stack region, not the CLI default
AWS_REGION=$(echo "$STATE_MACHINE_ARN" | cut -d: -f4)

# Get log group names from CloudFormation stack resources (single API call)
STACK_RESOURCES_JSON=$(aws cloudformation describe-stack-resources \
    --stack-name "$STACK_NAME" \
    --output json 2>/dev/null || echo '{"StackResources":[]}')

BUDGET_MANAGER_LOG_GROUP=$(echo "$STACK_RESOURCES_JSON" | \
    jq -r '.StackResources[] | select(.LogicalResourceId | startswith("BudgetManagerLogGroup")) | .PhysicalResourceId // empty')
QUEUE_PROCESSOR_LOG_GROUP=$(echo "$STACK_RESOURCES_JSON" | \
    jq -r '.StackResources[] | select(.LogicalResourceId | startswith("QueueProcessorLogGroup")) | .PhysicalResourceId // empty')
BEDROCK_PROCESSOR_LOG_GROUP=$(echo "$STACK_RESOURCES_JSON" | \
    jq -r '.StackResources[] | select(.LogicalResourceId | startswith("BedrockProcessorLogGroup")) | .PhysicalResourceId // empty')

# Create config.env
cat > config.env << EOF
# Semaphore Rate Limiter Configuration
# Auto-generated by deploy.sh on $(date)

# AWS Configuration
AWS_REGION=$AWS_REGION

# Step Functions
STATE_MACHINE_ARN=$STATE_MACHINE_ARN

# DynamoDB Table
SINGLE_TABLE_NAME=$SINGLE_TABLE_NAME

# Lambda Functions (ARNs for configuration updates)
BUDGET_MANAGER_ARN=$BUDGET_MANAGER_ARN
QUEUE_PROCESSOR_ARN=$QUEUE_PROCESSOR_ARN
FOUNDATION_MODEL_ARN=$FOUNDATION_MODEL_ARN

# CloudWatch Log Groups (for viewing logs)
BUDGET_MANAGER_LOG_GROUP=$BUDGET_MANAGER_LOG_GROUP
QUEUE_PROCESSOR_LOG_GROUP=$QUEUE_PROCESSOR_LOG_GROUP
BEDROCK_PROCESSOR_LOG_GROUP=$BEDROCK_PROCESSOR_LOG_GROUP

# Edge Layer (CloudFront + WAF)
API_GATEWAY_URL=$API_GATEWAY_URL
CLOUDFRONT_URL=$CLOUDFRONT_URL
CLOUDFRONT_DISTRIBUTION_ID=$CLOUDFRONT_DISTRIBUTION_ID
WAF_WEB_ACL_ARN=$WAF_WEB_ACL_ARN

# Dead Letter Queue
DLQ_URL=$DLQ_URL
DLQ_ARN=$DLQ_ARN

# Observability
DASHBOARD_URL=$DASHBOARD_URL

# Testing Configuration
BEDROCK_MODEL_ID=us.amazon.nova-2-lite-v1:0
NUM_REQUESTS=125
MAX_WORKERS=10
SUBMISSION_DURATION=10
EOF

echo "✅ config.env generated successfully"
echo ""

# Make test script executable
chmod +x scripts/test_reserve_release.sh
echo "✅ Made test_reserve_release.sh executable"
echo ""

# Create default model configurations (only during init)
if [ "$INIT_MODE" = true ]; then
    echo "Creating default model configurations..."
    # `model` is a positional arg (not --model), and the `if <cmd>` form keeps a
    # config failure from aborting under `set -e` so the warning path is reachable.
    # Defaults are active models (nova-2-lite / sonnet-5 / opus-5); the prior
    # opus-4-1 + jamba defaults were retired by AWS as Legacy (2026-08-24).
    for DEFAULT_MODEL in nova-2-lite sonnet-5 opus-5; do
        if python scripts/create_model_config.py "$DEFAULT_MODEL" > /dev/null 2>&1; then
            echo "✅ Created $DEFAULT_MODEL model config"
        else
            echo "⚠️  Failed to create $DEFAULT_MODEL config (may need AWS credentials refresh or model access)"
        fi
    done
    echo ""
fi

# Clean up temporary file
rm -f cdk-outputs.json

if [ "$INIT_MODE" = true ]; then
    echo "============================================================"
    echo "Setup Complete!"
    echo "============================================================"
    echo ""
    echo "✅ Default model configs created for Opus and Jamba"
    echo ""
    echo "Next steps:"
    echo "  1. (Optional) Override Opus config for queueing demo:"
    echo "     make create-config MODEL=nova-2-lite RPM=10 BURST_CAPACITY=2"
    echo ""
    echo "  2. Test the deployment:"
    echo "     make test"
    echo ""
    echo "  3. Monitor queue processing:"
    echo "     make check-queue"
    echo ""
    echo "  4. View logs:"
    echo "     make tail-budget"
    echo "     make tail-queue"
    echo ""
    echo "============================================================"
else
    echo "============================================================"
    echo "Redeploy Complete!"
    echo "============================================================"
    echo ""
    echo "Note: Model configs preserved (not recreated during redeploy)"
    echo ""
    echo "To update model configs:"
    echo "  make create-config MODEL=nova-2-lite RPM=10 BURST_CAPACITY=<value>"
    echo "  make create-config MODEL=sonnet-5"
    echo ""
    echo "============================================================"
fi
