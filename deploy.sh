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

# Check if the CDK CLI is installed and new enough to deploy this app.
#
# This floor is DICTATED BY the `aws-cdk-lib` pin in requirements.txt — it is not an
# independent choice. aws-cdk-lib 2.266.0 emits cloud-assembly schema 54, and only
# CDK CLI >= 2.1139.0 can read schema 54. A CLI below the floor passes this check's
# older forms, synthesizes cleanly, and then fails at deploy time with:
#     Cloud assembly schema version mismatch: Maximum schema version supported is
#     53.x.x, but found 54.0.0. You need at least CLI version 2.1139.0
# so the floor must be kept in lockstep with the library pin.
#
# If you bump aws-cdk-lib in requirements.txt, update BOTH:
#   1. MIN_CDK_VERSION below
#   2. the Tool/Version table under "Prerequisites" in README.md
# (CONTRIBUTING.md deliberately links to that table instead of restating the number.)
MIN_CDK_VERSION="2.1139.0"

# Single source of truth for the install hint, so the version can't drift between messages.
cdk_install_help() {
    echo "   npm install -g aws-cdk@${MIN_CDK_VERSION}"
    echo ""
    echo "   If 'npm install -g' fails with EACCES (npm's prefix is root-owned, which is"
    echo "   the default for Homebrew/system Node), install to a user-writable prefix:"
    echo "     npm install --prefix ~/.local/cdk-cli aws-cdk@${MIN_CDK_VERSION}"
    echo "     export PATH=\"\$HOME/.local/cdk-cli/node_modules/.bin:\$PATH\""
    echo "   ...or skip installing entirely and use:  npx aws-cdk@${MIN_CDK_VERSION}"
}

if ! command -v cdk &> /dev/null; then
    echo "❌ AWS CDK CLI not found. Install it with:"
    cdk_install_help
    exit 1
fi

CDK_VERSION=$(cdk --version | awk '{print $1}')
if [ "$(printf '%s\n' "$MIN_CDK_VERSION" "$CDK_VERSION" | sort -V | head -n1)" != "$MIN_CDK_VERSION" ]; then
    # The venv is active by this point, so report the library pin that sets the floor.
    CDK_LIB_VERSION=$(python -c "import importlib.metadata as m; print(m.version('aws-cdk-lib'))" 2>/dev/null || echo "unknown")
    echo "❌ AWS CDK CLI version $CDK_VERSION is too old to deploy this app."
    echo "   Minimum required: $MIN_CDK_VERSION (set by the aws-cdk-lib ${CDK_LIB_VERSION} pin"
    echo "   in requirements.txt, which emits cloud-assembly schema 54)."
    echo ""
    echo "   Update with:"
    cdk_install_help
    exit 1
fi

echo "   CDK CLI $CDK_VERSION (minimum $MIN_CDK_VERSION)"

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

# Refresh the quota cache and create the cache-driven starter package (only during
# init). The `if <cmd>` form keeps either step from aborting setup under `set -e` --
# get_bedrock_quotas.py already degrades gracefully on AccessDeniedException (empty
# quotas/profiles, hardcoded fallback downstream), but this also protects against any
# other unexpected failure so the starter package still gets a chance to run.
if [ "$INIT_MODE" = true ]; then
    echo "Refreshing Bedrock quota cache..."
    if python scripts/get_bedrock_quotas.py; then
        echo "✅ Quota cache refreshed (.bedrock_quota_cache.json)"
    else
        echo "⚠️  Quota cache refresh failed (may need AWS credentials refresh or model access) — starter package will use hardcoded fallback values"
    fi
    echo ""

    echo "Creating starter model configurations..."
    if python scripts/create_model_config.py --starter-package; then
        echo "✅ Starter package created"
    else
        echo "⚠️  Failed to create starter package (may need AWS credentials refresh or model access)"
    fi
    echo ""
fi

# Clean up temporary file
rm -f cdk-outputs.json

if [ "$INIT_MODE" = true ]; then
    # config/starter_models.json is an explicit list of inference profile IDs --
    # derive the counts from the file itself rather than a
    # hardcoded "12 across 6", which would silently lie the moment the list
    # changes (e.g. a base model with no global. profile on some account).
    STARTER_PROFILE_COUNT=$(jq 'length' config/starter_models.json 2>/dev/null || echo "?")
    STARTER_BASE_MODEL_COUNT=$(jq -r '.[] | sub("^(us\\.|global\\.)"; "")' config/starter_models.json 2>/dev/null | sort -u | wc -l | tr -d ' ')

    echo "============================================================"
    echo "Setup Complete!"
    echo "============================================================"
    echo ""
    echo "✅ Starter package created: ${STARTER_PROFILE_COUNT} model configs across ${STARTER_BASE_MODEL_COUNT} starter models"
    echo "   (nova-2-lite, sonnet-5, opus-5, gpt-5.6-luna, gpt-5.6-sol, gpt-5.6-terra) —"
    echo "   one entry per ACTIVE us./global. inference profile listed in config/starter_models.json"
    echo ""
    echo "Next steps:"
    echo "  1. Inspect the starter package:"
    echo "     make inspect-config MODEL=sonnet-5"
    echo ""
    echo "  2. Refresh quotas any time account limits change, then recreate configs:"
    echo "     make refresh-quotas && make create-starter-configs"
    echo ""
    echo "  3. (Optional) Override a config for queueing demo:"
    echo "     make create-config MODEL=nova-2-lite RPM=10 BURST_CAPACITY=2"
    echo ""
    echo "  4. Test the deployment:"
    echo "     make test"
    echo ""
    echo "  5. Monitor queue processing:"
    echo "     make check-queue"
    echo ""
    echo "  6. View logs:"
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
