#!/usr/bin/env python3
"""CDK app entry point for Semaphore Rate Limiter infrastructure."""

import aws_cdk as cdk
from cdk_nag import AwsSolutionsChecks
from semaphore_stack import SemaphoreRateLimiterStack


app = cdk.App()

# cdk-nag: run the AWS Solutions ruleset against the synthesized template.
# Findings are surfaced at `cdk synth`/`cdk deploy`. Known, intentional
# deviations for this internal load-test/demo-grade stack are documented via
# NagSuppressions inside SemaphoreRateLimiterStack (see semaphore_stack.py).
cdk.Aspects.of(app).add(AwsSolutionsChecks(verbose=True))

# Get environment configuration
env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1"
)

# Create the main stack
SemaphoreRateLimiterStack(
    app, 
    "SemaphoreRateLimiterStack",
    env=env,
    description="Semaphore-based rate limiter for AWS Step Functions and Bedrock"
)

app.synth()