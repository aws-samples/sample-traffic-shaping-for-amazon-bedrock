# Bedrock Traffic Shaper — Cost Model

**Date:** 2026-03-15
**Region:** us-east-1
**Stack:** SemaphoreRateLimiterStack

---

## Assumptions

### Request Flow

Each request flows through: API Gateway → Step Functions → Budget Manager Lambda → DynamoDB → (Burst path OR Queue path) → Bedrock Processor Lambda.

### DynamoDB Operations per Request

| Path | Operation | WCU | RCU | Notes |
|------|-----------|-----|-----|-------|
| **All requests** | Query consumption records | 0 | 1 | ConsistentRead |
| **All requests** | TransactWriteItems (admission gate) | 6 | 0 | 3 items × 2 WCU each |
| **Queue path only** | Enqueue item | 1 | 0 | PutItem |
| **Queue path only** | Dequeue item | 1 | 0 | DeleteItem |
| **Queue path only** | Queue consumption record | 1 | 0 | PutItem |

- **Burst path (70% of requests):** 6 WCU + 1 RCU per request
- **Queue path (30% of requests):** 9 WCU + 1 RCU per request
- **Weighted average:** 6.9 WCU + 1 RCU per request

### Step Functions

- ~5 state transitions per execution (Start → Budget Manager task → Choice → Callback wait → End)
- Standard Workflows: $0.025 per 1,000 state transitions

### Lambda Functions

| Function | Memory | Avg Duration | Invocations per Request |
|----------|--------|--------------|------------------------|
| Budget Manager | 128 MB | 500 ms | 1.0 |
| Bedrock Processor | 128 MB | 3 s | 1.0 |
| Queue Processor | 128 MB | 1 s per item | 0.3 (queue path only) |

### Path Split

- 70% burst path (immediate admission)
- 30% queue path (queued, RPM-paced drain)

Based on Phase 6g testing: with burst_capacity=5 and 150 requests, 4% burst / 96% queued. Production configs with higher burst_capacity will have ~70/30 split.

---

## AWS Pricing (us-east-1)

| Service | Unit | Price |
|---------|------|-------|
| DynamoDB On-Demand | WCU | $1.25 / million |
| DynamoDB On-Demand | RCU | $0.25 / million |
| Step Functions Standard | State transition | $0.025 / 1,000 |
| Lambda | Invocation | $0.20 / million |
| Lambda | GB-second | $0.0000166667 |
| API Gateway REST | Request | $3.50 / million |
| EventBridge | Event | $1.00 / million |
| CloudWatch Logs | GB ingested | $0.50 / GB |
| CloudWatch Metrics | Custom metric | $0.30 / metric / month |

---

## Monthly Cost by Tier

### Tier 1: 100 RPM Sustained (2.6M requests/month)

| Service | Calculation | Monthly Cost |
|---------|-------------|--------------|
| API Gateway | 2.6M × $3.50/M | $9.10 |
| Step Functions | 2.6M × 5 × $0.025/K | $32.50 |
| DynamoDB WCU | 2.6M × 6.9 × $1.25/M | $22.43 |
| DynamoDB RCU | 2.6M × 1 × $0.25/M | $0.65 |
| Lambda invocations | 2.6M × 2.3 × $0.20/M | $1.20 |
| Lambda duration (Budget Mgr) | 2.6M × 0.5s × 0.125GB × $0.0000167 | $2.71 |
| Lambda duration (Bedrock Proc) | 2.6M × 3s × 0.125GB × $0.0000167 | $16.25 |
| Lambda duration (Queue Proc) | 0.78M × 1s × 0.125GB × $0.0000167 | $1.63 |
| EventBridge | ~1M events × $1.00/M | $1.00 |
| CloudWatch Logs | ~5 GB × $0.50/GB | $2.50 |
| CloudWatch Metrics | ~20 metrics × $0.30 | $6.00 |
| **TOTAL** | | **$95.97** |

### Tier 2: 500 RPM Sustained (13M requests/month)

| Service | Calculation | Monthly Cost |
|---------|-------------|--------------|
| API Gateway | 13M × $3.50/M | $45.50 |
| Step Functions | 13M × 5 × $0.025/K | $162.50 |
| DynamoDB WCU | 13M × 6.9 × $1.25/M | $112.13 |
| DynamoDB RCU | 13M × 1 × $0.25/M | $3.25 |
| Lambda invocations | 13M × 2.3 × $0.20/M | $5.98 |
| Lambda duration (Budget Mgr) | 13M × 0.5s × 0.125GB × $0.0000167 | $13.54 |
| Lambda duration (Bedrock Proc) | 13M × 3s × 0.125GB × $0.0000167 | $81.25 |
| Lambda duration (Queue Proc) | 3.9M × 1s × 0.125GB × $0.0000167 | $8.13 |
| EventBridge | ~5M events × $1.00/M | $5.00 |
| CloudWatch Logs | ~25 GB × $0.50/GB | $12.50 |
| CloudWatch Metrics | ~20 metrics × $0.30 | $6.00 |
| **TOTAL** | | **$455.78** |

### Tier 3: 1000 RPM Sustained (26M requests/month)

| Service | Calculation | Monthly Cost |
|---------|-------------|--------------|
| API Gateway | 26M × $3.50/M | $91.00 |
| Step Functions | 26M × 5 × $0.025/K | $325.00 |
| DynamoDB WCU | 26M × 6.9 × $1.25/M | $224.25 |
| DynamoDB RCU | 26M × 1 × $0.25/M | $6.50 |
| Lambda invocations | 26M × 2.3 × $0.20/M | $11.96 |
| Lambda duration (Budget Mgr) | 26M × 0.5s × 0.125GB × $0.0000167 | $27.08 |
| Lambda duration (Bedrock Proc) | 26M × 3s × 0.125GB × $0.0000167 | $162.50 |
| Lambda duration (Queue Proc) | 7.8M × 1s × 0.125GB × $0.0000167 | $16.25 |
| EventBridge | ~10M events × $1.00/M | $10.00 |
| CloudWatch Logs | ~50 GB × $0.50/GB | $25.00 |
| CloudWatch Metrics | ~20 metrics × $0.30 | $6.00 |
| **TOTAL** | | **$905.54** |

---

## Summary

| Tier | Requests/Month | Monthly Cost | Cost/Request | Cost/1M Requests |
|------|----------------|--------------|--------------|------------------|
| **100 RPM** | 2,592,000 | **$95.97** | $0.000037 | $37.03 |
| **500 RPM** | 12,960,000 | **$455.78** | $0.000035 | $35.17 |
| **1000 RPM** | 25,920,000 | **$905.54** | $0.000035 | $34.94 |

### Cost Drivers (% of total at 1000 RPM)

| Service | % of Total | Monthly Cost |
|---------|-----------|--------------|
| Step Functions | 35.9% | $325.00 |
| DynamoDB | 25.5% | $230.75 |
| Lambda duration | 22.7% | $205.83 |
| API Gateway | 10.0% | $91.00 |
| Other (CW, EB) | 5.9% | $53.50 |

### Key Observations

1. **Step Functions is the dominant cost** (36%) due to the waitForTaskToken callback pattern requiring state transitions.
2. **DynamoDB is the second-largest cost** (26%) driven by TransactWriteItems at 6 WCU per admission.
3. **Infrastructure overhead is ~$0.035 per 1,000 requests** — negligible vs. Bedrock inference costs ($3-15 per 1,000 requests for Claude models).
4. **Economies of scale** are minimal (5.6% cost reduction from 100→1000 RPM) because fixed costs (CloudWatch) are small relative to per-request costs.

---

## What's Excluded

- **Bedrock inference costs** — model-specific, typically $3-15 per 1,000 requests
- **Data transfer** — negligible for typical request/response sizes
- **WAF** — regional WAFv2 Web ACL on the API Gateway `prod` stage; $5/month base + $1/million requests
- **SQS DLQ** — only on failures (target: 0 messages/month)
- **DynamoDB storage** — <1 GB with TTL cleanup, ~$0.25/month

---

## Cost Optimization Opportunities

If cost becomes a concern at high RPM:

1. **DynamoDB provisioned capacity** — Switch from on-demand ($1.25/M WCU) to provisioned ($0.00065/WCU-hour) at predictable load. Saves ~40% on DDB costs.
2. **Step Functions Express Workflows** — $0.000001 per 100ms (vs. $0.025/1K transitions for Standard). Saves ~70% on SFN costs, but loses waitForTaskToken — requires architecture change.
3. **Lambda ARM64** — Already using ARM64 (graviton2). No further optimization.
