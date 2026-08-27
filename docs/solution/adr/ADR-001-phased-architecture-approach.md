# ADR-001: Phased Architecture Approach for MVP Infrastructure

## Status
Superseded by ADR-002

**Note:** This ADR documents the original architectural planning before implementation. See [ADR-002: MVP Implementation and Beyond](ADR-002-mvp-implementation-and-beyond.md) for the actual implementation and revised phased approach based on learnings during development.

## Date
2026-01-30

## Context

We are building a semaphore-based rate limiter for AWS Step Functions that controls API calls to Amazon Bedrock Foundation Models. The system needs to handle request spikes better than traditional retry logic by using a queue-based approach with controlled throughput.

The full architecture requires AWS Step Functions "Wait for Callback with Task Token" pattern, where queued requests pause execution and resume when budget becomes available. However, implementing the complete architecture upfront creates deployment complexity and delays validation of core concepts.

We need to prove that the queue-based semaphore approach handles request spikes better than normal retry logic before investing in the full implementation.

## Decision

We will implement the infrastructure in **three phases**, starting with a minimal viable product (MVP) that proves the core concept, then iterating toward the full architecture.

### Phase 1: MVP - Queue with Graceful Degradation (Current)

**Goal:** Prove queue-based approach handles spikes better than retry logic

**Architecture:**
```
Step Functions Workflow:
1. Reserve Budget (Lambda)
   ↓
2. Choice State: Check if queued
   ├─ If allocated → Execute Foundation Model → Release Budget → Success
   └─ If queued → End with "queued" status

Background Process (EventBridge + Lambda):
- Queue Processor runs every 1 second
- Dequeues requests when budget available
- Allocates budget
- Executes Foundation Model (Bedrock call)
- Releases budget
- Logs completion
```

**Behavior:**
- First N requests (within quota): Get budget immediately via Step Functions, execute model, succeed
- Subsequent requests (quota exhausted): Get queued, Step Function ends with "queued" status
- Background processor steadily processes queue:
  - Dequeues next request
  - Allocates budget
  - Executes Foundation Model (Bedrock)
  - Releases budget
- **All requests eventually execute the model**, either via Step Functions (immediate) or Queue Processor (queued)

**What this proves:**
- ✅ Queue mechanics work correctly
- ✅ Budget allocation/release works
- ✅ Controlled throughput during spikes
- ✅ No retry storms or thundering herd
- ✅ Predictable queue processing
- ✅ All requests eventually execute (via Step Functions or Queue Processor)
- ✅ Foundation Model calls are rate-limited correctly

**Simplifications:**
- No priority parameter (default to 1)
- No callback pattern (Step Functions don't resume)
- Inline Lambda code initially (swap to real handlers later)
- Single DynamoDB table with pk/sk structure

**Success Metrics:**
- Queue depth grows during spike, drains steadily
- Allocation rate stays constant (respects quota)
- No failed requests (all queued successfully)
- Predictable processing time

### Phase 2: Real Implementation Integration

**Goal:** Replace inline code with production-ready implementations

**Changes:**
- Swap inline Lambda code for actual `budget_manager/handler.py`
- Integrate with `DynamoDBBudgetStore` (simplified for pk/sk table)
- Add proper error handling and logging
- Add CloudWatch metrics
- Keep same Step Functions workflow (no callback yet)

**What this proves:**
- ✅ Real code works with infrastructure
- ✅ Service layer integrations work
- ✅ Monitoring and observability work

### Phase 3: Full Callback Architecture

**Goal:** Complete the architecture with Step Functions callbacks

**Changes:**
- Add `task_token` parameter to Reserve Budget calls
- Change Step Functions to use `.waitForTaskToken` pattern
- Add Queue Processor callback logic (`SendTaskSuccess`)
- Queued requests now resume and execute Foundation Model

**Architecture:**
```
Step Functions Workflow:
1. Reserve Budget (Lambda with task token)
   ↓
2. Choice State: Check if queued
   ├─ If allocated → Execute Foundation Model
   └─ If queued → Wait for Callback
                   ↓ (paused, no Lambda running)
                   Queue Processor sends callback
                   ↓
                   Resume → Execute Foundation Model
   ↓
3. Release Budget
   ↓
4. Success

Background Process:
- Queue Processor runs every 1 second
- Dequeues requests when budget available
- Allocates budget
- Calls SendTaskSuccess(task_token, allocation_id)
- Step Function resumes and executes model via Step Functions workflow
```

**Key Difference from Phase 1:**
- Phase 1: Queue Processor executes model directly (outside Step Functions)
- Phase 3: Queue Processor sends callback, Step Functions resumes and executes model
- Phase 3 provides better observability (all executions visible in Step Functions history)

**What this proves:**
- ✅ Full end-to-end workflow
- ✅ Queued requests eventually execute
- ✅ Efficient resource usage (no blocking Lambdas)
- ✅ Complete value proposition

## Migration Path

**Phase 1 → Phase 2:**
- Replace Lambda code (no interface changes)
- Update DynamoDB store implementation
- No Step Functions changes needed

**Phase 2 → Phase 3:**
- Add `task_token` to Reserve Budget Lambda input (optional parameter)
- Update Step Functions state definition (one state change)
- Add `SendTaskSuccess` call to Queue Processor
- No changes to Reserve Budget Lambda logic (already returns correct structure)

**Key insight:** The migration is clean because:
- Lambda handler interface stays compatible
- DynamoDB schema already supports task_token (in existing code)
- Step Functions change is isolated to state definition
- No breaking changes to core logic

## Consequences

### Positive
- ✅ Can deploy and test infrastructure immediately
- ✅ Proves core concept before full investment
- ✅ Clear migration path to full architecture
- ✅ Each phase adds value incrementally
- ✅ Can validate with real load testing early
- ✅ Reduces risk of over-engineering

### Negative
- ⚠️ Phase 1 executes queued requests outside Step Functions (less observability)
- ⚠️ Need to explain to stakeholders that Phase 1 has split execution paths
- ⚠️ Some throwaway code (inline Lambdas)
- ⚠️ Multiple deployment cycles needed

### Neutral
- Phase 1 is sufficient for spike handling proof and all requests execute
- Background processor logic is reusable across all phases
- DynamoDB schema supports all phases
- Queue Processor needs to handle model execution in Phase 1, callbacks in Phase 3

## Alternatives Considered

### Alternative 1: Build Full Architecture Immediately
**Rejected because:**
- Higher complexity delays deployment
- Can't validate core concepts early
- Higher risk if approach doesn't work
- Harder to debug issues

### Alternative 2: Synchronous Blocking (Lambda polls queue)
**Rejected because:**
- Lambda blocks waiting, burning execution time
- Defeats purpose of queue-based approach
- Doesn't scale well
- Expensive

### Alternative 3: Wait/Retry Pattern
**Rejected because:**
- More complex migration to callback pattern
- Doesn't prove advantage over normal retry logic
- Still has retry storm potential
- Less clean architecture

## Implementation Notes

### Phase 1 MVP Components

**Infrastructure (CDK):**
1. DynamoDB table with pk/sk
2. Reserve Budget Lambda (inline code initially)
3. Release Budget Lambda (inline code initially)
4. Queue Processor Lambda (inline code initially)
5. Foundation Model Lambda (mock Bedrock call)
6. EventBridge rule (trigger Queue Processor every 1 second)
7. Step Functions state machine (Reserve → Choice → Execute → Release)

**Test Scenario:**
1. Set `max_allocations = 5`
2. Trigger 20 Step Functions executions simultaneously
3. Observe:
   - First 5 get budget, execute via Step Functions, succeed
   - Next 15 get queued, Step Functions end with "queued" status
   - Queue Processor steadily processes queue (dequeue → allocate → execute model → release)
   - All 20 requests eventually execute the Foundation Model
   - No retry storms

**Metrics to Collect:**
- Queue depth over time
- Allocation rate (should stay constant)
- Success vs queued ratio
- Processing latency
- Lambda execution times

### Configuration

**Environment Variables:**
- `TABLE_NAME`: DynamoDB table name
- `MAX_ALLOCATIONS`: Maximum concurrent allocations (default: 10)
- `REFRESH_RATE`: Allocations per second (default: 1.0)
- `QUEUE_TIMEOUT`: Max queue wait time in ms (default: 300000)

**EventBridge Schedule:**
- Queue Processor: `rate(1 second)`
- Budget Refresh: Based on `REFRESH_RATE` (Phase 2+)
- Orphaned Cleanup: `rate(5 minutes)` (Phase 2+)

## References

- [AWS Step Functions Wait for Callback Pattern](https://docs.aws.amazon.com/step-functions/latest/dg/callback-task-sample-sqs.html)
- [Semaphore Pattern for Rate Limiting](https://aws.amazon.com/blogs/compute/controlling-concurrency-in-distributed-systems-with-aws-step-functions/)
- Original requirements: `.kiro/specs/step-functions-architecture-refactor/requirements.md`

## Review and Updates

This ADR should be reviewed after:
- Phase 1 deployment and testing
- Load testing results
- Stakeholder feedback on approach
- Before starting Phase 2 implementation
