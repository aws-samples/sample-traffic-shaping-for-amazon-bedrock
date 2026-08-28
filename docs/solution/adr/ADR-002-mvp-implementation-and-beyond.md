# ADR-002: MVP Implementation and Beyond

## Status
Accepted

## Date
2026-02-01

## Context

This ADR revises and updates [ADR-001: Phased Architecture Approach](ADR-001-phased-architecture-approach.md) based on learnings from implementing Phase 1. The original ADR documented the planned approach; this ADR documents the actual implementation and path forward.

We are building a semaphore-based rate limiter for AWS Step Functions that controls API calls to Amazon Bedrock Foundation Models. The system needs to handle request spikes better than traditional retry logic by using a queue-based approach with controlled throughput.

### The Challenge

GenAI applications have inherently spiky workloads. Organizations want to maximize Bedrock quotas to handle these spikes, but traditional retry patterns (exponential backoff, jitter) create retry storms, burn the quota legitimate requests need, and still drop a meaningful fraction of requests to throttling under load (measured baseline throttling rates by model are in [`../../testing/results.md`](../../testing/results.md)).

The full architecture requires AWS Step Functions "Wait for Callback with Task Token" pattern, where queued requests pause execution and resume when budget becomes available. However, implementing the complete architecture upfront creates deployment complexity and delays validation of core concepts.

### The Approach

We need to prove that the queue-based semaphore approach handles request spikes better than normal retry logic before investing in the full implementation. This ADR documents a phased approach that:

1. **Phase 0** - Documents the baseline (no queue processor)
2. **Phase 1** - Adds event-driven queue processor with split execution paths (current implementation)
3. **Phase 2** - Refines implementation with production-hardened code
4. **Phase 3** - Completes the architecture with Step Functions callbacks and prompt passing

Each phase builds incrementally, allowing validation at each step while maintaining a clear migration path to the full solution.

## Decision

We will implement the infrastructure in **four phases**, starting from the baseline architecture and iterating toward the full solution.

### Phase 0: Baseline Architecture (Before Queue Processor)

**Status:** Superseded by Phase 1

**Goal:** Establish rate limiting without queuing mechanism

**Architecture:**
```
Step Functions Workflow:
1. Reserve Budget (Lambda)
   ↓
2. Choice State: Check if allocated
   ├─ If allocated → Execute Foundation Model → Release Budget → Success
   └─ If NOT allocated → End with "failed" status (request lost)
```

**Behavior:**
- First N requests (within quota): Get budget immediately, execute model, succeed
- Subsequent requests (quota exhausted): Budget reservation fails, Step Function ends with failure
- **Requests are lost** - no queuing, no retry, no recovery mechanism
- Rate limiting works, but at the cost of dropping requests

**Problems with Phase 0:**
- ❌ Requests get swallowed when quota exhausted
- ❌ No mechanism to handle traffic spikes
- ❌ Poor user experience (silent failures)
- ❌ Wasted opportunities (requests could be processed later)
- ❌ Defeats the purpose of having a semaphore (should queue, not drop)

**Why Phase 1 is needed:**
The semaphore pattern is incomplete without a queue. Rate limiting alone just drops requests—we need queue processing to handle spikes gracefully and ensure all requests eventually execute.

### Phase 1: Event-Driven Queue Processor (Current Implementation)

**Status:** Implemented, Deployed, and Validated

**Goal:** Add queue processing to handle traffic spikes without dropping requests, and prove the approach works under load

**Architecture:**
```
Step Functions Workflow (Immediate Execution Path):
1. Reserve Budget (Lambda)
   ↓
2. Choice State: Check if queued
   ├─ If allocated → Execute Foundation Model → Release Budget → Success
   └─ If queued → End with "queued" status

Background Process (Event-Driven Queue Processing):
- Budget Manager triggers EventBridge when first item is enqueued
- Queue Processor Lambda invoked by EventBridge
- Runs in loop until queue is empty:
  1. Try to reserve allocation (poll with delay if unavailable)
  2. Dequeue next request (FIFO)
  3. Execute Foundation Model (Bedrock call directly)
  4. Release allocation
  5. Repeat
- Uses idempotency lock to prevent concurrent processing
```

**Behavior:**
- First N requests (within quota): Get budget immediately via Step Functions, execute model, succeed
- Subsequent requests (quota exhausted): Get queued, Step Function ends with "queued" status
- Budget Manager publishes EventBridge event when first item is enqueued
- Queue Processor wakes up, processes queue in loop until empty
- **All requests eventually execute the model**, either via Step Functions (immediate) or Queue Processor (queued)

**Key Implementation Details:**

**Event-Driven Invocation (Not Polling):**
The Queue Processor is triggered by EventBridge only when the first item is enqueued, not on a fixed schedule.

**Why Event-Driven > Polling Every 1 Second:**
- ✅ **Cost Efficiency** - Lambda only runs when there's work to do (not continuously)
- ✅ **Testing Simplicity** - No race conditions from multiple scheduled invocations
- ✅ **No Concurrent Processors** - Single invocation processes entire queue in loop
- ✅ **Faster Response** - Immediate trigger when queue has items (no 1-second delay)
- ✅ **Cleaner Architecture** - Event-driven is more idiomatic for AWS serverless

**Idempotency Lock:**
- Prevents multiple Queue Processors from running simultaneously
- Uses DynamoDB conditional writes for atomic lock acquisition
- Lock timeout: 60 seconds (configurable)
- Ensures FIFO processing without conflicts

**Split Execution Paths:**
- Immediate requests: Execute via Step Functions workflow (full observability)
- Queued requests: Execute via Queue Processor Lambda (logs only)
- Both paths call the same Foundation Model Lambda for Bedrock integration

**MVP Validation:**

Phase 1 represents a **complete, validated MVP** demonstrating the core queue-based semaphore approach, exercised through comprehensive load testing (production deployment requires additional security review and hardening):

- ✅ **Load Testing Completed** - 125 concurrent requests tested against live Bedrock API
- ✅ **100% Success Rate** - All requests processed successfully (vs. 93-95% with direct calls) — based on internal load testing (N=125 requests; see [`docs/testing/results.md`](../../testing/results.md))
- ✅ **Zero Throttling** - No Bedrock throttling errors under load
- ✅ **Predictable Behavior** - Queue depth grows during spike, drains steadily
- ✅ **Cost-Effective** - Event-driven architecture minimizes Lambda execution time
- ✅ **Proof of Concept Validated** - Queue-based semaphore approach proven superior to retry logic

See [load-test results](../../testing/results.md) for detailed load testing methodology and results.

**What this proves:**
- ✅ Queue mechanics work correctly under load
- ✅ Budget allocation/release works atomically
- ✅ Controlled throughput during spikes
- ✅ No retry storms or thundering herd
- ✅ Predictable queue processing
- ✅ All requests eventually execute (no dropped requests)
- ✅ Foundation Model calls are rate-limited correctly
- ✅ Event-driven architecture is cost-effective and reliable
- ✅ **The approach works in production scenarios**

**Current Limitations:**
- ⚠️ Queued requests execute outside Step Functions (reduced observability)
- ⚠️ **No prompt passing mechanism** - Queue Processor uses hardcoded prompt (solved in Phase 3)
- ⚠️ Single Queue Processor (idempotency lock prevents horizontal scaling)
- ⚠️ 60-second lock timeout may cause orphaned requests with large queues

**Success Metrics:**
- Queue depth grows during spike, drains steadily
- Allocation rate stays constant (respects quota)
- No failed requests (all queued successfully)
- Predictable processing time
- 100% success rate (vs. 93-95% with direct calls) — internal load testing, N=125 (see `docs/testing/results.md`)

### Phase 2: Performance, Scalability, and Observability Improvements

**Status:** Planned

**Goal:** Harden the Phase 1 MVP for production use by improving performance, scalability, observability, and reducing technical debt

**Architecture:**
Same event-driven queue processor architecture as Phase 1, with optimizations and enhancements.

**Key Improvements:**

**Performance & Scalability:**
- Reduce load test execution time (currently ~100+ seconds for 125 requests)
- Optimize Queue Processor loop efficiency
- Implement queue sharding to enable multiple concurrent processors
- Add self-spawning logic for Queue Processor to handle large queues without timeout
- Increase lock timeout or implement graceful timeout handling (process until 15 min remaining, spawn new instance)
- Optimize DynamoDB read/write patterns for higher throughput

**Technical Debt Reduction:**
- Refactor inline Lambda code to use proper service layer implementations
- Consolidate duplicate code between Lambda handlers
- Improve error handling and retry logic
- Add comprehensive input validation
- Clean up legacy code in `src/` directory (if unused)

**Observability Enhancements:**
- Add CloudWatch metrics for queue depth, processing rate, allocation utilization
- Implement structured logging across all Lambda functions
- Add X-Ray tracing for end-to-end request tracking
- Create CloudWatch dashboards for system monitoring
- Add alarms for queue depth thresholds, timeout events, and error rates

**Configuration & Testing:**
- Make configuration more flexible (environment-based settings)
- Improve load testing scripts with better reporting
- Add automated integration tests
- Document configuration tuning guidelines for different workload patterns
- Add chaos testing scenarios (Lambda failures, DynamoDB throttling, etc.)

**Potential Approaches (Not Prescriptive):**

**Option A: Horizontal Scaling via Queue Sharding**
- Partition queue into N shards (e.g., by request_id hash)
- Allow N Queue Processors to run concurrently (one per shard)
- Increases throughput but adds complexity

**Option B: Vertical Optimization**
- Keep single Queue Processor but optimize loop efficiency
- Batch DynamoDB operations where possible
- Reduce polling delays when budget is available
- Simpler but may have throughput ceiling

**Option C: Hybrid Approach**
- Start with vertical optimizations (quick wins)
- Add sharding only if throughput requirements demand it
- Incremental complexity based on actual needs

**What Phase 2 Proves:**
- ✅ System can handle production-scale workloads efficiently
- ✅ Performance is acceptable for real-world use cases
- ✅ System is observable and debuggable in production
- ✅ Configuration is flexible for different deployment scenarios
- ✅ Technical debt is manageable for long-term maintenance

**Success Metrics:**
- Load test execution time reduced by 50%+ (target: <50 seconds for 125 requests)
- Queue processing throughput increased (requests/second)
- CloudWatch dashboards provide real-time visibility
- Zero orphaned requests during load testing
- Clean separation of concerns in codebase

**Migration from Phase 1:**
- No breaking changes to core architecture
- Incremental improvements can be deployed independently
- Step Functions workflow remains unchanged
- DynamoDB schema may evolve (backward compatible)

**Note:** Phase 2 is intentionally flexible—specific improvements should be prioritized based on actual production requirements and bottlenecks identified through monitoring.

### Phase 3: Full Callback Architecture with End-to-End Step Functions Integration

**Status:** Planned

**Goal:** Complete the architecture with Step Functions callbacks and enable full end-to-end observability through unified execution paths

**Architecture:**
```
Step Functions Workflow (Unified Execution Path):
1. Reserve Budget (Lambda with task token and prompt)
   ↓
2. Choice State: Check if queued
   ├─ If allocated → Execute Foundation Model (with prompt)
   └─ If queued → Wait for Callback (paused, no Lambda running)
                   ↓
                   Queue Processor sends callback with prompt
                   ↓
                   Resume → Execute Foundation Model (with prompt)
   ↓
3. Release Budget
   ↓
4. Success

Background Process (Event-Driven Queue Processing):
- Budget Manager triggers EventBridge when first item is enqueued
- Queue Processor Lambda invoked by EventBridge
- Runs in loop until queue is empty:
  1. Try to reserve allocation (poll with delay if unavailable)
  2. Dequeue next request (FIFO) - includes task_token and prompt
  3. Send callback to Step Functions with allocation and prompt data
  4. Step Function resumes and executes Foundation Model via workflow
  5. Repeat
- Uses idempotency lock to prevent concurrent processing
```

**Key Changes from Phase 1:**

**Unified Execution Path:**
- Phase 1: Split execution (Step Functions immediate, Queue Processor direct Bedrock calls)
- Phase 3: All executions flow through Step Functions workflow (immediate or resumed)
- Better observability - all executions visible in Step Functions history

**Prompt Passing:**
- Phase 1: Queue Processor uses hardcoded prompt (no mechanism to pass user prompt)
- Phase 3: Full prompt data flows from invocation → queue → callback → execution
- Enables dynamic, user-specified prompts for queued requests

**Application Integration Model:**
- Phase 1: Applications invoke Bedrock directly (with retry logic) or call Budget Manager
- Phase 3: Applications start Step Functions execution and wait for completion
- Step Functions handles all rate limiting, queuing, and execution internally
- Applications get consistent interface regardless of immediate or queued execution

**What Phase 3 Solves:**

**Critical Limitations from Phase 1:**
- ✅ **Prompt Passing** - User prompts now flow through the entire system
- ✅ **Unified Observability** - All executions visible in Step Functions (no split paths)
- ✅ **Efficient Resource Usage** - No blocking Lambdas (Step Functions paused, not running)
- ✅ **Complete Architecture** - Full semaphore pattern with callback resumption
- ✅ **Simplified Integration** - Applications just start Step Functions and wait

**Implementation Changes:**

**Step Functions Callback Pattern:**
- Reserve Budget Lambda receives task token from Step Functions
- Queued requests store task token and prompt in DynamoDB queue
- Queue Processor sends callback to resume paused Step Functions execution
- Step Functions resumes from paused state and executes Foundation Model with user's prompt

**Queue Data Storage:**
- Queue items now include task token and full prompt data
- Enables Queue Processor to resume Step Functions with correct context
- No data loss between enqueue and execution

**End-to-End Observability:**
- All requests (immediate and queued) visible in Step Functions console
- Complete execution history with timing, state transitions, and outcomes
- No hidden processing in Queue Processor logs
- Unified monitoring and debugging experience

**What Phase 3 Proves:**
- ✅ Full end-to-end workflow with dynamic prompts
- ✅ Queued requests eventually execute with correct user input
- ✅ Efficient resource usage (paused Step Functions don't consume resources)
- ✅ Complete value proposition of semaphore pattern
- ✅ Complete, validated architecture for the described use cases (production deployment requires additional security review and hardening)
- ✅ Simple application integration (just start Step Functions)

**Success Metrics:**
- All requests (immediate and queued) execute with correct prompts
- Step Functions history shows all executions (no hidden processing)
- No increase in costs (paused state is free)
- Applications have single integration point (Step Functions API)
- Backward compatible migration path from Phase 1

**Migration from Phase 2:**
- Add task token parameter to Reserve Budget Lambda
- Update Step Functions to use callback pattern
- Add callback logic to Queue Processor
- Update queue storage to include prompt and task token
- No breaking changes to Budget Manager core logic
- Incremental rollout possible

**Key Insight:**
Phase 3 completes the architecture by solving the prompt passing limitation and unifying execution paths. Applications simply start a Step Functions execution and wait—the system handles rate limiting, queuing, and execution transparently. All requests flow through Step Functions whether immediate or queued, providing complete observability and a consistent integration model.

## Consequences

### Positive
- ✅ Can deploy and test infrastructure immediately (Phase 1 complete)
- ✅ Proved core concept with real load testing (100% success rate)
- ✅ Clear migration path to full architecture (Phases 2-3)
- ✅ Each phase adds value incrementally
- ✅ Validated with production workloads early
- ✅ Reduces risk of over-engineering
- ✅ Event-driven architecture is cost-effective

### Negative
- ⚠️ Phase 1 executes queued requests outside Step Functions (reduced observability)
- ⚠️ No prompt passing in Phase 1 (hardcoded prompts)
- ⚠️ Multiple deployment cycles needed for full architecture
- ⚠️ Need to explain split execution paths to stakeholders

### Neutral
- Phase 1 is sufficient for spike handling proof and all requests execute
- Background processor logic is reusable across all phases
- DynamoDB schema supports all phases
- Queue Processor behavior evolves from direct execution (Phase 1) to callbacks (Phase 3)

## Alternatives Considered

### Alternative 1: Build Full Architecture Immediately
**Rejected because:**
- Higher complexity delays deployment
- Can't validate core concepts early
- Higher risk if approach doesn't work
- Harder to debug issues
- No early feedback from load testing

### Alternative 2: Polling-Based Queue Processor (Every 1 Second)
**Rejected because:**
- Higher costs (Lambda runs continuously)
- Testing complexity (race conditions from multiple invocations)
- Requires concurrent processor handling
- 1-second delay before processing starts
- Less idiomatic for AWS serverless

### Alternative 3: Synchronous Blocking (Lambda polls queue)
**Rejected because:**
- Lambda blocks waiting, burning execution time
- Defeats purpose of queue-based approach
- Doesn't scale well
- Expensive (15-minute Lambda executions)

### Alternative 4: Wait/Retry Pattern
**Rejected because:**
- More complex migration to callback pattern
- Doesn't prove advantage over normal retry logic
- Still has retry storm potential
- Less clean architecture

## References

- [AWS Step Functions Wait for Callback Pattern](https://docs.aws.amazon.com/step-functions/latest/dg/callback-task-sample-sqs.html)
- [Semaphore Pattern for Rate Limiting](https://aws.amazon.com/blogs/compute/controlling-concurrency-in-distributed-systems-with-aws-step-functions/)
- [Part 1: Why Traditional Retry Patterns Fail](https://builder.aws.com/content/34CVjaGLlDJXGBUv15vR3dLnoy2/managing-traffic-spikes-with-amazon-bedrock-why-traditional-retry-patterns-fail-part-1)
- [load-test results](../../testing/results.md) - Load testing methodology and results
- [README.md](../../../README.md) - Current implementation documentation

## Review and Updates

This ADR should be reviewed after:
- Phase 2 implementation begins
- Performance bottlenecks identified in production
- Before starting Phase 3 implementation
- Stakeholder feedback on observability requirements
