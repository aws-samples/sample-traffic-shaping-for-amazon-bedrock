# ADR-003: Production-Ready Architecture

## Status
Accepted

## Date
2026-02-03

> **Reference-implementation decision record — not a production-readiness attestation.**
> "Production-ready" here names the *design goal* of this ADR, not a certification. This stack is
> sample/reference code that has **not** had an independent application-security review; it must be
> security-reviewed and hardened before any production use — see
> [`../production-hardening.md`](../production-hardening.md) and the disclaimer in the root README.
> Quantified improvements in this document are **design targets/projections**, not measured
> guarantees; measured load-test results live in [`../../testing/results.md`](../../testing/results.md).

## Context

The current MVP implementation ([ADR-002](ADR-002-mvp-implementation-and-beyond.md) Phase 1) successfully validates the queue-based rate limiting approach with 100% success rates under load testing. However, several architectural limitations prevent production deployment at scale. This ADR consolidates improvements from the [Leaky Bucket Optimization](../design/Leaky-Bucket-Optimization.md) and [Queue Processor Trigger Improvements](../design/Queue-Processor-Trigger-Improvements.md) documents into a unified production-ready architecture.


The MVP architecture suffers from six critical limitations that prevent production use: (1) the semaphore-based allocation model creates write contention under burst traffic, limiting throughput to conservative levels; (2) queued requests execute outside Step Functions with hardcoded prompts, eliminating observability and dynamic prompt support; (3) the multi-table DynamoDB design creates operational complexity and prevents efficient query patterns; (4) single queue processor operation creates throughput bottlenecks and single points of failure; (5) the simple lock mechanism lacks failure recovery, causing orphaned requests when processors crash; and (6) event-driven triggering without capacity awareness causes unnecessary Lambda invocations during low-traffic periods while potentially missing triggers during high-load scenarios.

## Decision

We will implement a production-oriented architecture (a reference design — see the note above; not a production-certified system) that addresses all six limitations through coordinated improvements across data modeling, capacity management, execution patterns, and operational resilience. The solution maintains backward compatibility with the MVP's core value proposition—100% success rates with predictable queue-based traffic shaping—while enabling horizontal scaling, full observability, and cost-efficient operation.


### 1. Single Table Design with Consumption Tracking

The architecture transitions from a mutable semaphore state model to an immutable consumption tracking model using DynamoDB single-table design (see [architecture.md](../architecture.md) for complete schema). All entities—configuration, consumption records, queue items, and processor locks—coexist in one table using generic partition and sort keys with entity-type prefixes. Consumption records replace the semaphore's available_allocations counter, with separate partitions for burst capacity (50% of RPM) and queue capacity (40% of RPM), enabling independent scaling without write contention. Each consumption record includes a timestamp and TTL, allowing the system to calculate available capacity using a sliding 60-second window with continuous token regeneration awareness, eliminating the need for atomic decrements that create bottlenecks under concurrent load.


### 2. Optimistic Write Pattern for Race Condition Handling

The Budget Manager adopts an optimistic write-then-verify pattern to handle burst traffic race conditions without conditional writes (detailed in [Leaky-Bucket-Optimization.md](../design/Leaky-Bucket-Optimization.md)). When requests arrive, the manager immediately writes a consumption record with a unique timestamp-based sort key, then queries with consistent reads to verify total consumption remains within burst capacity limits. If over-consumption is detected, the manager deletes its own record and enqueues the request, creating a self-correcting system where the first N requests to verify capacity proceed while excess requests gracefully roll back. This approach eliminates write contention entirely since all writes succeed independently, with a small buffer capacity (10% of RPM) absorbing edge cases where 1-2 extra requests slip through during extreme concurrency.


### 3. Step Functions Callback Pattern for Unified Execution

All requests—both immediate and queued—flow through Step Functions workflows using the "Wait for Callback with Task Token" pattern, eliminating the split execution paths that reduce observability in the MVP. When the Budget Manager enqueues a request, it stores only the Step Functions task token in the queue item, while the Step Functions execution context retains the full prompt payload. Queue processors claim items, verify queue capacity availability, then send callbacks to resume the paused Step Functions execution, which continues with its original prompt data. This unified approach provides complete end-to-end observability in the Step Functions console, enables dynamic user-specified prompts for all requests, and eliminates resource waste since paused executions consume no Lambda runtime while waiting in queue.


### 4. Multi-Processor Architecture with Heartbeat-Based Locks

The queue processor scales horizontally through multiple lock slots (configurable, starting with 1-3 processors) using heartbeat-based ownership to enable concurrent processing while preventing duplicate work (see [Queue-Processor-Trigger-Improvements.md](../design/Queue-Processor-Trigger-Improvements.md) for detailed flow). Each processor attempts to acquire an available lock slot on startup, then refreshes the lock via heartbeat updates every 30 seconds to prove liveness, with a 2-minute TTL ensuring automatic recovery if a processor crashes. Processors claim queue items atomically using conditional writes that verify the item is unclaimed, allowing multiple processors to work on different items simultaneously while preserving FIFO ordering. The heartbeat pattern provides self-healing behavior where expired locks are automatically replaced by new processors, eliminating the orphaned request problem that plagues the MVP's simple lock mechanism.


### 5. Intelligent Processor Triggering Strategy

The Budget Manager implements smart triggering logic that ensures processors run during high-traffic periods while avoiding unnecessary costs during idle times. On every enqueue operation, the manager queries active processor locks (non-expired TTL) and triggers additional processors only when the active count falls below the configured maximum, with special handling to replace stale locks that have expired but not yet been cleaned up. Processors implement a double-check pattern on exit, verifying queue depth after releasing their lock and triggering a replacement processor if items remain, eliminating race conditions where requests arrive during shutdown. This approach maintains continuous processing during load spikes while naturally scaling to zero during idle periods, with EventBridge trigger costs remaining negligible due to AWS's generous free tier.


### 6. Shared Service Layer for Data Access

A dedicated DynamoDB service module encapsulates all table access patterns, providing consistent interfaces for consumption tracking, queue operations, lock management, and configuration retrieval across both Budget Manager and Queue Processor components. The service layer implements the regeneration-aware capacity calculation logic, handles the optimistic write-verify pattern, manages atomic item claiming, and provides query helpers for sliding window consumption records. This abstraction eliminates code duplication between Lambda handlers, ensures consistent application of business rules like proportional regeneration rates (50% for burst, 40% for queue), and simplifies testing by isolating data access logic from Lambda handler orchestration code.


## Implementation Strategy

The implementation prioritizes proving scalability over feature completeness, front-loading the high-value performance improvements while maintaining testable increments. Phase 1 establishes the shared DynamoDB service layer by refactoring existing Lambda handlers to use common interfaces, validating the abstraction works with current multi-table logic before introducing new patterns. Phase 2 delivers the first major performance gain by migrating to single-table design and implementing the Budget Manager's sliding window with optimistic writes, increasing burst capacity from 2 to 50 tokens and eliminating write contention—load testing at this checkpoint should demonstrate 20-25x improvement in burst handling. Phase 3 delivers the second major performance gain by implementing heartbeat-based locks and multi-processor support in the Queue Processor, enabling 2-3x faster queue draining through parallel processing—load testing validates horizontal scaling behavior and self-healing recovery. Phase 4 adds Step Functions callback integration to enable user prompt passing and unified execution paths, completing the feature set required for production use. Phase 5 implements intelligent triggering improvements to handle race conditions and optimize costs during variable load patterns. This sequence proves the architecture can handle scale (Phases 2-3) before adding operational completeness (Phases 4-5), with load testing checkpoints after each phase to validate incremental improvements.


## Consequences

### Positive

The production architecture eliminates all six MVP limitations while maintaining the core value proposition of 100% success rates with predictable traffic shaping. Performance improvements are substantial: burst capacity increases 25x (from 2 to 50 tokens), queue processing throughput increases 2-3x through parallel processors, and end-to-end latency for queued requests decreases by 60% due to continuous token regeneration awareness. The single-table design reduces operational complexity from managing three tables to one, while the shared service layer eliminates code duplication and ensures consistent business rule application. Horizontal scaling through multi-processor support and heartbeat-based self-healing provides production-grade resilience. Step Functions callback integration delivers complete observability and enables dynamic prompt support for all execution paths. The implementation strategy proves scalability early, reducing risk before investing in feature completeness.

### Negative

The architecture introduces additional complexity compared to the MVP, requiring careful coordination between consumption tracking, queue processing, and callback patterns. The optimistic write pattern creates temporary over-consumption scenarios (1-2 requests) that require buffer capacity to absorb. Multi-processor coordination adds operational overhead for monitoring lock health and processor activity. The phased implementation approach requires multiple deployment cycles and load testing checkpoints, extending the timeline to full production readiness. Migration from the MVP requires replacing all three DynamoDB tables and updating Lambda handler logic, making rollback more complex if issues arise during transition.

### Risks and Mitigations

**Risk:** DynamoDB query latency impacts burst request response times during capacity verification.
**Mitigation:** Queries with consistent reads typically complete in <10ms; the optimistic write pattern minimizes blocking since writes succeed immediately.

**Risk:** Regeneration rate calculations may be optimistic, leading to actual throughput below projections.
**Mitigation:** Load testing after Phase 2 validates actual performance; proportional regeneration rates (50% burst, 40% queue) are conservative estimates that can be tuned based on observed behavior.

**Risk:** Heartbeat refresh failures could cause premature processor termination during high load.
**Mitigation:** 2-minute TTL provides ample buffer for transient DynamoDB throttling; processors can retry heartbeat updates multiple times before lock expiration.

**Risk:** Step Functions callback integration adds latency to queued request execution.
**Mitigation:** Callback overhead is minimal (<100ms); the unified execution path provides observability benefits that outweigh the small latency increase.

## References

- [ADR-001: Phased Architecture Approach](ADR-001-phased-architecture-approach.md) - Original architecture planning
- [ADR-002: MVP Implementation and Beyond](ADR-002-mvp-implementation-and-beyond.md) - Current MVP implementation
- [Leaky-Bucket-Optimization.md](../design/Leaky-Bucket-Optimization.md) - Consumption tracking and optimistic write pattern details
- [Queue-Processor-Trigger-Improvements.md](../design/Queue-Processor-Trigger-Improvements.md) - Heartbeat locks and multi-processor architecture
- [architecture.md](../architecture.md) - Complete DynamoDB schema specification
- [results.md](../../testing/results.md) - Load testing methodology and results
- [AWS Step Functions Wait for Callback Pattern](https://docs.aws.amazon.com/step-functions/latest/dg/callback-task-sample-sqs.html)
- [Token Bucket Algorithm](https://en.wikipedia.org/wiki/Token_bucket)

## Review and Updates

This ADR should be reviewed after:
- Phase 2 implementation and load testing (single-table + sliding window validation)
- Phase 3 implementation and load testing (multi-processor validation)
- Phase 4 implementation (Step Functions callback integration)
- Production deployment and first 30 days of operation
- Any significant performance issues or architectural limitations discovered during implementation

