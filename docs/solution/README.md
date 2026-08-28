# Solution Docs — Bedrock Traffic Shaper

Everything about the rate-limiter itself: how it works, how to run it, what it costs, and the
decisions behind it. For load testing, see [`../testing/`](../testing/).

## Start here

| Doc | What it covers |
|-----|----------------|
| [architecture.md](architecture.md) | **How it works.** Request lifecycle (Step Functions callback), the sliding-window admission gate, the DynamoDB single-table schema, queue-drain pacing, reconciliation, the honest-outcomes terminal layer, multi-backend invocation, and 429-vs-503 classification. Diagram-anchored; the single "how" reference. |
| [runbook.md](runbook.md) | **Operating the shaper.** Alarm response, latency SLAs, DLQ consumer, inspection commands. |
| [cost-model.md](cost-model.md) | **Infrastructure cost analysis** per request and at scale. |
| [production-hardening.md](production-hardening.md) | **Roadmap.** Production-readiness sprints and open gaps. |

## Decision record

[`adr/`](adr/) — the canonical, dated decision log:

- [ADR-001](adr/ADR-001-phased-architecture-approach.md) — phased architecture approach *(superseded by ADR-002)*
- [ADR-002](adr/ADR-002-mvp-implementation-and-beyond.md) — MVP implementation and beyond
- [ADR-003](adr/ADR-003-production-ready-architecture.md) — production-ready leaky-bucket architecture
- [ADR-004](adr/ADR-004-counter-write-sharding.md) — counter write-sharding *(historical — the counter gate it describes was later replaced by the sliding-window consumption read; see architecture.md §3 and §11)*
- [ADR-005](adr/ADR-005-consumption-read-elimination.md) — consumption-read elimination *(historical — its core decision was reversed by the current window-read gate; see architecture.md §3 and §11)*

## Design rationale

[`design/`](design/) — the "why" behind the load-bearing choices, indexed by
[`design/README.md`](design/README.md):

- [Leaky-Bucket-Optimization.md](design/Leaky-Bucket-Optimization.md) — consumption-tracking + optimistic write-then-verify capacity model; why the conservative MVP throughput was raised. Basis for ADR-003
- [Queue-Processor-Trigger-Improvements.md](design/Queue-Processor-Trigger-Improvements.md) — heartbeat-based processor locks, multi-processor support, zero-cost-idle triggering, self-healing. Basis for ADR-003
- [HOT-PARTITION-FIX-VALIDATION.md](design/HOT-PARTITION-FIX-VALIDATION.md) — 5× extreme-spike validation of the hot-partition fix chain (99.94% success); documents the residual synchronous-admission burst-absorption limit. Referenced by ADR-005
