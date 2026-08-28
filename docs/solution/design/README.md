# Design Notes — Bedrock Traffic Shaper

Design *rationale* for the solution: the "why" behind decisions, kept as individual notes because
each is substantial and self-contained. For the canonical, dated decision record see
`../adr/`. For the "how it works" reference see `../architecture.md`.

| Note | What it covers |
|------|----------------|
| [Leaky-Bucket-Optimization.md](Leaky-Bucket-Optimization.md) | Consumption-tracking + optimistic write-then-verify capacity model; why the conservative MVP throughput was raised. Basis for ADR-003. |
| [Queue-Processor-Trigger-Improvements.md](Queue-Processor-Trigger-Improvements.md) | Heartbeat-based processor locks, multi-processor support, zero-cost-idle triggering, self-healing. Basis for ADR-003. |
| [HOT-PARTITION-FIX-VALIDATION.md](HOT-PARTITION-FIX-VALIDATION.md) | 5× extreme-spike validation of the hot-partition fix chain (99.94% success); documents the residual synchronous-admission burst-absorption limit. Referenced by ADR-005. |
