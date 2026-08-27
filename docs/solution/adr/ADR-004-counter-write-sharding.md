# ADR-004: Counter Write-Sharding for Hot Partition Elimination

> **SUPERSEDED / HISTORICAL.** The counter-based admission design described here (write-sharded
> counters) was later retired. The current implementation uses a sliding-window consumption-record
> read with no counters and no scheduled Reconciliation Lambda — see `docs/solution/architecture.md`.
> Retained as a decision record.

## Status

Superseded (was: Accepted)

## Date

2026-07-07

## Context

The admission counter uses a single DynamoDB item per model per window:

```
PK: MODEL#{model_id}#BURST#COUNTER
SK: {window_number}
```

Every admission attempt for a given model performs a `TransactWriteItems` that conditionally increments this single counter. Under load, concurrent increments on one item create **write contention** on a single partition (~1,000 WCU/s ceiling) and cascading `TransactionConflict` retries.

> **Correction (2026-07-07):** An earlier revision of this ADR claimed the ~3,000 RCU/s per-partition **read** ceiling was on this counter. That was inaccurate. The counter is a conditional `Update` — a **write** (WCU) path — so sharding it addresses **write** contention and `TransactionConflict` retry storms, NOT the read ceiling. The ~3,000 RCU/s ceiling was a *separate* problem: the post-gate consumption-record **read** that summed every record in the 60s window. That read is addressed in **[ADR-005](ADR-005-consumption-read-elimination.md)**, not by this write-sharding change. This ADR is scoped to what sharding actually fixes: counter write contention.

### Evidence

- **Batch-1 load testing (Jul 2026):** Under sustained load, the single counter item accumulated `TransactionConflict` cancellations as concurrent Lambdas contended on the same key.
- **5x spike simulation:** Caused compute-tier deadlock due to cascading `TransactionConflict` retries when all concurrent Lambdas contend on the same counter item.
- The per-shard transaction retry loop (exponential backoff + jitter) does not help when the fundamental bottleneck is single-item write contention — spreading writes across shards does.

### Options Considered

1. **Increase DynamoDB provisioned capacity** -- Does not help; the limit is per-partition, not per-table. DynamoDB distributes partitions by PK hash, so a single PK always maps to one partition regardless of table-level capacity.

2. **Write-shard the counter across N partition keys** -- Standard DynamoDB pattern (recommended by AWS documentation) for distributing hot writes. Each write targets a random shard; reads scatter-gather across all shards.

3. **Switch to DAX (DynamoDB Accelerator)** -- Only helps reads, not writes. The bottleneck is write throughput (conditional updates).

4. **Replace DynamoDB counter with ElastiCache/Redis atomic increment** -- Solves the throughput problem but adds a new service dependency, cross-AZ latency, and failure modes. Higher operational complexity for a single counter.

## Decision

Implement **write-sharding** (option 2) for the admission counter. Split the counter across `N` shards where `N` is configurable per model via the `counter_shards` config parameter.

### Key Design

- **Write path:** Each `TransactWriteItems` targets `MODEL#{model_id}#BURST#COUNTER#SHARD#{random(0, N-1)}` instead of the single counter key. Per-shard capacity = `ceil(total_capacity / N)`.

- **Admission enforcement:** The sharded counter is the atomic admission gate. Following [ADR-005](ADR-005-consumption-read-elimination.md), it is now the *sole* capacity check on the hot path (the former post-gate consumption-record query was eliminated). Bounded over-admission from ceiling-division per-shard caps is corrected by reconciliation — the counter does not need to be globally precise between reconciliation cycles.

- **Reconciliation:** The reconciliation Lambda resets each shard independently. Each shard's expected count = `actual_total_count / N` (distributed with remainder).

- **Backward compatible:** When `counter_shards` is absent from config or set to 1, the code produces the legacy single-counter key (`MODEL#{model_id}#BURST#COUNTER`), preserving exact current behavior.

### Partition Key Layout

| Shards | PK Pattern | DynamoDB Partitions |
|--------|-----------|-------------------|
| 1 (default) | `MODEL#{id}#BURST#COUNTER` | 1 |
| 5 | `MODEL#{id}#BURST#COUNTER#SHARD#{0..4}` | Up to 5 |
| 10 | `MODEL#{id}#BURST#COUNTER#SHARD#{0..9}` | Up to 10 |

### Throughput Ceiling

Sharding addresses the counter **write** path. Per-partition write ceiling is ~1,000 WCU/s; each shard is its own partition:

| N | Counter write ceiling | Max admission/sec (write-bound) |
|---|----------------------|-------------------------------|
| 1 | ~1,000 WCU/s | ~1,000 |
| 5 | ~5,000 WCU/s | ~5,000 |
| 10 | ~10,000 WCU/s | ~10,000 |

(The consumption-record **read** ceiling that previously bound throughput is removed entirely by ADR-005, not mitigated by adding shards.)

## Consequences

### Positive

- Eliminates the hot-partition bottleneck that caused deadlock at 5x spike.
- Linear throughput scaling: N=5 gives 5x headroom.
- Zero-downtime rollout: deploy code first (defaults to N=1), then update model configs to N=5.
- No new infrastructure dependencies.
- Backward compatible with existing single-shard deployments.

### Negative / Trade-offs

- **Bounded over-admission on admission count:** The per-shard cap is `ceil(total/N)`, which means up to `N-1` extra requests can be admitted beyond the true capacity per window. This is acceptable because:
  1. Reconciliation resets per-shard drift each cycle, bounding cumulative error.
  2. Slight over-admission is strictly better than hot-partition failure (requests dropped entirely), and is absorbed by the provider's own burst tolerance.

  (Note: post-ADR-005 there is no consumption-record query providing a "definitive" second check — the sharded counter *is* the enforcement. The bound above is the guarantee.)

- **Reconciliation complexity:** Each shard must be reset independently. The reconciliation Lambda now iterates `N` shards per model instead of 1 counter.

- **Configuration required:** Operators must set `counter_shards` in model config to activate sharding. Default is 1 (no change).

### Sizing Guidance

- **N=5** recommended for production models with burst_capacity > 25 RPM.
- **N=1** sufficient for dev/test or models with low concurrency (< 10 RPM burst).
- **N=10** for extreme traffic models (> 500 RPM burst) or multi-tenant deployments.

## Scope: Runtime Counter Write-Sharding

This ADR shards the runtime RPM admission counter's **write** path. The runtime TPM
dimension is enforced by an in-transaction sharded token counter (`{window}#TPM`) added
in [ADR-005](ADR-005-consumption-read-elimination.md), which shards on the same key.
The mantle admission path (`_put_allocation_mantle`) uses a single counter for its
RPM/iTPM/oTPM dimensions; sharding it is future work if mantle traffic scales:

1. Mantle-backed models currently see lower traffic volume.
2. The mantle path has separate iTPM/oTPM counters that would require independent sharding.
3. If mantle traffic scales beyond ~1,000 WCU/s on its counter, extend sharding to it.

**Action threshold:** If a mantle-backed model consistently exceeds ~500 WCU/s on its
counter partition (visible via CloudWatch ConsumedWriteCapacityUnits), enable sharding
for that model's mantle counters.

### RPM-Only and TPM-Gated Models

For all runtime models, the sharded counter(s) are the sole admission control on the hot
path (ADR-005 removed the post-gate consumption query). RPM is gated by the request
counter; TPM (when `tpm_burst_capacity > 0`) by the `{window}#TPM` token counter. Both
can over-admit by at most `N-1` per window (ceiling-division artifact). For N=5 and
burst_capacity=25, that is 4 extra requests (16% max over-admission) — bounded,
deterministic, corrected by reconciliation, and absorbed by the provider's burst
tolerance. For tighter enforcement, keep `counter_shards=1` (no over-admission, lower
throughput ceiling).

## Files Changed

- `infrastructure/lambda_layer/python/shared_service/dynamo.py` -- Core sharding logic in `put_allocation()` and `reset_burst_counter_drift()`
- `infrastructure/lambda_handlers/budget_manager.py` -- Reads `counter_shards` from config, passes to `put_allocation()`
- `infrastructure/lambda_handlers/reconciliation.py` -- Reads `counter_shards` from config, passes to `reset_burst_counter_drift()`
- `scripts/create_model_config.py` -- New `--counter-shards` CLI parameter
