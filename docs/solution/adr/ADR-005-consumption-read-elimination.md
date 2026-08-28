# ADR-005: Eliminate the Post-Gate Consumption-Record Read (the real ~3,000 RCU/s hot partition)

> **SUPERSEDED / HISTORICAL.** This decision (replace the consumption-record read with counters) was
> subsequently reversed. The current implementation reinstates a bounded sliding-window
> consumption-record read as the admission gate and removed the counters + the scheduled
> Reconciliation Lambda — see `docs/solution/architecture.md`. Retained as a decision record.

## Status

Superseded (was: Accepted)

## Date

2026-07-07

## Context

An independent scalability audit found that [ADR-004](ADR-004-counter-write-sharding.md) misattributed the recurring hot-partition problem. ADR-004 sharded the admission **counter** (a conditional `Update` = a **write**/WCU path) and claimed this fixed the ~3,000 RCU/s per-partition ceiling. It did not — the ~3,000 RCU/s ceiling is a **read**, and it was untouched by counter sharding.

### The actual bottleneck

After the atomic admission gate wrote the consumption record + incremented the counter, `budget_manager` ran a **post-gate verification**:

```
query_consumption_records(model_id, capacity_mode='BURST', window_seconds=60, consistent_read=...)
```

This query reads **every consumption record in the 60-second window** from the single partition `MODEL#{model_id}#BURST#CONSUMPTION`, then sums `estimated_tokens` to derive available TPM. Characteristics that made it the true hot partition:

- **Single partition, unsharded.** Unlike the counter (ADR-004), this PK was never sharded.
- **O(records-in-window) RCU per request → O(N²) aggregate.** Every admitted request reads all records written by all other requests in the window. At high RPM this hit DynamoDB's ~3,000 RCU/s per-partition read ceiling and threw `ThrottlingException`, surfaced to callers as 400s.
- **Runtime used `consistent_read=False` (half-cost, still O(records)); the mantle path used `consistent_read=True`** (`_try_reserve_mantle`) — full cost, completely unmitigated.

The counter, by contrast, was already an *exact* atomic gate: RPM via `#count < cap`, and (for mantle) iTPM/oTPM via `#count <= headroom` conditions inside the same `TransactWriteItems`. So the post-gate read was **redundant** for enforcement — its only job was TPM, and TPM could be enforced the same atomic way as everything else.

## Decision

**Move TPM enforcement into the atomic transaction and delete the post-gate read entirely.**

1. **Runtime TPM counters (single, NOT sharded).** `put_allocation()` adds TWO token-counter items to the admission `TransactWriteItems` on the base (unsharded) counter PK: a window counter `SK = '{window}#TPM'` (per-minute budget) and an epoch counter `SK = 'GLOBAL#{epoch}#TPM'` (5-minute budget, mirroring the RPM global counter's cross-window burst protection). Both increment by `estimated_tokens` and gate with `if_not_exists(#count, :zero) <= :headroom` where `headroom = effective_cap − increment`. Using `if_not_exists(...) <= headroom` (rather than `attribute_not_exists(#count) OR ...`) means the FIRST write in a window is bounded too, so a single request larger than the cap is correctly rejected (headroom negative → `0 <= negative` is false).

   **Why single, not sharded (adversarial-review finding):** sharding a *variable-increment* token counter is unsound with this data model — (a) a request larger than a per-shard cap could never be admitted though it fits the global budget; (b) reconciliation cannot recover true per-shard token sums (consumption records carry no shard id), so an even-split reset over/under-corrects and biases toward over-admission every cycle. A single counter is exactly enforceable and exactly reconcilable. The tradeoff is item-level write contention at very high RPM — a THROUGHPUT concern (TransactionConflict retries absorb transient contention), not correctness. If a load-test re-run shows TPM-counter write contention, the fix is shard-*tagged consumption records* (so reconciliation can compute true per-shard sums), not naive sharding. The mantle iTPM/oTPM counters follow the same single-counter, bounded-first-write pattern.

2. **Delete the runtime post-gate query + rollback.** Since the transaction now atomically enforces both RPM and TPM, reaching the post-gate point means both dimensions had headroom. There is no over-consumption to detect and no consumption record to roll back. `query_consumption_records` + `calculate_available_tpm` + the rollback/enqueue branch are removed from the runtime path.

3. **Delete the mantle post-gate consistent read + rollback.** The mantle 3-way gate (`_put_allocation_mantle`) already enforces RPM/iTPM/oTPM atomically. Its Step-3 `consistent_read=True` verification of every window record — the most expensive variant — is removed for the same reason.

4. **Reconciliation for the TPM counters (exact).** Because the TPM counters increment by *estimated* tokens, they can drift on rollbacks or crashes. `reset_burst_tpm_counter_drift()` reconciles BOTH the window counter (`{window}#TPM`, summed over the 60s window) and the epoch counter (`GLOBAL#{epoch}#TPM`, summed over the 5-min epoch) against the summed `estimated_tokens` of the matching consumption records. Because the counters are single/unsharded, the expected value is the EXACT sum — no even-split estimation. The race-safe `#count <= :read_value` guard ensures reconciliation never lowers a counter below what concurrent writers established. Wired into the reconciliation Lambda's 60-second cycle.

## Consequences

### Positive

- **Removes the true ~3,000 RCU/s hot partition.** The hot-path read of the consumption partition is gone for both runtime and mantle. Admission cost per request is now O(1) counter writes, not O(records) reads.
- **Fixes mantle, which was completely unmitigated** (it used the full-cost `consistent_read=True` variant).
- **Enforcement is now uniform and atomic** across RPM, TPM, iTPM, oTPM — all in-transaction counters, no read-derived checks on the hot path.
- **Consumption records are still written** (they carry `estimated_tokens`/split tokens for reconciliation, metrics, and post-call reconciliation to actuals) — only the hot *read* is removed.

### Negative / Trade-offs

- **TPM enforcement is exact within a window (no sharding artifact).** Because the TPM counters are single, admission is gated on the true running token total — there is no `ceil(cap/N)` per-shard over-admission. The only over-admission source is the standard optimistic-concurrency race window (multiple requests reading the same counter before either commits), bounded by the increment sizes in flight and corrected by reconciliation — the same property the RPM counter has at `counter_shards=1`. The window+epoch cap pair prevents cross-boundary token bursts.
- **Estimate-based gating.** The counter increments by *estimated* tokens at admission; actuals are reconciled post-call. This is unchanged from prior behavior (the old query also summed estimates) — but now the estimate lives in the counter, so estimate error is bounded by reconciliation rather than re-derived each request.
- **TransactWriteItems item count.** Runtime transaction grows from 3 to 4 items when TPM-gated (Put + RPM-window + RPM-global + TPM). Well under the 100-item / same-item-collision limits (distinct SKs).

## Verification

- `cdk synth` passes clean on the merged tree; cdk-nag `AwsSolutionsChecks` reports no errors/warnings.
- Hot-path admission no longer calls `query_consumption_records` in `budget_manager.py` (confirmed by grep — only comments reference it).
- Expressions validated locally against real (moto) DynamoDB: `tests/test_admission_expressions.py` (legal expressions, TPM gating, oversized rejection, RPM-only admit).
- **LOAD-VALIDATED at 5× (2026-07-09).** The 5× extreme-spike scenario that previously deadlocked (4.3% success, 43.8% `Sandbox.Timedout`, 51.9% SFN `TimedOut`) now completes at **99.94% success (7074/7078), 0 admission timeouts, 0 SFN timeouts, 0 Lambda throttles**. Full run-by-run detail and the residual burst-absorption limitation in [HOT-PARTITION-FIX-VALIDATION.md](../design/HOT-PARTITION-FIX-VALIDATION.md). Reaching that result required three additional fixes surfaced by the load test: Budget Manager memory 128MB→1024MB (compute starvation), a DynamoDB expression-legality fix (`if_not_exists()` is illegal in a ConditionExpression), and removing the admission-gate reserved-concurrency cap (full-pool access). The hot-partition read elimination itself is confirmed: 0 DynamoDB throttles across the run.

## Known limitation

The synchronous per-request admission gate has a finite instantaneous-burst ceiling — this shaper targets **quota overflow** (queue what exceeds TPM), not unbounded Lambda-invocation-burst absorption. For extreme instantaneous bursts, front the admission path with SQS or API Gateway throttling. Deferred as out-of-scope. See the validation doc.

## Files Changed

- `infrastructure/lambda_layer/python/shared_service/dynamo.py` — sharded `{window}#TPM` counter in `put_allocation()`; new `reset_burst_tpm_counter_drift()`.
- `infrastructure/lambda_handlers/budget_manager.py` — pass TPM config to `put_allocation()`; remove runtime + mantle post-gate reads, verifications, and rollbacks.
- `infrastructure/lambda_handlers/reconciliation.py` — call `reset_burst_tpm_counter_drift()` in the sweep cycle.

## Related

- [ADR-004](ADR-004-counter-write-sharding.md) — counter **write**-sharding (contention/`TransactionConflict`), corrected to not claim the read-ceiling fix.
