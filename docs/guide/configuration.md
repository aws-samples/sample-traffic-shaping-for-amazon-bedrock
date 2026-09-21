# Model Configuration Reference

The shaper stores one `CONFIG` record per model in the DynamoDB single table. That
record holds the model's quota (RPM and/or TPM), how the quota is split across the
burst / queue / buffer buckets, the token-estimation parameters, and the sliding
window used by the admission gate. The Lambdas read this record on every request.

You create and update these records with `make create-config`. This page documents
every field, the command surface, the model aliases, and worked examples.

All fields here are grounded in
[`scripts/create_model_config.py`](../../scripts/create_model_config.py).

---

## The `make create-config` command

```bash
make create-config MODEL=<alias-or-model-id> [KEY=VALUE ...]
```

`MODEL` is required and may be a short alias (see the table below) or a full
Bedrock model ID. Additional settings are passed as `KEY=VALUE` make variables,
which map to `create_model_config.py` flags:

| Make variable | Script flag | Meaning |
|---------------|-------------|---------|
| `BURST_CAPACITY` | `--burst-capacity` | Override the burst-bucket size (see [Capacity split](#capacity-split-burst--queue--buffer)). |
| `RPM` | `--rpm` | Override the requests-per-minute quota. `RPM=0` means "no RPM gate" (token-quota-only). |
| `TPM` | `--tpm` | Override the tokens-per-minute quota. |
| `BURST_FRACTION` | `--burst-fraction` | Fraction of quota in the burst bucket (default `0.00` — queue-only). |
| `QUEUE_FRACTION` | `--queue-fraction` | Fraction of quota in the queue bucket (default `0.85`). |
| `QUEUE_TARGET_TPM` | `--queue-target-tpm` | Even-spacing pacer target for the queue drain (tokens/min). Omitted = disabled. |

Flags accepted directly by `create_model_config.py` but **not** wired as make
variables (use them by editing the command, or extend the Makefile):
`--buffer-fraction`, `--bytes-per-token`, `--short-window-sec`,
`--long-window-sec`, `--backend`, `--api-style`, `--itpm`, `--otpm`.

> **Known drift — do not use `COUNTER_SHARDS` or `MAX_BURST_MULTIPLIER`.** The
> Makefile advertises `COUNTER_SHARDS` and `MAX_BURST_MULTIPLIER` variables (and
> `make help` shows a `COUNTER_SHARDS=5` example), but `create_model_config.py`
> does **not** define `--counter-shards` or `--max-burst-multiplier`. Passing them
> makes the underlying script error out on unrecognized arguments. Omit both.

---

## Model aliases

`MODEL` accepts an alias from the `MODEL_MAP` table in `create_model_config.py`,
or you can pass a full Bedrock model ID directly. `MODEL_MAP` is a pure
ergonomic alias-to-model-ID table — see its own comment in the source — it
carries **no** per-alias RPM, TPM, burndown, or bytes-per-token values. Every
quota and capacity value is resolved at run time from the model ID, never from
the alias:

- **RPM** is retired as a default dimension (owner decision 2026-09-16):
  every model resolves to `rpm=None` (token-quota-only) unless you pass
  `--rpm`/`RPM=` explicitly, in which case that value is pinned verbatim.
- **TPM** always comes from `--tpm`/`TPM=` when given, otherwise from
  `cache['profiles'][model_id]['tpm']` in `.bedrock_quota_cache.json`
  (populated by `make refresh-quotas`). **There is no general-purpose
  fallback value.** A model with no usable cache entry — no matching profile,
  or a `tpm: null` entry — is a hard error that tells you to run
  `make refresh-quotas` or pass `--tpm` explicitly (see `resolve_tpm()` in the
  source). Mantle bare model IDs and other on-demand bare IDs never have an
  inference profile and so can never appear in the cache — they always
  require an explicit `--tpm`, or (for `--backend mantle` with both
  `--itpm`/`--otpm` given) can omit `--tpm` entirely — see
  [Mantle backend](#backend-fields-tier-2) below.
  - **One narrow, named exception:** `DOCUMENTED_QUOTA_DEFAULTS` in
    `create_model_config.py` — a small, explicitly-sourced dict for a model
    too new for AWS Service Quotas to have published a discoverable rate
    quota yet (e.g. Kimi K3: 10M TPM, cited to an authoritative internal
    Bedrock model-limits document, verified 2026-09-21 that Service Quotas
    genuinely has zero rows for it). `resolve_tpm()` only consults this dict
    *after* a real cache miss, tags the result `tpm_source='documented_default'`
    (never confusable with `'cache'` in the printed summary), and it never
    applies to any model not explicitly listed. This is not a return to the
    old hardcoded-fallback design it replaced — every entry is scoped to one
    named model with a cited source, meant to be deleted once AWS publishes
    the real quota.

Some aliases carry an inline comment in the source noting which backend/API
style they're meant to be used with (e.g. a `-mantle` suffix alias), but that
is documentation, not a value the script looks up.

> **Real quota values live in your account, not in this repo.** Check Service
> Quotas for your account, or read `.bedrock_quota_cache.json` after running
> `make refresh-quotas`, for the current TPM figures — don't expect a fixed
> number here to still be right.

---

## Fields written to the `CONFIG` record

The record is keyed `pk = MODEL#<model_id>`, `sk = CONFIG`, with
`entity_type = model_config`. `create_model_config.py` derives and writes the
following.

### RPM dimension (optional)

Written only when the model has an RPM quota. Token-quota-only models (RPM
resolves to `None`) get a large sentinel `burst_capacity`/`queue_capacity` so the
RPM gate never binds and admission paces purely on TPM.

| Field | Meaning |
|-------|---------|
| `rpm_limit` | Requests-per-minute quota, or `null` for token-only models. |
| `rpm_quota_enabled` | `true` when an RPM gate is active. |
| `burst_capacity` | RPM burst-bucket size (`rpm * burst_fraction`, or the `BURST_CAPACITY` override). Also the counter the admission `TransactWriteItems` gate increments. |
| `burst_regeneration_rate` | RPM burst refill rate (requests/sec). |
| `queue_capacity` | Max queue depth per window (`rpm * queue_fraction`). |
| `queue_regeneration_rate` | Queue drain pace (requests/sec) — this is the RPM pacing the queue processor drains at. |
| `buffer_capacity` | RPM safety holdback (`rpm * buffer_fraction`). |

### TPM dimension (always written)

| Field | Meaning |
|-------|---------|
| `tpm_limit` | Tokens-per-minute quota. |
| `tpm_burst_capacity` | TPM burst-bucket size (`tpm * burst_fraction`). |
| `tpm_burst_regeneration_rate` | TPM burst refill rate (tokens/sec). |
| `tpm_queue_capacity` | TPM queue-bucket size (`tpm * queue_fraction`). |
| `tpm_queue_regeneration_rate` | TPM queue refill rate (tokens/sec). |
| `tpm_buffer_capacity` | TPM safety holdback (`tpm * buffer_fraction`). |
| `output_token_burndown_rate` | Output-token multiplier for TPM accounting. Derived per-model by `derive_default_burndown()` in the script — see that function's docstring for the current per-model-family rates, and AWS's [token-burndown documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/quotas-token-burndown.html) for the underlying rule. As of this writing it's three buckets on the runtime backend: a specific Anthropic point release at the top rate, the rest of the current Anthropic + all runtime-reachable OpenAI models at a middle rate, and older Anthropic models at a lower rate; `mantle` backend is always `1.0` (Mantle gates oTPM directly, so burndown accounting is unnecessary). |
| `bytes_per_token` | Bytes-per-token ratio used to estimate input tokens before the call. Derived by `derive_default_bytes_per_token()`: `3.0` for Nova, `4.0` for every other model including Claude. The Nova value is empirically validated (a 2026-07-10 load-test finding, cited in the function's docstring); the Claude/default `4.0` value is **not independently validated** anywhere in this repo or in public AWS documentation — treat it as an open question, not a settled fact, until someone runs that validation. |

### Queue and admission window

| Field | Default | Meaning |
|-------|---------|---------|
| `queue_batch_size` | `10` | Items released per queue-drain tick; each tick fires its batch at Bedrock in parallel. |
| `short_window_sec` | `2` | Short (rate-smoothing) admission window. |
| `long_window_sec` | `15` | Long (accuracy) admission window — long enough that reconciled actual usage dominates. |

### Backend fields (Tier 2)

| Field | Default | Meaning |
|-------|---------|---------|
| `backend` | `runtime` | `runtime` = the standard `bedrock-runtime` Converse path; `mantle` = the Mantle Anthropic Messages API with split iTPM/oTPM admission (queue-only). |
| `api_style` | `converse` (runtime) / `messages` (mantle) | Request API style. `responses` targets the OpenAI Responses API on Mantle. |
| `queue_target_tpm` | (unset) | Even-spacing pacer target; only written when `QUEUE_TARGET_TPM` is provided. |

For `--backend mantle`, `--itpm` and `--otpm` are **required**; the config is
forced queue-only (burst zeroed) and adds `itpm_limit`/`otpm_limit` plus their
queue capacities and regeneration rates.

`--tpm` is **optional** when `--backend mantle` is combined with both `--itpm`
and `--otpm`: `configure_mantle_queue_only()` gates admission purely on
iTPM/oTPM, so the generic `tpm_limit` field is informational/legacy only in
this case. When `--tpm` is omitted, `tpm_limit` defaults to the `--itpm` value
instead of requiring a (nonexistent) quota-cache entry for a mantle bare model
ID. If `--tpm` is passed explicitly, it wins as usual.

> **Runtime fields set by the Lambdas, not by `create-config`.** The operator
> runbook references `max_tokens_per_request` (per-request output cap, default
> 4096) and `circuit_breaker_disabled`. These are read at request time with
> defaults and are not produced by `create_model_config.py`; set them directly on
> the CONFIG item if you need to change them (see
> [`../solution/runbook.md`](../solution/runbook.md)).

---

## Capacity split: burst / queue / buffer

Both the RPM and the TPM quotas are split the same way, using three fractions:

- **burst** (`burst_fraction`, default `0.00`) — capacity for requests admitted
  immediately and sent straight to Bedrock. Zero by default: `calculate_config()`
  ships queue-only, with `burst_capacity` resolving to `0` (the admission gate's
  "burst disabled — route everything to the queue" state).
- **queue** (`queue_fraction`, default `0.85`) — capacity for overflow that gets
  enqueued and drained at pace.
- **buffer** (`buffer_fraction`, default `0.15`) — a safety holdback.

The three fractions do **not** have to sum to 1.0; the buffer is an independent
holdback and does not need to sum with the other two. `burst_fraction` and
`queue_fraction`, however, should not both be set nonzero for the same model.
Per [`../solution/capacity-model-rationale.md` §1.1](../solution/capacity-model-rationale.md#11-one-admission-lane-per-model--burst-only-or-queue-only-never-both),
each model should run a single admission lane — burst-only (e.g. `85 / 0 / 15`,
`--burst-fraction 0.85 --queue-fraction 0 --buffer-fraction 0.15`, biasing
toward immediate admission) or queue-only (e.g. `0 / 85 / 15`) — never a blend
of both. Both lanes draw against the same account-level Bedrock quota with no
coordination between them, so a blended config either wastes budget or
throttles.

The `BURST_CAPACITY` override sets the RPM burst bucket directly, independent of
the fraction math — this is the knob used to force queueing in tests.

---

## Worked examples

### Force queueing (low burst) — the demo config

```bash
make create-config MODEL=opus-5 BURST_CAPACITY=2
```

Only ~2 requests get an immediate burst slot; the rest queue. This is the config
used by the README Quick start walkthrough and `make test`.

### Model defaults

```bash
make create-config MODEL=nova-2-lite
```

`nova-2-lite` gets no `--rpm`, so `rpm_limit` resolves to `None` (token-quota-only).
`tpm_limit` comes from `cache['profiles'][<model_id>]['tpm']` in
`.bedrock_quota_cache.json` — run `make refresh-quotas` first if that entry is
missing or stale. Split 0/85/15 (queue-only, the script's default fractions).

### Set an explicit RPM and TPM

```bash
make create-config MODEL=nova-2-lite RPM=2000 TPM=8000000
```

### Token-quota-only model (no RPM gate)

```bash
make create-config MODEL=sonnet-5 TPM=6000000
```

`sonnet-5` has no RPM quota, so admission paces purely on TPM.

### Burst-only lane (latency-intolerant workloads)

```bash
make create-config MODEL=nova-2-lite BURST_FRACTION=0.85 QUEUE_FRACTION=0
```

Runs a single admission lane (burst) with nothing sent to the queue. Per
[`../solution/capacity-model-rationale.md` §1.1](../solution/capacity-model-rationale.md#11-one-admission-lane-per-model--burst-only-or-queue-only-never-both),
each model should run burst-only **or** queue-only, never both — both lanes
draw against the same account-level Bedrock quota with no coordination
between them, so mixing them either wastes budget or throttles. Use
burst-only for latency-intolerant workloads, ideally paired with failover to
a secondary model/provider for anything that misses an immediate slot; use
queue-only (the default) for latency-tolerant, batch-shaped workloads.

### Adjust burst capacity without recreating the config

To change only the burst capacity on an already-deployed model, without rebuilding
the whole record:

```bash
make set-capacity CAPACITY=50
```

Check the current value:

```bash
make get-capacity
```

---

## Verify your configuration

After creating or updating a config, read it back:

```bash
make inspect-config MODEL=opus-5
```

`inspect-config` (and all `inspect-*` commands) accept `MODEL=<alias>`, defaulting
to `opus`.
