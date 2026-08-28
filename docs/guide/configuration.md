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
| `BURST_FRACTION` | `--burst-fraction` | Fraction of quota in the burst bucket (default `0.50`). |
| `QUEUE_FRACTION` | `--queue-fraction` | Fraction of quota in the queue bucket (default `0.45`). |
| `QUEUE_TARGET_TPM` | `--queue-target-tpm` | Even-spacing pacer target for the queue drain (tokens/min). Omitted = disabled. |
| `ADAPTIVE_SHIFT_MAX` | `--adaptive-shift-max` | Max fraction of burst capacity to shift into the queue (default `0` = disabled). |
| `ADAPTIVE_QUEUE_THRESHOLD` | `--adaptive-queue-threshold` | Queue depth at which the max adaptive shift applies (default `50`). |

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

`MODEL` accepts these aliases (from the `MODEL_MAP` in `create_model_config.py`),
or you can pass a full Bedrock model ID. Each alias carries model-specific default
RPM, TPM, output-token burndown, and bytes-per-token values.

| Alias | Resolves to | Default RPM | Default TPM |
|-------|-------------|-------------|-------------|
| `opus-5` | `us.anthropic.claude-opus-5` | — (token-only) | 30,000,000 |
| `sonnet-5` | `us.anthropic.claude-sonnet-5` | — (token-only) | 6,000,000 |
| `nova-2-lite` | `us.amazon.nova-2-lite-v1:0` | 2,000 | 8,000,000 |
| `nova-lite` | `us.amazon.nova-lite-v1:0` | 2,000 | 8,000,000 |
| `nova-2-lite` | `us.amazon.nova-2-lite-v1:0` | 2,000 | 8,000,000 |
| `nova-pro` | `us.amazon.nova-pro-v1:0` | 500 | 2,000,000 |
| `opus-47` | `us.anthropic.claude-opus-4-7` | none (TPM-only) | 15,000,000 |
| `sonnet-5` | `us.anthropic.claude-sonnet-5` | none (TPM-only) | 6,000,000 |

The alias table in the script is the source of truth and includes additional
entries (single-region and Mantle variants). Unknown models default to no RPM gate
(token-quota-only) and a 40,000 TPM fallback, with a warning printed to stderr.

> **These TPM/RPM defaults are conservative placeholders.** Real per-account quotas
> vary — check Service Quotas for your account and override with `TPM=` / `RPM=`.

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
| `output_token_burndown_rate` | Output-token multiplier for TPM accounting (e.g. `5.0` for the Claude 3.7+ family, `1.0` for most others). |
| `bytes_per_token` | Bytes-per-token ratio used to estimate input tokens before the call (Claude ~3.5, Nova ~3.0, default 4.0). |

### Queue, admission window, and adaptive controls

| Field | Default | Meaning |
|-------|---------|---------|
| `queue_batch_size` | `10` | Items released per queue-drain tick; each tick fires its batch at Bedrock in parallel. |
| `short_window_sec` | `2` | Short (rate-smoothing) admission window. |
| `long_window_sec` | `15` | Long (accuracy) admission window — long enough that reconciled actual usage dominates. |
| `adaptive_shift_max` | `0` | Max fraction of burst capacity to shift into the queue (`0` disables adaptive capacity). |
| `adaptive_queue_threshold` | `50` | Queue depth at which the maximum adaptive shift applies. |

### Backend fields (Tier 2)

| Field | Default | Meaning |
|-------|---------|---------|
| `backend` | `runtime` | `runtime` = the standard `bedrock-runtime` Converse path; `mantle` = the Mantle Anthropic Messages API with split iTPM/oTPM admission (queue-only). |
| `api_style` | `converse` (runtime) / `messages` (mantle) | Request API style. `responses` targets the OpenAI Responses API on Mantle. |
| `queue_target_tpm` | (unset) | Even-spacing pacer target; only written when `QUEUE_TARGET_TPM` is provided. |

For `--backend mantle`, `--itpm` and `--otpm` are **required**; the config is
forced queue-only (burst zeroed) and adds `itpm_limit`/`otpm_limit` plus their
queue capacities and regeneration rates.

> **Runtime fields set by the Lambdas, not by `create-config`.** The operator
> runbook references `max_tokens_per_request` (per-request output cap, default
> 4096) and `circuit_breaker_disabled`. These are read at request time with
> defaults and are not produced by `create_model_config.py`; set them directly on
> the CONFIG item if you need to change them (see
> [`../solution/runbook.md`](../solution/runbook.md)).

---

## Capacity split: burst / queue / buffer

Both the RPM and the TPM quotas are split the same way, using three fractions:

- **burst** (`burst_fraction`, default `0.50`) — capacity for requests admitted
  immediately and sent straight to Bedrock.
- **queue** (`queue_fraction`, default `0.45`) — capacity for overflow that gets
  enqueued and drained at pace.
- **buffer** (`buffer_fraction`, default `0.05`) — a safety holdback.

The three fractions do **not** have to sum to 1.0; the buffer is an independent
holdback. For example `85 / 10 / 5` is a valid split (`--burst-fraction 0.85
--queue-fraction 0.10 --buffer-fraction 0.05`), biasing toward immediate admission.

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

Uses Jamba's default RPM (100) and TPM (100,000), split 50/45/5.

### Set an explicit RPM and TPM

```bash
make create-config MODEL=nova-2-lite RPM=2000 TPM=8000000
```

### Token-quota-only model (no RPM gate)

```bash
make create-config MODEL=sonnet-5 TPM=6000000
```

`sonnet-5` has no RPM quota, so admission paces purely on TPM.

### Bias capacity toward immediate admission

```bash
make create-config MODEL=nova-2-lite BURST_FRACTION=0.85 QUEUE_FRACTION=0.10
```

### Enable adaptive capacity

```bash
make create-config MODEL=nova-2-lite BURST_CAPACITY=5 ADAPTIVE_SHIFT_MAX=0.2 ADAPTIVE_QUEUE_THRESHOLD=10
```

Shifts up to 20% of burst capacity into the queue as the queue depth approaches 10.

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
