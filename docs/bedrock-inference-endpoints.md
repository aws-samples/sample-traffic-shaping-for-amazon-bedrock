# Amazon Bedrock inference endpoints: the four paths

A given Bedrock model can be reached through more than one distinct path, and each path
has its own model-ID convention, its own endpoint, and its own quota. This doc catalogs the
four paths that matter for this project's `MODEL_MAP` (`scripts/create_model_config.py`),
so that quota lookups, inference-profile matching, and endpoint selection are done against a
correct mental model instead of being re-derived from scratch each time.

## The four paths

### 1. Global CRIS (cross-region inference, global pool)

- **ID prefix:** `global.` — e.g. `global.anthropic.claude-opus-4-7`
- **Endpoint:** `bedrock-runtime`
- **What it is:** an inference profile that pools capacity across all AWS regions
  worldwide. Requests can land in any region in the pool.
- **Priority:** this is AWS's *preferred* path for customers, not a fallback or secondary
  option. Global and regional CRIS (below) are both first-class targets for this project.

### 2. Regional / geo CRIS (cross-region inference, regional pool)

- **ID prefix:** a region-group prefix such as `us.`, `eu.`, `apac.` — e.g.
  `us.anthropic.claude-sonnet-5`
- **Endpoint:** `bedrock-runtime`
- **What it is:** an inference profile that pools capacity across a smaller set of regions
  within one geography (e.g. all US regions), rather than worldwide.
- **Priority:** alongside global CRIS, this is also a primary/focus path for this project —
  not secondary to global, just narrower in scope.

### 3. Mantle (bare model ID, alternate endpoint)

- **ID form:** the bare model ID with no region or CRIS prefix — e.g.
  `anthropic.claude-opus-4-7`. In this repo, requested via `--backend mantle`.
- **Endpoint:** `bedrock-mantle` — a **separate endpoint entirely** from
  `bedrock-runtime`. It is not a variant of runtime; it is a different service surface with
  its own request/response shape (Anthropic Messages API, OpenAI Responses API, etc.,
  depending on the model).
- **Priority:** secondary/optional for this project's purposes. Use it only when a model
  requires it or when explicitly testing the mantle backend.

### 4. On-demand (bare model ID, single-region, no CRIS)

- **ID form:** the bare model ID, same shape as mantle's bare ID — e.g.
  `amazon.nova-lite-v1:0` — but invoked against `bedrock-runtime` directly, with no
  cross-region inference profile involved. Single-region, no pooling.
- **Endpoint:** `bedrock-runtime`
- **Status in this project:** this path exists in the account's real quota data, but as of
  this writing no entry in `MODEL_MAP` (`scripts/create_model_config.py`) currently uses it.
  It's documented here because it is a real, distinct fourth case — not a variant of mantle
  just because both use a bare model ID. The endpoint (`bedrock-runtime` vs `bedrock-mantle`)
  is what actually distinguishes them, not the ID shape.

## AWS quota-name patterns

Each of the four paths has its own Service Quotas entry, with a distinct naming pattern
(verified against real account quota data, not assumed from documentation):

| Path | Quota name pattern |
|---|---|
| Global CRIS | `Global cross-region model inference {tokens\|requests} per minute for X` |
| Regional/geo CRIS | `Cross-region model inference {tokens\|requests} per minute for X` |
| Mantle | `[bedrock-mantle endpoint] {Input\|Output} tokens per minute for X` |
| On-demand | `On-demand model inference {tokens\|requests} per minute for X` |

`X` is the model-family/name portion of the quota description. Note that mantle's quota
name is the only one split into separate Input/Output token quotas rather than a single
combined tokens-per-minute figure — this lines up with mantle's split `--itpm`/`--otpm`
configuration in `scripts/create_model_config.py`.

## The unique key is `inferenceProfileId`, not the base model

**The unique identity of an inference profile is its `inferenceProfileId` — not the base
model it resolves to.**

`bedrock.list_inference_profiles()` and `bedrock.get_inference_profile()` expose the
underlying base model directly, via `models[].modelArn` on each profile. But a `global.X`
profile and a `us.X` profile (or `eu.X`, `apac.X`, etc.) that both resolve to the same base
model `X` are **two distinct inference profiles**, each carrying its own separate quota.

This is expected and correct, not a data-quality problem. When processing inference-profile
or quota data:

- Never dedupe or merge two profiles just because `models[].modelArn` shows the same base
  model underneath. A shared base model is normal — it's the same underlying model exposed
  through two different pooling strategies (global vs. regional), each independently
  rate-limited.
- Use `inferenceProfileId` as the key for anything that needs to track "one distinct
  billable/throttleable thing." Grouping or keying by base model instead will silently
  collapse legitimately separate quotas into one.
- `list_inference_profiles()`/`get_inference_profile()` are the *only* reliable way to get
  ground-truth profile-to-base-model mapping. The prefix convention (`global.`, `us.`, bare
  ID) is a strong hint but is not a substitute for checking `models[].modelArn` — don't
  infer the base model from the ID string alone when the API can tell you directly.

## Scope: which models this applies to

This taxonomy is scoped to whatever is **currently listed in `MODEL_MAP`** in
`scripts/create_model_config.py` — i.e., the latest and previous-generation models this
project actively configures.

Explicitly **out of scope**, even though real inference-profile and quota data for them
exists in the account:

- Legacy Claude models: Sonnet 3, Sonnet 4, Sonnet 4.5, Haiku 3, Haiku 4
- Llama models

If you're matching quota data or inference profiles against `MODEL_MAP` entries, do not
expand that matching to cover these legacy/Llama models just because their data happens to
be present in the same account-level results. Treat their presence in raw quota/profile
listings as noise to filter out, not as additional coverage to add.

## Priority summary

For this project's purposes:

- **Focus (primary):** global CRIS and regional/geo CRIS, both on `bedrock-runtime`. Global
  is AWS's preferred customer path; regional is an equally valid, narrower-pooled
  alternative. Neither is a fallback for the other — they're two flavors of the same
  priority tier.
- **Secondary/optional:** mantle and on-demand. These are supported where a model requires
  them, but they are not the default target when adding new models or reconciling quota
  data against `MODEL_MAP`.
