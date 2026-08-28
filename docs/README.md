# Documentation

Documentation for the Bedrock Traffic Shaper, organized by audience and mapped to the
standard user-guide flow: **What is it → Setting up → Getting started → How it works →
Tasks → Security → Troubleshooting → Reference.**

The tree is split three ways:

- [`guide/`](guide/) — task references that complement the root README Quick start: configuration, the invoke API contract, and regional/partition notes.
- [`solution/`](solution/) — the **rate limiter itself**: how it works, how to operate it, and why it is built this way.
- [`testing/`](testing/) — **testing**: how to reproduce or extend the load tests, and the authoritative results.

## Where to start (by audience)

| You are a… | Start here | Then |
|------------|-----------|------|
| **Adopter** (deploy and run the shaper) | Root [`README.md`](../README.md) → Quick start | [`guide/configuration.md`](guide/configuration.md), [`solution/architecture.md`](solution/architecture.md), [`solution/cost-model.md`](solution/cost-model.md) |
| **Operator** (run it in production) | [`solution/runbook.md`](solution/runbook.md) | [`solution/production-hardening.md`](solution/production-hardening.md) |
| **Contributor** (change the code) | [`solution/architecture.md`](solution/architecture.md) | [`solution/adr/`](solution/adr/), [`solution/design/`](solution/design/), [`../SECURITY.md`](../SECURITY.md) |
| **Load-tester** (measure it) | [`testing/README.md`](testing/README.md) | [`testing/results.md`](testing/results.md) |

## User-guide flow

| Stage | What it answers | Where it lives |
|-------|-----------------|----------------|
| **What is it** | What the shaper does and the problem it solves | Root [`README.md`](../README.md) |
| **Setting up** | Prerequisites, deployment, configuration | Root [`README.md`](../README.md) (Quick start), [`guide/configuration.md`](guide/configuration.md), [`../config.env.template`](../config.env.template), `../cdk.json` |
| **Getting started** | First deploy and first test run | Root [`README.md`](../README.md) Quick start (`make setup` / `make test`) |
| **How it works** | Request lifecycle, capacity model, DynamoDB schema, admission gate, counter sharding | [`solution/architecture.md`](solution/architecture.md), [`solution/design/`](solution/design/) |
| **Tasks** | Operating the running system — configure models, inspect state, drain queues, respond to alarms | [`solution/runbook.md`](solution/runbook.md), `make help` (see [`../Makefile`](../Makefile)) |
| **Security** | Authorization model, encryption, IAM posture, vulnerability reporting | [`../SECURITY.md`](../SECURITY.md), [`solution/architecture.md`](solution/architecture.md) |
| **Troubleshooting** | Alarm response, failure modes, debug sequence | [`solution/runbook.md`](solution/runbook.md) |
| **Reference** | Decisions, cost, roadmap, command reference, config keys | [`solution/adr/`](solution/adr/), [`solution/cost-model.md`](solution/cost-model.md), [`solution/production-hardening.md`](solution/production-hardening.md), [`../Makefile`](../Makefile) |

## Guide docs (`guide/`)

| Doc | Contents |
|-----|----------|
| [`configuration.md`](guide/configuration.md) | Every model-config field, the model-alias table, and worked examples (including forcing queueing). |
| [`invoke-api.md`](guide/invoke-api.md) | The `POST /invoke` request/response contract, SigV4 auth, and the 200/429/503 outcome semantics. |
| [`regional-considerations.md`](guide/regional-considerations.md) | Confirming Bedrock model availability by region/partition before you deploy. |

## Solution docs (`solution/`)

| Doc | Contents |
|-----|----------|
| [`architecture.md`](solution/architecture.md) | Canonical "how it works": components, Step Functions callback flow, capacity model, single-table schema, admission gate, and counter write-sharding. |
| [`runbook.md`](solution/runbook.md) | Operator runbook — alarm response, normal baselines, latency SLAs, inspection/log commands, DLQ consumer guidance, key configuration. |
| [`cost-model.md`](solution/cost-model.md) | Infrastructure cost analysis. |
| [`production-hardening.md`](solution/production-hardening.md) | Production roadmap. |
| [`adr/`](solution/adr/) | Architecture Decision Records (ADR-001 phased approach → 002 MVP → 003 production-ready → 004 counter write-sharding → 005 consumption-read elimination). |
| [`design/`](solution/design/) | Design rationale notes referenced by the ADRs (leaky-bucket optimization, queue-processor triggers, hot-partition fix validation) — indexed by [`design/README.md`](solution/design/README.md). |

## Testing docs (`testing/`)

| Doc | Contents |
|-----|----------|
| [`README.md`](testing/README.md) | Testing index and the **honesty gate** — read before trusting any success rate. |
| [`results.md`](testing/results.md) | Authoritative load-test results, including the primary-evidence appendix. |

## Diagrams

Architecture diagrams live in [`../architecture/`](../architecture/): the editable
`traffic-shaper-architecture-v2.drawio` source plus rendered `.png` and `.svg`. (Older
`.svg` files in that folder predate the current design.)
