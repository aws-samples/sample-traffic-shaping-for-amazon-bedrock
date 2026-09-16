#!/usr/bin/env python3
"""
Create or update model configuration in the single table.

Usage:
    # Create nova-2-lite config with defaults
    python scripts/create_model_config.py nova-2-lite

    # Create nova-2-lite config with a low burst for testing
    python scripts/create_model_config.py nova-2-lite --rpm 10 --burst-capacity 2

    # Create sonnet-5 config with defaults
    python scripts/create_model_config.py sonnet-5

    # Override RPM for custom models
    python scripts/create_model_config.py custom-model --rpm 75
"""

import sys
import os
import json
import argparse
import boto3
from datetime import datetime, timezone, timedelta
from decimal import Decimal

# Add scripts directory for config_loader
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config_loader

# Defaults keep calculate_config() importable for offline tests. main() replaces
# these after loading config.env and verifying AWS access.
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
SINGLE_TABLE_NAME = os.environ.get('SINGLE_TABLE_NAME', 'semaphore-single-table')

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
QUOTA_CACHE_PATH = os.path.join(_SCRIPT_DIR, '..', '.bedrock_quota_cache.json')
STARTER_MODELS_PATH = os.path.join(_SCRIPT_DIR, '..', 'config', 'starter_models.json')

# Model ID mappings. Ergonomic input-alias table ONLY -- lets a human type
# `make create-config MODEL=opus-5` instead of the full inference profile ID.
# It must NEVER gate correctness: no quota or capacity value is looked up
# through this dict. Every quota value comes from cache['profiles'][model_id]
# (see resolve_tpm()), keyed by the model_id this table expands an alias to --
# never by the alias itself.
MODEL_MAP = {
    # Next-gen Claude — runtime CRIS forms (no -v1 suffix on 4.7+).
    # Mantle bare form — use with --backend mantle --itpm 10000000 --otpm 2000000
    'opus-47-mantle': 'anthropic.claude-opus-4-7',
    'opus-48': 'us.anthropic.claude-opus-4-8',
    # Global CRIS form of Opus 4.7 — token-only.
    'global-opus-47': 'global.anthropic.claude-opus-4-7',
    'opus-5': 'us.anthropic.claude-opus-5',
    'sonnet-46': 'us.anthropic.claude-sonnet-4-6',
    'sonnet-5': 'us.anthropic.claude-sonnet-5',
    'sonnet-5-mantle': 'anthropic.claude-sonnet-5',
    # Current fast Claude (active); token-only runtime shape like the rest of 4.x/5.
    'haiku-4-5': 'us.anthropic.claude-haiku-4-5-20251001-v1:0',
    'nova-lite': 'us.amazon.nova-lite-v1:0',
    'nova-lite-sr': 'amazon.nova-lite-v1:0',  # single-region: enforces per-region quotas
    'nova-micro': 'us.amazon.nova-micro-v1:0',  # smallest/fastest Nova (active)
    'nova-2-lite': 'us.amazon.nova-2-lite-v1:0',  # default runtime control model
    'nova-pro': 'us.amazon.nova-pro-v1:0',
    # New models (added 2026-08-21). Primary ID = us./base runtime ID where a
    # client-supported runtime (Converse) route exists; else the mantle base ID.
    'grok-4-6': 'us.xai.grok-4.6',                  # runtime Converse (cross-region only)
    'grok-4-3': 'xai.grok-4.3',                     # mantle responses (client not implemented)
    'gpt-5.6-cyber': 'openai.gpt-5.6-cyber',        # mantle responses (client not implemented)
    'gpt-5.6-daybreak-blue-sol': 'openai.gpt-daybreak-blue-5.6-sol',  # mantle responses
    'gpt-5.5': 'openai.gpt-5.5',                     # mantle responses (client not implemented)
    'gpt-5.4': 'openai.gpt-5.4',                     # mantle responses (client not implemented)
    'mythos-5': 'anthropic.claude-mythos-5',        # mantle messages (client supported)
    'fable-5': 'us.anthropic.claude-fable-5',       # runtime Converse + mantle messages
    'opus-4-8': 'us.anthropic.claude-opus-4-8',     # runtime Converse (VERIFIED quotas)
    'gemma-4-31b': 'google.gemma-4-31b',            # mantle responses (client not implemented)
    'gemma-4-26b-a4b': 'google.gemma-4-26b-a4b',    # mantle responses (client not implemented)
    'gemma-4-e2b': 'google.gemma-4-e2b',            # mantle responses (client not implemented)
    'nemotron-3-super-120b': 'nvidia.nemotron-super-3-120b',  # runtime Converse; mantle chat_completions not implemented
    'minimax-m2-5': 'minimax.minimax-m2.5',         # runtime Converse; mantle chat_completions not implemented
    # Added 2026-08-24 — active, runtime Converse via us. CRIS unless noted.
    'llama4-maverick': 'us.meta.llama4-maverick-17b-instruct-v1:0',
    'llama4-scout': 'us.meta.llama4-scout-17b-instruct-v1:0',
    'gpt-5.6-luna': 'us.openai.gpt-5.6-luna',       # runtime CRIS (distinct from the mantle cyber/daybreak variants)
    'gpt-5.6-sol': 'us.openai.gpt-5.6-sol',
    'gpt-5.6-terra': 'us.openai.gpt-5.6-terra',
    'glm-5': 'zai.glm-5',                           # ON_DEMAND direct (no CRIS profile)
}

def derive_default_burndown(model_id: str, backend: str) -> float:
    """
    Output token burndown rate for a model, derived from its model ID and backend.

    Anthropic/OpenAI models on a non-mantle (runtime) backend burn output tokens
    at ~10x their TPM weight; every other case (mantle backend, or any other
    provider) is the standard 1:1 burndown. Called unconditionally by
    process_model() -- there is no per-alias override table.
    """
    if backend == 'mantle':
        return 1.0
    lowered = model_id.lower()
    if 'anthropic' in lowered or 'openai' in lowered:
        return 10.0
    return 1.0


def derive_default_bytes_per_token(model_id: str) -> float:
    """
    Fallback bytes_per_token ratio for an alias with no explicit --bytes-per-token
    override.

    Nova tokenizer runs ~3.0 bytes/token (more tokens per byte than the 4.0 default).
    A 4.0 estimate under-counts input ~18%, so admission over-hands Bedrock and a 3x
    finite burst leaked ~15% TPM throttles (2026-07-10 validation). 3.0 + the 1.1
    safety margin over-counts slightly — the safe direction for a rate limiter. Every
    other model (including Claude) gets the standard 4.0 estimate. An explicit
    --bytes-per-token CLI override, when passed, always wins over this -- see
    process_model()'s args.bytes_per_token check.
    """
    if 'nova' in model_id.lower():
        return 3.0
    return 4.0


def resolve_tpm(model_id: str, explicit_tpm: int = None) -> tuple:
    """
    Resolve TPM for a model: explicit --tpm > cache['profiles'][model_id]['tpm'].

    cache['profiles'] (written by scripts/get_bedrock_quotas.py) is keyed directly by
    inferenceProfileId, already joined to its Service Quotas TPM value -- model_id IS
    the key. No prefix stripping (us./global.) and no variant re-derivation happens
    here; the cache writer already resolved that. There is no third (hardcoded) tier:
    a model with no usable cache entry is an error, not an invented number.

    Mantle bare IDs and on-demand bare IDs (e.g. a `*-mantle` alias's expansion, or
    glm-5) have no inference profile and so can never appear in cache['profiles'] --
    they only ever resolve via explicit_tpm (--tpm, or --itpm/--otpm for
    --backend mantle). That is a legitimate, permanent gap, not a broken cache.

    Returns (tpm, source) where source is 'explicit' or 'cache'. Raises LookupError
    (with a message identifying the cause and the fix) when no --tpm was given and
    the model has no usable cache entry -- callers must surface this via
    parser.error(), never invent a fallback.
    """
    if explicit_tpm:
        return explicit_tpm, 'explicit'

    if not os.path.exists(QUOTA_CACHE_PATH):
        raise LookupError(
            f"{QUOTA_CACHE_PATH} not found, so '{model_id}' cannot be resolved. Run "
            f"'make refresh-quotas' to populate it, or pass --tpm explicitly."
        )

    try:
        with open(QUOTA_CACHE_PATH, encoding='utf-8') as f:
            cache = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise LookupError(
            f"could not read {QUOTA_CACHE_PATH} ({e}), so '{model_id}' cannot be "
            f"resolved. Run 'make refresh-quotas' to regenerate it, or pass --tpm "
            f"explicitly."
        )

    last_refreshed = cache.get('lastRefreshedAt')
    if last_refreshed:
        try:
            refreshed_dt = datetime.fromisoformat(last_refreshed)
            if datetime.now(timezone.utc) - refreshed_dt > timedelta(days=7):
                print(
                    f"WARNING: .bedrock_quota_cache.json was last refreshed {last_refreshed} "
                    f"(>7 days ago). Run 'make refresh-quotas' for fresh data; proceeding with "
                    f"the stale cached value anyway.",
                    file=sys.stderr,
                )
        except ValueError:
            pass

    profile = cache.get('profiles', {}).get(model_id)
    if profile is None:
        raise LookupError(
            f"'{model_id}' has no entry in {QUOTA_CACHE_PATH}'s cached profiles. "
            f"Mantle and bare on-demand model IDs (e.g. a *-mantle alias's expansion, "
            f"or glm-5) have no inference profile and can NEVER appear here -- they "
            f"require an explicit --tpm (or --itpm/--otpm for --backend mantle). If "
            f"'{model_id}' does have an inference profile, run 'make refresh-quotas' "
            f"to populate/refresh the cache instead."
        )

    tpm = profile.get('tpm')
    if tpm is None:
        raise LookupError(
            f"'{model_id}' is in {QUOTA_CACHE_PATH}'s cached profiles but has "
            f"tpm: null (no matching Service Quotas value was found for it). Pass "
            f"--tpm explicitly, or run 'make refresh-quotas' once the quota is "
            f"visible in your account."
        )

    return int(tpm), 'cache'


def calculate_config(rpm, tpm: int, burndown_rate: float, burst_capacity_override: int = None,
                     adaptive_shift_max: float = 0, adaptive_queue_threshold: int = 50,
                     bytes_per_token: float = 4.0,
                     short_window_sec: int = 2, long_window_sec: int = 15,
                     burst_fraction: float = 0.0, queue_fraction: float = 0.85,
                     buffer_fraction: float = 0.15) -> dict:
    """
    Calculate configuration values from RPM and TPM.

    Capacity allocation (same split for both RPM and TPM):
    - Burst: burst_fraction  (default 0% — no immediate path; all traffic queues)
    - Queue: queue_fraction  (default 85% — queued requests, paced drain)
    - Buffer: buffer_fraction (default 15% — safety-margin holdback)

    The default 0/85/15 split is the queue-only shape: burst_capacity resolves to 0,
    which the admission gate treats as "burst disabled — route every request to the
    queue" (dynamo.put_allocation). The three fractions need not sum to 1.0;
    buffer_fraction is an independent holdback. Pass --burst-fraction>0 to re-enable
    an immediate path.

    Args:
        rpm: Requests per minute limit, or None for token-quota-only models
             (next-gen Claude on bedrock-runtime). When None, no RPM dimension is
             written and the admission gate paces purely on TPM.
        tpm: Tokens per minute limit
        burndown_rate: Output token burndown multiplier (5 for Claude 3.7+, 1 for others)
        burst_capacity_override: Optional override for burst capacity (for testing)
        adaptive_shift_max: Max fraction of burst capacity to shift to queue (0=disabled)
        adaptive_queue_threshold: Queue depth at which max shift applies
        bytes_per_token: Bytes per token ratio for token estimation (3.5 for Claude, 4.0 default)
        burst_fraction: Fraction of quota allocated to burst bucket (default 0.00)
        queue_fraction: Fraction of quota allocated to queue bucket (default 0.85)
        buffer_fraction: Fraction of quota held back as safety buffer (default 0.15)

    Returns:
        dict with all configuration values (TPM always; RPM only when rpm is set)
    """
    # Queue batch size — items released per drain tick. Each tick fires the batch at
    # Bedrock simultaneously (parallel). Owner-directed default as of 2026-08-18: 10.
    # Provenance: a 2026-07-13 run once associated batch=10 with ~9% Bedrock throttles
    # at ~57% nominal TPM (batch=3 showed 0). That observation is superseded by owner
    # decision, NOT by a new empirical test — no such test has been run.
    queue_batch_size = 10

    # TPM capacities (same percentage split) — always written
    tpm_burst_capacity = int(tpm * burst_fraction)
    tpm_queue_capacity = int(tpm * queue_fraction)
    tpm_buffer_capacity = int(tpm * buffer_fraction)

    # TPM regeneration rates (TPM tokens per second)
    tpm_burst_regen_rate = tpm / 60.0 * burst_fraction
    tpm_queue_regen_rate = tpm / 60.0 * queue_fraction

    # RPM config is optional. Token-quota-only models (rpm=None) get a TPM-paced
    # config: burst_capacity defaults to a large sentinel so the RPM admission
    # gate never binds. When rpm is set, the caller-specified fractions apply.
    if rpm is None:
        # No RPM quota. burst_capacity is the admission gate's burst switch:
        #   >0  → immediate path enabled (RPM never binds via the 1M sentinel)
        #   0   → burst disabled: every request queues (the 0/85/15 default)
        # Token-only pacing then rides the TPM queue regeneration rate.
        if burst_capacity_override is not None:
            token_burst = burst_capacity_override
        else:
            token_burst = 0 if burst_fraction == 0 else 1_000_000
        rpm_only = {
            'rpm_limit': None,
            'rpm_quota_enabled': False,
            'burst_capacity': token_burst,
            'burst_regeneration_rate': Decimal('0') if token_burst == 0 else Decimal('1000000'),
            'queue_capacity': 1_000_000,
            'queue_regeneration_rate': Decimal('1000000'),
            'buffer_capacity': 0,
        }
    else:
        burst_capacity = burst_capacity_override if burst_capacity_override is not None else int(rpm * burst_fraction)
        rpm_only = {
            'rpm_limit': rpm,
            'rpm_quota_enabled': True,
            'burst_capacity': burst_capacity,
            'burst_regeneration_rate': Decimal(str(round(rpm / 60.0 * burst_fraction, 4))),
            'queue_capacity': int(rpm * queue_fraction),
            'queue_regeneration_rate': Decimal(str(round(rpm / 60.0 * queue_fraction, 4))),
            'buffer_capacity': int(rpm * buffer_fraction),
        }

    return {
        # RPM config (optional dimension)
        **rpm_only,
        'queue_batch_size': queue_batch_size,
        # TPM config
        'tpm_limit': tpm,
        'tpm_burst_capacity': tpm_burst_capacity,
        'tpm_burst_regeneration_rate': Decimal(str(round(tpm_burst_regen_rate, 4))),
        'tpm_queue_capacity': tpm_queue_capacity,
        'tpm_queue_regeneration_rate': Decimal(str(round(tpm_queue_regen_rate, 4))),
        'tpm_buffer_capacity': tpm_buffer_capacity,
        'output_token_burndown_rate': Decimal(str(burndown_rate)),
        # Adaptive capacity (disabled by default — set adaptive_shift_max > 0 to enable)
        'adaptive_shift_max': Decimal(str(adaptive_shift_max)),
        'adaptive_queue_threshold': adaptive_queue_threshold,
        'bytes_per_token': Decimal(str(bytes_per_token)),
        # Sliding-window admission horizons (consumption-record read gate).
        #   short_window_sec — rate smoothing (2s): caps instantaneous dispatch
        #   long_window_sec  — accuracy horizon (15s): long enough that reconciled
        #                      ACTUALS dominate the window (Bedrock latency ~7.5s).
        # These replaced the counter-based gate + reconciliation Lambda.
        'short_window_sec': short_window_sec,
        'long_window_sec': long_window_sec,
    }


def configure_mantle_queue_only(config_values: dict, itpm: int, otpm: int,
                                queue_fraction: float, buffer_fraction: float) -> dict:
    """Force Mantle traffic through the paced queue and configure split quotas."""
    # Mantle is queue-only. Zero both the generic admission sentinel and every
    # token burst field so put_allocation() cannot select an immediate path.
    config_values['burst_capacity'] = 0
    config_values['burst_regeneration_rate'] = Decimal('0')
    config_values['tpm_burst_capacity'] = 0
    config_values['tpm_burst_regeneration_rate'] = Decimal('0')

    # Mantle is token-quota-only (iTPM/oTPM). Explicitly neutralize the generic RPM
    # dimension so a mantle model can NEVER carry a live RPM queue gate, regardless
    # of what `rpm` resolved to upstream. This mirrors the proven token-only shape
    # live on anthropic.claude-sonnet-5 (rpm_limit=None, queue_capacity=1_000_000).
    # Without this, an RPM-derived queue_capacity (e.g. the old 50→22 fallback) would
    # bind Gate 3's 60s request cap and crush drain throughput (B-019).
    config_values['rpm_limit'] = None
    config_values['rpm_quota_enabled'] = False
    config_values['queue_capacity'] = 1_000_000
    config_values['queue_regeneration_rate'] = Decimal('1000000')
    config_values['buffer_capacity'] = 0

    for dim, limit in (('itpm', itpm), ('otpm', otpm)):
        config_values[f'{dim}_limit'] = limit
        config_values[f'{dim}_burst_capacity'] = 0
        config_values[f'{dim}_burst_regeneration_rate'] = Decimal('0')
        config_values[f'{dim}_queue_capacity'] = int(limit * queue_fraction)
        config_values[f'{dim}_queue_regeneration_rate'] = Decimal(
            str(round(limit / 60.0 * queue_fraction, 4))
        )
        config_values[f'{dim}_buffer_capacity'] = int(limit * buffer_fraction)

    # Mantle reports actual tokens and gates oTPM directly, so burndown is disabled.
    config_values['output_token_burndown_rate'] = Decimal('1.0')
    return config_values


def create_model_config(model_id: str, config_values: dict, dry_run: bool = False):
    """
    Create or update model configuration in DynamoDB.

    Args:
        model_id: Full Bedrock model ID
        config_values: Configuration values from calculate_config()
        dry_run: When True, resolve and return the item without ever constructing
            a boto3 DynamoDB resource or calling put_item. This must skip resource
            construction itself, not just the write -- constructing the resource is
            what fails without credentials/a deployed table.
    """
    item = {
        'pk': f'MODEL#{model_id}',
        'sk': 'CONFIG',
        'entity_type': 'model_config',
        'model_id': model_id,
        **config_values
    }

    if dry_run:
        return item

    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = dynamodb.Table(SINGLE_TABLE_NAME)
    table.put_item(Item=item)
    return item


def process_model(model_arg: str, args, parser, model_id_override: str = None) -> dict:
    """
    Resolve, calculate, print, and write the DynamoDB config for one model.

    Shared by the single-model path and --starter-package batch mode so both go
    through identical TPM-sourcing/calculation/backend logic.

    model_id_override lets a caller supply the actual model_id to write (e.g. a
    specific inference profile's inferenceProfileId) so --starter-package can pass
    each profile ID through directly. TPM, burndown rate, and bytes_per_token are
    all resolved from model_id itself (resolve_tpm, derive_default_burndown,
    derive_default_bytes_per_token) -- model_short only feeds the MODEL_MAP alias
    expansion below and the printed/returned summary; it plays no role in any
    quota or capacity value.

    Returns a summary dict: {model_short, model_id, tpm, tpm_source}.
    """
    # Resolve model ID
    model_short = model_arg.lower()
    model_id = model_id_override if model_id_override is not None else MODEL_MAP.get(model_short, model_arg)

    # Determine RPM.
    #   RPM is retired as a default dimension (owner decision 2026-09-16): every
    #   model resolves to rpm=None (token-quota-only shape) unless --rpm is passed
    #   explicitly, in which case it pins a live RPM gate.
    #   --rpm 0 explicitly means "no RPM gate" (mapped to None), NOT a zero-throughput
    #   gate. Using an `is not None` test alone would let 0 flow to the else branch and
    #   compute queue_capacity=int(0*frac)=0, gating the model to ZERO drain — strictly
    #   worse than no gate at all. Any positive --rpm is honored verbatim.
    rpm = (args.rpm if args.rpm != 0 else None) if args.rpm is not None else None

    # Determine TPM: explicit --tpm > cache['profiles'][model_id]['tpm']. No third
    # (hardcoded) tier -- a model with no usable cache entry is a hard error.
    try:
        tpm, tpm_source = resolve_tpm(model_id, explicit_tpm=args.tpm)
    except LookupError as e:
        parser.error(str(e))

    # Determine burndown rate: derive from provider+backend (see derive_default_burndown).
    burndown_rate = derive_default_burndown(model_id, args.backend)

    # Determine bytes per token ratio: explicit --bytes-per-token wins; otherwise
    # derive from the model ID (see derive_default_bytes_per_token).
    bytes_per_token = (
        args.bytes_per_token
        if args.bytes_per_token is not None
        else derive_default_bytes_per_token(model_id)
    )

    # Calculate configuration
    config_values = calculate_config(rpm, tpm, burndown_rate, args.burst_capacity,
                                     adaptive_shift_max=args.adaptive_shift_max,
                                     adaptive_queue_threshold=args.adaptive_queue_threshold,
                                     bytes_per_token=bytes_per_token,
                                     short_window_sec=args.short_window_sec,
                                     long_window_sec=args.long_window_sec,
                                     burst_fraction=args.burst_fraction,
                                     queue_fraction=args.queue_fraction,
                                     buffer_fraction=args.buffer_fraction)

    # Tier 2: backend + split-quota fields. Runtime configs get backend='runtime'
    # and are byte-identical to pre-Tier-2 behavior aside from the explicit marker.
    api_style = args.api_style or ('messages' if args.backend == 'mantle' else 'converse')
    config_values['backend'] = args.backend
    config_values['api_style'] = api_style
    # Even-spacing pacer target (queue processor). Only written when provided so
    # existing configs are unaffected; queue_processor reads 0/absent as "disabled".
    if args.queue_target_tpm is not None:
        config_values['queue_target_tpm'] = args.queue_target_tpm
    if args.backend == 'mantle':
        if args.itpm is None or args.otpm is None:
            parser.error("--backend mantle requires --itpm and --otpm")
        configure_mantle_queue_only(
            config_values, args.itpm, args.otpm,
            queue_fraction=args.queue_fraction,
            buffer_fraction=args.buffer_fraction,
        )

    if args.dry_run:
        print(f"\n{'#'*60}")
        print(f"# DRY RUN — resolving only. No boto3 DynamoDB resource will be")
        print(f"# constructed and nothing will be written.")
        print(f"{'#'*60}")

    print(f"\nCreating model configuration...")
    print(f"  backend: {config_values['backend']} | api_style: {config_values['api_style']}")
    if args.backend == 'mantle':
        print(f"  itpm_limit: {config_values['itpm_limit']} (burst {config_values['itpm_burst_capacity']})")
        print(f"  otpm_limit: {config_values['otpm_limit']} (burst {config_values['otpm_burst_capacity']})")
    print(f"{'='*60}")
    print(f"Table: {SINGLE_TABLE_NAME}")
    print(f"Model ID: {model_id}")
    print(f"{'='*60}")
    print(f"\nRPM Configuration:")
    print(f"  rpm_limit: {config_values['rpm_limit']}")
    print(f"  burst_capacity: {config_values['burst_capacity']}")
    print(f"  burst_regeneration_rate: {config_values['burst_regeneration_rate']}")
    print(f"  queue_capacity: {config_values['queue_capacity']}")
    print(f"  queue_regeneration_rate: {config_values['queue_regeneration_rate']}")
    print(f"  buffer_capacity: {config_values['buffer_capacity']}")
    print(f"  queue_batch_size: {config_values['queue_batch_size']}")
    print(f"\nTPM Configuration:")
    print(f"  tpm_limit: {config_values['tpm_limit']} (source: {tpm_source})")
    print(f"  tpm_burst_capacity: {config_values['tpm_burst_capacity']}")
    print(f"  tpm_burst_regeneration_rate: {config_values['tpm_burst_regeneration_rate']}")
    print(f"  tpm_queue_capacity: {config_values['tpm_queue_capacity']}")
    print(f"  tpm_queue_regeneration_rate: {config_values['tpm_queue_regeneration_rate']}")
    print(f"  tpm_buffer_capacity: {config_values['tpm_buffer_capacity']}")
    print(f"  output_token_burndown_rate: {config_values['output_token_burndown_rate']}")
    print(f"  bytes_per_token: {config_values['bytes_per_token']}")
    print(f"\nAdmission Control (sliding-window read gate):")
    print(f"  short_window_sec: {config_values['short_window_sec']} (rate smoothing)")
    print(f"  long_window_sec: {config_values['long_window_sec']} (accuracy horizon; reconciled actuals dominate)")
    print(f"\nAdaptive Capacity:")
    print(f"  adaptive_shift_max: {config_values['adaptive_shift_max']} (0=disabled, 0.2=shift up to 20%)")
    print(f"  adaptive_queue_threshold: {config_values['adaptive_queue_threshold']}")

    # Create config (or, under --dry-run, only resolve it -- no AWS resource is
    # constructed and nothing is written).
    item = create_model_config(model_id, config_values, dry_run=args.dry_run)

    if args.dry_run:
        print(f"\nDRY RUN — config resolved successfully; nothing written")
    else:
        print(f"\nConfig created/updated successfully")
    print(f"  PK: {item['pk']}")
    print(f"  SK: {item['sk']}")

    return {
        'model_short': model_short,
        'model_id': model_id,
        'tpm': tpm,
        'tpm_source': tpm_source,
    }


def main():
    global AWS_REGION, SINGLE_TABLE_NAME

    parser = argparse.ArgumentParser(
        description='Create or update model configuration in the single table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Create nova-2-lite config with defaults
    python scripts/create_model_config.py nova-2-lite

    # Create nova-2-lite config with a low burst for testing queue behavior
    python scripts/create_model_config.py nova-2-lite --rpm 10 --burst-capacity 2

    # Create sonnet-5 config with defaults
    python scripts/create_model_config.py sonnet-5

    # Use custom RPM
    python scripts/create_model_config.py nova-2-lite --rpm 30

Model short names:
    nova-2-lite -> us.amazon.nova-2-lite-v1:0 (token-only)
    sonnet-5    -> us.anthropic.claude-sonnet-5 (token-only)
    opus-5      -> us.anthropic.claude-opus-5 (token-only)

    Or provide the full model ID directly.
        """
    )

    parser.add_argument(
        'model',
        nargs='?',
        default=None,
        help='Model short name (nova-2-lite, sonnet-5, opus-5, ...) or full model ID. '
             'Not required with --starter-package.'
    )
    parser.add_argument(
        '--starter-package',
        action='store_true',
        help='Create/overwrite configs for every model in config/starter_models.json '
             'instead of a single model. No positional model argument required.'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Resolve and print the full config exactly as the real path would, but '
             'never construct a boto3 DynamoDB resource or call put_item -- nothing '
             'is written. Skips the config.env/AWS-access check entirely, so this '
             'works with no deployed table and no SINGLE_TABLE_NAME set. Works for '
             'both the single-model and --starter-package paths. Exits non-zero if '
             'resolution itself fails.'
    )
    parser.add_argument(
        '--burst-capacity',
        type=int,
        help='Override burst capacity (for testing). Default: 50%% of RPM'
    )
    parser.add_argument(
        '--rpm',
        type=int,
        help='Pin an explicit RPM gate. --rpm 0 = no RPM gate. RPM is retired as a '
             'default dimension: every model resolves to NO RPM gate (token-quota-only) '
             'unless --rpm is passed explicitly.'
    )
    parser.add_argument(
        '--tpm',
        type=int,
        help='Override TPM limit. Default: looked up from cache[\'profiles\'][model_id] '
             '(.bedrock_quota_cache.json, populated by \'make refresh-quotas\'). REQUIRED '
             'for mantle/bare on-demand model IDs, which have no inference profile and so '
             'have no cache entry.'
    )
    parser.add_argument(
        '--adaptive-shift-max',
        type=float,
        default=0,
        help='Max fraction of burst capacity to shift to queue (0=disabled, 0.2=20%%). Default: 0'
    )
    parser.add_argument(
        '--adaptive-queue-threshold',
        type=int,
        default=50,
        help='Queue depth at which max shift applies. Default: 50'
    )
    parser.add_argument(
        '--short-window-sec',
        type=int,
        default=2,
        help='Short (rate-smoothing) admission window in seconds. Default: 2'
    )
    parser.add_argument(
        '--long-window-sec',
        type=int,
        default=15,
        help='Long (accuracy) admission window in seconds. Long enough that reconciled '
             'actuals dominate the window (Bedrock latency ~7.5s). Default: 15'
    )
    parser.add_argument(
        '--burst-fraction',
        type=float,
        default=0.0,
        help='Fraction of quota allocated to burst bucket (default: 0.00 — queue-only)'
    )
    parser.add_argument(
        '--queue-fraction',
        type=float,
        default=0.85,
        help='Fraction of quota allocated to queue bucket (default: 0.85)'
    )
    parser.add_argument(
        '--buffer-fraction',
        type=float,
        default=0.15,
        help='Fraction of quota held back as safety buffer (default: 0.15)'
    )
    parser.add_argument(
        '--bytes-per-token',
        type=float,
        default=None,
        help='Override the model input estimator bytes/token ratio.'
    )
    # Tier 2: dual-backend (runtime | mantle)
    parser.add_argument(
        '--backend',
        choices=['runtime', 'mantle'],
        default='runtime',
        help="Inference backend. 'mantle' uses the bedrock-mantle Anthropic Messages API with "
             "split iTPM/oTPM admission and requires --itpm/--otpm. Default: runtime."
    )
    parser.add_argument(
        '--api-style',
        choices=['converse', 'messages', 'responses'],
        default=None,
        help="Request API style. Default: converse for runtime, messages for mantle. "
             "'responses' = OpenAI Responses API on mantle (GPT-5.6 variants)."
    )
    parser.add_argument('--itpm', type=int, help='Input tokens/min limit (REQUIRED when --backend mantle).')
    parser.add_argument('--otpm', type=int, help='Output tokens/min limit (REQUIRED when --backend mantle).')
    parser.add_argument(
        '--queue-target-tpm',
        type=int,
        default=None,
        help='Even-spacing pacer target (tokens/min) for the queue processor. When set, '
             'each queued item is spaced item_tokens/(target/60) seconds after the prior '
             'dispatch — holds the actual Bedrock arrival rate at this target with no '
             'sub-second bursts. Omit/0 = disabled (four sliding-window gates only).'
    )

    args = parser.parse_args()

    # Deployment commands require live AWS access, while --dry-run stays fully
    # offline: no config.env read, no bedrock access check, and (in
    # create_model_config) no boto3 DynamoDB resource construction at all.
    if not args.dry_run:
        config = config_loader.get_config_with_aws_check()
        AWS_REGION = config.get('AWS_REGION', 'us-east-1')
        SINGLE_TABLE_NAME = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')

    if args.starter_package:
        if args.model is not None:
            parser.error("--starter-package takes no positional model argument")
        try:
            with open(STARTER_MODELS_PATH, encoding='utf-8') as f:
                starter_profile_ids = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            parser.error(f"could not read {STARTER_MODELS_PATH}: {e}")

        try:
            with open(QUOTA_CACHE_PATH, encoding='utf-8') as f:
                cache = json.load(f)
        except (OSError, json.JSONDecodeError):
            cache = {}
        cached_profiles = cache.get('profiles', {})

        # config/starter_models.json is now an explicit list of inference profile
        # IDs (regional + global) -- no MODEL_MAP lookup and no ACTIVE-profile
        # fan-out. With an explicit list there is nothing to fall back to: a
        # listed profile ID absent from the per-profile cache (cache['profiles'],
        # populated by get_bedrock_quotas.py), or present with tpm: null, means the
        # cache refresh silently dropped/never joined it, and that must fail loudly
        # -- and up front, before any config is written -- rather than let
        # resolve_tpm() raise mid-loop after earlier profiles already wrote.
        unresolved = [
            pid for pid in starter_profile_ids
            if pid not in cached_profiles or cached_profiles[pid].get('tpm') is None
        ]
        if unresolved:
            parser.error(
                "the following starter profile IDs have no usable tpm in "
                f"{QUOTA_CACHE_PATH}'s cached profiles (missing entry or tpm: null): "
                f"{', '.join(unresolved)}. Run 'make refresh-quotas' to refresh the "
                "cache, or remove the ID from config/starter_models.json if it is no "
                "longer ACTIVE."
            )

        print(
            f"Starter package: creating/overwriting configs for "
            f"{len(starter_profile_ids)} inference profiles..."
        )
        summaries = []
        for profile_id in starter_profile_ids:
            summaries.append(process_model(profile_id, args, parser, model_id_override=profile_id))

        print(f"\n{'='*60}")
        print(f"Starter package summary: {len(summaries)} entries")
        for s in summaries:
            print(f"  {s['model_id']}: tpm={s['tpm']} (source: {s['tpm_source']})")
        return

    if args.model is None:
        parser.error("model is required unless --starter-package is passed")

    process_model(args.model, args, parser)


if __name__ == '__main__':
    main()
