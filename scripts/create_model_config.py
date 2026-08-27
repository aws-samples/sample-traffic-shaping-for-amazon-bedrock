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
import argparse
import boto3
from decimal import Decimal

# Add scripts directory for config_loader
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config_loader

# Defaults keep calculate_config() importable for offline tests. main() replaces
# these after loading config.env and verifying AWS access.
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
SINGLE_TABLE_NAME = os.environ.get('SINGLE_TABLE_NAME', 'semaphore-single-table')

# Model ID mappings
MODEL_MAP = {
    # Next-gen Claude — runtime CRIS forms (no -v1 suffix on 4.7+).
    'opus-47': 'us.anthropic.claude-opus-4-7',
    # Mantle bare form — use with --backend mantle --itpm 10000000 --otpm 2000000
    'opus-47-mantle': 'anthropic.claude-opus-4-7',
    'opus-48': 'us.anthropic.claude-opus-4-8',
    # Global CRIS form of Opus 4.7 — token-only, same as opus-47.
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
    'opus-4-7': 'us.anthropic.claude-opus-4-7',     # runtime Converse (VERIFIED quotas)
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

# Default RPM limits per model
# Next-gen Claude (Opus 4.7/4.8, Sonnet 4.6) are token-quota-only on
# bedrock-runtime — no RPM quota. None ⇒ create-config writes a TPM-only config.
DEFAULT_RPM = {
    'opus-47': None,
    'opus-47-mantle': None,   # token-only; iTPM/oTPM via --itpm/--otpm
    'opus-48': None,
    'global-opus-47': None,
    'opus-5': None,
    'sonnet-46': None,
    'sonnet-5': None,
    'sonnet-5-mantle': None,
    'nova-lite': 2000,
    'nova-lite-sr': 2000,
    'nova-micro': 2000,
    'nova-2-lite': 2000,
    'nova-pro': 500,
    'haiku-4-5': None,
    # New models (added 2026-08-21) — all token-quota-only (no RPM on their cards).
    'grok-4-6': None,
    'grok-4-3': None,
    'gpt-5.6-cyber': None,
    'gpt-5.6-daybreak-blue-sol': None,
    'gpt-5.5': None,
    'gpt-5.4': None,
    'mythos-5': None,
    'fable-5': None,
    'opus-4-8': None,
    'opus-4-7': None,
    'gemma-4-31b': None,
    'gemma-4-26b-a4b': None,
    'gemma-4-e2b': None,
    'nemotron-3-super-120b': None,
    'minimax-m2-5': None,
    # Added 2026-08-24 — token-quota-only shape (no RPM gate).
    'llama4-maverick': None,
    'llama4-scout': None,
    'gpt-5.6-luna': None,
    'gpt-5.6-sol': None,
    'gpt-5.6-terra': None,
    'glm-5': None,
}

# Default TPM limits per model (account-specific — check Service Quotas for your account)
# These are conservative defaults; request increases via Service Quotas console
DEFAULT_TPM = {
    'opus-47': 15000000,   # 15M consolidated TPM (runtime), per gap analysis §2
    'opus-48': 30000000,   # 30M consolidated TPM (runtime), per coauthor blog edit
    'opus-5': 30000000,
    'sonnet-46': 6000000,  # 6M TPM — confirmed against Service Quotas 2026-07-06 (was 1M placeholder)
    'sonnet-5': 6000000,
    'sonnet-5-mantle': 3000000,
    'nova-lite': 8000000,
    'nova-lite-sr': 4000000,  # single-region quota (check Service Quotas for your account)
    'nova-micro': 4000000,  # PLACEHOLDER — refresh from account Service Quotas
    'nova-2-lite': 8000000,
    'nova-pro': 2000000,
    'haiku-4-5': 2000000,  # PLACEHOLDER — refresh from account Service Quotas
    # New models (added 2026-08-21). Opus 4.8 / 4.7 are VERIFIED (30M consolidated
    # runtime TPM, from the model cards). Every other new model has NO numeric
    # quota on its card — the values below are PLACEHOLDERS, not real quotas.
    'opus-4-8': 30000000,   # 30M consolidated TPM (runtime) — VERIFIED from model card
    'opus-4-7': 30000000,   # 30M consolidated TPM (runtime) — VERIFIED from model card
    'grok-4-6': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'grok-4-3': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'gpt-5.6-cyber': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'gpt-5.6-daybreak-blue-sol': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'gpt-5.5': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'gpt-5.4': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'mythos-5': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'fable-5': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'gemma-4-31b': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'gemma-4-26b-a4b': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'gemma-4-e2b': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'nemotron-3-super-120b': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    'minimax-m2-5': 1000000,  # PLACEHOLDER — refresh from account Service Quotas
    # Added 2026-08-24 — PLACEHOLDER quotas; refresh from account Service Quotas.
    'llama4-maverick': 1000000,
    'llama4-scout': 1000000,
    'gpt-5.6-luna': 1000000,
    'gpt-5.6-sol': 1000000,
    'gpt-5.6-terra': 1000000,
    'glm-5': 1000000,
}

# Output token burndown rate per model family
# Claude 3.7+: 5x (1 output token = 5 TPM tokens)
# All other models: 1x
OUTPUT_BURNDOWN_RATE = {
    'opus-47': 5.0,    # runtime path; mantle config overrides to 1.0
    'opus-48': 5.0,    # runtime path; mantle config overrides to 1.0
    'opus-5': 10.0,
    'sonnet-46': 5.0,  # Claude 4.x family
    'sonnet-5': 10.0,
    'sonnet-5-mantle': 1.0,
    'nova-lite': 1.0,  # Standard 1:1 burndown
    'nova-lite-sr': 1.0,
    'nova-micro': 1.0,
    'nova-2-lite': 1.0,
    'nova-pro': 1.0,   # Standard 1:1 burndown
    'haiku-4-5': 5.0,  # Claude 4.x family
    # New models (added 2026-08-21).
    'opus-4-8': 10.0,  # runtime path; mantle config overrides to 1.0 (matches opus-5)
    'opus-4-7': 10.0,  # runtime path; mantle config overrides to 1.0 (matches opus-5)
    'fable-5': 10.0,   # next-gen Claude runtime family (matches sonnet-5/opus-5); mantle overrides to 1.0
    'mythos-5': 1.0,   # mantle-only (messages) — mantle enforces actual oTPM, burndown disabled
    'grok-4-6': 1.0,   # non-Claude — standard 1:1 burndown
    'grok-4-3': 1.0,
    'gpt-5.6-cyber': 1.0,
    'gpt-5.6-daybreak-blue-sol': 1.0,
    'gpt-5.5': 1.0,
    'gpt-5.4': 1.0,
    'gemma-4-31b': 1.0,
    'gemma-4-26b-a4b': 1.0,
    'gemma-4-e2b': 1.0,
    'nemotron-3-super-120b': 1.0,
    'minimax-m2-5': 1.0,
    # Added 2026-08-24 — non-Claude, standard 1:1 burndown.
    'llama4-maverick': 1.0,
    'llama4-scout': 1.0,
    'gpt-5.6-luna': 1.0,
    'gpt-5.6-sol': 1.0,
    'gpt-5.6-terra': 1.0,
    'glm-5': 1.0,
}

# Bytes per token ratio per model family
# Claude: ~3.5 bytes/token, others: ~4.0 bytes/token
BYTES_PER_TOKEN = {
    'opus-47': 3.5,
    'opus-48': 3.5,
    'opus-5': 3.5,
    'sonnet-46': 3.5,
    'sonnet-5': 3.5,
    'sonnet-5-mantle': 3.5,
    'haiku-4-5': 3.5,  # Claude family
    # Nova tokenizer runs ~3.0 bytes/token (more tokens per byte than the 4.0 default).
    # A 4.0 estimate under-counts input ~18%, so admission over-hands Bedrock and a 3x
    # finite burst leaked ~15% TPM throttles (2026-07-10 validation). 3.0 + the 1.1
    # safety margin over-counts slightly — the safe direction for a rate limiter.
    'nova-lite': 3.0,
    'nova-lite-sr': 3.0,
    'nova-micro': 3.0,
    'nova-2-lite': 3.0,
    'nova-pro': 3.0,
    # New models (added 2026-08-21) — mirrors estimation.bytes_per_input_token in
    # config/models.yml (Claude family 3.5, all others 4.0).
    'opus-4-8': 3.5,
    'opus-4-7': 3.5,
    'fable-5': 3.5,
    'mythos-5': 3.5,
    'grok-4-6': 4.0,
    'grok-4-3': 4.0,
    'gpt-5.6-cyber': 4.0,
    'gpt-5.6-daybreak-blue-sol': 4.0,
    'gpt-5.5': 4.0,
    'gpt-5.4': 4.0,
    'gemma-4-31b': 4.0,
    'gemma-4-26b-a4b': 4.0,
    'gemma-4-e2b': 4.0,
    'nemotron-3-super-120b': 4.0,
    'minimax-m2-5': 4.0,
    # Added 2026-08-24 — non-Claude, ~4.0 bytes/token.
    'llama4-maverick': 4.0,
    'llama4-scout': 4.0,
    'gpt-5.6-luna': 4.0,
    'gpt-5.6-sol': 4.0,
    'gpt-5.6-terra': 4.0,
    'glm-5': 4.0,
}


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


def create_model_config(model_id: str, config_values: dict):
    """
    Create or update model configuration in DynamoDB.

    Args:
        model_id: Full Bedrock model ID
        config_values: Configuration values from calculate_config()
    """
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = dynamodb.Table(SINGLE_TABLE_NAME)

    item = {
        'pk': f'MODEL#{model_id}',
        'sk': 'CONFIG',
        'entity_type': 'model_config',
        'model_id': model_id,
        **config_values
    }

    table.put_item(Item=item)
    return item


def main():
    global AWS_REGION, SINGLE_TABLE_NAME

    # Deployment commands require live AWS access, while calculation helpers remain
    # importable for offline tests.
    config = config_loader.get_config_with_aws_check()
    AWS_REGION = config.get('AWS_REGION', 'us-east-1')
    SINGLE_TABLE_NAME = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')

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
    nova-2-lite -> us.amazon.nova-2-lite-v1:0 (default RPM: 2000)
    sonnet-5    -> us.anthropic.claude-sonnet-5 (token-only)
    opus-5      -> us.anthropic.claude-opus-5 (token-only)

    Or provide the full model ID directly.
        """
    )

    parser.add_argument(
        'model',
        help='Model short name (nova-2-lite, sonnet-5, opus-5, ...) or full model ID'
    )
    parser.add_argument(
        '--burst-capacity',
        type=int,
        help='Override burst capacity (for testing). Default: 50%% of RPM'
    )
    parser.add_argument(
        '--rpm',
        type=int,
        help='Override RPM limit (opus=50, jamba=100, ...). --rpm 0 = no RPM gate. '
             'Models absent from DEFAULT_RPM default to NO RPM gate (token-quota-only).'
    )
    parser.add_argument(
        '--tpm',
        type=int,
        help='Override TPM limit. Default: model-specific (opus=40000, jamba=100000)'
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

    # Resolve model ID
    model_short = args.model.lower()
    model_id = MODEL_MAP.get(model_short, args.model)

    # Determine RPM.
    #   Precedence: explicit --rpm wins; otherwise the per-model DEFAULT_RPM entry.
    #   An unknown model (absent from DEFAULT_RPM) now resolves to None => NO RPM gate
    #   (token-quota-only shape), NOT the old silent 50 fallback. The 50 fallback
    #   derived queue_capacity=int(50*queue_fraction)=22 and queue_regen=0.375/s, which
    #   Gate 3 reads as a ~0.375 rps 60s request cap — this crippled GPT-5.6 Luna/Sol
    #   by 63x regardless of token headroom (B-019).
    #   --rpm 0 explicitly means "no RPM gate" (mapped to None), NOT a zero-throughput
    #   gate. Using an `is not None` test alone would let 0 flow to the else branch and
    #   compute queue_capacity=int(0*frac)=0, gating the model to ZERO drain — strictly
    #   worse than the bug. Any positive --rpm is honored verbatim.
    if args.rpm is not None:
        rpm = args.rpm if args.rpm != 0 else None
    else:
        rpm = DEFAULT_RPM.get(model_short, None)  # unknown model => no RPM gate

    # Determine TPM
    if args.tpm:
        tpm = args.tpm
    else:
        tpm = DEFAULT_TPM.get(model_short, 40000)  # Default to 40K if unknown model

    # Warn (never error) when the model is unknown to the lookup tables so the operator
    # knows invented defaults were applied rather than model-specific values. TPM keeps
    # its historical 40000 fallback (behavior unchanged) — the warning just surfaces it.
    if args.tpm is None and model_short not in DEFAULT_TPM:
        print(
            f"WARNING: model '{model_short}' is absent from DEFAULT_TPM; using invented "
            f"default tpm={tpm}. Pass --tpm N to set the real quota.",
            file=sys.stderr,
        )
    if args.rpm is None and model_short not in DEFAULT_RPM:
        print(
            f"WARNING: model '{model_short}' is absent from DEFAULT_RPM; applying NO RPM "
            f"gate (rpm=None, token-quota-only shape). Pass --rpm N for an explicit RPM quota.",
            file=sys.stderr,
        )

    # Determine burndown rate
    burndown_rate = OUTPUT_BURNDOWN_RATE.get(model_short, 1.0)

    # Determine bytes per token ratio
    bytes_per_token = (
        args.bytes_per_token
        if args.bytes_per_token is not None
        else BYTES_PER_TOKEN.get(model_short, 4.0)
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
    print(f"  tpm_limit: {config_values['tpm_limit']}")
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

    # Create config
    item = create_model_config(model_id, config_values)

    print(f"\nConfig created/updated successfully")
    print(f"  PK: {item['pk']}")
    print(f"  SK: {item['sk']}")


if __name__ == '__main__':
    main()
