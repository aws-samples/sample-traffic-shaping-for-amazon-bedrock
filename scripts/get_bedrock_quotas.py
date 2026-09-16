#!/usr/bin/env python3
"""
Dump this account's Bedrock foundation models and service quotas to a
gitignored JSON cache (.bedrock_quota_cache.json at the repo root).

Raw dump only — no matching of quotas to specific model IDs. That mapping
is a separate, future piece of work; this script just reliably captures
the two data sources so later tooling can use real numbers instead of
hand-maintained constants.

Usage:
    python scripts/get_bedrock_quotas.py
    AWS_PROFILE=<profile> python scripts/get_bedrock_quotas.py
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.bedrock_quota_cache.json')

# Base model IDs excluded from the profile-driven matching path (see
# docs/bedrock-inference-endpoints.md). Everything else with an ACTIVE inference
# profile and a list_foundation_models record is in scope -- profile-driven matching
# is strictly safer than the fuzzy quota-name path below, so there is no allowlist,
# only this denylist. IDs are bare (CRIS-prefix-stripped), matching baseModelId as
# returned by get_inference_profiles().
PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS = (
    'meta.llama4-maverick-17b-instruct-v1:0',
    'meta.llama4-scout-17b-instruct-v1:0',
)

# Only keep quotas that shape our rate limiter: per-model TPM/RPM (runtime) and
# iTPM/oTPM (bedrock-mantle endpoint). Everything else — batch inference,
# provisioned throughput, model customization, tokens-per-day ceilings, and
# unrelated Bedrock features (Guardrails, Agents, Flows, etc.) — is noise here.
RATE_QUOTA_PATTERNS = [
    re.compile(r'model inference tokens per minute for .+'),
    re.compile(r'model inference requests per minute for .+'),
    re.compile(r'^InvokeModel requests per minute for .+'),
    re.compile(r'^\[bedrock-mantle endpoint\] Input tokens per minute for .+'),
    re.compile(r'^\[bedrock-mantle endpoint\] Output tokens per minute for .+'),
]


def is_rate_quota(quota_name):
    return any(p.search(quota_name) for p in RATE_QUOTA_PATTERNS)


# --- Grouping: associate each filtered quota back to the model it belongs to ---

RUNTIME_VARIANTS = ['cross_region', 'global_cross_region', 'on_demand', 'on_demand_latency_optimized']

# A trailing/embedded generic profile-version marker ('V1', 'v1:0') that AWS appends to some
# quota-name suffixes with no corresponding meaning in the model record. The negative lookahead
# protects a *real* decimal version ('v2.7', 'V3.2') from being mangled into a dangling
# fragment -- those digits are meaningful model-family identity, not noise to strip.
VERSION_TOKEN_RE = re.compile(r'\bv\d+(:\d+)?\b(?!\.\d)', re.IGNORECASE)


def normalize_match_key(s):
    """Collapse a provider/model-name or quota-name-suffix string into a single lowercase,
    punctuation-free key for exact-match lookups.

    Stripping *all* non-alphanumerics (not just whitespace) is what lets 'DeepSeek-R1' and
    'DeepSeek R1' collapse to the same key, likewise 'TwelveLabs' / 'Twelve Labs' and
    '(25.02)' / '25.02' -- these are spacing/punctuation quirks, not different models.
    """
    s = s.lower()
    s = VERSION_TOKEN_RE.sub('', s)
    s = re.sub(r'[^a-z0-9]+', '', s)
    return s


def match_key_tokens(s):
    """Tokenize the same normalized string as normalize_match_key(), but as separate words
    instead of one concatenated key. Used only by the suffix-match fallback below, which needs
    real word boundaries to compare 'trailing words' safely -- concatenating everything into one
    string (as normalize_match_key does) would make that comparison a raw substring check and
    risk exactly the kind of accidental partial-word collision the fuzzy matcher must avoid.
    """
    s = s.lower()
    s = VERSION_TOKEN_RE.sub('', s)
    return tuple(re.findall(r'[a-z0-9]+', s))


def _is_token_suffix(haystack_tokens, needle_tokens):
    """True if needle_tokens are exactly the trailing words of haystack_tokens.

    Requires at least two words in needle_tokens -- a single generic word (e.g. a bare
    'large' or 'v1') is not distinctive enough to safely anchor a suffix match, and matching
    on it would reintroduce the cross-pool contamination risk this fallback exists to avoid.
    """
    return (
        len(needle_tokens) >= 2
        and len(haystack_tokens) >= len(needle_tokens)
        and haystack_tokens[-len(needle_tokens):] == needle_tokens
    )


def split_suffix(quota_name):
    idx = quota_name.lower().rfind(' for ')
    if idx == -1:
        return None
    return quota_name[idx + len(' for '):]


def classify_quota(quota_name):
    """Return ('mantle', 'itpm'|'otpm') or ('runtime', 'tpm'|'rpm', variant), or None."""
    lname = quota_name.lower()
    if 'bedrock-mantle' in lname:
        if 'input tokens' in lname:
            return ('mantle', 'itpm')
        if 'output tokens' in lname:
            return ('mantle', 'otpm')
        return None

    if 'tokens per minute' in lname:
        metric = 'tpm'
    elif 'requests per minute' in lname:
        metric = 'rpm'
    else:
        return None

    if 'global cross-region' in lname or 'global cross region' in lname:
        variant = 'global_cross_region'
    elif 'cross-region' in lname or 'cross region' in lname:
        variant = 'cross_region'
    elif 'latency' in lname and ('on-demand' in lname or 'on demand' in lname):
        variant = 'on_demand_latency_optimized'
    elif 'on-demand' in lname or 'on demand' in lname:
        variant = 'on_demand'
    elif lname.startswith('invokemodel'):
        # Generic AWS-documented RPM name with no explicit variant; treated as on-demand.
        variant = 'on_demand'
    else:
        return None

    return ('runtime', metric, variant)


def build_model_index(models):
    by_full_name = {}
    by_bare_name = {}
    # (bare-name tokens, model_id) pairs, for match_quota_model()'s suffix fallback below.
    # Only models whose bare modelName has 2+ words are eligible -- see _is_token_suffix().
    bare_token_candidates = []
    for m in models:
        model_id = m.get('modelId')
        full_key = normalize_match_key(f"{m.get('providerName') or ''} {m.get('modelName') or ''}")
        bare_key = normalize_match_key(m.get('modelName') or '')
        by_full_name.setdefault(full_key, model_id)
        by_bare_name.setdefault(bare_key, model_id)
        bare_tokens = match_key_tokens(m.get('modelName') or '')
        if len(bare_tokens) >= 2:
            bare_token_candidates.append((bare_tokens, model_id))
    return by_full_name, by_bare_name, bare_token_candidates


def match_quota_model(quota_name, by_full_name, by_bare_name, bare_token_candidates):
    suffix = split_suffix(quota_name)
    if suffix is None:
        return None
    key = normalize_match_key(suffix)
    exact = by_full_name.get(key) or by_bare_name.get(key)
    if exact is not None:
        return exact

    # Fallback: the quota suffix carries a provider-name variant the model record spells
    # differently (e.g. quota 'Writer AI Palmyra X4 V1' vs providerName 'Writer', or
    # 'Mistral Pixtral Large 25.02 V1' vs providerName 'Mistral AI') -- neither the full nor
    # bare exact key can account for that, so fall back to: does the quota suffix *end with*
    # some model's bare name? Only trust this if exactly one model qualifies -- an ambiguous
    # suffix match (e.g. a quota name that omits the distinguishing suffix of two sibling
    # model versions) is treated as no match, per this module's strictness guardrail.
    quota_tokens = match_key_tokens(suffix)
    matches = {
        model_id for bare_tokens, model_id in bare_token_candidates
        if _is_token_suffix(quota_tokens, bare_tokens)
    }
    if len(matches) == 1:
        return next(iter(matches))
    return None


def empty_runtime():
    return {
        'tpm': {variant: None for variant in RUNTIME_VARIANTS},
        'rpm': {variant: None for variant in RUNTIME_VARIANTS},
    }


def group_quotas(quotas, models):
    by_full_name, by_bare_name, bare_token_candidates = build_model_index(models)
    models_by_id = {m['modelId']: m for m in models}
    grouped = {}
    unmatched_quotas = []

    for q in quotas:
        quota_name = q.get('QuotaName', '')
        classification = classify_quota(quota_name)
        if classification is None:
            unmatched_quotas.append(q)
            continue

        model_id = match_quota_model(quota_name, by_full_name, by_bare_name, bare_token_candidates)
        if model_id is None:
            unmatched_quotas.append(q)
            continue

        entry = grouped.setdefault(model_id, {
            'providerName': models_by_id[model_id].get('providerName'),
            'modelName': models_by_id[model_id].get('modelName'),
            'runtime': empty_runtime(),
            'mantle': {'itpm': None, 'otpm': None},
        })

        if classification[0] == 'runtime':
            _, metric, variant = classification
            entry['runtime'][metric][variant] = q.get('Value')
        else:
            _, mantle_field = classification
            entry['mantle'][mantle_field] = q.get('Value')

    return grouped, unmatched_quotas


def profile_variant(inference_profile_id):
    """Derive regional-vs-global strictly from the profile ID's own prefix.

    The single source of truth for this derivation -- used both by
    match_profile_driven_quotas() and by build_profiles_cache() -- so there is
    never a second, possibly-drifting way to compute it.
    """
    return 'global_cross_region' if (inference_profile_id or '').startswith('global.') else 'cross_region'


def match_profile_driven_quotas(quotas, models, inference_profiles):
    """Match every ACTIVE, non-denylisted inference profile to its own quota family.

    Unlike match_quota_model()'s fuzzy quota-name matching, this determines
    regional-vs-global strictly from each profile's own inferenceProfileId prefix
    ('global.' -> global_cross_region, anything else -> cross_region) and never
    checks the other family for a given profile -- the fix for the cross-pool risk
    fuzzy matching alone couldn't distinguish. Applies to every base model with an
    ACTIVE inference profile and a list_foundation_models record, except
    PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS; profiles for denylisted base models are
    left untouched here, so their quotas still flow only through the existing
    fuzzy path.

    Returns {base_model_id: {'tpm': {variant: value}, 'rpm': {variant: value}}},
    where variant is always 'cross_region' or 'global_cross_region'.
    """
    models_by_id = {m['modelId']: m for m in models}

    # Quota values usable by this path: runtime cross_region/global_cross_region only,
    # indexed by (variant, metric, normalized "for X" suffix key) for the exact-match fast
    # path, and by (variant, metric) -> [(suffix tokens, value), ...] for the suffix-match
    # fallback below (see match_quota_model()'s docstring for why the fallback exists).
    quota_index = {}
    quota_token_index = {}
    for q in quotas:
        classification = classify_quota(q.get('QuotaName', ''))
        if classification is None or classification[0] != 'runtime':
            continue
        _, metric, variant = classification
        if variant not in ('cross_region', 'global_cross_region'):
            continue
        suffix = split_suffix(q.get('QuotaName', ''))
        if suffix is None:
            continue
        key = normalize_match_key(suffix)
        quota_index.setdefault((variant, metric), {})[key] = q.get('Value')
        quota_token_index.setdefault((variant, metric), []).append(
            (match_key_tokens(suffix), q.get('Value')),
        )

    results = {}
    for p in inference_profiles:
        if p.get('status') != 'ACTIVE':
            continue
        base_model_id = p.get('baseModelId')
        if base_model_id is None or base_model_id in PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS:
            continue
        model = models_by_id.get(base_model_id)
        if model is None:
            continue

        variant = profile_variant(p.get('inferenceProfileId'))
        full_key = normalize_match_key(f"{model.get('providerName') or ''} {model.get('modelName') or ''}")
        bare_key = normalize_match_key(model.get('modelName') or '')
        bare_tokens = match_key_tokens(model.get('modelName') or '')

        entry = results.setdefault(base_model_id, {'tpm': {}, 'rpm': {}})
        for metric in ('tpm', 'rpm'):
            variant_map = quota_index.get((variant, metric), {})
            value = variant_map.get(full_key, variant_map.get(bare_key))
            if value is None:
                # Same provider-name-variant fallback as match_quota_model(): does exactly one
                # quota in this model's own variant/metric family end with this model's bare
                # name? Ambiguous (0 or 2+ candidates) is treated as no match, never a guess.
                candidates = {
                    v for toks, v in quota_token_index.get((variant, metric), [])
                    if _is_token_suffix(toks, bare_tokens)
                }
                if len(candidates) == 1:
                    value = next(iter(candidates))
            if value is not None:
                entry[metric][variant] = value

    return results


def build_profiles_cache(models, inference_profiles, profile_matches):
    """Build the top-level 'profiles' cache: one entry per ACTIVE inference profile,
    keyed by inferenceProfileId, already carrying the resolved base model ID and
    this profile's own TPM -- so a config consumer can do a dictionary lookup
    instead of re-deriving the prefix-to-variant mapping resolve_tpm() does today.

    'tpm' comes solely from profile_matches[baseModelId]['tpm'][variant], the
    same value match_profile_driven_quotas() computed for this exact profile's
    own pool -- never the other variant's. A profile with no entry in
    profile_matches (e.g. its base model is in PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS)
    or no matched quota for its own variant gets tpm: None, so the gap stays
    visible in the cache rather than silently inheriting the wrong pool or being
    omitted.
    """
    models_by_id = {m['modelId']: m for m in models}
    profiles_cache = {}
    for p in inference_profiles:
        if p.get('status') != 'ACTIVE':
            continue
        profile_id = p.get('inferenceProfileId')
        if profile_id is None:
            continue

        base_model_id = p.get('baseModelId')
        variant = profile_variant(profile_id)
        model = models_by_id.get(base_model_id, {})
        tpm = profile_matches.get(base_model_id, {}).get('tpm', {}).get(variant)

        profiles_cache[profile_id] = {
            'inferenceProfileId': profile_id,
            'baseModelId': base_model_id,
            'variant': variant,
            'providerName': model.get('providerName'),
            'modelName': model.get('modelName'),
            'tpm': tpm,
        }

    return profiles_cache


def get_models(bedrock):
    response = bedrock.list_foundation_models()
    return [
        {
            'modelId': m['modelId'],
            'providerName': m.get('providerName'),
            'modelName': m.get('modelName'),
        }
        for m in response.get('modelSummaries', [])
    ]


def base_model_id_from_arn(model_arn):
    idx = model_arn.rfind('foundation-model/')
    if idx == -1:
        return None
    return model_arn[idx + len('foundation-model/'):]


def get_inference_profiles(bedrock):
    """Ground-truth inference profile list, keyed by inferenceProfileId (not base model).

    A global.X and a us.X profile sharing a base model are two distinct entries here —
    never merged. baseModelId dedup only applies within one profile's own multi-region
    models[] list (see docs/bedrock-inference-endpoints.md).
    """
    profiles = []
    next_token = None
    while True:
        kwargs = {}
        if next_token:
            kwargs['nextToken'] = next_token
        response = bedrock.list_inference_profiles(**kwargs)
        for p in response.get('inferenceProfileSummaries', []):
            base_model_ids = []
            for model in p.get('models', []):
                base_model_id = base_model_id_from_arn(model.get('modelArn', ''))
                if base_model_id and base_model_id not in base_model_ids:
                    base_model_ids.append(base_model_id)
            profiles.append({
                'inferenceProfileId': p.get('inferenceProfileId'),
                'inferenceProfileName': p.get('inferenceProfileName'),
                'status': p.get('status'),
                'baseModelId': base_model_ids[0] if base_model_ids else None,
            })
        next_token = response.get('nextToken')
        if not next_token:
            break
    return profiles


def get_quotas(service_quotas):
    quotas = []
    next_token = None
    while True:
        kwargs = {'ServiceCode': 'bedrock'}
        if next_token:
            kwargs['NextToken'] = next_token
        response = service_quotas.list_service_quotas(**kwargs)
        for q in response.get('Quotas', []):
            if not is_rate_quota(q.get('QuotaName', '')):
                continue
            quotas.append({
                'QuotaCode': q.get('QuotaCode'),
                'QuotaName': q.get('QuotaName'),
                'Value': q.get('Value'),
                'Unit': q.get('Unit'),
                'Adjustable': q.get('Adjustable'),
            })
        next_token = response.get('NextToken')
        if not next_token:
            break
    return quotas


def main():
    session = boto3.Session()
    region = session.region_name

    bedrock = session.client('bedrock', region_name=region)
    service_quotas = session.client('service-quotas', region_name=region)

    output = {
        'lastRefreshedAt': datetime.now(timezone.utc).isoformat(),
        'models': get_models(bedrock),
    }

    try:
        output['quotas'] = get_quotas(service_quotas)
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') != 'AccessDeniedException':
            raise
        message = 'AccessDenied: missing IAM action servicequotas:ListServiceQuotas'
        output['quotas'] = []
        output['quota_access_error'] = message
        print(message, file=sys.stderr)

    output['grouped'], output['unmatched_quotas'] = group_quotas(output['quotas'], output['models'])

    try:
        output['inference_profiles'] = get_inference_profiles(bedrock)
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') != 'AccessDeniedException':
            raise
        message = 'AccessDenied: missing IAM action bedrock:ListInferenceProfiles'
        output['inference_profiles'] = []
        output['inference_profile_access_error'] = message
        print(message, file=sys.stderr)

    # Profile-driven overlay: for every non-denylisted base model with an ACTIVE
    # inference profile, this decisively supersedes whatever the fuzzy path above
    # wrote into runtime.tpm/rpm.{cross_region|global_cross_region} -- it only ever
    # touches those two variant keys, never mantle/on_demand/on_demand_latency_optimized,
    # and never a denylisted base model (see PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS).
    models_by_id = {m['modelId']: m for m in output['models']}
    profile_matches = match_profile_driven_quotas(
        output['quotas'], output['models'], output['inference_profiles'],
    )
    for base_model_id, runtime_values in profile_matches.items():
        entry = output['grouped'].setdefault(base_model_id, {
            'providerName': models_by_id[base_model_id].get('providerName'),
            'modelName': models_by_id[base_model_id].get('modelName'),
            'runtime': empty_runtime(),
            'mantle': {'itpm': None, 'otpm': None},
        })
        for metric in ('tpm', 'rpm'):
            for variant, value in runtime_values[metric].items():
                entry['runtime'][metric][variant] = value

    # Per-profile cache: one entry per ACTIVE inference profile, additive to
    # everything above -- 'grouped'/'models'/'quotas'/'inference_profiles' are
    # unchanged in shape. See build_profiles_cache() for the tpm sourcing rule.
    output['profiles'] = build_profiles_cache(
        output['models'], output['inference_profiles'], profile_matches,
    )

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
