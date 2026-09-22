#!/usr/bin/env python3
"""
Dump this account's Bedrock foundation models, inference profiles, and service
quotas to a gitignored JSON cache (.bedrock_quota_cache.json at the repo root),
along with a resolved per-inference-profile TPM lookup.

Cache keys written:
    region              the region this cache was built against -- config.env's
                        AWS_REGION (the deployed stack's own region) when present,
                        else the session/profile default, else 'us-east-1'
    models              raw list_foundation_models records (id/provider/name)
    inference_profiles  raw list_inference_profiles records, one per profile ID
    quotas              raw list_service_quotas records, filtered to the rate
                        quotas that shape this rate limiter (RATE_QUOTA_PATTERNS)
    profiles            THE CONSUMED KEY — one entry per ACTIVE inference profile,
                        keyed by inferenceProfileId, carrying that profile's own
                        resolved TPM. This is what create_model_config.py reads.
    lastRefreshedAt     ISO-8601 UTC timestamp, used for the staleness warning

The point of 'profiles' is that regional vs global is derived from each profile's
own ID prefix, so a us.X and a global.X profile sharing a base model can never
borrow each other's quota pool. See match_profile_driven_quotas().

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

OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", ".bedrock_quota_cache.json"
)

# Base model IDs excluded from quota matching (see docs/bedrock-inference-endpoints.md).
# Everything else with an ACTIVE inference profile and a list_foundation_models record
# is in scope, so there is no allowlist, only this denylist. A denylisted profile still
# appears in the 'profiles' cache, with tpm: None -- the gap stays visible rather than
# being filled from the wrong pool. IDs are bare (CRIS-prefix-stripped), matching
# baseModelId as returned by get_inference_profiles().
PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS = (
    "meta.llama4-maverick-17b-instruct-v1:0",
    "meta.llama4-scout-17b-instruct-v1:0",
)

# Only keep quotas that shape our rate limiter: per-model TPM/RPM (runtime) and
# iTPM/oTPM (bedrock-mantle endpoint). Everything else — batch inference,
# provisioned throughput, model customization, tokens-per-day ceilings, and
# unrelated Bedrock features (Guardrails, Agents, Flows, etc.) — is noise here.
RATE_QUOTA_PATTERNS = [
    re.compile(r"model inference tokens per minute for .+"),
    re.compile(r"model inference requests per minute for .+"),
    re.compile(r"^InvokeModel requests per minute for .+"),
    re.compile(r"^\[bedrock-mantle endpoint\] Input tokens per minute for .+"),
    re.compile(r"^\[bedrock-mantle endpoint\] Output tokens per minute for .+"),
]


def is_rate_quota(quota_name):
    return any(p.search(quota_name) for p in RATE_QUOTA_PATTERNS)


# --- Matching: associate each filtered quota back to the model it belongs to ---

# A trailing/embedded generic profile-version marker ('V1', 'v1:0') that AWS appends to some
# quota-name suffixes with no corresponding meaning in the model record. The negative lookahead
# protects a *real* decimal version ('v2.7', 'V3.2') from being mangled into a dangling
# fragment -- those digits are meaningful model-family identity, not noise to strip.
VERSION_TOKEN_RE = re.compile(r"\bv\d+(:\d+)?\b(?!\.\d)", re.IGNORECASE)


def normalize_match_key(s):
    """Collapse a provider/model-name or quota-name-suffix string into a single lowercase,
    punctuation-free key for exact-match lookups.

    Stripping *all* non-alphanumerics (not just whitespace) is what lets 'DeepSeek-R1' and
    'DeepSeek R1' collapse to the same key, likewise 'TwelveLabs' / 'Twelve Labs' and
    '(25.02)' / '25.02' -- these are spacing/punctuation quirks, not different models.
    """
    s = s.lower()
    s = VERSION_TOKEN_RE.sub("", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def match_key_tokens(s):
    """Tokenize the same normalized string as normalize_match_key(), but as separate words
    instead of one concatenated key. Used only by the suffix-match fallback in
    match_profile_driven_quotas(), which needs real word boundaries to compare 'trailing
    words' safely -- concatenating everything into one string (as normalize_match_key does)
    would make that comparison a raw substring check and risk exactly the kind of accidental
    partial-word collision the fallback must avoid.
    """
    s = s.lower()
    s = VERSION_TOKEN_RE.sub("", s)
    return tuple(re.findall(r"[a-z0-9]+", s))


def _is_token_suffix(haystack_tokens, needle_tokens):
    """True if needle_tokens are exactly the trailing words of haystack_tokens.

    Requires at least two words in needle_tokens -- a single generic word (e.g. a bare
    'large' or 'v1') is not distinctive enough to safely anchor a suffix match, and matching
    on it would reintroduce the cross-pool contamination risk this fallback exists to avoid.
    """
    return (
        len(needle_tokens) >= 2
        and len(haystack_tokens) >= len(needle_tokens)
        and haystack_tokens[-len(needle_tokens) :] == needle_tokens
    )


def split_suffix(quota_name):
    idx = quota_name.lower().rfind(" for ")
    if idx == -1:
        return None
    return quota_name[idx + len(" for ") :]


def classify_quota(quota_name):
    """Return ('mantle', 'itpm'|'otpm') or ('runtime', 'tpm'|'rpm', variant), or None."""
    lname = quota_name.lower()
    if "bedrock-mantle" in lname:
        if "input tokens" in lname:
            return ("mantle", "itpm")
        if "output tokens" in lname:
            return ("mantle", "otpm")
        return None

    if "tokens per minute" in lname:
        metric = "tpm"
    elif "requests per minute" in lname:
        metric = "rpm"
    else:
        return None

    if "global cross-region" in lname or "global cross region" in lname:
        variant = "global_cross_region"
    elif "cross-region" in lname or "cross region" in lname:
        variant = "cross_region"
    elif "latency" in lname and ("on-demand" in lname or "on demand" in lname):
        variant = "on_demand_latency_optimized"
    elif "on-demand" in lname or "on demand" in lname:
        variant = "on_demand"
    elif lname.startswith("invokemodel"):
        # Generic AWS-documented RPM name with no explicit variant; treated as on-demand.
        variant = "on_demand"
    else:
        return None

    return ("runtime", metric, variant)


def profile_variant(inference_profile_id):
    """Derive regional-vs-global strictly from the profile ID's own prefix.

    The single source of truth for this derivation -- used both by
    match_profile_driven_quotas() and by build_profiles_cache() -- so there is
    never a second, possibly-drifting way to compute it.
    """
    return (
        "global_cross_region"
        if (inference_profile_id or "").startswith("global.")
        else "cross_region"
    )


def match_profile_driven_quotas(quotas, models, inference_profiles):
    """Match every ACTIVE, non-denylisted inference profile to its own quota family.

    Regional-vs-global is determined strictly from each profile's own
    inferenceProfileId prefix ('global.' -> global_cross_region, anything else ->
    cross_region), and a given profile is never checked against the other family --
    that separation is the whole point of matching per profile rather than per base
    model, since a us.X and a global.X profile share a base model but draw on two
    different account quotas. Applies to every base model with an ACTIVE inference
    profile and a list_foundation_models record, except
    PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS, which are skipped entirely.

    Returns {base_model_id: {'tpm': {variant: value}, 'rpm': {variant: value}}},
    where variant is always 'cross_region' or 'global_cross_region'.
    """
    models_by_id = {m["modelId"]: m for m in models}

    # Quota values usable by this path: runtime cross_region/global_cross_region only,
    # indexed by (variant, metric, normalized "for X" suffix key) for the exact-match fast
    # path, and by (variant, metric) -> [(suffix tokens, value), ...] for the suffix-match
    # fallback below.
    quota_index = {}
    quota_token_index = {}
    for q in quotas:
        classification = classify_quota(q.get("QuotaName", ""))
        if classification is None or classification[0] != "runtime":
            continue
        _, metric, variant = classification
        if variant not in ("cross_region", "global_cross_region"):
            continue
        suffix = split_suffix(q.get("QuotaName", ""))
        if suffix is None:
            continue
        key = normalize_match_key(suffix)
        quota_index.setdefault((variant, metric), {})[key] = q.get("Value")
        quota_token_index.setdefault((variant, metric), []).append(
            (match_key_tokens(suffix), q.get("Value")),
        )

    results = {}
    for p in inference_profiles:
        if p.get("status") != "ACTIVE":
            continue
        base_model_id = p.get("baseModelId")
        if (
            base_model_id is None
            or base_model_id in PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS
        ):
            continue
        model = models_by_id.get(base_model_id)
        if model is None:
            continue

        variant = profile_variant(p.get("inferenceProfileId"))
        full_key = normalize_match_key(
            f"{model.get('providerName') or ''} {model.get('modelName') or ''}"
        )
        bare_key = normalize_match_key(model.get("modelName") or "")
        bare_tokens = match_key_tokens(model.get("modelName") or "")

        entry = results.setdefault(base_model_id, {"tpm": {}, "rpm": {}})
        for metric in ("tpm", "rpm"):
            variant_map = quota_index.get((variant, metric), {})
            value = variant_map.get(full_key, variant_map.get(bare_key))
            if value is None:
                # Neither exact key matched, which happens when the quota suffix carries a
                # provider-name variant the model record spells differently (quota
                # 'Writer AI Palmyra X4 V1' vs providerName 'Writer'; 'Mistral Pixtral Large
                # 25.02 V1' vs providerName 'Mistral AI'). Fall back to: does exactly one
                # quota in this model's own variant/metric family end with this model's bare
                # name? Ambiguous (0 or 2+ candidates) is treated as no match, never a guess --
                # e.g. 'Twelve Labs Marengo' carries no token distinguishing the 2.7 sibling
                # from the 3.0 one, so it must resolve to nothing rather than pick one.
                candidates = {
                    v
                    for toks, v in quota_token_index.get((variant, metric), [])
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
    models_by_id = {m["modelId"]: m for m in models}
    profiles_cache = {}
    for p in inference_profiles:
        if p.get("status") != "ACTIVE":
            continue
        profile_id = p.get("inferenceProfileId")
        if profile_id is None:
            continue

        base_model_id = p.get("baseModelId")
        variant = profile_variant(profile_id)
        model = models_by_id.get(base_model_id, {})
        tpm = profile_matches.get(base_model_id, {}).get("tpm", {}).get(variant)

        profiles_cache[profile_id] = {
            "inferenceProfileId": profile_id,
            "baseModelId": base_model_id,
            "variant": variant,
            "providerName": model.get("providerName"),
            "modelName": model.get("modelName"),
            "tpm": tpm,
        }

    return profiles_cache


def get_models(bedrock):
    response = bedrock.list_foundation_models()
    return [
        {
            "modelId": m["modelId"],
            "providerName": m.get("providerName"),
            "modelName": m.get("modelName"),
        }
        for m in response.get("modelSummaries", [])
    ]


def base_model_id_from_arn(model_arn):
    idx = model_arn.rfind("foundation-model/")
    if idx == -1:
        return None
    return model_arn[idx + len("foundation-model/") :]


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
            kwargs["nextToken"] = next_token
        response = bedrock.list_inference_profiles(**kwargs)
        for p in response.get("inferenceProfileSummaries", []):
            base_model_ids = []
            for model in p.get("models", []):
                base_model_id = base_model_id_from_arn(model.get("modelArn", ""))
                if base_model_id and base_model_id not in base_model_ids:
                    base_model_ids.append(base_model_id)
            profiles.append(
                {
                    "inferenceProfileId": p.get("inferenceProfileId"),
                    "inferenceProfileName": p.get("inferenceProfileName"),
                    "status": p.get("status"),
                    "baseModelId": base_model_ids[0] if base_model_ids else None,
                }
            )
        next_token = response.get("nextToken")
        if not next_token:
            break
    return profiles


def resolve_region(session):
    """Prefer the deployed stack's own region (config.env's AWS_REGION, written by
    deploy.sh from the live state machine ARN) over the CLI/profile default, so the
    quota cache never silently reflects the wrong region's models/profiles/quotas.

    Reads config.env directly and tolerantly rather than via config_loader.load_config(),
    which sys.exit(1)s if config.env is missing -- that would break `make refresh-quotas`
    run standalone before a first deploy.
    """
    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "config.env"
    )
    if os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    if key.strip() == "AWS_REGION" and value.strip():
                        return value.strip()

    return session.region_name or "us-east-1"


def get_quotas(service_quotas):
    quotas = []
    next_token = None
    while True:
        kwargs = {"ServiceCode": "bedrock"}
        if next_token:
            kwargs["NextToken"] = next_token
        response = service_quotas.list_service_quotas(**kwargs)
        for q in response.get("Quotas", []):
            if not is_rate_quota(q.get("QuotaName", "")):
                continue
            quotas.append(
                {
                    "QuotaCode": q.get("QuotaCode"),
                    "QuotaName": q.get("QuotaName"),
                    "Value": q.get("Value"),
                    "Unit": q.get("Unit"),
                    "Adjustable": q.get("Adjustable"),
                }
            )
        next_token = response.get("NextToken")
        if not next_token:
            break
    return quotas


def main():
    session = boto3.Session()
    region = resolve_region(session)

    bedrock = session.client("bedrock", region_name=region)
    service_quotas = session.client("service-quotas", region_name=region)

    output = {
        "lastRefreshedAt": datetime.now(timezone.utc).isoformat(),
        "region": region,
        "models": get_models(bedrock),
    }

    try:
        output["quotas"] = get_quotas(service_quotas)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "AccessDeniedException":
            raise
        message = "AccessDenied: missing IAM action servicequotas:ListServiceQuotas"
        output["quotas"] = []
        output["quota_access_error"] = message
        print(message, file=sys.stderr)

    try:
        output["inference_profiles"] = get_inference_profiles(bedrock)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "AccessDeniedException":
            raise
        message = "AccessDenied: missing IAM action bedrock:ListInferenceProfiles"
        output["inference_profiles"] = []
        output["inference_profile_access_error"] = message
        print(message, file=sys.stderr)

    # The consumed output: one entry per ACTIVE inference profile, each carrying only
    # its own pool's TPM. See build_profiles_cache() for the tpm sourcing rule.
    profile_matches = match_profile_driven_quotas(
        output["quotas"],
        output["models"],
        output["inference_profiles"],
    )
    output["profiles"] = build_profiles_cache(
        output["models"],
        output["inference_profiles"],
        profile_matches,
    )

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
