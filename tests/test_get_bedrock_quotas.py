"""Regression test for profile-driven quota matching's regional/global pool separation.

match_profile_driven_quotas() derives regional-vs-global strictly from each inference
profile's own inferenceProfileId prefix ('global.' -> global_cross_region, anything else
-> cross_region) and must never let one family borrow the other's quota value, even when
both profiles resolve to the same base model. That separation is the whole point of
matching per inference profile rather than per base model, and widening its scope must
not weaken it.

Run: python -m pytest tests/test_get_bedrock_quotas.py -q
"""

import pathlib
import sys

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from get_bedrock_quotas import (  # noqa: E402
    build_profiles_cache,
    match_profile_driven_quotas,
)

BASE_MODEL_ID = "anthropic.claude-sonnet-5"

MODELS = [
    {
        "modelId": BASE_MODEL_ID,
        "providerName": "Anthropic",
        "modelName": "Claude Sonnet 5",
    },
]

QUOTAS = [
    {
        "QuotaName": "Global cross-region model inference tokens per minute for Anthropic Claude Sonnet 5",
        "Value": 4_000_000,
    },
    {
        "QuotaName": "Cross-region model inference tokens per minute for Anthropic Claude Sonnet 5",
        "Value": 400_000,
    },
    {
        "QuotaName": "Global cross-region model inference requests per minute for Anthropic Claude Sonnet 5",
        "Value": 4_000,
    },
    {
        "QuotaName": "Cross-region model inference requests per minute for Anthropic Claude Sonnet 5",
        "Value": 400,
    },
]

INFERENCE_PROFILES = [
    {
        "inferenceProfileId": f"global.{BASE_MODEL_ID}",
        "inferenceProfileName": "global-sonnet-5",
        "status": "ACTIVE",
        "baseModelId": BASE_MODEL_ID,
    },
    {
        "inferenceProfileId": f"us.{BASE_MODEL_ID}",
        "inferenceProfileName": "us-sonnet-5",
        "status": "ACTIVE",
        "baseModelId": BASE_MODEL_ID,
    },
]


def test_global_and_regional_profiles_never_cross_contaminate():
    results = match_profile_driven_quotas(QUOTAS, MODELS, INFERENCE_PROFILES)

    entry = results[BASE_MODEL_ID]

    # Each pool got its own, distinct value.
    assert entry["tpm"]["global_cross_region"] == 4_000_000
    assert entry["tpm"]["cross_region"] == 400_000
    assert entry["rpm"]["global_cross_region"] == 4_000
    assert entry["rpm"]["cross_region"] == 400

    # Neither family ever borrowed the other's value.
    assert entry["tpm"]["global_cross_region"] != entry["tpm"]["cross_region"]
    assert entry["rpm"]["global_cross_region"] != entry["rpm"]["cross_region"]
    assert set(entry["tpm"].keys()) == {"global_cross_region", "cross_region"}
    assert set(entry["rpm"].keys()) == {"global_cross_region", "cross_region"}


def test_inactive_profile_is_ignored():
    inactive_profiles = [
        {**INFERENCE_PROFILES[0], "status": "PENDING"},
        INFERENCE_PROFILES[1],
    ]

    results = match_profile_driven_quotas(QUOTAS, MODELS, inactive_profiles)

    entry = results[BASE_MODEL_ID]
    assert entry["tpm"].get("global_cross_region") is None
    assert entry["tpm"]["cross_region"] == 400_000


def test_denylisted_base_model_is_excluded():
    from get_bedrock_quotas import PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS

    denylisted_id = PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS[0]
    models = [
        {
            "modelId": denylisted_id,
            "providerName": "Meta",
            "modelName": "Llama 4 Maverick",
        }
    ]
    profiles = [
        {
            "inferenceProfileId": f"us.{denylisted_id}",
            "inferenceProfileName": "us-llama4-maverick",
            "status": "ACTIVE",
            "baseModelId": denylisted_id,
        },
    ]
    quotas = [
        {
            "QuotaName": "Cross-region model inference tokens per minute for Meta Llama 4 Maverick",
            "Value": 999,
        },
    ]

    results = match_profile_driven_quotas(quotas, models, profiles)

    assert denylisted_id not in results


def test_profiles_cache_keeps_each_profile_own_pool():
    """build_profiles_cache() must key by inferenceProfileId (not base model), and
    each entry's tpm must come solely from that profile's own variant -- never the
    other one -- even though both profiles here share a base model."""
    profile_matches = match_profile_driven_quotas(QUOTAS, MODELS, INFERENCE_PROFILES)

    profiles_cache = build_profiles_cache(MODELS, INFERENCE_PROFILES, profile_matches)

    assert set(profiles_cache.keys()) == {
        f"global.{BASE_MODEL_ID}",
        f"us.{BASE_MODEL_ID}",
    }

    global_entry = profiles_cache[f"global.{BASE_MODEL_ID}"]
    us_entry = profiles_cache[f"us.{BASE_MODEL_ID}"]

    assert global_entry["baseModelId"] == BASE_MODEL_ID
    assert global_entry["variant"] == "global_cross_region"
    assert global_entry["providerName"] == "Anthropic"
    assert global_entry["modelName"] == "Claude Sonnet 5"
    assert global_entry["tpm"] == 4_000_000

    assert us_entry["baseModelId"] == BASE_MODEL_ID
    assert us_entry["variant"] == "cross_region"
    assert us_entry["tpm"] == 400_000

    # Neither entry ever borrowed the other's pool.
    assert global_entry["tpm"] != us_entry["tpm"]


def test_profiles_cache_emits_null_tpm_for_unmatched_or_denylisted():
    """A profile with no quota match -- including one entirely skipped by
    match_profile_driven_quotas() because its base model is denylisted -- must
    still appear in the cache, with tpm: None rather than being omitted."""
    from get_bedrock_quotas import PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS

    denylisted_id = PROFILE_MATCH_EXCLUDED_BASE_MODEL_IDS[0]
    models = [
        {
            "modelId": denylisted_id,
            "providerName": "Meta",
            "modelName": "Llama 4 Maverick",
        }
    ]
    profiles = [
        {
            "inferenceProfileId": f"us.{denylisted_id}",
            "inferenceProfileName": "us-llama4-maverick",
            "status": "ACTIVE",
            "baseModelId": denylisted_id,
        },
    ]
    quotas = [
        {
            "QuotaName": "Cross-region model inference tokens per minute for Meta Llama 4 Maverick",
            "Value": 999,
        },
    ]

    profile_matches = match_profile_driven_quotas(quotas, models, profiles)
    profiles_cache = build_profiles_cache(models, profiles, profile_matches)

    entry = profiles_cache[f"us.{denylisted_id}"]
    assert entry["baseModelId"] == denylisted_id
    assert entry["variant"] == "cross_region"
    assert entry["tpm"] is None


def test_profiles_cache_ignores_inactive_profiles():
    inactive_profiles = [
        {**INFERENCE_PROFILES[0], "status": "PENDING"},
        INFERENCE_PROFILES[1],
    ]
    profile_matches = match_profile_driven_quotas(QUOTAS, MODELS, inactive_profiles)

    profiles_cache = build_profiles_cache(MODELS, inactive_profiles, profile_matches)

    assert set(profiles_cache.keys()) == {f"us.{BASE_MODEL_ID}"}


# --- normalize_match_key()/suffix-fallback regression coverage ---
#
# Real mismatch classes observed against account data, all of which the exact-key lookup
# alone cannot bridge: hyphen vs space ('DeepSeek-R1' vs 'DeepSeek R1'), parenthesized vs
# bare version ('(25.02)' vs '25.02'), and a provider-name variant embedded in the quota
# suffix that the model record spells differently ('Writer AI Palmyra X4' vs providerName
# 'Writer', 'Mistral Pixtral Large' vs providerName 'Mistral AI'). None of these may come
# at the cost of a model claiming a quota that isn't demonstrably its own -- see the
# ambiguity-guard tests at the bottom.
#
# These exercise match_profile_driven_quotas() because that is the only consumer of
# normalize_match_key()/match_key_tokens()/_is_token_suffix(); the standalone fuzzy
# quota-name matcher they were originally written against had no production consumer and
# was removed.

FIXER_MODELS = [
    {
        "modelId": "deepseek.r1-v1:0",
        "providerName": "DeepSeek",
        "modelName": "DeepSeek-R1",
    },
    {
        "modelId": "writer.palmyra-x4-v1:0",
        "providerName": "Writer",
        "modelName": "Palmyra X4",
    },
    {
        "modelId": "writer.palmyra-x5-v1:0",
        "providerName": "Writer",
        "modelName": "Palmyra X5",
    },
    {
        "modelId": "mistral.pixtral-large-2502-v1:0",
        "providerName": "Mistral AI",
        "modelName": "Pixtral Large (25.02)",
    },
]


def us_profiles(models):
    """One ACTIVE regional (cross_region) inference profile per model."""
    return [
        {
            "inferenceProfileId": f"us.{m['modelId']}",
            "inferenceProfileName": f"us-{m['modelId']}",
            "status": "ACTIVE",
            "baseModelId": m["modelId"],
        }
        for m in models
    ]


def test_matching_normalizes_hyphen_space_and_punctuation():
    """Quota suffix 'DeepSeek R1 V1' vs modelName 'DeepSeek-R1': hyphen-vs-space plus a
    generic 'V1' profile-version token. Must still resolve to this model's own TPM."""
    models = [FIXER_MODELS[0]]
    quotas = [
        {
            "QuotaName": "Cross-region model inference tokens per minute for DeepSeek R1 V1",
            "Value": 200_000,
        },
    ]

    results = match_profile_driven_quotas(quotas, models, us_profiles(models))

    assert results["deepseek.r1-v1:0"]["tpm"]["cross_region"] == 200_000


def test_matching_resolves_provider_name_spelled_differently():
    """'Writer AI' in the quota suffix vs providerName 'Writer' -- and 'Mistral' in the quota
    suffix vs providerName 'Mistral AI' -- neither the full nor the bare exact key can match
    this on their own; the suffix fallback must resolve it via the model's bare name alone,
    and must still give each Palmyra sibling its own distinct value."""
    quotas = [
        {
            "QuotaName": "Cross-region model inference tokens per minute for Writer AI Palmyra X4 V1",
            "Value": 150_000,
        },
        {
            "QuotaName": "Cross-region model inference tokens per minute for Writer AI Palmyra X5 V1",
            "Value": 151_000,
        },
        {
            "QuotaName": "Cross-region model inference tokens per minute for Mistral Pixtral Large 25.02 V1",
            "Value": 80_000,
        },
    ]

    results = match_profile_driven_quotas(quotas, FIXER_MODELS, us_profiles(FIXER_MODELS))

    assert results["writer.palmyra-x4-v1:0"]["tpm"]["cross_region"] == 150_000
    assert results["writer.palmyra-x5-v1:0"]["tpm"]["cross_region"] == 151_000
    assert results["mistral.pixtral-large-2502-v1:0"]["tpm"]["cross_region"] == 80_000

    # The two siblings resolved independently -- neither borrowed the other's value.
    assert (
        results["writer.palmyra-x4-v1:0"]["tpm"]["cross_region"]
        != results["writer.palmyra-x5-v1:0"]["tpm"]["cross_region"]
    )


def test_matching_refuses_ambiguous_suffix_with_two_candidate_quotas():
    """The suffix fallback must only fire when exactly one quota in this model's own
    variant/metric family ends with its bare name. Here two quotas both end with
    'Nano Pro' at different values -- picking either would be a coin flip, so the model
    must come back with no TPM at all."""
    models = [
        {
            "modelId": "acme.nano-pro-v1:0",
            "providerName": "Acme",
            "modelName": "Nano Pro",
        }
    ]
    quotas = [
        {
            "QuotaName": "Cross-region model inference tokens per minute for Acme AI Nano Pro V1",
            "Value": 100_000,
        },
        {
            "QuotaName": "Cross-region model inference tokens per minute for Acme Labs Nano Pro V1",
            "Value": 200_000,
        },
    ]

    results = match_profile_driven_quotas(quotas, models, us_profiles(models))

    assert results["acme.nano-pro-v1:0"]["tpm"] == {}


def test_matching_never_credits_a_sibling_with_an_underspecified_quota():
    """Real-world case: 'Twelve Labs Marengo' is AWS's quota-name for the 2.7 sibling, but
    carries no 'Embed'/version token distinguishing it from the 3.0 sibling. Neither sibling
    may claim it. The 3.0 sibling still gets its own fully-specified quota."""
    siblings = [
        {
            "modelId": "twelvelabs.marengo-embed-2-7-v1:0",
            "providerName": "TwelveLabs",
            "modelName": "Marengo Embed v2.7",
        },
        {
            "modelId": "twelvelabs.marengo-embed-3-0-v1:0",
            "providerName": "TwelveLabs",
            "modelName": "Marengo Embed 3.0",
        },
    ]
    quotas = [
        # Underspecified -- could tempt a loose matcher into crediting either sibling.
        {
            "QuotaName": "Cross-region model inference requests per minute for Twelve Labs Marengo",
            "Value": 200,
        },
        {
            "QuotaName": "Cross-region model inference requests per minute for TwelveLabs Marengo Embed 3.0",
            "Value": 1000,
        },
    ]

    results = match_profile_driven_quotas(quotas, siblings, us_profiles(siblings))

    # The 200 value is never attributed to anyone.
    assert results["twelvelabs.marengo-embed-2-7-v1:0"]["rpm"] == {}
    assert results["twelvelabs.marengo-embed-3-0-v1:0"]["rpm"]["cross_region"] == 1000
