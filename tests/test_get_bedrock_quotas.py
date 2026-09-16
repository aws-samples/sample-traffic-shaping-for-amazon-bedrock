"""Regression test for profile-driven quota matching's regional/global pool separation.

match_profile_driven_quotas() derives regional-vs-global strictly from each inference
profile's own inferenceProfileId prefix ('global.' -> global_cross_region, anything else
-> cross_region) and must never let one family borrow the other's quota value, even when
both profiles resolve to the same base model. This is the whole point of the
profile-driven path over fuzzy quota-name matching, and widening its scope must
not weaken it.

Run: python -m pytest tests/test_get_bedrock_quotas.py -q
"""
import pathlib
import sys

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from get_bedrock_quotas import (  # noqa: E402
    build_model_index,
    build_profiles_cache,
    group_quotas,
    match_profile_driven_quotas,
    match_quota_model,
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
    models = [{"modelId": denylisted_id, "providerName": "Meta", "modelName": "Llama 4 Maverick"}]
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
    models = [{"modelId": denylisted_id, "providerName": "Meta", "modelName": "Llama 4 Maverick"}]
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


# --- normalize_match_key()/match_quota_model() regression coverage ---
#
# Real mismatch classes observed against account data: hyphen vs space ('DeepSeek-R1' vs
# 'DeepSeek R1'), parenthesized vs bare version ('(25.02)' vs '25.02'), and a provider-name
# variant embedded in the quota suffix that the model record spells differently
# ('Writer AI Palmyra X4' vs providerName 'Writer', 'Mistral Pixtral Large' vs providerName
# 'Mistral AI'). None of these should ever come at the cost of a sibling model wrongly
# claiming another model's quota -- see the ambiguity-guard tests below.

FIXER_MODELS = [
    {"modelId": "deepseek.r1-v1:0", "providerName": "DeepSeek", "modelName": "DeepSeek-R1"},
    {"modelId": "writer.palmyra-x4-v1:0", "providerName": "Writer", "modelName": "Palmyra X4"},
    {"modelId": "writer.palmyra-x5-v1:0", "providerName": "Writer", "modelName": "Palmyra X5"},
    {
        "modelId": "mistral.pixtral-large-2502-v1:0",
        "providerName": "Mistral AI",
        "modelName": "Pixtral Large (25.02)",
    },
]


def test_match_quota_model_normalizes_hyphen_space_and_punctuation():
    by_full_name, by_bare_name, bare_token_candidates = build_model_index(FIXER_MODELS)

    model_id = match_quota_model(
        "Cross-region model inference tokens per minute for DeepSeek R1 V1",
        by_full_name, by_bare_name, bare_token_candidates,
    )

    assert model_id == "deepseek.r1-v1:0"


def test_match_quota_model_resolves_provider_name_spelled_differently():
    """'Writer AI' in the quota suffix vs providerName 'Writer' -- and 'Mistral' in the quota
    suffix vs providerName 'Mistral AI' -- neither the full nor the bare exact key can match
    this on their own; the suffix fallback must resolve it via the model's bare name alone."""
    by_full_name, by_bare_name, bare_token_candidates = build_model_index(FIXER_MODELS)

    palmyra_x4 = match_quota_model(
        "Cross-region model inference tokens per minute for Writer AI Palmyra X4 V1",
        by_full_name, by_bare_name, bare_token_candidates,
    )
    palmyra_x5 = match_quota_model(
        "Cross-region model inference tokens per minute for Writer AI Palmyra X5 V1",
        by_full_name, by_bare_name, bare_token_candidates,
    )
    pixtral = match_quota_model(
        "Cross-region model inference tokens per minute for Mistral Pixtral Large 25.02 V1",
        by_full_name, by_bare_name, bare_token_candidates,
    )

    assert palmyra_x4 == "writer.palmyra-x4-v1:0"
    assert palmyra_x5 == "writer.palmyra-x5-v1:0"
    assert pixtral == "mistral.pixtral-large-2502-v1:0"


def test_match_quota_model_refuses_ambiguous_suffix_between_sibling_versions():
    """Real-world case: 'Twelve Labs Marengo' is AWS's quota-name for the 2.7 sibling, but
    carries no 'Embed'/version token distinguishing it from the 3.0 sibling. A matcher loose
    enough to ignore 'Embed <version>' entirely would let *both* siblings claim this quota
    (or arbitrarily pick one) -- per the design's guardrail, this must resolve to no match
    at all rather than guess."""
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
    by_full_name, by_bare_name, bare_token_candidates = build_model_index(siblings)

    model_id = match_quota_model(
        "Cross-region model inference requests per minute for Twelve Labs Marengo",
        by_full_name, by_bare_name, bare_token_candidates,
    )

    assert model_id is None


def test_no_quota_value_claimed_by_more_than_one_base_model():
    """group_quotas() must never let two different base models share credit for the same
    underlying quota record. Exercises the full model set above (including the ambiguous
    Marengo siblings) plus every quota, and asserts each QuotaCode's value appears under at
    most one base model's grouped entry."""
    all_models = FIXER_MODELS + [
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
    # Values are deliberately distinct (even where the real account has two models sharing an
    # identical quota tier, e.g. Palmyra X4/X5 both at 150k TPM) so each quota's value can be
    # traced unambiguously back to its own QuotaCode below -- a coincidental value collision
    # between two correctly-matched, unrelated quotas is not the bug this test guards against.
    all_quotas = [
        {"QuotaCode": "Q1", "QuotaName": "Cross-region model inference tokens per minute for DeepSeek R1 V1", "Value": 200_000},
        {"QuotaCode": "Q2", "QuotaName": "Cross-region model inference tokens per minute for Writer AI Palmyra X4 V1", "Value": 150_000},
        {"QuotaCode": "Q3", "QuotaName": "Cross-region model inference tokens per minute for Writer AI Palmyra X5 V1", "Value": 151_000},
        {"QuotaCode": "Q4", "QuotaName": "Cross-region model inference tokens per minute for Mistral Pixtral Large 25.02 V1", "Value": 80_000},
        # Deliberately ambiguous -- no 'Embed'/version token, could tempt a loose matcher
        # into crediting either Marengo sibling.
        {"QuotaCode": "Q5", "QuotaName": "Cross-region model inference requests per minute for Twelve Labs Marengo", "Value": 200},
        {"QuotaCode": "Q6", "QuotaName": "Cross-region model inference requests per minute for TwelveLabs Marengo Embed 3.0", "Value": 1000},
    ]
    value_by_code = {q["QuotaCode"]: q["Value"] for q in all_quotas}

    grouped, unmatched = group_quotas(all_quotas, all_models)

    # Every quota's own value shows up under exactly the one base model it belongs to -- never
    # under a second one, even when a sibling model happens to be a suffix-match candidate.
    quota_code_owners = {}
    for model_id, entry in grouped.items():
        for metric in ("tpm", "rpm"):
            for variant, value in entry["runtime"][metric].items():
                if value is None:
                    continue
                for code, expected_value in value_by_code.items():
                    if value == expected_value:
                        quota_code_owners.setdefault(code, set()).add(model_id)

    for code, owners in quota_code_owners.items():
        assert len(owners) == 1, f"QuotaCode {code} claimed by more than one base model: {owners}"

    # The genuinely ambiguous quota (Q5) must land in unmatched_quotas, not silently attributed
    # to either Marengo sibling -- neither sibling gets a grouped entry from it at all.
    assert "Q5" in {q["QuotaCode"] for q in unmatched}
    assert "twelvelabs.marengo-embed-2-7-v1:0" not in grouped
    assert grouped["twelvelabs.marengo-embed-3-0-v1:0"]["runtime"]["rpm"]["cross_region"] == 1000

    # The unambiguous, correctly-matched quotas landed on the right model, not a sibling.
    assert grouped["deepseek.r1-v1:0"]["runtime"]["tpm"]["cross_region"] == 200_000
    assert grouped["writer.palmyra-x4-v1:0"]["runtime"]["tpm"]["cross_region"] == 150_000
    assert grouped["writer.palmyra-x5-v1:0"]["runtime"]["tpm"]["cross_region"] == 151_000
    assert grouped["mistral.pixtral-large-2502-v1:0"]["runtime"]["tpm"]["cross_region"] == 80_000
