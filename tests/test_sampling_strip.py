"""should_strip_sampling_params(): which model IDs get temperature/top_p/top_k removed.

REJECTS_TEMPERATURE lists the IDs the processor must strip sampling params for.
The models added in the 2026-11 refresh were live-tested with temperature on
Converse and returned a 400 ("`temperature` is deprecated for this model" for
Claude, "doesn't support the temperature field" for Grok 4.7). Not every ID here
had its own live call: us.anthropic.claude-fable-5 is covered by the shared
claude-fable-5 substring, and the kept 4.x IDs are carried over from the
original strip list for callers still migrating off them. Listing them here
means pruning an entry from _STRIP_SAMPLING_SUBSTRINGS fails a test.
"""

import pathlib
import sys

import pytest

HANDLERS = pathlib.Path(__file__).resolve().parents[1] / "infrastructure" / "lambda_handlers"
LAYER = pathlib.Path(__file__).resolve().parents[1] / "infrastructure" / "lambda_layer" / "python"
sys.path.insert(0, str(LAYER))
sys.path.insert(0, str(HANDLERS))

import bedrock_processor  # noqa: E402

REJECTS_TEMPERATURE = [
    "us.anthropic.claude-haiku-5-5",
    "global.anthropic.claude-haiku-5-5",
    "us.anthropic.claude-sonnet-5-5",
    "global.anthropic.claude-sonnet-5-5",
    "us.anthropic.claude-opus-5-5",
    "global.anthropic.claude-opus-5-5",
    "us.anthropic.claude-fable-5",
    "us.anthropic.claude-fable-5-1",
    "global.anthropic.claude-fable-5-1",
    "us.xai.grok-4.7",
    "global.xai.grok-4.7",
    # Kept 4.x entries -- still in MODEL_MAP-independent use by migrating callers.
    "us.anthropic.claude-opus-4-7",
    "global.anthropic.claude-opus-4-7",
    "us.anthropic.claude-opus-4-8",
    "global.anthropic.claude-opus-4-8",
    "us.anthropic.claude-sonnet-4-6",
    "global.anthropic.claude-sonnet-4-6",
]


@pytest.mark.parametrize("model_id", REJECTS_TEMPERATURE)
def test_models_that_reject_temperature_are_stripped(model_id):
    assert bedrock_processor.should_strip_sampling_params(model_id)


@pytest.mark.parametrize(
    "model_id",
    ["us.amazon.nova-2-lite-v1:0", "us.meta.llama4-scout-17b-instruct-v1:0", "zai.glm-5", ""],
)
def test_models_that_accept_temperature_keep_it(model_id):
    assert not bedrock_processor.should_strip_sampling_params(model_id)
