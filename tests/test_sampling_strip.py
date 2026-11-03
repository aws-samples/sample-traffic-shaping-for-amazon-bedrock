"""should_strip_sampling_params(): which model IDs get temperature/top_p/top_k removed.

Every ID in REJECTS_TEMPERATURE returned a 400 on a live Converse call that set
temperature ("`temperature` is deprecated for this model" for Claude, "doesn't
support the temperature field" for Grok 4.7), so the processor must strip it.
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
