"""Regression tests for queue-only model configuration."""

from decimal import Decimal
import pathlib
import sys


SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from create_model_config import (  # noqa: E402
    calculate_config,
    configure_mantle_queue_only,
    derive_default_burndown,
)


def test_mantle_configuration_disables_every_immediate_path():
    config = calculate_config(
        rpm=None,
        tpm=10_000_000,
        burndown_rate=5.0,
        burst_fraction=0,
        queue_fraction=0.95,
        buffer_fraction=0.05,
    )

    configure_mantle_queue_only(
        config,
        itpm=10_000_000,
        otpm=2_000_000,
        queue_fraction=0.95,
        buffer_fraction=0.05,
    )

    assert config["burst_capacity"] == 0
    assert config["burst_regeneration_rate"] == Decimal("0")
    assert config["tpm_burst_capacity"] == 0
    assert config["tpm_burst_regeneration_rate"] == Decimal("0")
    assert config["itpm_burst_capacity"] == 0
    assert config["itpm_burst_regeneration_rate"] == Decimal("0")
    assert config["otpm_burst_capacity"] == 0
    assert config["otpm_burst_regeneration_rate"] == Decimal("0")


def test_mantle_split_quotas_honor_queue_and_buffer_fractions():
    config = calculate_config(
        rpm=None,
        tpm=10_000_000,
        burndown_rate=5.0,
        burst_fraction=0,
        queue_fraction=0.95,
        buffer_fraction=0.05,
    )

    configure_mantle_queue_only(
        config,
        itpm=10_000_000,
        otpm=2_000_000,
        queue_fraction=0.95,
        buffer_fraction=0.05,
    )

    assert config["itpm_queue_capacity"] == 9_500_000
    assert config["itpm_buffer_capacity"] == 500_000
    assert config["otpm_queue_capacity"] == 1_900_000
    assert config["otpm_buffer_capacity"] == 100_000
    assert config["output_token_burndown_rate"] == Decimal("1.0")


def test_burndown_claude_4_8_is_15x():
    assert derive_default_burndown("anthropic.claude-opus-4-8", "runtime") == 15.0
    assert derive_default_burndown("us.anthropic.claude-opus-4-8", "runtime") == 15.0


def test_burndown_claude_5_plus_is_10x():
    assert derive_default_burndown("us.anthropic.claude-sonnet-5", "runtime") == 10.0
    assert derive_default_burndown("us.anthropic.claude-opus-5", "runtime") == 10.0
    assert derive_default_burndown("us.anthropic.claude-fable-5-1", "runtime") == 10.0


def test_burndown_claude_4_7_and_below_is_5x():
    assert derive_default_burndown("anthropic.claude-opus-4-7", "runtime") == 5.0
    assert derive_default_burndown("us.anthropic.claude-sonnet-4-6", "runtime") == 5.0
    assert derive_default_burndown(
        "us.anthropic.claude-haiku-4-5-20251001-v1:0", "runtime"
    ) == 5.0


def test_burndown_unrecognized_anthropic_shape_defaults_to_5x_not_10x():
    assert derive_default_burndown("anthropic.claude-instant-v1", "runtime") == 5.0


def test_burndown_openai_runtime_is_10x():
    assert derive_default_burndown("us.openai.gpt-5.6-luna", "runtime") == 10.0


def test_burndown_mantle_backend_is_always_1x_regardless_of_provider():
    assert derive_default_burndown("us.anthropic.claude-opus-4-8", "mantle") == 1.0


def test_mantle_backend_resolves_tpm_from_itpm_without_explicit_tpm():
    """--backend mantle --itpm --otpm with no --tpm must not require a quota
    cache entry -- see create_model_config.py's process_model()."""
    import argparse

    from create_model_config import process_model

    parser = argparse.ArgumentParser()
    args = argparse.Namespace(
        rpm=None,
        tpm=None,
        backend="mantle",
        itpm=10_000_000,
        otpm=2_000_000,
        bytes_per_token=None,
        burst_capacity=None,
        short_window_sec=2,
        long_window_sec=15,
        burst_fraction=0.0,
        queue_fraction=0.85,
        buffer_fraction=0.15,
        api_style=None,
        queue_target_tpm=None,
        dry_run=True,
    )

    summary = process_model("opus-47-mantle", args, parser)

    assert summary["tpm"] == 10_000_000
    assert summary["tpm_source"] == "mantle-itpm"
