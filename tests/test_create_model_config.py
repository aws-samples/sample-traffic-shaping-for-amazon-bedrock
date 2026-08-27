"""Regression tests for queue-only model configuration."""

from decimal import Decimal
import pathlib
import sys


SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from create_model_config import calculate_config, configure_mantle_queue_only  # noqa: E402


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
