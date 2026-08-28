"""
Shared simulation package for bedrock-traffic-shaper algorithm proofs.

This package contains the quota/workload configuration vocabulary and core
simulation primitives (fake clock, items, result types) that are shared
between all algorithm simulations.

Each algorithm has its own top-level simulation script:
  scripts/test_queue_processor_sim.py  — queue processor drain algorithm
  scripts/test_budget_manager_sim.py   — (future) burst admission algorithm

Both scripts import from here so quota profiles, workload presets, and token
size ranges are defined in exactly one place.
"""
from .config import (
    SimQuota,
    WorkloadPreset,
    SimConfig,
    QUOTA_PROFILES,
    WORKLOAD_PRESETS,
    build_config,
    MAX_INPUT_TOKENS,
    MAX_TOTAL_TOKENS,
)
from .core import (
    Item,
    FakeClock,
    SimResult,
    AssertionResult,
    make_items_for_preset,
)

__all__ = [
    # config
    "SimQuota", "WorkloadPreset", "SimConfig",
    "QUOTA_PROFILES", "WORKLOAD_PRESETS", "build_config",
    "MAX_INPUT_TOKENS", "MAX_TOTAL_TOKENS",
    # core
    "Item", "FakeClock", "SimResult", "AssertionResult",
    "make_items_for_preset",
]
