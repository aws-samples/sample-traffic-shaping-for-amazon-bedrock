"""Tests for the demo's offered-load timeline (DEMO_LOAD_PROFILE).

Pacing is driven by an ordered list of (duration_s, request_count) phases so a
multi-phase load (e.g. 1x for a minute, 3x for two, idle for one) can be
expressed later. The default must stay the single phase "140 requests over 25s"
and must pace EXACTLY as the old evenly-spaced (n, window_s) arithmetic did, so
existing runs stay comparable. Pure schedule arithmetic only -- no sleeping, no
AWS.

Run: python -m pytest tests/test_demo_load_profile.py -q
"""

import sys
import pathlib

import pytest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from demo import (  # noqa: E402
    _schedule_offsets,
    DEMO_LOAD_PROFILE,
    DEMO_REQUEST_COUNT,
    DEMO_SUBMIT_WINDOW_S,
)


def test_default_profile_is_140_over_25s():
    assert DEMO_LOAD_PROFILE == [(25, 140)]
    assert DEMO_REQUEST_COUNT == 140
    assert DEMO_SUBMIT_WINDOW_S == 25


def test_default_schedule_is_identical_to_the_old_even_spacing():
    """Bit-for-bit, not approx: the previous _paced_indices computed
    (i - 1) * (window_s / n), and the default profile must reproduce it."""
    n, window_s = 140, 25
    interval = window_s / n
    old = [(i - 1) * interval for i in range(1, n + 1)]
    assert _schedule_offsets(DEMO_LOAD_PROFILE) == old


def test_phases_play_back_to_back_with_their_own_spacing():
    offsets = _schedule_offsets([(10, 2), (4, 4)])
    assert offsets == [0.0, 5.0, 10.0, 11.0, 12.0, 13.0]


def test_zero_count_phase_is_an_idle_gap_that_advances_the_clock():
    offsets = _schedule_offsets([(10, 1), (60, 0), (10, 2)])
    assert offsets == [0.0, 70.0, 75.0]


def test_total_count_matches_sum_of_phases():
    profile = [(60, 35), (120, 210), (30, 9), (60, 0)]
    offsets = _schedule_offsets(profile)
    assert len(offsets) == 254
    assert offsets == sorted(offsets)
    assert offsets[-1] < 60 + 120 + 30


@pytest.mark.parametrize("bad", [[(-1, 5)], [(10, -1)]])
def test_negative_phase_is_rejected(bad):
    with pytest.raises(ValueError):
        _schedule_offsets(bad)
