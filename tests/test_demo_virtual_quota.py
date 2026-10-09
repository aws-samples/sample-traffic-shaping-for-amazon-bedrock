"""Tests for the direct arms' SIMULATED quota: a regenerating token bucket (bead
sample-traffic-shaping-for-amazon-bedrock-xht.2).

The gate used to be a rolling 60s window of charges. Nothing aged out inside a
~30s run, so after the first 37 admits the window stayed full for the rest of the
run and BOTH direct arms landed on exactly 37 served -- the retry arm burned 132
extra attempts and 0 of its 99 retries succeeded. Retry futility was an artifact
of the gate's shape, not a finding about retries. A bucket that regenerates its
whole ceiling over DEMO_QUOTA_REFILL_S (200,000 / 60 tokens per second,
continuously) lets a request that waits actually find room, so `direct` and
`direct+retry` can diverge honestly.

What that makes testable, and is pinned below:
  - the ceiling is spent, not just bounded: a full bucket admits
    floor(200,000 / 2,820) = 70 requests back to back and rejects the 71st;
  - a rejected attempt costs nothing (rejection must not deepen the deficit),
    including when another admit interleaves with it -- which is what pins the
    clock read inside the lock;
  - _direct_call's reconcile credits the bucket back DOWN to actual usage, so the
    sign at the call site that computes est - actual is pinned too;
  - regeneration is continuous, so ~0.85s after the bucket is drained -- the time
    to regenerate one whole estimate -- one more request is admitted;
  - the reconcile credit (up-front estimate minus measured actual usage) is
    visible to the next admit;
  - the level never exceeds the ceiling, neither by credit nor by idling;
  - concurrent admits never overspend the ceiling;
  - the starting level is an argument, so an EMPTY starting bucket can be
    evaluated later (bead 386) without touching the gate.

Every test drives a hand-cranked clock through the injected `now` parameter: no
sleeping, no AWS, no Bedrock spend. `make demo` costs real converse() spend and
is deliberately not the validation path for this behavior.

Run: python -m pytest tests/test_demo_virtual_quota.py -q
"""

import sys
import pathlib
import threading
import types

import pytest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

# Imported unguarded on purpose (bead asx): scripts/demo.py and the modules it
# pulls in must be importable with no config.env and no AWS credentials, so an
# import failure here is a real defect and has to be a hard error.
from demo import (  # noqa: E402
    _build_filler_prompt,
    _direct_call,
    _new_virtual_budget,
    _virtual_budget_admit,
    _virtual_budget_credit,
    DEMO_MAX_OUTPUT_TOKENS,
    DEMO_QUOTA_REFILL_S,
    DEMO_TOKENS_PER_REQUEST_ESTIMATE,
    DEMO_TPM_OVERRIDE,
)

PROMPT = _build_filler_prompt()
BURNDOWN = 1.0
EST = DEMO_TOKENS_PER_REQUEST_ESTIMATE
CAP = DEMO_TPM_OVERRIDE

# Derived from the module's own constants rather than pinned, so a change to the
# request profile or the ceiling moves these with it -- but the headline figures
# they currently produce ARE pinned (see test_full_bucket_admits_70_and_rejects
# _the_71st), because a silent regression in them is the whole point of the bead.
EXPECTED_ADMITS = CAP // EST  # 70
LEFTOVER = CAP - EXPECTED_ADMITS * EST  # 2,600 -- under one estimate, so no 71st
REGEN_PER_S = CAP / DEMO_QUOTA_REFILL_S  # 3,333.33 tokens/s
ONE_EST_REGEN_S = EST / REGEN_PER_S  # ~0.846s to regenerate a whole estimate

# A measured success from a live run: the up-front charge is the 2,820-token
# estimate, actual usage came in at 2,628, so reconcile credits 192 back.
MEASURED_ACTUAL_TOKENS = 2628
MEASURED_IN_TOKENS = 1628
MEASURED_OUT_TOKENS = 1000
RECONCILE_CREDIT = EST - MEASURED_ACTUAL_TOKENS

# A regeneration boundary is a float knife-edge, and how it lands depends on the
# MAGNITUDE of the clock readings: at start=1_000_000.0 a whole-estimate refill
# computes as 2820.0000000651926 (just over est), at a realistic epoch as
# 2819.9998... (just under). Boundary tests therefore step ONE_EST_REGEN_S plus
# BOUNDARY_EPS and are run at every epoch below, so they assert the regeneration
# RATE rather than an accident of the fake clock's origin.
BOUNDARY_EPS = 1e-6
CLOCK_EPOCHS = (100.0, 1_000_000.0, 1_758_700_000.0)

# Slack for assertions downstream of a BOUNDARY_EPS overshoot (worth ~1.7e-3
# tokens) plus float noise at a realistic epoch.
TOKEN_TOL = 0.01

# Bound on how long the lock-scope test's second thread waits for the first. It
# only ELAPSES in the correct case (where the first thread is blocked on the lock
# and so can never signal); in the buggy case the signal arrives at once. Bounded,
# so the test cannot hang either way.
RACE_WAIT_S = 0.4


def _fake_clock(start=1_000_000.0):
    """A hand-cranked clock: returns (now, advance). Frozen until advanced, so a
    test that never advances it observes zero regeneration."""
    state = {"t": float(start)}

    def now():
        return state["t"]

    def advance(seconds):
        state["t"] += seconds

    return now, advance


def _admit(budget, now):
    """One gate decision at the fake clock's current reading."""
    return _virtual_budget_admit(budget, PROMPT, DEMO_MAX_OUTPUT_TOKENS, now=now)


def _stub_bedrock(in_tok, out_tok, calls):
    """Stand-in for the boto3 bedrock-runtime client carrying only the one method
    _direct_call uses, so the success path can run with no boto3 client, no
    network, no AWS credentials and no patching of Bedrock. Records the kwargs it
    was called with."""

    def converse(**kwargs):
        calls.append(kwargs)
        return {"usage": {"inputTokens": in_tok, "outputTokens": out_tok}}

    return types.SimpleNamespace(converse=converse)


def _drained_bucket(start=1_000_000.0):
    """A full bucket spent down to LEFTOVER on a frozen clock, plus its clock.
    The state every "what happens when there is no room" test starts from."""
    now, advance = _fake_clock(start)
    budget = _new_virtual_budget(BURNDOWN, now=now)
    for _ in range(EXPECTED_ADMITS):
        charge, _, _ = _admit(budget, now)
        assert charge is not None
    assert budget["level"] == pytest.approx(LEFTOVER)
    return budget, now, advance


def test_full_bucket_admits_70_and_rejects_the_71st():
    """The ceiling is spent, not merely bounded. 70 and the 71st's rejection are
    pinned as literals as well as derived: 200,000 / 2,820 leaves 2,600 tokens,
    less than one more estimate.

    `used` is the third return value and means how far the bucket is DRAWN DOWN
    before this request's own debit -- so it reads 0 on a full bucket, which is
    the contract test_demo_aggregation.py's
    test_both_arms_charge_the_same_tokens_for_the_same_request relies on.
    """
    assert (EXPECTED_ADMITS, EST, CAP) == (70, 2820, 200_000)
    now, advance = _fake_clock()
    budget = _new_virtual_budget(BURNDOWN, now=now)
    assert budget["level"] == CAP

    used_readings = []
    for _ in range(EXPECTED_ADMITS):
        charge, est, used = _admit(budget, now)
        assert charge == {"est": EST} and est == EST
        used_readings.append(used)

    # Each admit sees the drawdown left by its predecessors, not its own debit.
    assert used_readings[0] == 0
    assert used_readings[1] == pytest.approx(EST)
    assert used_readings[-1] == pytest.approx((EXPECTED_ADMITS - 1) * EST) == 194_580
    assert LEFTOVER == 2600
    assert budget["level"] == pytest.approx(LEFTOVER)
    assert budget["level"] < EST

    charge, est, used = _admit(budget, now)
    assert charge is None
    assert est == EST
    assert used == pytest.approx(CAP - LEFTOVER)


def test_a_rejected_attempt_costs_nothing():
    """Rejection must not deepen the deficit, or a retrying arm would dig itself
    further out of reach with every attempt it makes."""
    budget, now, _ = _drained_bucket()
    for _ in range(20):
        charge, _, _ = _admit(budget, now)
        assert charge is None
        assert budget["level"] == pytest.approx(LEFTOVER)


@pytest.mark.parametrize("epoch", CLOCK_EPOCHS)
def test_one_more_is_admitted_once_a_whole_estimate_has_regenerated(epoch):
    """Regeneration is continuous, at CAP / DEMO_QUOTA_REFILL_S per second. A
    short tick is not enough; ONE_EST_REGEN_S (~0.85s, the time to regenerate a
    whole estimate) is.

    The bare 220-token deficit would clear sooner (~0.066s), so 0.05s is asserted
    only as a lower bound that must still be rejected -- the claim being pinned is
    the regeneration RATE, via the one-full-estimate figure the epic quotes.

    Run at every CLOCK_EPOCHS reading: an exact ONE_EST_REGEN_S step lands within
    float noise of `est` and admits or rejects depending on the clock's magnitude,
    so the step carries BOUNDARY_EPS and the test proves the rate at each epoch.
    """
    assert ONE_EST_REGEN_S == pytest.approx(0.846, abs=1e-3)
    budget, now, advance = _drained_bucket(epoch)

    advance(0.05)
    charge, _, _ = _admit(budget, now)
    assert charge is None
    assert budget["level"] == pytest.approx(LEFTOVER + 0.05 * REGEN_PER_S, abs=TOKEN_TOL)

    advance(ONE_EST_REGEN_S - 0.05 + BOUNDARY_EPS)
    charge, est, used = _admit(budget, now)
    assert charge == {"est": EST}
    # A whole estimate regenerated on top of what was already left over.
    assert used == pytest.approx(CAP - (LEFTOVER + EST), abs=TOKEN_TOL)
    assert budget["level"] == pytest.approx(LEFTOVER, abs=TOKEN_TOL)


def test_reconcile_credit_is_visible_on_the_next_admit():
    """A successful call's actual usage came in under its up-front estimate, so
    _virtual_budget_credit returns the difference; the very next admit must see
    the bucket that much fuller."""
    now, _ = _fake_clock()
    budget = _new_virtual_budget(BURNDOWN, now=now)
    charge, _, _ = _admit(budget, now)
    assert charge is not None

    _virtual_budget_credit(budget, RECONCILE_CREDIT)
    assert RECONCILE_CREDIT == 192
    assert budget["level"] == pytest.approx(CAP - MEASURED_ACTUAL_TOKENS)

    _, _, used = _admit(budget, now)
    assert used == pytest.approx(MEASURED_ACTUAL_TOKENS)
    assert used == pytest.approx(EST - RECONCILE_CREDIT)


def test_reconcile_credits_buy_further_admits_with_no_regeneration():
    """The credits from 70 measured successes are worth 70 x 192 = 13,440 tokens,
    which with the 2,600 leftover is room for exactly 5 more requests -- on a FROZEN clock, so this is the
    reconcile credit doing the work and not regeneration."""
    budget, now, _ = _drained_bucket()
    for _ in range(EXPECTED_ADMITS):
        _virtual_budget_credit(budget, RECONCILE_CREDIT)
    credited = EXPECTED_ADMITS * RECONCILE_CREDIT
    assert credited == 13440
    assert budget["level"] == pytest.approx(LEFTOVER + credited)

    admitted = 0
    while _admit(budget, now)[0] is not None:
        admitted += 1
    assert admitted == 5
    assert budget["level"] == pytest.approx(LEFTOVER + credited - 5 * EST)


def test_direct_call_reconciles_the_bucket_down_to_actual_usage():
    """The SIGN of the reconcile, pinned at the one place that computes it.

    Calling _virtual_budget_credit directly with a pre-computed positive credit
    does NOT pin the direction: inverting _direct_call's call site to
    (in + out) - est leaves such tests green while every successful call silently
    DEBITS the bucket instead of crediting it. Running the success path end to end
    against a stub client is what kills that mutant -- the bucket must end up down
    by the ACTUAL usage, not by est + actual and not by 2*est - actual.
    """
    budget = _new_virtual_budget(BURNDOWN)
    calls = []
    res = _direct_call(
        _stub_bedrock(MEASURED_IN_TOKENS, MEASURED_OUT_TOKENS, calls),
        1,
        "stub-model-id",
        PROMPT,
        DEMO_MAX_OUTPUT_TOKENS,
        budget,
        "direct",
    )

    assert len(calls) == 1
    assert calls[0]["modelId"] == "stub-model-id"
    assert calls[0]["inferenceConfig"] == {"maxTokens": DEMO_MAX_OUTPUT_TOKENS}
    assert (res["ok"], res["attempts"], res["bedrock_calls"]) == (True, 1, 1)
    assert res["in"] + res["out"] == MEASURED_ACTUAL_TOKENS == 2628

    # Charged the 2,820 estimate up front, credited 192 back on reconcile.
    assert budget["level"] == pytest.approx(CAP - MEASURED_ACTUAL_TOKENS) == 197_372
    # The two ways the call site can be wrong: sign inverted, or credit skipped.
    assert budget["level"] != pytest.approx(CAP - EST - MEASURED_ACTUAL_TOKENS)
    assert budget["level"] != pytest.approx(CAP - EST)


def test_credit_cannot_push_the_level_past_the_ceiling():
    """An over-large credit (or a double credit) must not manufacture quota."""
    now, _ = _fake_clock()
    budget = _new_virtual_budget(BURNDOWN, now=now)
    charge, _, _ = _admit(budget, now)
    assert charge is not None and budget["level"] < CAP

    _virtual_budget_credit(budget, 10**9)
    assert budget["level"] == CAP


def test_idling_cannot_push_the_level_past_the_ceiling():
    """Regeneration saturates: ten refill windows of idling leave a full bucket,
    not ten ceilings' worth of banked quota."""
    budget, now, advance = _drained_bucket()
    advance(10 * DEMO_QUOTA_REFILL_S)

    charge, _, used = _admit(budget, now)
    assert charge is not None
    assert used == 0  # the bucket was exactly full when this admit arrived
    assert budget["level"] == pytest.approx(CAP - EST)


def test_concurrent_admits_never_overspend_the_ceiling():
    """The lock has to make refill-check-debit atomic. Many threads race on a
    frozen clock (no regeneration to muddy the arithmetic), each attempting more
    admits than the bucket can possibly grant; exactly EXPECTED_ADMITS may win."""
    threads_n, per_thread = 16, 8
    assert threads_n * per_thread > EXPECTED_ADMITS
    now, _ = _fake_clock()
    budget = _new_virtual_budget(BURNDOWN, now=now)
    ready = threading.Barrier(threads_n)
    lock = threading.Lock()
    admitted = []

    def worker():
        ready.wait()
        mine = 0
        for _ in range(per_thread):
            if _admit(budget, now)[0] is not None:
                mine += 1
        with lock:
            admitted.append(mine)

    workers = [threading.Thread(target=worker) for _ in range(threads_n)]
    for t in workers:
        t.start()
    for t in workers:
        t.join(timeout=30)
        assert not t.is_alive()

    total = sum(admitted)
    assert total == EXPECTED_ADMITS == 70
    assert total * EST <= CAP
    assert budget["level"] >= 0
    assert budget["level"] == pytest.approx(LEFTOVER)


def test_a_rejection_cannot_debit_the_bucket_when_another_admit_interleaves():
    """Lock SCOPE, pinned: now() has to be read INSIDE the lock.

    Read outside it, a thread can be descheduled between reading the clock and
    acquiring the lock, then compute its refill from a `ts` the other thread has
    already moved FORWARD. That negative refill rewinds `ts` and debits the bucket
    for an attempt that was REJECTED, breaking "rejected attempts cost nothing".
    With the clock read outside the lock this interleaving produces:

        A admitted=True  -> level=  146.667  ts=base+1.0
        B admitted=False -> level=-1520.000  ts=base       <-- both wrong

    A frozen clock cannot reach this and the plain concurrency test above does not
    either (CPython's switch interval never preempts that short a critical
    section), so the interleaving is driven explicitly.
    """
    budget, _, _ = _drained_bucket()
    base = budget["ts"]
    reading = threading.Event()  # B is inside the gate and has read the clock
    a_done = threading.Event()  # A has finished its admit
    out = {}

    def b_clock():
        """B's clock: announce the read, then give A every chance to go first.
        With the read inside the lock B is HOLDING the lock here, so A cannot
        proceed and this wait just times out -- which is the correct outcome, not
        a hang."""
        reading.set()
        a_done.wait(RACE_WAIT_S)
        return base

    def admit_b():
        out["b"] = _admit(budget, b_clock)[0]

    def admit_a():
        out["a"] = _admit(budget, lambda: base + 1.0)[0]
        a_done.set()

    b = threading.Thread(target=admit_b)
    b.start()
    assert reading.wait(10), "B never reached the gate"
    a = threading.Thread(target=admit_a)
    a.start()
    for t in (a, b):
        t.join(timeout=30)
        assert not t.is_alive()

    assert out["b"] is None  # B had no room on either code path
    assert out["a"] == {"est": EST}  # A's one second of regeneration bought room
    assert budget["ts"] == base + 1.0  # ts never moved backward
    assert budget["level"] >= 0
    # Only A's debit landed: B's rejection cost the bucket nothing.
    assert budget["level"] == pytest.approx(LEFTOVER + REGEN_PER_S - EST)


@pytest.mark.parametrize("epoch", CLOCK_EPOCHS)
def test_starting_level_is_an_argument(epoch):
    """The hook for bead 386's empty-starting-bucket evaluation: level=0 rejects
    immediately, then admits once a whole estimate has regenerated. Nothing about
    the gate changes -- only where it starts. Run at every CLOCK_EPOCHS reading
    for the same boundary reason as the regeneration test above."""
    now, advance = _fake_clock(epoch)
    budget = _new_virtual_budget(BURNDOWN, level=0, now=now)
    assert budget["level"] == 0

    charge, est, used = _admit(budget, now)
    assert charge is None
    assert est == EST
    assert used == CAP

    advance(ONE_EST_REGEN_S + BOUNDARY_EPS)
    charge, _, _ = _admit(budget, now)
    assert charge == {"est": EST}
    assert budget["level"] == pytest.approx(0, abs=TOKEN_TOL)


def test_default_starting_level_is_a_full_bucket():
    """The default the live demo runs with, stated on its own so a change to it
    is a deliberate edit to this assertion rather than a surprise."""
    now, _ = _fake_clock()
    assert _new_virtual_budget(BURNDOWN, now=now)["level"] == CAP == 200_000
