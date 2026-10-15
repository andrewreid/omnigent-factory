"""Active-time and cost samples are persisted only when they matter.

The observer used to store an ``ActiveTimeSample`` on every pass the estimate moved
(about every 10 s per running build: ~75% of all stored events) and a ``CostSample`` on
every changed total. Active time is now stored rounded up to a step, so the stored value
always covers the estimate (a crash or restart can only over-count) and the grant's limit
is still reached on exactly the pass it was before; cost totals (audit only) are stored
at most every few minutes.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Event
from omnigent_factory.omnigent.tree import NodeState, TreeObservation
from omnigent_factory.service.observer import (
    ACTIVE_TIME_STEP_US,
    COST_SAMPLE_INTERVAL_US,
    OmnigentObserver,
    persisted_active_us,
)
from omnigent_factory.testing.builders import REPO_ID
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
PASS_US = 10 * 1_000_000  # the observer's active cadence


class _Service:
    """Just enough service for the observer: events go through the real reducer."""

    def __init__(self, h: Harness) -> None:
        self.h = h
        self.config = SimpleNamespace(repo_id=REPO_ID)
        self.events: list[Event] = []
        self.db = self

    async def call(self, operation: Any) -> Any:
        return self.h.p(P)

    async def apply_event(self, event: Event, **_: object) -> Any:
        self.events.append(event)
        return self.h.apply(event)

    def samples(self, kind: type) -> list[Any]:
        return [e.body for e in self.events if isinstance(e.body, kind)]


def _observer(service: _Service, clock: FakeClock) -> OmnigentObserver:
    return OmnigentObserver(
        service,  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        clock,
        interval_seconds=10,
    )


def _running_build() -> tuple[Harness, _Service, FakeClock]:
    h = Harness()
    h.to_building()
    clock = FakeClock(start_us=h.f(P).now + 1_000_000)
    return h, _Service(h), clock


async def _drive(h: Harness, service: _Service, clock: FakeClock, passes: int) -> list[int]:
    """Run ``passes`` observer passes over a root that keeps running; returns the pass
    indexes at which the stored grant was spent. Checks the stored value covers the
    estimate after every pass (what a restart would start from)."""
    observer = _observer(service, clock)
    run = h.cur()
    tracker = observer._tracker(run.session_id, run.grant.grant_id, run.grant.consumed_us)
    tracker.observe_status("root", "running")
    spent: list[int] = []
    for i in range(passes):
        clock.advance(PASS_US)
        await observer._activity(h.p(P), tracker)
        stored = h.cur().grant
        assert stored.consumed_us >= tracker.estimate().upper_us  # never under-counted
        if stored.remaining_us == 0:
            spent.append(i)
    return spent


def test_stored_active_time_covers_the_estimate_and_meets_the_limit_exactly():
    duration = 4 * 3600 * 1_000_000
    for actual in (*range(0, 3 * ACTIVE_TIME_STEP_US, 7_000_001), duration - 1, duration):
        value = persisted_active_us(actual, duration)
        assert value >= actual
        assert (value >= duration) == (actual >= duration)  # the limit, never early
        assert value - actual < ACTIVE_TIME_STEP_US or actual >= duration
    assert persisted_active_us(duration + 5, duration) == duration + ACTIVE_TIME_STEP_US
    assert persisted_active_us(0, duration) == 0


@pytest.mark.asyncio
async def test_active_limit_is_reached_on_the_same_pass_with_far_fewer_rows():
    h, service, clock = _running_build()
    duration = h.cur().grant.duration_us
    passes = duration // PASS_US + 3
    spent = await _drive(h, service, clock, passes)
    # Before: the estimate was stored every pass, so the grant was spent on the pass the
    # estimate reached its duration. Still so.
    expected = next(i for i in range(passes) if (i + 1) * PASS_US >= duration)
    assert spent[0] == expected
    rows = service.samples(ev.ActiveTimeSample)
    every_pass = duration // PASS_US  # what the old observer stored
    assert len(rows) <= duration // ACTIVE_TIME_STEP_US + 2
    assert len(rows) * 20 < every_pass


@pytest.mark.asyncio
async def test_a_restart_between_steps_only_over_counts():
    h, service, clock = _running_build()
    await _drive(h, service, clock, 45)  # 7.5 min: one step stored ahead of the estimate
    estimate_before_crash = 45 * PASS_US
    stored = h.cur().grant.consumed_us
    assert stored >= estimate_before_crash and stored - estimate_before_crash < ACTIVE_TIME_STEP_US
    # Crash: no shutdown flush. The new process starts its tracker from the stored value.
    clock.reboot()
    duration = h.cur().grant.duration_us
    passes = (duration - stored) // PASS_US + 3
    spent = await _drive(h, service, clock, passes)
    # The grant is spent no later than an uninterrupted run would spend it.
    assert (spent[0] + 1) * PASS_US + estimate_before_crash <= duration + PASS_US


@pytest.mark.asyncio
async def test_cost_totals_are_stored_at_most_every_interval():
    h, service, clock = _running_build()
    observer = _observer(service, clock)
    run = h.cur()
    assert run.root_id is not None
    passes = 2 * COST_SAMPLE_INTERVAL_US // PASS_US + 1
    for i in range(passes):
        clock.advance(PASS_US)
        root = NodeState(run.root_id, None, "running", total_cost_usd=0.01 * (i + 1))
        await observer._observe(h.p(P), TreeObservation(run.root_id, True, {run.root_id: root}))
    costs = service.samples(ev.CostSample)
    assert len(costs) == 3  # the first total, then one per interval (was: one per pass)
    assert costs[0].microdollars == 10_000
