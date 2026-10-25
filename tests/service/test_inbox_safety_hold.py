"""Adding a card to the board is an observation, never a stop (the #716 Inbox stall).

The project's "Item added" workflow sets Status to Inbox with ``from: null``. That write
once read as a leftward move: every new issue got a safety hold nothing cleared, and
auto-triage counted all of them as known to the factory (``eligible_inbox=0``).
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Event, EventKind, Provenance
from omnigent_factory.core.types import Hold, Lifecycle, Parcel, SessionKind, Stage, Via
from omnigent_factory.service.auto_triage import (
    EXCLUSIONS,
    eligible_candidates,
    known_to_factory,
)
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness
from tests.github.test_webhook_config_setup import OPTION_IDS, project_drag, resolve_drag
from tests.service.test_auto_triage import DAY, NOW, auto_config, issue, rig, triage_running

#: ``github-project-automation[bot]``: the actor of the project's built-in workflows.
PROJECT_AUTOMATION = 113052574


def item_added(*, to: str = "Inbox", actor: int = PROJECT_AUTOMATION, absent: bool = False):
    """The Status write that adds a card to the board, as #716's delivery carried it."""
    data = project_drag(actor=actor, new=to)
    field = data["changes"]["field_value"]
    if absent:
        del field["from"]
    else:
        field["from"] = None
    field["to"] = {"id": OPTION_IDS[to], "name": to}
    return data


def stale_hold(h: Harness, pid: str) -> None:
    """A safety hold on an untouched Inbox card, as the old translation left it."""
    h.send(
        pid,
        ev.LeftwardMove(from_stage=Stage.TRIAGED, to_stage=Stage.INBOX),
        actor=PROJECT_AUTOMATION,
    )
    p = h.p(pid)
    assert p.holds == frozenset({Hold.SAFETY}) and p.stage == Stage.INBOX


# ------------------------------------------------------------------ webhook


@pytest.mark.asyncio
@pytest.mark.parametrize("absent", [False, True])
async def test_item_added_to_the_board_is_an_observation(absent: bool):
    (event,) = (await resolve_drag(item_added(absent=absent))).events
    assert isinstance(event.body, ev.ColumnObserved)
    assert event.body.stage == Stage.INBOX


@pytest.mark.asyncio
@pytest.mark.parametrize("actor", [PROJECT_AUTOMATION, 114979, 222])
async def test_first_status_write_with_no_prior_column_is_an_observation(actor: int):
    (event,) = (await resolve_drag(item_added(to="Triaged", actor=actor))).events
    assert isinstance(event.body, ev.ColumnObserved)
    assert event.body.stage == Stage.TRIAGED


@pytest.mark.asyncio
@pytest.mark.parametrize("column", ["Inbox", "Triaged", "Building"])
async def test_a_write_of_the_column_the_card_is_in_is_an_observation(column: str):
    (event,) = (await resolve_drag(project_drag(old=column, new=column))).events
    assert isinstance(event.body, ev.ColumnObserved)
    assert event.body.stage == Stage(column)


@pytest.mark.asyncio
async def test_a_real_leftward_drag_is_still_a_leftward_move():
    (event,) = (await resolve_drag(project_drag(old="Triaged", new="Inbox"))).events
    assert isinstance(event.body, ev.LeftwardMove)
    assert event.body.from_stage == Stage.TRIAGED


# ------------------------------------------------------------------ reducer


@pytest.mark.asyncio
async def test_replay_716_item_added_leaves_no_hold_and_the_issue_eligible():
    """#716: LeftwardMove (webhook, item added), RelatedMarked, GitHubSnapshot."""
    (added,) = (await resolve_drag(item_added())).events
    h = Harness()
    r = h.apply(added)
    assert r.audit.accepted, r.audit.reason
    pid = added.parcel_id
    assert pid is not None
    h.send(pid, ev.RelatedMarked(source_issue=656, relation="overlaps"))
    h.eligible(pid)
    p = h.p(pid)
    assert p.holds == frozenset() and p.stage == Stage.INBOX
    assert not known_to_factory(p)
    board = [issue(716, node_id=pid)]
    assert [i.number for i in eligible_candidates(board, {pid: p}, now_us=NOW, min_age_us=DAY)] == [
        716
    ]


def test_a_legacy_leftward_move_with_no_column_anywhere_sets_no_hold():
    """A stored delivery the old translation turned into ``LeftwardMove(from=None)``."""
    h = Harness()
    r = h.send(
        "A", ev.LeftwardMove(from_stage=None, to_stage=Stage.INBOX), actor=PROJECT_AUTOMATION
    )
    assert r.audit.accepted, r.audit.reason
    assert h.p("A").holds == frozenset() and h.p("A").stage == Stage.INBOX


def owner_drag_back(h: Harness, pid: str, body: ev.EventBody) -> None:
    from omnigent_factory.testing.builders import OWNER_ID

    r = h.send(pid, body, actor=OWNER_ID)
    assert r.audit.accepted, r.audit.reason


@pytest.mark.parametrize(
    "body",
    [
        ev.LeftwardMove(from_stage=Stage.TRIAGED, to_stage=Stage.INBOX),
        # The column is known here even when the delivery names none.
        ev.LeftwardMove(from_stage=None, to_stage=Stage.INBOX),
        # A first Status write (now an observation) to Inbox of a card known elsewhere.
        ev.ColumnObserved(stage=Stage.INBOX),
    ],
)
def test_an_owner_drag_back_to_inbox_of_a_card_with_a_session_still_stops(body: ev.EventBody):
    h = Harness()
    h.eligible("A")
    h.send("A", ev.RequestTriage(via=Via.DRAG))
    h.create_ok("A")
    run = h.cur("A")
    assert run.kind == SessionKind.TRIAGE and run.lifecycle == Lifecycle.ACTIVE
    owner_drag_back(h, "A", body)
    p = h.p("A")
    assert Hold.SAFETY in p.holds and p.stage == Stage.INBOX
    assert p.session(run.session_id).lifecycle == Lifecycle.DRAINING  # type: ignore[union-attr]
    assert known_to_factory(p)


def test_auto_triage_takes_an_issue_whose_only_hold_is_stale():
    h = Harness()
    h.eligible("A")
    stale_hold(h, "A")
    f = h.f("A")
    r = h.apply(f.make(ev.AutoTriage(), evidence=snapshot(stage=Stage.INBOX, read_at_us=f.now + 1)))
    assert r.audit.accepted, r.audit.reason
    p = h.p("A")
    assert Hold.SAFETY not in p.holds and p.stage == Stage.TRIAGED
    assert [a.kind for a in p.authorizations] == [SessionKind.TRIAGE]


def test_auto_triage_still_refuses_a_held_issue_the_factory_worked_on():
    h = Harness()
    h.eligible("A")
    h.send("A", ev.RequestTriage(via=Via.DRAG))
    h.create_ok("A")
    owner_drag_back(h, "A", ev.LeftwardMove(from_stage=Stage.TRIAGED, to_stage=Stage.INBOX))
    f = h.f("A")
    r = h.apply(f.make(ev.AutoTriage(), evidence=snapshot(stage=Stage.INBOX, read_at_us=f.now + 1)))
    assert not r.audit.accepted and r.audit.reason == "auto-triage-issue-known"
    assert Hold.SAFETY in h.p("A").holds


def test_stale_hold_repair_event_is_refused_for_any_parcel_with_work():
    h = Harness()
    h.eligible("A")
    h.send("A", ev.RequestTriage(via=Via.DRAG))
    h.create_ok("A")
    owner_drag_back(h, "A", ev.LeftwardMove(from_stage=Stage.TRIAGED, to_stage=Stage.INBOX))
    r = h.send("A", ev.StaleHoldCleared(), provenance=Provenance.SCHEDULER)
    assert not r.audit.accepted and r.audit.reason == "stale-hold-not-stale"
    for provenance in (Provenance.WEBHOOK, Provenance.OPERATOR, Provenance.ADAPTER):
        h.eligible("B")
        stale_hold(h, "B")
        r = h.send("B", ev.StaleHoldCleared(), provenance=provenance)
        assert not r.audit.accepted


# ------------------------------------------------------------------ service


async def seed(r: Any, parcel_id: str, body: ev.EventBody, **kw: Any) -> None:
    now = r.clock.now_utc_us()
    result = await r.service.apply_event(
        Event(
            event_id=f"seed:{parcel_id}:{body.KIND.value}:{now}",
            repo_id=r.service.config.repo_id,
            parcel_id=parcel_id,
            source_time_us=now,
            provenance=kw.pop("provenance", Provenance.WEBHOOK),
            body=body,
            actor_id=kw.pop("actor", PROJECT_AUTOMATION),
            **kw,
        )
    )
    assert result.accepted, result.reason
    r.clock.advance(1_000_000)


@pytest.mark.asyncio
async def test_repair_clears_only_stale_inbox_holds_and_is_idempotent(
    service_config: ServiceConfig,
):
    async with rig(auto_config(service_config)) as r:
        # #3 gets a real triage run, then is dragged back to Inbox: it keeps its stop.
        r.inbox(3)
        assert await r.triager.run_once() == "started #3"
        await triage_running(r, "I_auto_3")
        await seed(r, "I_auto_3", ev.LeftwardMove(from_stage=Stage.TRIAGED, to_stage=Stage.INBOX))
        # #4 and #5: untouched Inbox cards with the old item-added hold.
        for pid in ("I_auto_4", "I_auto_5"):
            await seed(r, pid, ev.LeftwardMove(from_stage=Stage.TRIAGED, to_stage=Stage.INBOX))
        # #6: an untouched card held while left of nothing (Scoped): not Inbox, kept.
        await seed(r, "I_auto_6", ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.SCOPED))
        before = {pid: await r.parcel(pid) for pid in ("I_auto_3", "I_auto_4", "I_auto_6")}
        assert all(p is not None and Hold.SAFETY in p.holds for p in before.values())

        assert await r.triager.repair_stale_holds() == 2
        held = await r.parcel("I_auto_3")
        assert held is not None and Hold.SAFETY in held.holds and held.sessions
        for pid in ("I_auto_4", "I_auto_5"):
            p = await r.parcel(pid)
            assert p is not None and p.holds == frozenset() and p.stage == Stage.INBOX
        kept = await r.parcel("I_auto_6")
        assert kept is not None and kept.holds == frozenset({Hold.SAFETY})

        assert await r.triager.repair_stale_holds() == 0  # idempotent
        rows = await r.service.db.call(
            lambda store: store.query(
                "SELECT parcel_id, provenance, accepted FROM events WHERE kind = ? "
                "ORDER BY sequence",
                (EventKind.STALE_HOLD_CLEARED.value,),
            )
        )
        assert [tuple(row) for row in rows] == [
            ("I_auto_4", "scheduler", 1),
            ("I_auto_5", "scheduler", 1),
        ]


@pytest.mark.asyncio
async def test_startup_runs_the_stale_hold_repair(service_config: ServiceConfig):
    class Triager:
        repaired = 0

        async def repair_stale_holds(self) -> int:
            self.repaired += 1
            return 0

        async def run_once(self) -> str:
            return "disabled"

    service = FactoryService(service_config, clock=FakeClock())
    triager = Triager()
    service.auto_triager = triager
    await service.start()
    try:
        assert triager.repaired == 1
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_status_breaks_down_why_inbox_issues_are_left_out(service_config: ServiceConfig):
    async with rig(auto_config(service_config)) as r:
        r.inbox(3, 4)
        r.board.extend(
            [
                issue(5, assigned=True),
                issue(6, labels=("factory:skip",)),
                issue(7, created_at_us=NOW - DAY // 2),
                issue(8),
                issue(9, stage=Stage.TRIAGED),  # not an Inbox issue: not counted
            ]
        )
        await seed(r, "I_auto_4", ev.LeftwardMove(from_stage=Stage.TRIAGED, to_stage=Stage.INBOX))
        r.github.snapshots["I_auto_8"] = snapshot(stage=Stage.INBOX)
        assert await r.triager.run_once() == "started #3"
        await triage_running(r, "I_auto_3")
        await seed(r, "I_auto_8", ev.Stop(), actor=114979, evidence=snapshot(stage=Stage.INBOX))
        status = await r.service.operator_command("auto-triage", {"action": "status"})
        # #4's stale hold counts as eligible; #3 runs; #8 was stopped by the owner.
        assert status["eligible_inbox"] == 1
        assert status["next_candidate"] == {"issue": 4, "title": "Issue 4"}
        assert status["excluded_inbox"] == {
            "assigned": 1,
            "skip_label": 1,
            "too_new": 1,
            "known_to_factory": 2,
            "refused_or_parked": 0,
        }
        assert tuple(status["excluded_inbox"]) == EXCLUSIONS  # type: ignore[arg-type]
        _ = json.dumps(status)


def test_a_stale_hold_alone_is_not_known_to_the_factory():
    h = Harness()
    h.eligible("A")
    stale_hold(h, "A")
    p: Parcel = h.p("A")
    assert not known_to_factory(p)
    assert known_to_factory(Parcel(parcel_id="B", repo_id="R", holds=frozenset({Hold.STOPPED})))
    worked = Parcel(
        parcel_id="C",
        repo_id="R",
        holds=frozenset({Hold.SAFETY}),
        pending_authorization_id="auth-1",
    )
    assert known_to_factory(worked)
