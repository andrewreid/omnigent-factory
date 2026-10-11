"""A dependent's stored blockers catch up when its blocker closes, and epic autopilot then
advances it (live #754 blocked by #900 in epic #878, #852 blocked by #851 in #821: the
blocker closed, the dependent was re-read, yet the note said "waits on" for hours).

The re-read's ack (``RECONCILE_PARCEL``) dropped the read's links, so the dependent kept
its stored "blocker open"; the autopilot pass trusted only that stored state.
"""

from __future__ import annotations

import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import issue_links_from_data
from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.events import EventKind
from omnigent_factory.core.types import IssueLinks, IssueSnapshot, LinkedIssue
from omnigent_factory.service.autopilot import MAX_REREADS_PER_PASS, REREAD_INTERVAL_US
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.links import NativeLinks
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.harness import Harness
from tests.service.test_epic_autopilot import S3, Rig, card, history, on, rig
from tests.service.test_pilot_677 import eventually
from tests.test_epic_autopilot_reducer import S1, S2, epic_links, sub_links

pytestmark = pytest.mark.asyncio

#: Issue #4 (S3) blocks #2 (S1), as #900 blocks #754 and #851 blocks #852.
BLOCKER = 4


def blocked(is_open: bool) -> IssueLinks:
    return sub_links(blockers=(LinkedIssue(BLOCKER, is_open, title="the blocker"),))


def blocked_history() -> Harness:
    """Epic #1 on Full autopilot; #2 stored as waiting on open #4; #4 stored as blocking #2."""
    h = history()
    f = h.f(S1)
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, links=blocked(True))))
    f = h.f(S3)  # issue #4
    blocker_links = IssueLinks(blocking=(LinkedIssue(2, True),))
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, links=blocker_links)))
    return h


def reconcile_ack(snap: IssueSnapshot) -> Ack:
    """What the GitHub adapter acks for a ``RECONCILE_PARCEL`` read (``github.adapter``),
    through JSON as a persisted ack would be."""
    detail = asdict(snap)
    detail["stage"] = None if snap.stage is None else snap.stage.value
    return Ack("read", json.loads(json.dumps(detail)))


async def stored_open_blockers(r: Rig) -> tuple[int, ...]:
    links = (await r.parcel(S1)).links
    assert links is not None
    return tuple(b.number for b in links.open_blockers)


async def test_a_closed_blocker_re_reads_its_dependent_and_autopilot_advances_it(
    service_config: ServiceConfig,
):
    """#754/#900 replay: the blocker's close re-reads the dependent (``on_issue_state``),
    the read's links reach the stored parcel, and the next pass plans the dependent."""
    fresh = snapshot(read_at_us=10**9, links=blocked(False))
    async with rig(on(service_config), blocked_history(), sub_reads={S1: blocked(False)}) as r:
        assert r.cards[2].links is None  # the board read cannot tell: only the stored state
        assert await stored_open_blockers(r) == (BLOCKER,)
        r.github.script(EffectKind.RECONCILE_PARCEL, reconcile_ack(fresh))
        links = NativeLinks(r.service, None, lambda _s: None, r.autopilot.cards)  # type: ignore[arg-type]
        assert await links.on_issue_state(S3) == 1  # #4 closed: its dependent #2

        async def refreshed() -> bool:
            return await stored_open_blockers(r) == ()

        await eventually(refreshed)
        assert await r.autopilot.run_once() == "#1: started #2"
        assert "waits on" not in (await r.parcel("I_epic")).epic_note


async def test_the_pass_trusts_its_fresh_board_read_over_stale_stored_blockers(
    service_config: ServiceConfig,
):
    """The periodic backstop: no re-read ever reached #2, but the pass's board read shows
    #4 closed; #2 is planned on a fresh per-issue read and its stored state re-read."""
    async with rig(on(service_config), blocked_history(), sub_reads={S1: blocked(False)}) as r:
        r.cards[2] = replace(card(2, created=2), links=blocked(False))
        r.github.script(
            EffectKind.RECONCILE_PARCEL,
            reconcile_ack(snapshot(read_at_us=10**9, links=blocked(False))),
        )
        assert await r.autopilot.run_once() == "#1: started #2"
        note = (await r.parcel("I_epic")).epic_note
        assert note == "Epic · 0/2 done · autopilot: planning #2, next #3"
        assert await stored_open_blockers(r) == ()


async def test_a_stale_waiting_sub_issue_is_re_read_bounded_and_throttled(
    service_config: ServiceConfig,
):
    """The board read cannot confirm #2's blockers (unreadable): #2 still waits (fail
    closed) and is re-read at most once per interval."""
    async with rig(on(service_config), blocked_history()) as r:
        assert r.cards[2].links is None
        before = await r.count(EventKind.RECONCILE_DUE)
        await r.autopilot.run_once()
        await r.autopilot.run_once()
        assert await r.count(EventKind.RECONCILE_DUE) == before + 1
        assert (await r.parcel(S1)).autopilot_claim is None  # #3 was started instead
        r.clock.advance(REREAD_INTERVAL_US)
        await r.autopilot.run_once()
        assert await r.count(EventKind.RECONCILE_DUE) == before + 2


async def test_re_reads_per_pass_are_bounded(service_config: ServiceConfig):
    many = tuple(f"I_sub_{n}" for n in range(3, 3 + MAX_REREADS_PER_PASS + 2))
    h = history(subs=(S1, S2, *many))
    for pid in (S1, S2, *many):
        f = h.f(pid)
        h.apply(
            f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, links=blocked(True)))
        )
    numbers = range(2, 4 + MAX_REREADS_PER_PASS + 2)
    epic = epic_links(*((n, True) for n in numbers))
    f = h.f("I_epic")
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now + 5, links=epic)))
    cards = {n: card(n, created=n) for n in (1, *numbers)}
    async with rig(on(service_config), h, cards=cards) as r:
        before = await r.count(EventKind.RECONCILE_DUE)
        await r.autopilot.run_once()
        assert await r.count(EventKind.RECONCILE_DUE) == before + MAX_REREADS_PER_PASS


async def test_never_planned_on_a_board_read_the_fresh_issue_read_contradicts(
    service_config: ServiceConfig,
):
    """The board read shows #4 closed, the per-issue read (the reducer's evidence) still
    shows it open: nothing is planned (fail closed)."""
    async with rig(on(service_config), blocked_history(), sub_reads={S1: blocked(True)}) as r:
        r.cards[2] = replace(card(2, created=2), links=blocked(False))
        assert await r.autopilot.run_once() == "#1: started #3"  # #2 refused, #3 next
        assert (await r.parcel(S1)).autopilot_claim is None


async def test_a_restart_after_the_refresh_plans_the_dependent_once(
    service_config: ServiceConfig, tmp_path: Path
):
    config = on(service_config)
    async with rig(config, blocked_history(), sub_reads={S1: blocked(False)}) as r:
        r.cards[2] = replace(card(2, created=2), links=blocked(False))
        assert await r.autopilot.run_once() == "#1: started #2"
    async with rig(config, None, sub_reads={S1: blocked(False)}) as r:
        r.cards[2] = replace(card(2, created=2), links=blocked(False))
        await r.autopilot.run_once()
        await r.autopilot.run_once()
        assert await r.count(EventKind.AUTOPILOT_PLAN) == 1


async def test_reconcile_ack_links_decode_and_fail_closed():
    links = blocked(False)
    data: Any = json.loads(json.dumps(asdict(links)))
    assert issue_links_from_data(data) == links
    assert issue_links_from_data(None) is None
    assert issue_links_from_data({"blocked_by": [{"number": "x"}]}) is None
