"""Close-candidate backfill: a triage published before ``factory:close-candidate``
existed gets its label once from the stale-triage scan (``CloseCandidateBackfill``),
bound to that triage's generation like a publication; and the triage text says where the
label stands (pending, labelled, removed by the owner) instead of assuming it."""

from __future__ import annotations

from functools import partial
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import EventKind
from omnigent_factory.core.protocol import CLOSE_CANDIDATE_LABEL
from omnigent_factory.core.types import Parcel, Stage
from omnigent_factory.service.auto_retriage import MAX_BACKFILLS_PER_PASS, AutoRetriager
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import (
    LABEL_ADDED,
    LABEL_LEFT_TRIAGE,
    LABEL_OWNER_REMOVED,
    LABEL_PENDING,
    close_candidate_label,
    stage_result_text,
)
from omnigent_factory.testing.builders import EventFactory
from tests.service.test_auto_retriage import Retriage, retriage_rig
from tests.service.test_comment_style import FORBIDDEN
from tests.service.test_pilot_677 import eventually

pytestmark = pytest.mark.asyncio


async def _label_rows(t: Retriage, pid: str) -> list[tuple[str, str, str]]:
    rows = await t.r.service.db.call(
        lambda store: store.query(
            "SELECT json_extract(payload_json, '$.args.action'), "
            "json_extract(payload_json, '$.args.session_id'), state FROM effects "
            "WHERE parcel_id = ? AND kind = ? ORDER BY rowid",
            (pid, EffectKind.SET_CLOSE_CANDIDATE.value),
        )
    )
    return [(str(r[0]), str(r[1]), str(r[2])) for r in rows]


async def _backfills(t: Retriage, pid: str) -> int:
    rows = await t.r.service.db.call(
        lambda store: store.query(
            "SELECT COUNT(*) FROM events WHERE parcel_id = ? AND kind = ? AND accepted = 1",
            (pid, EventKind.CLOSE_CANDIDATE_BACKFILL.value),
        )
    )
    return int(rows[0][0])


async def _set(t: Retriage, pid: str, **fields: Any) -> None:
    """Rewrite stored aggregate fields (a parcel as an older daemon left it)."""
    for name, value in fields.items():
        await t.r.service.db.call(
            lambda store, name=name, value=value: store.query(
                "UPDATE parcels SET aggregate_json = "
                f"json_set(aggregate_json, '$.parcel.{name}', ?) WHERE parcel_id = ?",
                (value, pid),
            )
        )


async def _predeploy(t: Retriage, number: int, recommendation: str = "wont_fix") -> str:
    """A Triaged card whose triage was published before the label existed: no label
    generation and no label effect."""
    pid = await t.triaged(number, recommendation=recommendation)
    await eventually(lambda: bool(t.r.github.executed(EffectKind.SET_CLOSE_CANDIDATE)))
    await t.r.service.db.call(
        lambda store: store.query(
            "DELETE FROM effects WHERE parcel_id = ? AND kind = ?",
            (pid, EffectKind.SET_CLOSE_CANDIDATE.value),
        )
    )
    await _set(t, pid, close_candidate_session="", close_candidate_at_us=0)
    t.r.github.calls.clear()
    parcel = await t.parcel(pid)
    assert parcel.close_candidate_session == "" and await _label_rows(t, pid) == []
    return pid


async def _label(t: Retriage, parcel: Parcel, sid: str) -> str:
    state: str = await t.r.service.db.call(
        partial(close_candidate_label, parcel=parcel, session_id=sid)
    )
    return state


def _adds(t: Retriage) -> list[str]:
    return [
        e.args["session_id"]
        for e in t.r.github.executed(EffectKind.SET_CLOSE_CANDIDATE)
        if e.args["action"] == "triage"
    ]


# ------------------------------------------------------------------ backfill


async def test_a_predeploy_closure_triage_gets_exactly_one_label_add(
    service_config: ServiceConfig,
):
    async with retriage_rig(service_config) as t:
        pid = await _predeploy(t, 971)
        sid = t.directory.fresh[pid].session_id
        before = await t.parcel(pid)
        assert await _label(t, before, sid) == LABEL_PENDING

        await t.retriager.scan(force=True)
        await eventually(lambda: _adds(t) == [sid])

        async def landed() -> bool:
            return await _label_rows(t, pid) == [("triage", sid, "done")]

        await eventually(landed)
        parcel = await t.parcel(pid)
        assert parcel.close_candidate_session == sid
        assert await _label(t, parcel, sid) == LABEL_ADDED
        # Later passes (and a forced status scan) add nothing.
        for _ in range(3):
            await t.retriager.scan(force=True)
        assert await t.retriager.run_once() == "disabled"
        assert _adds(t) == [sid]
        assert await _label_rows(t, pid) == [("triage", sid, "done")]
        assert await _backfills(t, pid) == 1


async def test_a_restart_never_backfills_again(service_config: ServiceConfig):
    async with retriage_rig(service_config) as t:
        pid = await _predeploy(t, 971)
        sid = t.directory.fresh[pid].session_id
        await t.retriager.scan(force=True)
        await eventually(lambda: _adds(t) == [sid])
        # A new pass object (no memory, as after a restart) reads only the stored state.
        fresh_pass = AutoRetriager(
            t.r.service,
            t.retriager.board,
            t.retriager.snapshot,
            directory=t.directory,  # type: ignore[arg-type]
            workspaces=t.workspaces,  # type: ignore[arg-type]
            triager=t.r.triager,
        )
        await fresh_pass.scan(force=True)
        assert _adds(t) == [sid]
        assert len(await _label_rows(t, pid)) == 1
        # The same backfill event again, or a new one for the issued generation: no-op.
        parcel = await t.parcel(pid)
        factory = EventFactory(pid, issue_number=971, start_us=t.r.clock.now_utc_us())
        again = await t.r.service.apply_event(
            factory.make(
                ev.CloseCandidateBackfill(session_id=sid),
                provenance=ev.Provenance.ADAPTER,
                event_id=f"{pid}:backfill-test-1",
            )
        )
        assert not again.accepted and again.reason == "close-candidate-issued"
        assert (await t.parcel(pid)).close_candidate_session == parcel.close_candidate_session
        assert len(await _label_rows(t, pid)) == 1


async def test_an_owner_removal_of_that_generation_is_never_backfilled(
    service_config: ServiceConfig,
):
    async with retriage_rig(service_config) as t:
        pid = await _predeploy(t, 971)
        sid = t.directory.fresh[pid].session_id
        await _set(t, pid, close_candidate_removed=sid)
        await t.retriager.scan(force=True)
        assert _adds(t) == [] and await _label_rows(t, pid) == []
        assert await _label(t, await t.parcel(pid), sid) == LABEL_OWNER_REMOVED
        factory = EventFactory(pid, issue_number=971, start_us=t.r.clock.now_utc_us())
        refused = await t.r.service.apply_event(
            factory.make(
                ev.CloseCandidateBackfill(session_id=sid),
                provenance=ev.Provenance.ADAPTER,
                event_id=f"{pid}:backfill-test-2",
            )
        )
        assert not refused.accepted and refused.reason == "close-candidate-removal-recorded"


async def test_a_non_candidate_triage_needs_nothing(service_config: ServiceConfig):
    async with retriage_rig(service_config) as t:
        pid = await _predeploy(t, 972, recommendation="fix")
        await t.retriager.scan(force=True)
        assert _adds(t) == [] and await _label_rows(t, pid) == []
        assert (await t.parcel(pid)).close_candidate_session == ""


async def test_a_card_not_in_triaged_is_never_backfilled(service_config: ServiceConfig):
    async with retriage_rig(service_config) as t:
        pid = await _predeploy(t, 971)
        sid = t.directory.fresh[pid].session_id
        # The parcel left Triaged; the board read still shows it there (stale).
        await _set(t, pid, stage=Stage.SCOPED.value)
        await t.retriager.scan(force=True)
        assert _adds(t) == [] and await _label_rows(t, pid) == []
        factory = EventFactory(pid, issue_number=971, start_us=t.r.clock.now_utc_us())
        refused = await t.r.service.apply_event(
            factory.make(
                ev.CloseCandidateBackfill(session_id=sid),
                provenance=ev.Provenance.ADAPTER,
                event_id=f"{pid}:backfill-test-3",
            )
        )
        assert not refused.accepted
        assert refused.reason == "close-candidate-backfill-not-triaged"


async def test_an_older_triage_generation_is_refused(service_config: ServiceConfig):
    async with retriage_rig(service_config) as t:
        pid = await _predeploy(t, 971)
        factory = EventFactory(pid, issue_number=971, start_us=t.r.clock.now_utc_us())
        refused = await t.r.service.apply_event(
            factory.make(
                ev.CloseCandidateBackfill(session_id="ss_older"),
                provenance=ev.Provenance.ADAPTER,
                event_id=f"{pid}:backfill-test-4",
            )
        )
        assert not refused.accepted
        assert refused.reason == "close-candidate-backfill-not-newest-triage"


async def test_backfill_is_bounded_per_pass(service_config: ServiceConfig):
    async with retriage_rig(service_config) as t:
        pids = [await _predeploy(t, 900 + n) for n in range(MAX_BACKFILLS_PER_PASS + 1)]
        await t.retriager.scan(force=True)
        issued = [p for p in pids if (await t.parcel(p)).close_candidate_session]
        assert len(issued) == MAX_BACKFILLS_PER_PASS
        await t.retriager.scan(force=True)  # the rest on the next pass
        assert all([(await t.parcel(p)).close_candidate_session for p in pids])


# ------------------------------------------------------------------ wording

WONT_FIX = {
    "kind": "triage",
    "summary": "Out of scope for the timesheets app.",
    "recommendation": "wont_fix",
    "priority": "P3",
    "size": "S",
}


@pytest.mark.parametrize(
    ("label", "line"),
    [
        (
            LABEL_PENDING,
            f"**Close candidate:** label pending (`{CLOSE_CANDIDATE_LABEL}`); the factory "
            "never closes the issue",
        ),
        (
            LABEL_ADDED,
            f"**Close candidate:** labelled `{CLOSE_CANDIDATE_LABEL}`; the factory never "
            "closes the issue",
        ),
        (
            LABEL_OWNER_REMOVED,
            "**Close candidate:** label removed by the owner; the factory never closes the issue",
        ),
        (
            LABEL_LEFT_TRIAGE,
            "**Close candidate:** label removed when the card left Triage; the factory never "
            "closes the issue",
        ),
    ],
)
async def test_the_close_candidate_line_says_where_the_label_stands(label: str, line: str):
    text = stage_result_text(WONT_FIX, close_candidate=label)
    assert line in text
    if label != LABEL_ADDED:
        assert "labelled" not in text
    for bad in FORBIDDEN:
        assert bad not in text, bad


async def test_the_default_never_claims_the_label():
    assert "labelled" not in stage_result_text(WONT_FIX)
    assert "Close candidate" not in stage_result_text({**WONT_FIX, "recommendation": "fix"})
