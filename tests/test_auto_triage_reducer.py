"""Idle-time auto-triage, triage slots and related marks in the pure reducer."""

from __future__ import annotations

from dataclasses import replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.admission import ADMISSION
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import EventKind, Provenance
from omnigent_factory.core.projection import project_note, triage_queued_note
from omnigent_factory.core.types import (
    BotState,
    Hold,
    Lifecycle,
    RelatedMark,
    SessionKind,
    Size,
    Stage,
    Via,
)
from omnigent_factory.service.auto_triage import busy_reason
from omnigent_factory.testing.builders import OWNER_ID, config, result_candidate, snapshot
from omnigent_factory.testing.harness import Harness


def auto(h: Harness, pid: str, **snap: object):
    """The clock's AutoTriage for ``pid`` with a fresh Inbox read."""
    f = h.f(pid)
    fields = {"stage": Stage.INBOX, "read_at_us": f.now + 1, **snap}
    return h.apply(f.make(ev.AutoTriage(), evidence=snapshot(**fields)))  # type: ignore[arg-type]


def running_triage(h: Harness, pid: str) -> None:
    h.eligible(pid)
    h.send(pid, ev.RequestTriage(via=Via.DRAG))
    h.create_ok(pid)
    assert h.cur(pid).lifecycle == Lifecycle.ACTIVE


def finish_triage(h: Harness, pid: str) -> None:
    s = h.cur(pid)
    assert s.root_id is not None
    h.send(pid, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.TRIAGE))
    h.quiesce(pid, s.session_id)


# ----------------------------------------------------------------- admission


def test_auto_triage_is_admitted_only_from_the_trusted_clock():
    assert ADMISSION[EventKind.AUTO_TRIAGE].provenances == frozenset({Provenance.SCHEDULER})
    h = Harness()
    f = h.f("A")
    for provenance in (Provenance.WEBHOOK, Provenance.OPERATOR, Provenance.ADAPTER):
        r = h.apply(
            f.make(
                ev.AutoTriage(),
                provenance=provenance,
                actor=OWNER_ID,
                evidence=snapshot(stage=Stage.INBOX, read_at_us=f.now + 1),
            )
        )
        assert not r.audit.accepted and r.audit.reason == "provenance-not-admitted"
    assert h.p("A").authorizations == ()


# ----------------------------------------------------------------- start


def test_auto_triage_starts_triage_like_an_inbox_to_triage_drag():
    h = Harness()
    r = auto(h, "A")
    assert r.audit.accepted, r.audit.reason
    # The daemon moves the card (no owner drag put it there), then triage runs.
    [move] = h.of(r, EffectKind.MOVE_CARD)
    assert move.args == {"to": Stage.TRIAGED.value, "expected_from": Stage.INBOX.value}
    p = h.p("A")
    [auth] = p.authorizations
    assert auth.kind == SessionKind.TRIAGE and auth.source_event_id == r.audit.event_id
    assert p.stage == Stage.TRIAGED and p.current_session is not None  # move acked
    s = h.create_ok("A")
    assert s.kind == SessionKind.TRIAGE and s.lifecycle == Lifecycle.ACTIVE
    assert h.p("A").bot == BotState.WORKING
    finish_triage(h, "A")
    p = h.p("A")
    # The normal triage outcome: published, Idle, never past Triage.
    assert p.stage == Stage.TRIAGED and p.bot == BotState.IDLE
    assert p.pending_authorization_id is None
    assert Hold.PUBLICATION_PENDING in p.holds


def test_the_bot_card_move_is_not_a_leftward_owner_drag():
    h = Harness()
    auto(h, "A")
    f = h.f("A")
    # Webhook echo of the bot's own move (non-owner) and a later fresh read: no stop.
    h.apply(f.make(ev.ColumnObserved(stage=Stage.TRIAGED), provenance=Provenance.WEBHOOK))
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(stage=Stage.TRIAGED, read_at_us=f.now)))
    p = h.p("A")
    assert not p.holds & {Hold.STOPPED, Hold.SAFETY}
    assert p.pending_authorization_id is not None or p.current_session is not None


@pytest.mark.parametrize(
    ("snap", "reason"),
    [
        ({"human_assigned": True}, "parcel-not-dispatchable"),
        ({"open": False}, "parcel-not-dispatchable"),
        ({"in_project": False}, "parcel-not-dispatchable"),
        ({"stage": Stage.TRIAGED}, "auto-triage-not-in-inbox"),
        ({"stage": Stage.BUILDING}, "auto-triage-not-in-inbox"),
    ],
)
def test_auto_triage_refuses_an_ineligible_or_non_inbox_issue(snap, reason):
    h = Harness()
    r = auto(h, "A", **snap)
    assert not r.audit.accepted and r.audit.reason == reason
    assert h.of(r, EffectKind.MOVE_CARD) == []
    assert h.p("A").authorizations == ()


def test_auto_triage_needs_a_fresh_read():
    h = Harness()
    r = h.apply(h.f("A").make(ev.AutoTriage(), evidence=None))
    assert not r.audit.accepted and r.audit.reason == "auto-triage-without-fresh-read"


def test_auto_triage_never_takes_an_issue_the_factory_worked_on():
    h = Harness()
    h.triage("A")
    f = h.f("A")
    # The owner dragged it back to Inbox afterwards (a stop): known, never auto-taken.
    h.apply(f.make(ev.LeftwardMove(from_stage=Stage.TRIAGED, to_stage=Stage.INBOX)))
    r = auto(h, "A", read_at_us=f.now + 5)
    assert not r.audit.accepted and r.audit.reason == "auto-triage-issue-known"


def test_auto_triage_respects_pause_and_holds():
    h = Harness()
    h.admission = replace(h.admission, paused=True)
    r = auto(h, "A")
    assert not r.audit.accepted and r.audit.reason == "paused"
    h.admission = replace(h.admission, paused=False)
    h.apply(
        h.f("B").make(ev.InboxHoldSet(delivery_guid="g-1"), evidence=snapshot(stage=Stage.INBOX))
    )
    r = auto(h, "B")
    assert not r.audit.accepted  # a parked/unresolved delivery holds the parcel


def test_auto_triage_refused_while_a_queued_build_can_start():
    h = Harness()
    h.plan_published("C")
    h.approve("C")  # approved, queued (not yet admitted), a build slot free
    assert h.admission.queue and h.admission.building_count == 0
    r = auto(h, "A")
    assert not r.audit.accepted and r.audit.reason == "auto-triage-factory-busy"


def test_a_queued_build_without_a_free_pr_slot_does_not_refuse_auto_triage():
    h = Harness(cfg=config(max_open_bot_prs=0))
    h.plan_published("C")
    h.approve("C")  # queued, a build slot free, but no open-PR slot: it cannot start now
    assert h.admission.queue_entry("C") is not None and h.admission.building_count == 0
    r = auto(h, "A")
    assert r.audit.accepted, r.audit.reason


def test_a_held_build_slot_alone_does_not_refuse_auto_triage():
    """Whether a run works is the service's idle check; admission only shows the slot."""
    h = Harness()
    h.to_building("C")
    h.plan_published("D")
    h.approve("D")  # queued behind C: no build slot free, so it cannot start now
    assert h.admission.building_count == 1 and h.admission.queue_entry("D") is not None
    r = auto(h, "A")
    assert r.audit.accepted, r.audit.reason
    assert h.p("A").stage == Stage.TRIAGED


def test_a_running_auto_triage_finishes_when_a_build_arrives():
    h = Harness()
    auto(h, "A")
    triage = h.create_ok("A")
    h.to_building("C")  # a build is approved and admitted while the triage runs
    assert h.admission.building_count == 1
    s = h.cur("A")
    assert s.session_id == triage.session_id and s.lifecycle == Lifecycle.ACTIVE
    assert not s.fences
    finish_triage(h, "A")
    assert h.cur("A").lifecycle == Lifecycle.RETIRED
    assert Hold.PUBLICATION_PENDING in h.p("A").holds  # the triage was accepted
    # ... and no new auto-triage starts while the build runs (the service's idle check).
    assert busy_reason([h.p("C")]) == "build running on #2"


# ----------------------------------------------------------------- triage slots


def test_owner_triage_beyond_the_concurrency_waits_for_a_slot():
    h = Harness()
    running_triage(h, "A")
    h.eligible("B")
    r = h.send("B", ev.RequestTriage(via=Via.DRAG))
    assert r.audit.accepted
    p = h.p("B")
    # Recorded and queued, not dropped: card in Triage, no run yet.
    assert p.stage == Stage.TRIAGED and p.pending_authorization_id is not None
    assert p.current_session is None
    assert h.of(r, EffectKind.CREATE_SESSION) == [] and h.of(r, EffectKind.PREPARE_SESSION) == []
    assert p.note == triage_queued_note() and project_note(p, p.bot) == triage_queued_note()
    # A later event still finds every slot taken.
    r = h.send("B", ev.ReconcileDue())
    assert h.p("B").current_session is None
    # The running triage finishes: the waiting one starts on its next event.
    finish_triage(h, "A")
    r = h.send("B", ev.ReconcileDue())
    assert h.of(r, EffectKind.CREATE_SESSION)
    assert h.cur("B").kind == SessionKind.TRIAGE


def test_triage_concurrency_two_runs_both():
    h = Harness(cfg=config(triage_concurrency=2))
    running_triage(h, "A")
    running_triage(h, "B")
    h.eligible("C")
    h.send("C", ev.RequestTriage(via=Via.DRAG))
    assert h.p("C").current_session is None  # third one waits


def test_auto_triage_refused_while_every_triage_slot_is_taken():
    h = Harness()
    running_triage(h, "A")
    r = auto(h, "B")
    assert not r.audit.accepted and r.audit.reason == "auto-triage-slot-busy"


def test_a_triage_waiting_on_the_owner_frees_its_slot():
    h = Harness()
    running_triage(h, "A")
    s = h.cur("A")
    h.send("A", ev.OwnerQuestion(session_id=s.session_id, question_key="q-1", summary="?"))
    assert h.cur("A").lifecycle == Lifecycle.WAITING
    h.eligible("B")
    h.send("B", ev.RequestTriage(via=Via.DRAG))
    assert h.p("B").current_session is not None


# ----------------------------------------------------------------- related marks


def mark(h: Harness, pid: str, source: int, relation: str, **kw: object):
    return h.send(pid, ev.RelatedMarked(source_issue=source, relation=relation), **kw)


def test_related_mark_sets_the_note_without_a_comment():
    h = Harness()
    h.eligible("B")
    r = mark(h, "B", 12, "overlaps")
    assert r.audit.accepted
    assert h.of(r, EffectKind.POST_COMMENT) == []
    [note] = h.of(r, EffectKind.SET_NOTE)
    assert note.args == {"note": "Related: #12 (overlap)"}
    r = mark(h, "B", 9, "conflicts")
    [note] = h.of(r, EffectKind.SET_NOTE)
    assert note.args == {"note": "Related: #9 (conflict), #12 (overlap)"}
    # The same mark again changes nothing; a new relation from #12 replaces its old one.
    assert not mark(h, "B", 9, "conflicts").audit.accepted
    mark(h, "B", 12, "duplicate")
    assert h.p("B").related_marks == (RelatedMark(9, "conflicts"), RelatedMark(12, "duplicate"))


def test_related_mark_never_clobbers_a_more_important_note():
    h = Harness()
    running_triage(h, "A")
    s = h.cur("A")
    h.send("A", ev.OwnerQuestion(session_id=s.session_id, question_key="q-1", summary="?"))
    p = h.p("A")
    assert p.bot == BotState.NEEDS_YOU
    before = p.board_note
    r = mark(h, "A", 12, "overlaps")
    assert r.audit.accepted and h.of(r, EffectKind.SET_NOTE) == []
    assert h.p("A").board_note == before and before.startswith("Needs you")
    # A status reason (refused command, queue position, ...) outranks it too.
    h2 = Harness()
    h2.eligible("B")
    h2.send("B", ev.ApprovePlan(via=Via.COMMAND))  # refused: no plan to approve
    refused = h2.p("B").board_note
    assert refused.startswith("Command refused:")
    r = mark(h2, "B", 12, "overlaps")
    assert r.audit.accepted and h2.of(r, EffectKind.SET_NOTE) == []
    assert h2.p("B").board_note == refused


def test_related_marks_clear_when_the_card_moves_on():
    h = Harness()
    h.eligible("B")
    h.apply(h.f("B").make(ev.GitHubSnapshot(), evidence=snapshot(stage=Stage.INBOX)))
    mark(h, "B", 12, "overlaps")
    assert h.p("B").board_note == "Related: #12 (overlap)"
    h.send("B", ev.RequestTriage(via=Via.DRAG))
    p = h.p("B")
    assert p.related_marks == () and not p.board_note.startswith("Related")


@pytest.mark.parametrize(("source", "relation"), [(0, "overlaps"), (12, "nope"), (1, "blocks")])
def test_invalid_related_marks_are_refused(source, relation):
    h = Harness()
    h.eligible("I_parcel_1")  # issue #1
    r = mark(h, "I_parcel_1", source, relation)
    assert not r.audit.accepted and r.audit.reason == "invalid-related-mark"


def test_related_marks_come_only_from_the_daemon():
    assert ADMISSION[EventKind.RELATED_MARKED].provenances == frozenset({Provenance.ADAPTER})
    h = Harness()
    r = mark(h, "B", 12, "overlaps", provenance=Provenance.WEBHOOK)
    assert not r.audit.accepted and r.audit.reason == "provenance-not-admitted"


def test_triage_block_is_size_s_like_an_owner_triage():
    h = Harness()
    auto(h, "A")
    [auth] = h.p("A").authorizations
    assert auth.grant_duration_us == h.cfg.block_us(Size.S)


def test_auto_triage_clears_marks_of_a_parcel_first_seen_in_inbox():
    h = Harness()
    mark(h, "A", 12, "overlaps")  # first event for the issue: no column known yet
    assert h.p("A").related_marks
    auto(h, "A")
    assert h.p("A").stage == Stage.TRIAGED and h.p("A").related_marks == ()
