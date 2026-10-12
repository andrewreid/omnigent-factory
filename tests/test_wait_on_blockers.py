"""Waiting on native blockers is not Blocked, and an approval given while blockers are
open is held until they close (#888).

* A build run whose ``blocked`` result names only other issues (``depends_on``) waits
  once each is an open native blocker: Bot Idle, ``Waiting on #904``, no build slot; a
  fresh read showing every blocker closed wakes it exactly once through admission.
* ``/approve`` in Planning while the issue has open native blockers records the
  approval and waits the same way; the build starts once, through the queue.
* An owner drag (or ``factory:build``) still force-starts with the existing note.
* A blocked result citing anything else, and a stopped or closed issue, are unchanged.
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.projection import project_bot, project_note
from omnigent_factory.core.types import (
    BotState,
    Hold,
    IssueLinks,
    LinkedIssue,
    QueueStatus,
    ReservationKind,
    Stage,
    Via,
)
from omnigent_factory.testing.builders import result_candidate, snapshot
from omnigent_factory.testing.harness import Harness

A = "I_wait_a"
B = "I_wait_b"

OPEN_904 = LinkedIssue(904, True, "identity register")
CLOSED_747 = LinkedIssue(747, False, "topology")
BLOCKED = IssueLinks(blocked_by=(OPEN_904, CLOSED_747))
UNBLOCKED = IssueLinks(blocked_by=(LinkedIssue(904, False), CLOSED_747))
NO_904 = IssueLinks(blocked_by=(CLOSED_747,))


def read(h: Harness, links: IssueLinks | None, pid: str = A, **snap: object):
    f = h.f(pid)
    return h.apply(
        f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, links=links, **snap))  # type: ignore[arg-type]
    )


def control(h: Harness, body: ev.EventBody, links: IssueLinks, pid: str = A, **snap: object):
    f = h.f(pid)
    return h.apply(f.make(body, evidence=snapshot(read_at_us=f.now, links=links, **snap)))  # type: ignore[arg-type]


def planned(h: Harness, links: IssueLinks, pid: str = A) -> None:
    h.plan_published(pid)
    read(h, links, pid, stage=Stage.SCOPED)


def building_with_blocker(h: Harness, pid: str = A) -> None:
    """#888 before the fix: a build started (the owner's drag) with #904 open."""
    planned(h, BLOCKED, pid)
    plan = h.cur(pid)
    r = control(h, ev.ApprovePlan(via=Via.DRAG), BLOCKED, pid, stage=Stage.BUILDING)
    assert r.audit.accepted, r.audit.reason
    h.quiesce(pid, plan.session_id)
    f = h.f(pid)
    h.apply(f.make(ev.CapacityAvailable(), evidence=snapshot(read_at_us=f.now, links=BLOCKED)))
    h.create_ok(pid)


def report_blocked(h: Harness, blocked_on: tuple[int, ...], pid: str = A):
    s = h.cur(pid)
    assert s.root_id is not None
    r = h.send(
        pid,
        result_candidate(
            s.session_id, s.root_id, s.revision, ev.ResultKind.BLOCKED, blocked_on=blocked_on
        ),
    )
    h.quiesce(pid, s.session_id)
    return r


def messages(r, purpose: MessagePurpose) -> list:
    return [
        e
        for e in r.effects
        if e.kind == EffectKind.SEND_MESSAGE and e.args.get("purpose") == purpose.value
    ]


def holds_slot(h: Harness, pid: str = A) -> bool:
    return any(x.parcel_id == pid for x in h.admission.live_reservations(ReservationKind.BUILDING))


def queued(h: Harness, pid: str = A) -> bool:
    entry = h.admission.queue_entry(pid)
    return entry is not None and entry.status == QueueStatus.QUEUED


def assert_waiting(h: Harness, pid: str = A, text: str = "Waiting on #904") -> None:
    p = h.p(pid)
    assert Hold.BLOCKERS in p.holds and Hold.AGENT_BLOCKED not in p.holds, p.holds
    assert p.bot == BotState.IDLE == project_bot(p)
    assert p.board_note == text == project_note(p, p.bot)
    assert not holds_slot(h, pid)


# ------------------------------------------------- 1. a blocked run waits (#888 replay)


def test_888_blocked_on_an_open_native_blocker_waits_and_resumes_once():
    h = Harness()
    building_with_blocker(h)
    run = h.cur(A).session_id
    report_blocked(h, (904,))
    assert_waiting(h)
    assert h.p(A).stage == Stage.BUILDING and h.p(A).current_approval is not None
    # Still open: nothing happens.
    r = read(h, BLOCKED)
    assert not messages(r, MessagePurpose.BLOCKERS_CLOSED)
    assert_waiting(h)
    # #904 closes: one fresh read wakes the same run once and takes the free slot.
    r = read(h, UNBLOCKED)
    [wake] = messages(r, MessagePurpose.BLOCKERS_CLOSED)
    assert wake.preconditions.session_id == run and wake.args["blockers"] == "#904, #747"
    p = h.p(A)
    assert Hold.BLOCKERS not in p.holds and p.current_session_id == run
    assert holds_slot(h) and h.admission.queue_entry(A).status == QueueStatus.RESERVED  # type: ignore[union-attr]
    # Later reads never wake it again.
    for _ in range(2):
        assert not messages(read(h, UNBLOCKED), MessagePurpose.BLOCKERS_CLOSED)


def test_a_dependency_that_becomes_a_native_link_turns_blocked_into_waiting():
    h = Harness()
    building_with_blocker(h)
    read(h, NO_904)  # the native link from the result's depends_on is not written yet
    report_blocked(h, (904,))
    assert Hold.AGENT_BLOCKED in h.p(A).holds and h.p(A).bot == BotState.BLOCKED
    read(h, BLOCKED)  # the link webhook's re-read: #904 now blocks it
    assert_waiting(h)


def test_resume_waits_for_a_free_slot_through_admission():
    h = Harness()  # max_building=1 by default
    building_with_blocker(h)
    report_blocked(h, (904,))
    assert_waiting(h)
    h.to_building(B)  # another build takes the only slot
    assert holds_slot(h, B)
    r = read(h, UNBLOCKED)
    assert not messages(r, MessagePurpose.BLOCKERS_CLOSED)  # held until re-admitted
    p = h.p(A)
    assert Hold.BLOCKERS not in p.holds and p.held_wakes and p.bot == BotState.QUEUED
    assert queued(h) and not holds_slot(h)


def test_boot_recovery_of_an_already_blocked_run():
    """The live #888 state: Blocked by a result accepted before ``blocked_on`` existed."""
    h = Harness()
    building_with_blocker(h)
    report_blocked(h, ())
    assert Hold.AGENT_BLOCKED in h.p(A).holds
    s = h.cur(A).session_id
    r = h.send(A, ev.AgentBlockedOn(session_id=s, issues=(904,)))
    assert r.audit.accepted, r.audit.reason
    assert_waiting(h)
    assert not h.send(A, ev.AgentBlockedOn(session_id=s, issues=(904,))).audit.accepted
    [_] = messages(read(h, UNBLOCKED), MessagePurpose.BLOCKERS_CLOSED)


# ------------------------------------------------- 2. an approval waits on blockers


def test_approve_while_blockers_are_open_is_held_then_starts_once():
    h = Harness()
    planned(h, BLOCKED)
    plan = h.cur(A).session_id
    r = control(h, ev.ApprovePlan(via=Via.COMMAND), BLOCKED, stage=Stage.SCOPED)
    assert r.audit.accepted, r.audit.reason
    p = h.p(A)
    assert p.stage == Stage.SCOPED and p.current_approval is not None
    assert h.admission.queue_entry(A) is None
    assert_waiting(h)
    # Repeating /approve while held records nothing new.
    control(h, ev.ApprovePlan(via=Via.COMMAND), BLOCKED, stage=Stage.SCOPED)
    assert len(h.p(A).approvals) == 1
    read(h, BLOCKED, stage=Stage.SCOPED)
    assert h.admission.queue_entry(A) is None
    # Every blocker closed: the approval starts through the queue, once.
    read(h, UNBLOCKED, stage=Stage.SCOPED)
    p = h.p(A)
    assert p.stage == Stage.BUILDING and Hold.BLOCKERS not in p.holds and queued(h)
    entry = h.admission.queue_entry(A)
    read(h, UNBLOCKED, stage=Stage.BUILDING)
    assert h.admission.queue_entry(A) == entry and len(h.p(A).approvals) == 1
    h.quiesce(A, plan)
    run = h.admit(A)
    assert run.kind.value == "build" and holds_slot(h)


# ------------------------------------------------- 3. force-starts are unchanged


def test_drag_and_build_label_still_force_start_with_the_note():
    h = Harness()
    planned(h, BLOCKED)
    r = control(h, ev.ApprovePlan(via=Via.DRAG), BLOCKED, stage=Stage.BUILDING)
    assert r.audit.accepted, r.audit.reason
    p = h.p(A)
    assert p.stage == Stage.BUILDING and queued(h) and Hold.BLOCKERS not in p.holds
    assert p.note == "Started despite open blocker #904"

    h.eligible(B)
    read(h, BLOCKED, B)
    r = control(h, ev.WaivePlan(via=Via.LABEL), BLOCKED, B)
    assert r.audit.accepted, r.audit.reason
    assert h.p(B).note == "Started despite open blocker #904"
    assert Hold.BLOCKERS not in h.p(B).holds


def test_drag_of_a_held_approval_force_starts_it():
    h = Harness()
    planned(h, BLOCKED)
    control(h, ev.ApprovePlan(via=Via.COMMAND), BLOCKED, stage=Stage.SCOPED)
    assert_waiting(h)
    control(h, ev.ApprovePlan(via=Via.DRAG), BLOCKED, stage=Stage.BUILDING)
    p = h.p(A)
    assert p.stage == Stage.BUILDING and queued(h) and Hold.BLOCKERS not in p.holds
    assert p.note == "Started despite open blocker #904"


# ------------------------------------------------- 4. other blocked results unchanged


def test_a_blocked_result_citing_something_else_stays_blocked():
    h = Harness()
    building_with_blocker(h)
    report_blocked(h, ())  # e.g. a refused credential
    p = h.p(A)
    assert Hold.AGENT_BLOCKED in p.holds and Hold.BLOCKERS not in p.holds
    assert p.bot == BotState.BLOCKED
    assert not messages(read(h, UNBLOCKED), MessagePurpose.BLOCKERS_CLOSED)
    assert Hold.AGENT_BLOCKED in h.p(A).holds


def test_a_blocked_result_on_an_issue_that_is_no_native_blocker_stays_blocked():
    h = Harness()
    building_with_blocker(h)
    report_blocked(h, (905,))
    assert Hold.AGENT_BLOCKED in h.p(A).holds and h.p(A).bot == BotState.BLOCKED


# ------------------------------------------------- 5. never resumes a stopped issue


def test_a_stopped_waiting_run_never_resumes():
    h = Harness()
    building_with_blocker(h)
    report_blocked(h, (904,))
    assert_waiting(h)
    control(h, ev.Stop(), BLOCKED)
    assert Hold.STOPPED in h.p(A).holds and Hold.BLOCKERS not in h.p(A).holds
    r = read(h, UNBLOCKED)
    assert not messages(r, MessagePurpose.BLOCKERS_CLOSED)
    assert not holds_slot(h)


def test_a_stopped_held_approval_never_starts():
    h = Harness()
    planned(h, BLOCKED)
    control(h, ev.ApprovePlan(via=Via.COMMAND), BLOCKED, stage=Stage.SCOPED)
    control(h, ev.Stop(), BLOCKED, stage=Stage.SCOPED)
    read(h, UNBLOCKED, stage=Stage.SCOPED)
    assert h.p(A).stage == Stage.SCOPED and h.admission.queue_entry(A) is None


def test_an_assigned_or_closed_issue_never_resumes():
    h = Harness()
    planned(h, BLOCKED)
    control(h, ev.ApprovePlan(via=Via.COMMAND), BLOCKED, stage=Stage.SCOPED)
    read(h, UNBLOCKED, stage=Stage.SCOPED, human_assigned=True)  # opted out
    assert h.p(A).stage == Stage.SCOPED and h.admission.queue_entry(A) is None

    h2 = Harness()
    building_with_blocker(h2)
    report_blocked(h2, (904,))
    r = read(h2, UNBLOCKED, open=False)
    assert not messages(r, MessagePurpose.BLOCKERS_CLOSED) and not holds_slot(h2)
