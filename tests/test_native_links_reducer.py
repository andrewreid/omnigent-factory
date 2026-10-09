"""GitHub-native links in the pure reducer: auto-build gating on open blockers (fail
closed on an unreadable link set), manual builds noted but not blocked, epics never
built, and the epic progress note."""

from __future__ import annotations

from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.projection import project_note
from omnigent_factory.core.types import (
    BotState,
    IssueLinks,
    LinkedIssue,
    Stage,
    Via,
)
from omnigent_factory.store.sqlite import snapshot_key
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.harness import Harness

A = "I_links_a"

OPEN_823 = LinkedIssue(823, True, "Worker framework")
CLOSED_822 = LinkedIssue(822, False, "ADR")
BLOCKED = IssueLinks(blocked_by=(OPEN_823, CLOSED_822))
UNBLOCKED = IssueLinks(blocked_by=(LinkedIssue(823, False), CLOSED_822))
EPIC = IssueLinks(sub_issues=(LinkedIssue(822, False), OPEN_823), sub_total=8, sub_completed=1)


def read(h: Harness, links: IssueLinks | None, pid: str = A, **snap: object):
    f = h.f(pid)
    return h.apply(
        f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, links=links, **snap))  # type: ignore[arg-type]
    )


def planned(h: Harness, links: IssueLinks) -> None:
    h.plan_published(A)
    read(h, links, stage=Stage.SCOPED)
    assert h.p(A).links == links


def mark(h: Harness, links: IssueLinks):
    f = h.f(A)
    return h.apply(
        f.make(
            ev.AutoBuildMarked(option="Queued"),
            evidence=snapshot(read_at_us=f.now, stage=Stage.SCOPED, links=links),
        )
    )


def auto_build(h: Harness, links: IssueLinks | None):
    f = h.f(A)
    return h.apply(
        f.make(
            ev.AutoBuild(),
            evidence=snapshot(read_at_us=f.now + 1, stage=Stage.SCOPED, links=links),
        )
    )


def note(h: Harness) -> str:
    p = h.p(A)
    return project_note(p, p.bot)


def test_a_fresh_read_stores_the_links_and_a_read_without_them_keeps_them():
    h = Harness()
    h.eligible(A)
    read(h, BLOCKED)
    assert h.p(A).links == BLOCKED
    read(h, None)  # a read that could not see the links changes nothing
    assert h.p(A).links == BLOCKED
    assert snapshot_key(snapshot(links=BLOCKED)) != snapshot_key(snapshot(links=UNBLOCKED))


def test_auto_build_waits_on_an_open_blocker_and_starts_once_it_closes():
    h = Harness()
    planned(h, BLOCKED)
    assert mark(h, BLOCKED).audit.accepted
    assert h.p(A).auto_build is not None
    # Queued, Idle in Planning: the card says what it waits on.
    h.parcels[A] = replace(h.p(A), note="")
    assert note(h) == "Waiting on #823"
    r = auto_build(h, BLOCKED)
    assert not r.audit.accepted and r.audit.reason == "auto-build-blocked"
    assert h.p(A).approvals == () and h.p(A).auto_build is not None  # still queued
    # The blocker closes: the next start (its own fresh read) goes ahead.
    r = auto_build(h, UNBLOCKED)
    assert r.audit.accepted, r.audit.reason
    assert h.p(A).stage == Stage.BUILDING and h.p(A).links == UNBLOCKED


def test_auto_build_fails_closed_on_an_unreadable_link_set():
    h = Harness()
    planned(h, IssueLinks())
    mark(h, IssueLinks())
    r = auto_build(h, None)
    assert not r.audit.accepted and r.audit.reason == "auto-build-links-unreadable"
    assert auto_build(h, IssueLinks()).audit.accepted


def test_a_manual_build_is_not_blocked_and_says_so():
    h = Harness()
    planned(h, BLOCKED)
    plan = h.cur(A)
    f = h.f(A)
    r = h.apply(
        f.make(
            ev.ApprovePlan(via=Via.DRAG),
            evidence=snapshot(read_at_us=f.now, stage=Stage.BUILDING, links=BLOCKED),
        )
    )
    assert r.audit.accepted, r.audit.reason
    assert h.p(A).current_approval is not None and h.p(A).stage == Stage.BUILDING
    assert h.p(A).note == "Started despite open blocker #823"
    h.quiesce(A, plan.session_id)
    # No comment for it: the note is the only trace.
    assert not [e for e in r.effects if e.kind.value == "post_comment"]


def test_a_build_label_on_an_issue_with_open_blockers_proceeds_with_a_note():
    h = Harness()
    h.eligible(A)
    read(h, BLOCKED)
    f = h.f(A)
    r = h.apply(
        f.make(ev.WaivePlan(via=Via.LABEL), evidence=snapshot(read_at_us=f.now, links=BLOCKED))
    )
    assert r.audit.accepted, r.audit.reason
    assert h.p(A).note == "Started despite open blocker #823"


def test_an_epic_is_never_built():
    h = Harness()
    planned(h, EPIC)
    # A drag to Building is refused and the card goes back, with the epic note.
    f = h.f(A)
    r = h.apply(
        f.make(
            ev.ApprovePlan(via=Via.DRAG),
            evidence=snapshot(read_at_us=f.now, stage=Stage.BUILDING, links=EPIC),
        )
    )
    assert not r.audit.accepted and r.audit.reason == "epic"
    assert h.p(A).current_approval is None
    assert h.p(A).note == "Epic: build its sub-issues; card moved back to Scoped"
    # factory:build too (no rollback for a label).
    h2 = Harness()
    h2.eligible(A)
    read(h2, EPIC)
    f = h2.f(A)
    r = h2.apply(
        f.make(ev.WaivePlan(via=Via.LABEL), evidence=snapshot(read_at_us=f.now, links=EPIC))
    )
    assert not r.audit.accepted and h2.p(A).note == "Epic: build its sub-issues"
    # An auto-build mark on an epic is cleared; a start is refused.
    h3 = Harness()
    planned(h3, EPIC)
    mark(h3, EPIC)
    assert h3.p(A).auto_build is None
    assert h3.p(A).note == "Auto-build cleared: Epic: build its sub-issues"


def test_an_auto_build_start_on_an_issue_that_became_an_epic_is_refused():
    h = Harness()
    planned(h, IssueLinks())
    mark(h, IssueLinks())
    r = auto_build(h, EPIC)
    assert not r.audit.accepted and r.audit.reason == "auto-build-epic"


def test_epic_progress_note_is_set_on_change_only_and_is_the_lowest_note():
    h = Harness()
    h.eligible(A)
    read(h, EPIC)
    r = h.send(A, ev.EpicProgress(text="Epic · 1/8 done · next: #823"))
    assert r.audit.accepted
    assert h.p(A).epic_note == "Epic · 1/8 done · next: #823"
    assert h.p(A).bot == BotState.IDLE and note(h) == "Epic · 1/8 done · next: #823"
    r = h.send(A, ev.EpicProgress(text="Epic · 1/8 done · next: #823"))
    assert not r.audit.accepted and r.audit.reason == "epic-progress-unchanged"
    # A status reason outranks it.
    h.parcels[A] = replace(h.p(A), note="Command refused: x")
    assert note(h) == "Command refused: x"
