"""Epic autopilot in the pure reducer: the owner's Autopilot field, the epic plan and its
approval, autopilot's plan/queue steps on sub-issues, Delayed starts and their cancel
paths, drift, withdrawal, gates and questions."""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.admission import ADMISSION
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import EventKind, Provenance
from omnigent_factory.core.reducer import AUTOPILOT_UNCONFIRMED_NOTE
from omnigent_factory.core.types import (
    MICROS_PER_MINUTE,
    AutoBuildStatus,
    IssueLinks,
    LinkedIssue,
    SessionKind,
    Size,
    Stage,
    Via,
)
from omnigent_factory.testing.builders import (
    OTHER_USER_ID,
    OWNER_ID,
    contract_text,
    result_candidate,
    snapshot,
)
from omnigent_factory.testing.harness import Harness

EPIC, S1, S2 = "I_epic", "I_sub_1", "I_sub_2"
PLAN_HASH = "e" * 64
DELAY = 60 * MICROS_PER_MINUTE


def epic_links(*subs: tuple[int, bool]) -> IssueLinks:
    subs = subs or ((2, True), (3, True))
    return IssueLinks(
        sub_issues=tuple(LinkedIssue(n, is_open) for n, is_open in subs),
        sub_total=len(subs),
        sub_completed=sum(1 for _, is_open in subs if not is_open),
    )


def sub_links(epic: int = 1, *, blockers: tuple[LinkedIssue, ...] = (), nested: bool = False):
    return IssueLinks(
        parent=LinkedIssue(epic, True),
        blocked_by=blockers,
        sub_issues=(LinkedIssue(99, True),) if nested else (),
        sub_total=1 if nested else 0,
    )


def esnap(h: Harness, **kw: object):
    f = h.f(EPIC)
    return snapshot(read_at_us=f.now + 1, links=epic_links(), **kw)  # type: ignore[arg-type]


def send_epic(h: Harness, body: ev.EventBody, **kw: object):
    """An event on the epic carrying a fresh read that shows its sub-issues."""
    return h.send(EPIC, body, evidence=esnap(h), **kw)


def new_epic(h: Harness) -> None:
    for pid in (EPIC, S1, S2):  # issue numbers 1, 2, 3
        h.f(pid)
    h.apply(h.f(EPIC).make(ev.GitHubSnapshot(), evidence=esnap(h)))


def post_epic_plan(h: Harness, plan_hash: str = PLAN_HASH) -> None:
    """The epic planning run submits the epic plan and its comment is posted."""
    s = h.cur(EPIC)
    if s.root_id is None:
        s = h.create_ok(EPIC)
    assert s.kind == SessionKind.TRIAGE and s.root_id is not None
    r = h.send(
        EPIC,
        result_candidate(
            s.session_id, s.root_id, s.revision, ev.ResultKind.TRIAGE, epic_plan_hash=plan_hash
        ),
    )
    assert r.audit.accepted, r.audit.reason
    h.quiesce(EPIC, s.session_id)
    [publish] = h.of(r, EffectKind.PUBLISH_TRIAGE)
    h.send(
        EPIC,
        ev.PublicationAcked(
            effect_id=publish.effect_id,
            effect_kind=publish.kind.value,
            session_id=s.session_id,
            comment_id="c-epic-plan",
        ),
    )


def on_autopilot(h: Harness, level: str = "Full", *, approve: bool = True) -> None:
    new_epic(h)
    r = send_epic(h, ev.AutopilotMarked(option=level))
    assert r.audit.accepted, r.audit.reason
    post_epic_plan(h)
    if approve:
        r = send_epic(h, ev.ApprovePlan(via=Via.COMMAND))
        assert r.audit.accepted, r.audit.reason
        assert h.p(EPIC).autopilot is not None and h.p(EPIC).autopilot.approved  # type: ignore[union-attr]


def epoch(h: Harness) -> str:
    ap = h.p(EPIC).autopilot
    assert ap is not None
    return ap.source_event_id


def plan_sub(h: Harness, pid: str = S1, **links_kw: object):
    if pid not in h.parcels:
        h.eligible(pid)
    f = h.f(pid)
    return h.apply(
        f.make(
            ev.AutopilotPlan(epic=1, epoch=epoch(h)),
            evidence=snapshot(read_at_us=f.now + 1, links=sub_links(**links_kw)),  # type: ignore[arg-type]
        )
    )


def post_sub_plan(h: Harness, pid: str = S1, fit: str | None = "within") -> None:
    s = h.cur(pid)
    if s.root_id is None:
        s = h.create_ok(pid)
    assert s.root_id is not None
    p = h.p(pid)
    r = h.send(
        pid,
        result_candidate(
            s.session_id,
            s.root_id,
            p.revision,
            ev.ResultKind.PLAN,
            publication_kind=ev.PublicationKind.CONTRACT,
            contract_canonical=contract_text("M", f"Ship {pid}"),
            size=Size.M,
            epic_fit=fit,
        ),
    )
    assert r.audit.accepted, r.audit.reason
    [publish] = h.of(r, EffectKind.PUBLISH_CONTRACT)
    cid = str(publish.args["contract_id"])
    h.send(
        pid,
        ev.ContractPublished(
            contract_id=cid, comment_id=f"c-{cid}", verified=True, posted_at_us=h.f(pid).now
        ),
    )


def queue(h: Harness, pid: str = S1, delay_us: int = 0, *, show: bool = True):
    f = h.f(pid)
    ap = h.p(EPIC).autopilot
    assert ap is not None
    return h.apply(
        f.make(
            ev.AutopilotQueue(
                epic=1,
                epoch=ap.source_event_id,
                owner_id=ap.approved_by,
                approval_event_id=ap.approval_event_id,
                delay_us=delay_us,
                show_auto_build=show,
            ),
            evidence=snapshot(stage=Stage.SCOPED, read_at_us=f.now + 1, links=sub_links()),
        )
    )


def auto_build(h: Harness, pid: str = S1, *, at: int | None = None):
    f = h.f(pid)
    if at is not None:
        f.tick(at - f.now)
    return h.apply(
        f.make(ev.AutoBuild(), evidence=snapshot(stage=Stage.SCOPED, read_at_us=f.now + 1))
    )


def claimed(h: Harness, pid: str = S1, *, fit: str | None = "within") -> None:
    r = plan_sub(h, pid)
    assert r.audit.accepted, r.audit.reason
    post_sub_plan(h, pid, fit)


# ------------------------------------------------------------------ admission (1)


def test_the_field_is_an_owner_webhook_control_and_steps_are_the_clocks_only():
    assert ADMISSION[EventKind.AUTOPILOT_MARKED].provenances == frozenset(
        {Provenance.WEBHOOK, Provenance.RECOVERY}
    )
    for kind in (
        EventKind.AUTOPILOT_PLAN,
        EventKind.AUTOPILOT_QUEUE,
        EventKind.AUTOPILOT_WITHDRAW,
    ):
        assert ADMISSION[kind].provenances == frozenset({Provenance.SCHEDULER})
    h = Harness()
    new_epic(h)
    r = send_epic(h, ev.AutopilotMarked(option="Full"), actor=OTHER_USER_ID)
    assert not r.audit.accepted and r.audit.reason == "control-from-non-owner"
    for provenance in (Provenance.ADAPTER, Provenance.SCHEDULER, Provenance.RECONCILER):
        r = send_epic(h, ev.AutopilotMarked(option="Full"), provenance=provenance)
        assert not r.audit.accepted and r.audit.reason == "provenance-not-admitted"
    assert h.p(EPIC).autopilot is None
    r = h.send(S1, ev.AutopilotPlan(epic=1, epoch="x"), provenance=Provenance.WEBHOOK)
    assert not r.audit.accepted and r.audit.reason == "provenance-not-admitted"


def test_a_read_showing_a_level_never_enables_autopilot_and_notes_it_once():
    h = Harness()
    new_epic(h)
    f = h.f(EPIC)
    evidence = snapshot(read_at_us=f.now + 5, links=epic_links(), autopilot="Full")
    r = h.apply(f.make(ev.GitHubSnapshot(), evidence=evidence))
    p = h.p(EPIC)
    assert p.autopilot is None and p.note == AUTOPILOT_UNCONFIRMED_NOTE
    assert h.of(r, EffectKind.CREATE_SESSION) == [] and p.pending_authorization_id is None
    version = p.version
    r = h.apply(f.make(ev.GitHubSnapshot(), evidence=evidence))
    assert h.of(r, EffectKind.SET_NOTE) == [] and h.p(EPIC).version == version + 1
    # The owner's own selection is the control.
    send_epic(h, ev.AutopilotMarked(option=""))
    r = send_epic(h, ev.AutopilotMarked(option="Full"))
    assert r.audit.accepted and h.p(EPIC).autopilot is not None


def test_setting_autopilot_on_an_epic_starts_the_epic_planning_pass():
    h = Harness()
    new_epic(h)
    r = send_epic(h, ev.AutopilotMarked(option="Delayed"))
    assert r.audit.accepted, r.audit.reason
    p = h.p(EPIC)
    assert p.autopilot is not None and p.autopilot.level == "Delayed"
    assert p.autopilot.owner_id == OWNER_ID and not p.autopilot.approved
    assert p.stage == Stage.TRIAGED
    s = h.create_ok(EPIC)
    assert s.kind == SessionKind.TRIAGE
    # A level change keeps the plan pass (no second run).
    r = send_epic(h, ev.AutopilotMarked(option="Full"))
    assert h.p(EPIC).autopilot.level == "Full"  # type: ignore[union-attr]
    assert h.of(r, EffectKind.CREATE_SESSION) == [] and h.cur(EPIC).session_id == s.session_id


@pytest.mark.parametrize("case", ["not-epic", "unknown-option"])
def test_autopilot_on_a_non_epic_or_unknown_option_is_cleared(case: str):
    h = Harness()
    h.eligible(EPIC)  # no sub-issues
    option = "Full" if case == "not-epic" else "?"
    evidence = snapshot(read_at_us=h.f(EPIC).now + 1) if case == "not-epic" else esnap(h)
    r = h.send(EPIC, ev.AutopilotMarked(option=option), evidence=evidence)
    assert r.audit.accepted
    p = h.p(EPIC)
    assert p.autopilot is None and p.note.startswith("Autopilot cleared")
    assert [e.args["value"] for e in h.of(r, EffectKind.SET_AUTOPILOT)] == [""]


# ------------------------------------------------------------------ epic plan gate (4)


def test_the_epic_plan_must_be_posted_then_approved_hash_bound():
    h = Harness()
    on_autopilot(h, approve=False)
    p = h.p(EPIC)
    assert p.autopilot is not None and p.autopilot.plan is not None
    assert p.autopilot.plan.full_hash == PLAN_HASH and p.autopilot.plan.posted_at_us
    # A drag never approves an epic; a wrong hash is refused.
    r = send_epic(h, ev.ApprovePlan(via=Via.DRAG))
    assert not r.audit.accepted
    r = send_epic(h, ev.ApprovePlan(via=Via.COMMAND, hash_text="f" * 12))
    assert not r.audit.accepted and r.audit.reason == "hash-does-not-identify-latest"
    assert not h.p(EPIC).autopilot.approved  # type: ignore[union-attr]
    r = send_epic(h, ev.ApprovePlan(via=Via.COMMAND, hash_text=PLAN_HASH[:12]))
    assert r.audit.accepted, r.audit.reason
    ap = h.p(EPIC).autopilot
    assert ap is not None and ap.approved and ap.approved_by == OWNER_ID


def test_approve_before_the_plan_is_posted_is_refused():
    h = Harness()
    new_epic(h)
    send_epic(h, ev.AutopilotMarked(option="Full"))
    h.create_ok(EPIC)
    r = send_epic(h, ev.ApprovePlan(via=Via.COMMAND))
    assert not r.audit.accepted and r.audit.reason == "epic-plan-not-posted"
    assert h.p(EPIC).note == "Command refused: epic-plan-not-posted"


def test_an_owner_comment_revises_the_epic_plan_and_voids_its_approval():
    h = Harness()
    on_autopilot(h)
    first = h.cur(EPIC).session_id
    r = send_epic(h, ev.PlanFeedback(text_digest="split #3"))
    assert r.audit.accepted, r.audit.reason
    ap = h.p(EPIC).autopilot
    assert ap is not None and ap.revision_pending and not ap.approved
    # The planning pass re-runs (a new triage run of the epic).
    assert h.cur(EPIC).session_id != first and h.cur(EPIC).kind == SessionKind.TRIAGE
    # The revised plan must be approved again (even with the same content).
    h.create_ok(EPIC)
    post_epic_plan(h, plan_hash="d" * 64)
    ap = h.p(EPIC).autopilot
    assert ap is not None and not ap.revision_pending and not ap.approved
    assert send_epic(h, ev.ApprovePlan(via=Via.COMMAND)).audit.accepted
    assert h.p(EPIC).autopilot.approved  # type: ignore[union-attr]


def test_a_stop_on_the_epic_turns_autopilot_off():
    h = Harness()
    on_autopilot(h)
    r = send_epic(h, ev.Stop())
    assert h.p(EPIC).autopilot is None
    assert [e.args["value"] for e in h.of(r, EffectKind.SET_AUTOPILOT)] == [""]


# ------------------------------------------------------------------ next selection (3, 7)


def test_autopilot_plans_an_unblocked_sub_issue_straight_from_the_inbox_once():
    h = Harness()
    on_autopilot(h)
    r = plan_sub(h)
    assert r.audit.accepted, r.audit.reason
    p = h.p(S1)
    assert p.stage == Stage.SCOPED  # triage skipped
    assert p.autopilot_claim is not None and p.autopilot_claim.epoch == epoch(h)
    s = h.create_ok(S1)
    assert s.kind == SessionKind.PLAN
    # Never twice in one epoch: no second run or session.
    r = plan_sub(h)
    assert not r.audit.accepted and r.audit.reason == "autopilot-already-claimed"
    assert len([x for x in h.p(S1).sessions if x.kind == SessionKind.PLAN]) == 1


@pytest.mark.parametrize(
    ("links", "reason"),
    [
        ({"blockers": (LinkedIssue(500, True),)}, "autopilot-blocked"),  # outside the epic
        ({"nested": True}, "autopilot-nested-epic"),
        ({"epic": 77}, "autopilot-not-a-sub-issue"),
    ],
)
def test_blocked_nested_or_foreign_sub_issues_are_never_started(
    links: dict[str, object], reason: str
):
    h = Harness()
    on_autopilot(h)
    r = plan_sub(h, **links)
    assert not r.audit.accepted and r.audit.reason == reason
    assert h.p(S1).autopilot_claim is None and h.p(S1).pending_authorization_id is None


def test_a_closed_external_blocker_no_longer_blocks_and_unread_links_fail_closed():
    h = Harness()
    on_autopilot(h)
    assert plan_sub(h, blockers=(LinkedIssue(500, False),)).audit.accepted
    h.eligible(S2)
    f = h.f(S2)
    r = h.apply(
        f.make(
            ev.AutopilotPlan(epic=1, epoch=epoch(h)),
            evidence=snapshot(read_at_us=f.now + 1, links=None),
        )
    )
    assert not r.audit.accepted and r.audit.reason == "autopilot-links-unreadable"


# ------------------------------------------------------------------ drift (6) and levels (2)


def test_a_plan_beyond_its_part_of_the_epic_is_never_queued():
    h = Harness()
    on_autopilot(h)
    claimed(h, fit="exceeds")
    claim = h.p(S1).autopilot_claim
    assert claim is not None and claim.drift and claim.drift_hash
    r = queue(h)
    assert not r.audit.accepted and r.audit.reason == "autopilot-plan-drift"
    assert h.p(S1).auto_build is None
    # A plan that does not say is drift too (fails closed).
    h2 = Harness()
    on_autopilot(h2)
    claimed(h2, fit=None)
    assert not queue(h2).audit.accepted


def test_full_queues_at_once_and_the_build_carries_the_epic_plan_approval():
    h = Harness()
    on_autopilot(h, "Full")
    claimed(h)
    r = queue(h)
    assert r.audit.accepted, r.audit.reason
    p = h.p(S1)
    mark = p.auto_build
    assert mark is not None and mark.status == AutoBuildStatus.QUEUED
    assert mark.autopilot_epic == 1 and mark.not_before_us == 0
    assert [e.args["value"] for e in h.of(r, EffectKind.SET_AUTO_BUILD)] == ["Queued"]
    assert not queue(h).audit.accepted  # each plan once
    plan = h.cur(S1)
    r = auto_build(h)
    assert r.audit.accepted, r.audit.reason
    approval = h.p(S1).current_approval
    ap = h.p(EPIC).autopilot
    assert approval is not None and ap is not None
    assert approval.owner_id == ap.approved_by == OWNER_ID
    assert approval.source_event_id == ap.approval_event_id
    assert h.p(S1).stage == Stage.BUILDING
    h.quiesce(S1, plan.session_id)
    assert h.admit(S1).kind == SessionKind.BUILD
    assert h.admission.auto_build_count == 1  # counts as an auto-build


def test_an_unshown_autopilot_mark_never_writes_the_auto_build_field():
    h = Harness()
    on_autopilot(h)
    claimed(h)
    r = queue(h, show=False)
    assert r.audit.accepted and h.of(r, EffectKind.SET_AUTO_BUILD) == []
    r = auto_build(h)
    assert r.audit.accepted and h.of(r, EffectKind.SET_AUTO_BUILD) == []


def test_delayed_starts_only_after_the_delay_from_posting():
    h = Harness()
    on_autopilot(h, "Delayed")
    claimed(h)
    posted = h.p(S1).current_contract.posted_at_us  # type: ignore[union-attr]
    assert posted is not None
    r = queue(h, delay_us=DELAY)
    assert r.audit.accepted, r.audit.reason
    mark = h.p(S1).auto_build
    assert mark is not None and mark.not_before_us == posted + DELAY
    assert h.p(S1).note.startswith("Autopilot: build starts in")
    r = auto_build(h)
    assert not r.audit.accepted and r.audit.reason == "auto-build-not-due"
    assert h.p(S1).auto_build is not None  # still waiting, exactly once later
    r = auto_build(h, at=posted + DELAY)
    assert r.audit.accepted, r.audit.reason
    assert not auto_build(h).audit.accepted  # fires once


@pytest.mark.parametrize("objection", ["comment", "drag-left", "stop", "assign", "clear-mark"])
def test_an_owner_objection_in_the_delay_window_cancels_the_start(objection: str):
    h = Harness()
    on_autopilot(h, "Delayed")
    claimed(h)
    assert queue(h, delay_us=DELAY).audit.accepted
    f = h.f(S1)
    if objection == "comment":
        h.send(S1, ev.PlanFeedback(text_digest="no, do it differently"))
    elif objection == "drag-left":
        h.send(S1, ev.LeftwardMove(from_stage=Stage.SCOPED, to_stage=Stage.TRIAGED))
    elif objection == "stop":
        h.send(S1, ev.Stop())
    elif objection == "assign":
        h.apply(
            f.make(ev.AssignedHuman(), evidence=snapshot(human_assigned=True, read_at_us=f.now))
        )
    else:
        h.send(S1, ev.AutoBuildMarked(option=""))
    p = h.p(S1)
    assert p.auto_build is None
    assert p.autopilot_claim is not None and p.autopilot_claim.released
    posted = DELAY + (p.current_contract.posted_at_us or 0) if p.current_contract else DELAY
    assert not auto_build(h, at=max(posted, h.f(S1).now) + 1).audit.accepted
    assert h.p(S1).approvals == ()


def test_withdraw_drops_a_queued_autopilot_mark_and_never_touches_a_build():
    h = Harness()
    on_autopilot(h, "Delayed")
    claimed(h)
    queue(h, delay_us=DELAY)
    r = h.send(S1, ev.AutopilotWithdraw(epic=1, reason="autopilot was turned off on #1"))
    assert r.audit.accepted, r.audit.reason
    p = h.p(S1)
    assert p.auto_build is None and p.autopilot_claim.released  # type: ignore[union-attr]
    assert [e.args["value"] for e in h.of(r, EffectKind.SET_AUTO_BUILD)] == [""]
    assert not h.send(S1, ev.AutopilotWithdraw(epic=1, reason="again")).audit.accepted
    # A started build is not withdrawn.
    h2 = Harness()
    on_autopilot(h2)
    claimed(h2)
    queue(h2)
    auto_build(h2)
    r = h2.send(S1, ev.AutopilotWithdraw(epic=1, reason="off"))
    assert h2.p(S1).current_approval is not None and h2.p(S1).current_approval.valid  # type: ignore[union-attr]


def test_an_answer_to_the_plan_question_keeps_the_claim():
    h = Harness()
    on_autopilot(h)
    plan_sub(h)
    s = h.create_ok(S1)
    h.send(S1, ev.OwnerQuestion(session_id=s.session_id, question_key="q", summary="Which?"))
    h.send(S1, ev.PlanFeedback(text_digest="the first one"))  # the answer
    claim = h.p(S1).autopilot_claim
    assert claim is not None and claim.active
    post_sub_plan(h)
    assert queue(h).audit.accepted


# ------------------------------------------------------------------ gates (5) and questions


def test_gate_records_are_idempotent():
    h = Harness()
    on_autopilot(h)
    assert h.send(EPIC, ev.AutopilotGateCreated(key="dns", number=40)).audit.accepted
    assert not h.send(EPIC, ev.AutopilotGateCreated(key="dns", number=41)).audit.accepted
    assert h.send(EPIC, ev.AutopilotGateCreated(key="dns", number=40, linked=True)).audit.accepted
    assert not h.send(
        EPIC, ev.AutopilotGateCreated(key="dns", number=40, linked=True)
    ).audit.accepted
    assert [(g.key, g.number, g.linked) for g in h.p(EPIC).autopilot_gates] == [("dns", 40, True)]


def test_a_question_is_asked_once_per_epoch():
    h = Harness()
    on_autopilot(h)
    r = h.send(EPIC, ev.AutopilotQuestion(key="order"))
    assert r.audit.accepted
    [comment] = h.of(r, EffectKind.POST_COMMENT)
    assert comment.args["template"] == "autopilot-order"
    r = h.send(EPIC, ev.AutopilotQuestion(key="order"))
    assert not r.audit.accepted and h.of(r, EffectKind.POST_COMMENT) == []


def test_status_sets_the_epic_note_and_pause_without_a_comment():
    h = Harness()
    on_autopilot(h)
    text = "Autopilot paused: #2 needs you"
    r = h.send(EPIC, ev.AutopilotStatus(text=text, paused="#2 needs you"))
    assert r.audit.accepted and h.of(r, EffectKind.POST_COMMENT) == []
    p = h.p(EPIC)
    assert p.epic_note == text and p.autopilot.paused == "#2 needs you"  # type: ignore[union-attr]
    assert p.board_note == text
    assert not h.send(EPIC, ev.AutopilotStatus(text=text, paused="#2 needs you")).audit.accepted
