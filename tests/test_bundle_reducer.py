"""The ``bundled`` hold in the reducer: a bundle member is never triaged, planned or built on
its own, its card notes the bundle and mirrors its Bot, an ejected member goes back to
Triage, and the hold never blocks completion. Plus the bundle PR's all-of closing reason."""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.projection import bundle_member_refusal, project_note
from omnigent_factory.core.reducer import _failure_reason, _unfinished_reason
from omnigent_factory.core.types import (
    AUTO_BUILD_QUEUED,
    BotState,
    Hold,
    Readiness,
    Stage,
    Via,
)
from omnigent_factory.testing.harness import Harness

A, B = "I_a", "I_b"


def triaged() -> Harness:
    h = Harness()
    h.triage(A)
    return h


def planned() -> Harness:
    h = Harness()
    h.triage(B)
    h.plan_published(B)
    return h


def test_a_held_member_notes_its_bundle_and_refuses_its_own_plan_and_build():
    h = triaged()
    r = h.send(A, ev.BundleHold(bundle=9, bot="Idle"))
    assert r.audit.accepted
    p = h.p(A)
    assert Hold.BUNDLED in p.holds and p.bundled_into == 9
    assert p.note == "In bundle #9" and project_note(p, p.bot) == "In bundle #9"
    for body in (
        ev.RequestPlan(via=Via.COMMAND),
        ev.PlanFeedback(text_digest="d" * 64),  # a plain comment in Triage re-runs triage
        ev.WaivePlan(via=Via.LABEL),  # factory:build
    ):
        r = h.send(A, body)
        assert not r.audit.accepted and r.audit.reason == "bundled", body
        assert h.p(A).note.startswith("In bundle #9: work on the bundle issue")
    assert Hold.BUNDLED in h.p(A).holds and h.p(A).current_session is not None
    assert h.p(A).stage == Stage.TRIAGED


def test_a_drag_of_a_held_member_to_building_is_moved_back():
    h = planned()
    assert h.send(B, ev.BundleHold(bundle=9)).audit.accepted
    r = h.send(B, ev.ApprovePlan(via=Via.DRAG))
    assert not r.audit.accepted and r.audit.reason == "bundled"
    assert h.p(B).note.endswith("card moved back to Scoped")
    assert h.p(B).stage == Stage.SCOPED and h.p(B).current_approval is None


def test_an_auto_build_mark_on_a_held_member_is_cleared():
    h = planned()
    h.send(B, ev.BundleHold(bundle=9))
    h.send(B, ev.AutoBuildMarked(option=AUTO_BUILD_QUEUED))
    p = h.p(B)
    assert p.auto_build is None and p.note == "Auto-build cleared: In bundle #9"


def test_a_member_with_work_running_is_not_held():
    h = Harness()
    h.eligible(A)
    h.send(A, ev.RequestTriage(via=Via.DRAG))
    h.create_ok(A)  # the triage run is working
    r = h.send(A, ev.BundleHold(bundle=9))
    assert not r.audit.accepted and r.audit.reason.startswith("bundle-member:")
    assert Hold.BUNDLED not in h.p(A).holds
    assert bundle_member_refusal(h.p(A)) == "work on it is running or queued"


def test_a_member_in_another_bundle_or_the_bundle_itself_is_refused():
    h = triaged()
    assert h.send(A, ev.BundleHold(bundle=9)).audit.accepted
    assert h.send(A, ev.BundleHold(bundle=10)).audit.reason == "in-another-bundle"
    assert h.send(A, ev.BundleHold(bundle=9)).audit.reason == "bundle-hold-unchanged"
    other = Harness()
    other.triage(A)
    issue = other.p(A).issue_number
    assert issue is not None
    assert other.send(A, ev.BundleHold(bundle=issue)).audit.reason == "bundle-is-this-issue"


def test_the_member_card_mirrors_the_bundle_bot():
    h = triaged()
    h.send(A, ev.BundleHold(bundle=9, bot="Idle"))
    assert h.send(A, ev.BundleHold(bundle=9, bot="Working")).audit.accepted
    assert h.p(A).bot == BotState.WORKING and h.p(A).bundle_bot == "Working"


def test_an_ejected_member_returns_to_triage_with_the_reason():
    h = planned()
    h.send(B, ev.BundleHold(bundle=9))
    r = h.send(B, ev.BundleHold(bundle=0, note="Ejected from bundle #9: too big", to_triage=True))
    assert r.audit.accepted
    p = h.p(B)
    assert Hold.BUNDLED not in p.holds and p.bundled_into == 0 and not p.bundle_bot
    assert p.stage == Stage.TRIAGED and p.note == "Ejected from bundle #9: too big"
    # On its own again: it may be planned.
    assert h.send(B, ev.RequestPlan(via=Via.COMMAND)).audit.accepted


def test_a_released_member_keeps_its_column_and_can_be_worked_again():
    h = planned()
    h.send(B, ev.BundleHold(bundle=9))
    h.send(B, ev.BundleHold(bundle=0))
    p = h.p(B)
    assert p.stage == Stage.SCOPED and p.note == "Released from bundle #9"
    assert h.send(B, ev.BundleHold(bundle=0)).audit.reason == "not-bundled"
    assert h.send(B, ev.ApprovePlan(via=Via.DRAG)).audit.accepted


def test_the_hold_never_blocks_completion():
    h = triaged()
    h.send(A, ev.BundleHold(bundle=9, bot="Working"))
    h.send(A, ev.Closed())
    p = h.p(A)
    assert Hold.COMPLETED in p.holds and p.bot == BotState.IDLE


def test_a_bundle_pr_missing_member_closes_is_not_ready_and_says_which():
    r = Readiness(session_id="s", pr_number=7, head_sha="a" * 40)
    body = ev.ReadinessEvidence(
        session_id="s",
        pr_number=7,
        head_sha="a" * 40,
        checks=ev.ChecksState.GREEN,
        closes_issue=False,
        missing_closes="#12, #13",
    )
    reason = _failure_reason(r, body, 9)
    assert "`Closes #12`" in reason and "`Closes #13`" in reason
    assert _unfinished_reason(r, body, 9) == "PR #7 lacks Closes lines for #12, #13"
    plain = ev.ReadinessEvidence(session_id="s", pr_number=7, checks=ev.ChecksState.GREEN)
    assert "missing" not in _failure_reason(r, plain, 9)
