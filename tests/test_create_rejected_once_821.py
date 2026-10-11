"""#821: a retried create refused for the same reason posted a second, identical Blocked
comment. The same reason again only updates the note; a new reason, or the same one after
a session was created in between, still comments."""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.types import BotState, Hold, Via
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
WHY = "workspace-rejected: workspace does not exist"


def _comments(result: ev.EventBody | object) -> list[object]:
    return Harness.of(result, EffectKind.POST_COMMENT)  # type: ignore[arg-type]


def _rejected_plan(h: Harness, reason: str = WHY) -> list[object]:
    s = h.cur()
    r = h.send(P, ev.CreateRejected(session_id=s.session_id, reason=reason))
    assert h.p().bot == BotState.BLOCKED and Hold.CREATE_REJECTED in h.p().holds
    return _comments(r)


def test_same_reason_on_retry_updates_the_note_only() -> None:
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    assert len(_rejected_plan(h)) == 1
    retry = h.send(P, ev.PlanFeedback(text_digest="retry"))  # the owner's "Retry"
    assert Harness.of(retry, EffectKind.CREATE_SESSION)
    assert _rejected_plan(h) == []
    assert h.p().note == "Blocked again: Omnigent refused to create the session (same reason)"


def test_a_different_reason_still_comments() -> None:
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    assert len(_rejected_plan(h)) == 1
    h.send(P, ev.PlanFeedback(text_digest="retry"))
    assert len(_rejected_plan(h, "create rejected: HTTP 422")) == 1


def test_a_created_session_clears_the_recorded_reason() -> None:
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    assert len(_rejected_plan(h)) == 1
    h.send(P, ev.PlanFeedback(text_digest="retry"))
    h.create_ok(P)
    assert h.p().create_rejected_reason == ""
