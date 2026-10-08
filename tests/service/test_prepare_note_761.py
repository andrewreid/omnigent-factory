"""#761: a refused preparation that names its cause shows it on the card.

The card said only "Blocked: session prepare failed"; the cause (the issue worktree was
detached by a read-only stage) was visible only in the daemon log.
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectIntent, EffectKind, Preconditions
from omnigent_factory.core.types import BotState, Hold, Via
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.executor import EffectExecutor, ParcelSerializers
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
NOTE = "worktree off branch factory/issue-761 (detached at 469e2af, uncommitted changes)"


def test_refused_prepare_note_reaches_the_card_and_survives_the_drain() -> None:
    h = Harness()
    h.triage()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    s = h.cur()
    h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="root-x", nonce=s.nonce))
    h.send(P, ev.Prepared(session_id=s.session_id, ok=False, note=NOTE))
    h.quiesce(P, s.session_id)
    p = h.p()
    assert p.bot == BotState.BLOCKED and Hold.PREPARE_FAILED in p.holds
    assert p.board_note == f"Blocked: {NOTE}"


def test_refused_prepare_without_a_note_keeps_the_generic_text() -> None:
    h = Harness()
    h.triage()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    s = h.cur()
    h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="root-x", nonce=s.nonce))
    h.send(P, ev.Prepared(session_id=s.session_id, ok=False))
    h.quiesce(P, s.session_id)
    assert h.p().board_note == "Blocked: session prepare failed"


def test_executor_carries_the_prepare_note(service_config: ServiceConfig) -> None:
    executor = EffectExecutor(
        None,  # type: ignore[arg-type]
        service_config.trusted,
        FakeClock(),
        (),
        ParcelSerializers(),
        poll_seconds=1,
    )
    prepare = EffectIntent(
        effect_id="ef_prepare",
        kind=EffectKind.PREPARE_SESSION,
        parcel_id=P,
        target="S1",
        preconditions=Preconditions(1, 0, session_id="S1"),
        args={"root_id": "root-x"},
    )
    detail = {"ok": False, "unexpected_turn": False, "unusable": False, "note": NOTE}
    event = executor._ack_event(prepare, Ack("root-x", {**detail, "reason": f"workspace: {NOTE}"}))
    assert event is not None and isinstance(event.body, ev.Prepared)
    assert event.body.note == NOTE and event.body.ok is False
