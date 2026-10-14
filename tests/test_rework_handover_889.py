"""#889: a live build run under an older authorization gives way to the owner's queued
rework at its next idle point.

Live state (2026-10-14): the crash restart re-ran the old episode (gen-2, ``au_0ec96d``)
although the owner's comment had already queued a rework (``au_b56f54``). Nothing then
let the rework start: admission waits for every run to settle, a later owner comment
was only relayed to gen-2, and a green read moved the card to Ready while the rework was
still Queued. Now gen-2 is never interrupted mid-turn; when its turn ends (with or
without a result, or at Needs you) it is retired as superseded and the rework is
admitted under its own authorization. The harness asserts "Ready is never Queued or
Working" on every transition.
"""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core import reducer
from omnigent_factory.core.codec import (
    admission_from_data,
    admission_to_data,
    parcel_from_json,
    parcel_to_json,
)
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.types import Hold, Lifecycle, QueueStatus, Stage
from omnigent_factory.testing.builders import result_candidate
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
PR = 1022
HEAD = "30613e90c0d2" + "a" * 28
NEXT = "b" * 40
NOTE = "Switching to your feedback: earlier build stopped at its next pause"


def _submit(h: Harness, head: str = HEAD) -> None:
    s = h.cur(P)
    assert s.root_id is not None
    r = h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=PR,
            head_sha=head,
        ),
    )
    assert r.audit.accepted, r.audit.reason


def _read(h: Harness, head: str, **kw: object) -> ev.ReadinessEvidence:
    r = h.p().readiness
    sid = r.session_id if r is not None else h.cur(P).session_id
    return ev.ReadinessEvidence(session_id=sid, pr_number=PR, head_sha=head, **kw)  # type: ignore[arg-type]


def _red(h: Harness, head: str = HEAD) -> ev.ReadinessEvidence:
    return _read(h, head, checks=ev.ChecksState.FAILED, findings_open=True, review_accepted=False)


def _green(h: Harness, head: str = NEXT) -> ev.ReadinessEvidence:
    return _read(
        h,
        head,
        verified=True,
        checks=ev.ChecksState.GREEN,
        checks_summary="23 checks: 23 success",
        review_bot_pending_since_us=0,
    )


def _live_889() -> Harness:
    """Needs you, crash, owner comment during the drain, old episode restarted (gen-2
    ACTIVE under the old authorization), rework QUEUED, readiness cleared. The restart
    of the old episode is the earlier release's behaviour (``_rework_supersedes`` off)."""
    h = Harness()
    h.to_building()
    _submit(h)
    h.quiesce(P, h.cur(P).session_id)
    h.send(P, _red(h))  # the one wake
    h.quiesce(P, h.cur(P).session_id)
    h.send(P, _red(h))
    assert Hold.READINESS_FAILED in h.p().holds
    crashed = h.cur(P)
    h.send(P, ev.SessionCrashed(session_id=crashed.session_id))
    h.send(P, ev.PlanFeedback(text_digest="owner feedback"))
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(reducer, "_rework_supersedes", lambda ctx, s: False)
        h.quiesce(P, crashed.session_id)  # restarts the old episode (the bug)
        gen2 = h.create_ok(P)
    assert gen2.authorization_id == crashed.authorization_id
    assert gen2.lifecycle == Lifecycle.ACTIVE and not gen2.quiescent
    entry = h.admission.queue_entry(P)
    assert entry is not None and entry.status == QueueStatus.QUEUED
    rework = h.p().authorizations[-1]
    assert rework.rework and rework.authorization_id != gen2.authorization_id
    assert h.p().readiness is None
    return h


def _live_sessions(h: Harness) -> list[str]:
    return [
        s.session_id
        for s in h.p().sessions
        if s.lifecycle not in (Lifecycle.RETIRED, Lifecycle.FENCED)
    ]


def _handed_over(h: Harness, gen2_id: str) -> None:
    """gen-2 retires through a normal drain; the rework is admitted, alone."""
    p = h.p()
    assert p.stage == Stage.BUILDING and p.note == NOTE
    old = p.session(gen2_id)
    assert old is not None and old.lifecycle == Lifecycle.DRAINING and not old.fences
    r = h.quiesce(P, gen2_id)  # the drain's scan
    assert EffectKind.CREATE_SESSION not in [e.kind for e in r.effects]
    old = h.p().session(gen2_id)
    assert old is not None and old.lifecycle == Lifecycle.RETIRED
    rework = h.admit(P)
    auth = h.p().authorization(rework.authorization_id)
    assert auth is not None and auth.rework
    assert _live_sessions(h) == [rework.session_id]
    assert h.admission.queue_entry(P).status == QueueStatus.RESERVED  # type: ignore[union-attr]
    assert not h.p().holds & {Hold.READINESS_FAILED, Hold.RESTART_EXHAUSTED}


def _no_messages_to(result: object, session_id: str) -> None:
    effects = getattr(result, "effects", ())
    assert not [
        e
        for e in effects
        if e.kind == EffectKind.SEND_MESSAGE and e.preconditions.session_id == session_id
    ]


def test_A_gen2_reaches_green_but_hands_over_instead_of_ready():
    h = _live_889()
    gen2 = h.cur(P).session_id
    _submit(h, NEXT)
    h.send(P, _green(h))  # green before the turn is seen ending: never Ready
    assert h.p().stage == Stage.BUILDING
    r = h.quiesce(P, gen2)  # the turn ends: the handover
    _no_messages_to(r, gen2)
    h.send(P, _green(h))  # a late read changes nothing
    assert h.p().stage == Stage.BUILDING
    _handed_over(h, gen2)


def test_A_green_read_after_the_turn_ended_hands_over_without_ready():
    h = _live_889()
    gen2 = h.cur(P).session_id
    _submit(h, NEXT)
    with pytest.MonkeyPatch.context() as mp:  # the scan landed before the fix's check
        mp.setattr(reducer, "_rework_supersedes", lambda ctx, s: False)
        h.quiesce(P, gen2)
    r = h.send(P, _green(h))
    assert h.p().stage == Stage.BUILDING
    assert EffectKind.PUBLISH_REPORT not in [e.kind for e in r.effects]
    _handed_over(h, gen2)


def test_B_gen2_ends_its_turn_without_a_result():
    h = _live_889()
    gen2 = h.cur(P).session_id
    r = h.quiesce(P, gen2)
    _no_messages_to(r, gen2)
    _handed_over(h, gen2)


def test_C_gen2_at_needs_you_hands_over_on_the_next_read_or_owner_comment():
    for trigger in ("read", "comment"):
        h = _live_889()
        gen2 = h.cur(P).session_id
        with pytest.MonkeyPatch.context() as mp:  # reached Needs you before the fix
            mp.setattr(reducer, "_rework_supersedes", lambda ctx, s: False)
            _submit(h, NEXT)
            h.quiesce(P, gen2)
            h.send(P, _red(h, NEXT))  # its wake
            h.quiesce(P, gen2)
            h.send(P, _red(h, NEXT))
        assert Hold.READINESS_FAILED in h.p().holds
        if trigger == "read":
            r = h.send(P, _red(h, NEXT))
        else:
            r = h.send(P, ev.PlanFeedback(text_digest="any news?"))
        _no_messages_to(r, gen2)  # the comment goes to the rework, not to gen-2
        assert not [e for e in r.effects if e.kind == EffectKind.POST_COMMENT]
        _handed_over(h, gen2)


def test_a_comment_while_gen2_is_mid_turn_waits_for_the_turn_to_end():
    h = _live_889()
    gen2 = h.cur(P).session_id
    r = h.send(P, ev.PlanFeedback(text_digest="also fix the docs"))
    _no_messages_to(r, gen2)
    s = h.p().session(gen2)
    assert s is not None and s.lifecycle == Lifecycle.ACTIVE  # never interrupted
    assert EffectKind.INTERRUPT_TREE not in [e.kind for e in r.effects]
    r = h.quiesce(P, gen2)
    assert EffectKind.INTERRUPT_TREE not in [e.kind for e in r.effects]
    _no_messages_to(r, gen2)
    _handed_over(h, gen2)


def test_a_busy_scan_never_starts_the_handover():
    h = _live_889()
    gen2 = h.cur(P).session_id
    h.send(P, ev.TreeQuiescent(session_id=gen2, complete=True, busy=True))
    h.send(P, ev.TreeQuiescent(session_id=gen2, complete=False, busy=False))
    s = h.p().session(gen2)
    assert s is not None and s.lifecycle == Lifecycle.ACTIVE
    assert h.p().note != NOTE


def test_a_stop_still_wins_over_the_handover():
    h = _live_889()
    gen2 = h.cur(P).session_id
    h.send(P, ev.Stop())
    s = h.p().session(gen2)
    assert s is not None and s.fences  # the stop's own fence and drain
    h.quiesce(P, gen2)
    r = h.send(P, ev.CapacityAvailable())
    assert EffectKind.CREATE_SESSION not in [e.kind for e in r.effects]
    assert len(_live_sessions(h)) == 0


def test_restart_mid_handover_admits_the_rework_once():
    h = _live_889()
    gen2 = h.cur(P).session_id
    turn_end = h.f(P).make(ev.TreeQuiescent(session_id=gen2, complete=True, busy=False))
    start = len(h.log)
    before = {x.session_id for x in h.p().sessions}
    h.apply(turn_end)
    assert h.p().note == NOTE
    # Restart: the aggregate and admission come back from their stored form.
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))
    h.admission = admission_from_data(admission_to_data(h.admission))
    # The same observation redelivered, and an early admission attempt: no effect.
    r = h.apply(turn_end)
    assert EffectKind.CREATE_SESSION not in [e.kind for e in r.effects]
    r = h.send(P, ev.CapacityAvailable())
    assert EffectKind.CREATE_SESSION not in [e.kind for e in r.effects]
    _handed_over(h, gen2)
    r = h.send(P, ev.CapacityAvailable())
    assert not r.audit.accepted  # admitted once: nothing queued any more
    new = [x.session_id for x in h.p().sessions if x.session_id not in before]
    assert new == [h.cur(P).session_id]  # one rework run, admitted once
    notes = [
        e
        for _, res in h.log[start:]
        for e in res.effects
        if e.kind == EffectKind.SET_NOTE and e.args.get("note") == NOTE
    ]
    assert len(notes) == 1  # one handover, recorded once


def test_purpose_is_unchanged_for_a_run_without_a_queued_rework():
    """No rework queued: an idle run at Needs you still gets the owner comment relayed
    (steering), as before."""
    h = Harness()
    h.to_building()
    _submit(h)
    h.quiesce(P, h.cur(P).session_id)
    r = h.send(P, _red(h))
    [wake] = [e for e in r.effects if e.kind == EffectKind.SEND_MESSAGE]
    assert wake.args["purpose"] == MessagePurpose.READINESS_WAKE.value
    assert wake.preconditions.session_id == h.cur(P).session_id  # what _no_messages_to reads
    assert h.p().note != NOTE
