"""#889 / PR #1022: an omni-host restart crashed the build run while it sat at Needs you.

The owner replied during the crashed run's drain, which queued a rework (a new build
authorization carrying the feedback). The drain then restarted the crashed run's *old*
episode anyway (``restart_pending``), without the feedback, and the rework sat queued
behind it. Now the queued rework replaces the crashed run: no restart of the old episode,
the building slot is freed for the rework, and only one run exists at a time.
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.types import Hold, Lifecycle, QueueStatus
from omnigent_factory.testing.builders import result_candidate
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
PR = 1022
HEAD = "30613e90c0d2" + "a" * 28


def _needs_you(h: Harness) -> None:
    """Build submitted, the one wake spent, the woken turn ended idle: Needs you."""
    h.to_building()
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
            head_sha=HEAD,
        ),
    )
    assert r.audit.accepted, r.audit.reason
    h.quiesce(P, s.session_id)
    red = ev.ReadinessEvidence(
        session_id=s.session_id,
        pr_number=PR,
        head_sha=HEAD,
        checks=ev.ChecksState.FAILED,
        findings_open=True,
        review_accepted=False,
    )
    h.send(P, red)  # the one wake
    h.quiesce(P, s.session_id)
    h.send(P, red)
    assert Hold.READINESS_FAILED in h.p().holds


def _live(h: Harness) -> list[str]:
    return [
        x.session_id
        for x in h.p().sessions
        if x.lifecycle not in (Lifecycle.RETIRED, Lifecycle.FENCED)
    ]


def test_owner_feedback_during_a_crash_drain_replaces_the_restart():
    h = Harness()
    _needs_you(h)
    crashed = h.cur(P)
    old_auth = crashed.authorization_id
    h.send(P, ev.SessionCrashed(session_id=crashed.session_id))
    assert h.p().session(crashed.session_id).restart_pending
    # The owner replies while the crashed run drains: a rework is queued.
    h.send(P, ev.PlanFeedback(text_digest="please fix the failing check"))
    entry = h.admission.queue_entry(P)
    assert entry is not None and entry.status == QueueStatus.QUEUED
    # The drain completes: the crashed run is not restarted on its old episode.
    r = h.quiesce(P, crashed.session_id)
    assert EffectKind.CREATE_SESSION not in [e.kind for e in r.effects]
    old = h.p().session(crashed.session_id)
    assert old.lifecycle == Lifecycle.RETIRED and not old.restart_pending
    assert Hold.RESTART_EXHAUSTED not in h.p().holds
    assert h.admission.building_count == 0  # the slot is free for the rework
    entry = h.admission.queue_entry(P)
    assert entry is not None and entry.status == QueueStatus.QUEUED
    # The rework is admitted next and carries the feedback (its own authorization).
    rework = h.admit(P)
    auth = h.p().authorization(rework.authorization_id)
    assert auth is not None and auth.rework and auth.authorization_id != old_auth
    assert _live(h) == [rework.session_id]
    assert h.admission.building_count == 1


def test_a_crash_without_queued_feedback_still_restarts_once():
    h = Harness()
    _needs_you(h)
    crashed = h.cur(P)
    h.send(P, ev.SessionCrashed(session_id=crashed.session_id))
    r = h.quiesce(P, crashed.session_id)
    assert EffectKind.CREATE_SESSION in [e.kind for e in r.effects]
    replacement = h.cur(P)
    assert replacement.session_id != crashed.session_id
    assert replacement.authorization_id == crashed.authorization_id
