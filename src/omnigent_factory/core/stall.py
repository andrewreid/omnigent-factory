"""Stalled-run gates shared by the health sweep (proposes) and the reducer (decides).

A run is only ever nudged or escalated while it is the parcel's current run, ACTIVE,
unfenced and not closed, with nothing else in charge of it: no open owner question, no
owner/blocking hold, no queued owner comment, no submitted result, no queued rework, no
parked build slot and no unconfirmed message. A nudge additionally needs the ordinary
work gate (and, for a build, a valid approval): the sweep never adds authority.
"""

from __future__ import annotations

from datetime import UTC, datetime

from omnigent_factory.core.events import StallAction
from omnigent_factory.core.predicates import approval_ok, message_uncertain, work_allowed
from omnigent_factory.core.types import (
    BLOCKING_HOLDS,
    NEEDS_YOU_HOLDS,
    AdmissionSnapshot,
    Hold,
    Lifecycle,
    Parcel,
    QueueStatus,
    SessionKind,
    Stage,
    StageSession,
    StallEscalation,
    StallWatch,
)

#: Holds under which someone else is in charge of the run (Hold.STALLED aside: that is
#: the sweep's own escalation, handled by the episode rules).
_OWNED_HOLDS = (
    BLOCKING_HOLDS
    | NEEDS_YOU_HOLDS
    | {
        Hold.SAFETY,
        Hold.STOPPED,
        Hold.AWAITING_OWNER,
        Hold.PUBLICATION_PENDING,
        Hold.EXTERNAL_ACTIVITY,
        Hold.BLOCKERS,
        Hold.COMPLETED,
        Hold.INBOX,
    }
) - {Hold.STALLED}

#: Card notes the sweep writes (cleared with the hold).
STALLED_NOTE_PREFIX = "Stalled: "


def stall_gate(p: Parcel, admission: AdmissionSnapshot, action: StallAction) -> str | None:
    """Why the sweep may not act on ``p``'s current run now (None: it may)."""
    s = p.current_session
    if s is None or s.root_id is None:
        return "no-current-run"
    if not p.eligible or p.stage in (Stage.DONE, Stage.READY):
        return "stage"
    if (
        s.lifecycle != Lifecycle.ACTIVE
        or s.fences
        or s.execution_closed
        or s.restart_pending
        or s.external_active
    ):
        return "run-not-active"
    if p.open_decisions:
        return "open-question"
    if p.holds & _OWNED_HOLDS:
        return "held"
    if p.unknown_effects or message_uncertain(p, s):
        return "unconfirmed-write"
    if s.comment_pending:
        return "feedback-pending"  # the turn's end (or the next reconcile) relays it
    if p.readiness is not None and p.readiness.session_id == s.session_id:
        return "result-submitted"  # readiness owns it (wakes, Ready, Needs you)
    if _rework_queued(p, admission, s):
        return "rework-queued"  # it gives way at its idle point (#889)
    w = s.stall
    if action == StallAction.PROGRESS:
        # An escalated build parked its slot (Needs you); progress takes it back.
        return None if w.escalated is not None else "not-escalated"
    if p.slot_parked or any(h.session_id == s.session_id for h in p.held_wakes):
        return "parked"
    if w.escalated is not None or Hold.STALLED in p.holds:
        return "escalated"
    if action == StallAction.FROZEN:
        return "not-busy" if s.quiescent else None
    if not s.quiescent:
        return "busy"
    if action == StallAction.NUDGE and (
        not work_allowed(p, s) or (s.kind == SessionKind.BUILD and not approval_ok(p))
    ):
        return "work-gate"
    return None


def _rework_queued(p: Parcel, admission: AdmissionSnapshot, s: StageSession) -> bool:
    """A newer rework authorization of the current approval waits for a slot (#889)."""
    a = p.current_approval
    entry = admission.queue_entry(p.parcel_id)
    if (
        s.kind != SessionKind.BUILD
        or a is None
        or entry is None
        or entry.approval_id != a.approval_id
        or entry.status != QueueStatus.QUEUED
    ):
        return False
    queued = next(
        (
            x
            for x in reversed(p.authorizations)
            if x.approval_id == a.approval_id and not x.cancelled
        ),
        None,
    )
    return queued is not None and queued.rework and queued.authorization_id != s.authorization_id


def clock_text(us: int) -> str:
    """A UTC time for owner-facing text, e.g. ``Oct 14 23:34 UTC`` ("unknown" if 0)."""
    if us <= 0:
        return "an unknown time"
    return datetime.fromtimestamp(us / 1_000_000, UTC).strftime("%b %d %H:%M UTC")


def stalled_note(
    kind: StallEscalation, *, idle_since_us: int, nudged_at_us: int, nudges: int
) -> str:
    if kind == StallEscalation.FROZEN:
        return f"{STALLED_NOTE_PREFIX}no activity since {clock_text(idle_since_us)}"
    if kind == StallEscalation.LOOP:
        return f"{STALLED_NOTE_PREFIX}{nudges} nudges without a new commit or result"
    return f"{STALLED_NOTE_PREFIX}nudged {clock_text(nudged_at_us)}, no progress since"


def stall_view(watch: StallWatch) -> dict[str, object]:
    """A run's ``StallWatch`` for ``status``/``explain`` (times as ISO UTC)."""
    data: dict[str, object] = {
        "nudges_sent": watch.nudges_sent,
        "nudged_at": iso_utc(watch.nudged_at_us),
        "idle_since": iso_utc(watch.idle_since_us),
        "recovery": watch.recovery,
        "budget": watch.budget,
        "escalated": watch.escalated.value if watch.escalated is not None else None,
        "escalations": watch.escalations,
    }
    return data


def iso_utc(us: int) -> str | None:
    """``us`` as an ISO 8601 UTC time (None for 0)."""
    return datetime.fromtimestamp(us / 1_000_000, UTC).isoformat() if us > 0 else None


__all__ = [
    "STALLED_NOTE_PREFIX",
    "clock_text",
    "iso_utc",
    "stall_gate",
    "stall_view",
    "stalled_note",
]
