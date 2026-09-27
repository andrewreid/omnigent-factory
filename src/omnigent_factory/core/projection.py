"""Derived board projection and admission arithmetic (architecture §2.7).

``Bot`` is derived from state, never used as authority. Precedence:
Blocked > Checkpoint > Needs you > Working > Idle.
"""

from __future__ import annotations

from omnigent_factory.core.types import (
    BLOCKING_HOLDS,
    NEEDS_YOU_HOLDS,
    AdmissionSnapshot,
    BotState,
    FenceKind,
    Lifecycle,
    Parcel,
    QueueEntry,
    QueueStatus,
    Stage,
    TrustedConfig,
)

_SETTLED = frozenset({Lifecycle.RETIRED, Lifecycle.FENCED})
_WORKING = frozenset(
    {
        Lifecycle.INTENT,
        Lifecycle.CREATING,
        Lifecycle.PREPARING,
        Lifecycle.ACTIVE,
        Lifecycle.WAITING,
        Lifecycle.DRAINING,
    }
)


def finished(p: Parcel) -> bool:
    """Closed/merged (no longer eligible) or on the Done column, with every stage session
    settled: nothing is left for the bot to do, so leftover holds no longer apply."""
    return (p.stage == Stage.DONE or not p.eligible) and all(
        s.lifecycle in _SETTLED for s in p.sessions
    )


def project_bot(p: Parcel) -> BotState:
    cur = p.current_session
    lifecycles = {s.lifecycle for s in p.sessions}
    if finished(p) and not p.unknown_effects:
        return BotState.IDLE
    if (
        p.holds & BLOCKING_HOLDS
        or p.unknown_effects
        or lifecycles
        & {
            Lifecycle.BLOCKED,
            Lifecycle.UNKNOWN,
            Lifecycle.FAILED,
        }
    ):
        return BotState.BLOCKED
    if cur is not None and (
        cur.lifecycle in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT)
        or (cur.fences == frozenset({FenceKind.CHECKPOINT}) and cur.lifecycle != Lifecycle.RETIRED)
    ):
        return BotState.CHECKPOINT
    if p.open_decisions or p.holds & NEEDS_YOU_HOLDS:
        return BotState.NEEDS_YOU
    # A safety/stop drain is never Idle until the tree is observed quiescent.
    if lifecycles & _WORKING:
        return BotState.WORKING
    return BotState.IDLE


def queue_head(admission: AdmissionSnapshot) -> QueueEntry | None:
    """Oldest valid queued entry by persisted approval sequence, then parcel ID."""
    queued = [q for q in admission.queue if q.status == QueueStatus.QUEUED]
    if not queued:
        return None
    return min(queued, key=lambda q: (q.sequence, q.parcel_id))


def building_capacity_available(admission: AdmissionSnapshot, config: TrustedConfig) -> bool:
    return admission.building_count < config.max_building


def pr_capacity_available(admission: AdmissionSnapshot, config: TrustedConfig) -> bool:
    return admission.prospective_pr_count < config.max_open_bot_prs
