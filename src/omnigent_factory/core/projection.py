"""Derived board projection and admission arithmetic (architecture §2.7).

``Bot`` is derived from state, never used as authority. Precedence:
Blocked > Checkpoint > Needs you > Queued > Working > Idle.
"""

from __future__ import annotations

from omnigent_factory.core.types import (
    BLOCKING_HOLDS,
    NEEDS_YOU_HOLDS,
    AdmissionSnapshot,
    BotState,
    FenceKind,
    Hold,
    Lifecycle,
    Parcel,
    QueueEntry,
    QueueStatus,
    Stage,
    StageSession,
    TrustedConfig,
    WaitReason,
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


def project_bot(p: Parcel, *, queued: bool = False) -> BotState:
    """``queued``: the parcel's admission queue entry is waiting for build capacity."""
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
    if sync_red(p):
        return BotState.BLOCKED  # the base branch broke a required check: owner fixes main
    if cur is not None and (
        cur.lifecycle in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT)
        or (cur.fences == frozenset({FenceKind.CHECKPOINT}) and cur.lifecycle != Lifecycle.RETIRED)
    ):
        return BotState.CHECKPOINT
    if p.open_decisions or p.holds & NEEDS_YOU_HOLDS:
        return BotState.NEEDS_YOU
    if queued:
        return BotState.QUEUED
    if (
        p.stage == Stage.READY
        and p.readiness is not None
        and not p.readiness.ready
        and Hold.COMPLETED not in p.holds
    ):
        return BotState.WORKING  # in Ready, waiting on a new head's or re-run's checks
    # A safety/stop drain is never Idle until the tree is observed quiescent. A plan run
    # waiting for approval has finished its stage: the next move is the owner's (Idle).
    if any(s.lifecycle in _WORKING and not _awaiting_approval(s) for s in p.sessions):
        return BotState.WORKING
    return BotState.IDLE


def sync_red(p: Parcel) -> bool:
    """In Ready on a base-sync head whose required check is red (see ``Readiness``)."""
    r = p.readiness
    return (
        p.stage == Stage.READY
        and r is not None
        and r.sync_red
        and not r.ready
        and Hold.COMPLETED not in p.holds
    )


def sync_red_note(red_checks: str) -> str:
    return f"Required check red: {red_checks or 'see the PR checks'}"[:NOTE_MAX]


def _awaiting_approval(s: StageSession) -> bool:
    return s.lifecycle == Lifecycle.WAITING and s.wait_reason == WaitReason.PLAN_APPROVAL


#: Longest "Factory note" the board gets (one short line).
NOTE_MAX = 120

_HOLD_TEXT = {
    Hold.STOP_UNVERIFIED: "stop not verified",
    Hold.RESULT_INVALID: "stage result invalid",
    Hold.CREATE_REJECTED: "session create refused",
    Hold.PREPARE_FAILED: "session prepare failed",
    Hold.PUBLICATION_FAILED: "comment publication failed",
    Hold.RESTART_EXHAUSTED: "session could not be restarted",
    Hold.INBOX: "webhook delivery held",
    Hold.AGENT_BLOCKED: "agent reported blocked",
    Hold.CHECKS_FAILED: "checks failed",
    Hold.READINESS_FAILED: "PR not ready, no fix attempt left",
    Hold.REMEDIATION_EXHAUSTED: "fix budget used up",
    Hold.REWORK_CONTROL_REQUIRED: "needs a new stage control",
    Hold.UNSUPPORTED_REWORK: "rework not supported",
    Hold.PR_CLOSED: "PR closed unmerged",
    Hold.NO_PROJECT_ITEM: "not on the board",
    Hold.APPROVAL_VOIDED: "approval voided",
    Hold.EXTERNAL_ACTIVITY: "external session activity",
}


def project_note(p: Parcel, bot: BotState) -> str:
    """The board's "Factory note": the latest status reason, else one derived from ``bot``."""
    if p.note:
        return p.note[:NOTE_MAX]
    if bot == BotState.BLOCKED:
        why = [_HOLD_TEXT[h] for h in sorted(p.holds & BLOCKING_HOLDS) if h in _HOLD_TEXT]
        if p.unknown_effects:
            why.append("unconfirmed write")
        if not why and sync_red(p):
            assert p.readiness is not None  # noqa: S101 - sync_red checks it
            return sync_red_note(p.readiness.red_checks)
        if not why:
            why.append("session failed")
        text = f"Blocked: {', '.join(why)}"
    elif bot == BotState.CHECKPOINT:
        text = "Checkpoint: comment /continue to grant more time"
    elif bot == BotState.NEEDS_YOU:
        n = len(p.open_decisions)
        why = [f"{n} open question(s)"] if n else []
        why += [_HOLD_TEXT[h] for h in sorted(p.holds & NEEDS_YOU_HOLDS) if h in _HOLD_TEXT]
        text = f"Needs you: {', '.join(why) or 'owner decision'}"
    elif bot == BotState.QUEUED:
        text = "Queued: waiting for build capacity"
    else:
        text = ""
    return text[:NOTE_MAX]


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
