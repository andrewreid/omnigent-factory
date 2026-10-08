"""Derived board projection and admission arithmetic (architecture §2.7).

``Bot`` is derived from state, never used as authority. Precedence:
Blocked > Checkpoint > Needs you > Queued > Working > Idle.

A card in Ready is only Idle, Blocked or Needs you: any work for it (rework, a fix wake,
a replan) moves it to Building first (``ready_bot_ok``).
"""

from __future__ import annotations

from omnigent_factory.core.types import (
    BLOCKING_HOLDS,
    NEEDS_YOU_HOLDS,
    RELATIONS,
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
        return BotState.BLOCKED  # the bot's work is done; a required check is red
    if cur is not None and (
        cur.lifecycle in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT)
        or (cur.fences == frozenset({FenceKind.CHECKPOINT}) and cur.lifecycle != Lifecycle.RETIRED)
    ):
        return BotState.CHECKPOINT
    if p.open_decisions or p.holds & NEEDS_YOU_HOLDS:
        return BotState.NEEDS_YOU
    if queued:
        return BotState.QUEUED
    if _running(p):
        return BotState.WORKING
    return BotState.IDLE


def _running(p: Parcel) -> bool:
    """A stage run is doing work.

    In Ready waiting on a new head's or a re-run's checks the bot has nothing to do:
    Idle, with the note saying checks are running (``checks_running``).
    A safety/stop drain is never Idle until the tree is observed quiescent. A plan run
    waiting for approval has finished its stage: the next move is the owner's (Idle).
    """
    return any(
        s.lifecycle in _WORKING and not _awaiting_approval(s) and not _ready_waiting(p, s)
        for s in p.sessions
    )


def work_live(p: Parcel, *, queued: bool = False) -> bool:
    """Work for the card runs or waits for capacity, whatever ``Bot`` shows: Blocked and
    Needs you outrank Working, so the Bot value alone cannot say a run is live."""
    return queued or _running(p)


#: Bot values a card in the Ready column may show: no work runs while it is there.
READY_BOT_STATES = frozenset({BotState.IDLE, BotState.BLOCKED, BotState.NEEDS_YOU})


def ready_bot_ok(p: Parcel, bot: BotState) -> bool:
    """Ready and Working/Queued/Checkpoint are mutually exclusive."""
    return p.stage != Stage.READY or bot in READY_BOT_STATES


def checks_running(p: Parcel) -> bool:
    """In Ready while a new head's or a re-run's checks are evaluated (not red)."""
    r = p.readiness
    return (
        p.stage == Stage.READY
        and r is not None
        and not r.ready
        and not r.sync_red
        and Hold.COMPLETED not in p.holds
    )


def sync_red(p: Parcel) -> bool:
    """In Ready, the bot's work done, a required check red (see ``Readiness.sync_red``)."""
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


def _ready_waiting(p: Parcel, s: StageSession) -> bool:
    """In Ready, a build run that submitted and only waits on checks does no work (the
    next evidence read retires it or moves the card back to Building)."""
    return (
        p.stage == Stage.READY
        and s.lifecycle == Lifecycle.WAITING
        and s.wait_reason == WaitReason.CHECKS
        and not s.fences
    )


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

#: Not an owner action: the hold clears by itself once the issue session goes idle.
_EXTERNAL_ACTIVITY_NOTE = (
    "Waiting: the Omnigent session is busy outside the factory; work resumes when it is idle"
)


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
    elif bot in (BotState.IDLE, BotState.WORKING) and Hold.EXTERNAL_ACTIVITY in p.holds:
        text = _EXTERNAL_ACTIVITY_NOTE
    elif bot == BotState.IDLE and checks_running(p):
        assert p.readiness is not None  # noqa: S101 - checks_running checks it
        text = f"Checks running on `{p.readiness.head_sha[:7]}`"
    else:
        # Lowest precedence: other issues' triage named this one. Any status reason or
        # derived Blocked/Needs you/Checkpoint/Queued note above outranks it.
        text = related_note(p)
    return text[:NOTE_MAX]


#: How a related mark reads on the marked card (the source issue's relation to it).
RELATION_NOTE = dict(
    zip(
        RELATIONS,
        (
            "duplicate",
            "overlap",
            "conflict",
            "depends on this",
            "blocks this",
            "supersedes this",
        ),
        strict=True,
    )
)


def related_note(p: Parcel) -> str:
    """``Related: #12 (overlap), #9 (conflict)``, newest first ("" without marks)."""
    if not p.related_marks:
        return ""
    parts = [
        f"#{m.issue} ({RELATION_NOTE.get(m.relation, m.relation)})"
        for m in reversed(p.related_marks)
    ]
    return f"Related: {', '.join(parts)}"[:NOTE_MAX]


def triage_queued_note() -> str:
    """A triage request waiting for a free triage slot (``triage_concurrency``)."""
    return "Queued: triage starts when a triage slot is free"


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
