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
    AutoBuildStatus,
    BotState,
    FenceKind,
    Hold,
    Lifecycle,
    Parcel,
    QueueEntry,
    QueueStatus,
    Readiness,
    ReservationKind,
    Stage,
    StageSession,
    TrustedConfig,
    WaitReason,
    blockers_text,
    is_leftward,
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
    if Hold.BUNDLED in p.holds and p.bundle_bot in _BOT_VALUES:
        return BotState(p.bundle_bot)  # a bundle member mirrors its bundle issue
    if queued:
        return BotState.QUEUED
    if Hold.BLOCKERS in p.holds and (cur is None or cur.quiescent):
        return BotState.IDLE  # waits on open native blockers: nothing to do until they close
    if _running(p):
        return BotState.WORKING
    return BotState.IDLE


_BOT_VALUES = frozenset(b.value for b in BotState)


def bundle_note(bundle: int) -> str:
    return f"In bundle #{bundle}"


def bundle_member_refusal(p: Parcel, *, queued: bool = False) -> str | None:
    """Why ``p`` cannot be held in a bundle now (None: it can): only an open, unassigned
    issue in Triage or Planning with no stage work running, queued or marked."""
    if not p.eligible:
        return "closed, assigned or unverified"
    if p.stage not in (Stage.TRIAGED, Stage.SCOPED):
        return "not in Triage or Planning"
    if p.pending_authorization_id is not None or queued or _running(p):
        return "work on it is running or queued"
    if p.auto_build is not None:
        return "it has an auto-build mark"
    if p.autopilot_claim is not None and p.autopilot_claim.active:
        return "epic autopilot drives it"
    if p.open_decisions or p.holds & (NEEDS_YOU_HOLDS | BLOCKING_HOLDS):
        return "it needs you"
    return None


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
    """In Ready while a new head's or a re-run's checks are evaluated (not red, or red
    not yet confirmed on a fresh head)."""
    r = p.readiness
    return (
        p.stage == Stage.READY
        and r is not None
        and not r.ready
        and (not r.sync_red or red_unconfirmed(r))
        and Hold.COMPLETED not in p.holds
    )


def red_unconfirmed(r: Readiness) -> bool:
    """A first red read on a fresh head, not yet confirmed (see
    ``Readiness.red_unconfirmed_until_us``): shown as checks running, not Blocked."""
    return r.sync_red and r.red_unconfirmed_until_us > 0


def unexplained_move(p: Parcel) -> bool:
    """The card was moved right on the board (seen by a read or a non-owner) and no owner
    control explains it yet: it sits there with no authority."""
    m = p.observed_move
    return m is not None and m.to_stage == p.stage and not is_leftward(m.from_stage, m.to_stage)


def unexplained_move_note(stage: Stage) -> str:
    return (
        f"Moved to {stage.value} without an owner command seen: "
        "drag it again or add a factory: label"
    )[:NOTE_MAX]


def sync_red(p: Parcel) -> bool:
    """In Ready, the bot's work done, a required check red (see ``Readiness.sync_red``)."""
    r = p.readiness
    return (
        p.stage == Stage.READY
        and r is not None
        and r.sync_red
        and not red_unconfirmed(r)
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
    Hold.STALLED: "run stalled",
    Hold.REPLAN_LIMIT: "plan must change again: /replan",
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
    elif bot == BotState.IDLE and unexplained_move(p):
        assert p.stage is not None  # noqa: S101 - unexplained_move checks it
        text = unexplained_move_note(p.stage)
    elif Hold.BUNDLED in p.holds and p.bundled_into:
        text = bundle_note(p.bundled_into)
    elif waiting_on_note(p):
        text = waiting_on_note(p)
    else:
        # Lowest precedence: an epic's progress, else other issues' triage named this
        # one. Any status reason or derived Blocked/Needs you/Checkpoint/Queued note
        # above outranks it.
        text = p.epic_note or related_note(p)
    return text[:NOTE_MAX]


def waiting_on_note(p: Parcel) -> str:
    """``Waiting on #823``: a queued auto-build mark, a held approval or a parked build run
    waiting on open native blockers ("" if none)."""
    mark = p.auto_build
    queued = mark is not None and mark.status == AutoBuildStatus.QUEUED
    if not (queued or Hold.BLOCKERS in p.holds) or p.links is None:
        return ""
    blockers = p.links.open_blockers
    return f"Waiting on {blockers_text(blockers)}" if blockers else ""


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


def admission_key(q: QueueEntry) -> tuple[bool, bool, int, str]:
    """Admission order: parked builds resuming first, then every manually approved build
    before any auto-build, then the persisted approval sequence, then parcel ID."""
    return (not q.resume, q.auto, q.sequence, q.parcel_id)


def queue_head(admission: AdmissionSnapshot) -> QueueEntry | None:
    """The next queued entry to admit (``admission_key``)."""
    queued = [q for q in admission.queue if q.status == QueueStatus.QUEUED]
    if not queued:
        return None
    return min(queued, key=admission_key)


def admissible_head(admission: AdmissionSnapshot, config: TrustedConfig) -> QueueEntry | None:
    """The next queued entry capacity may admit: the queue head, passing over auto-build
    entries while every auto-build slot is taken (they never hold up a manual build)."""
    queued = sorted(
        (q for q in admission.queue if q.status == QueueStatus.QUEUED), key=admission_key
    )
    auto_full = not auto_build_capacity_available(admission, config)
    for q in queued:
        if q.auto and auto_full:
            continue
        return q
    return None


def holds_pr_slot(
    admission: AdmissionSnapshot,
    parcel_id: str,
    *,
    open_pr_parcels: frozenset[str] = frozenset(),
) -> bool:
    """Whether ``parcel_id`` can build without consuming another open-PR slot.

    Reservations are the durable ownership record. ``open_pr_parcels`` supplements it
    with parcel aggregates whose linked PR is in ``open_bot_prs`` (used by the service
    while choosing a parcel; the reducer normally sees the same ownership as a bound
    reservation).
    """
    return parcel_id in open_pr_parcels or any(
        r.parcel_id == parcel_id and r.kind == ReservationKind.OPEN_PR and r.live
        for r in admission.reservations
    )


def admissible_build(
    admission: AdmissionSnapshot,
    config: TrustedConfig,
    *,
    open_pr_parcels: frozenset[str] = frozenset(),
    excluded_parcels: frozenset[str] = frozenset(),
) -> QueueEntry | None:
    """The ordered queued entry capacity may try next.

    Normally this is :func:`admissible_head`. If (and only if) that entry is blocked by
    the open-PR cap, the first later entry which already owns a PR slot may pass it. As
    soon as a slot is free the head wins again. Auto-build entries whose separate cap is
    full and temporarily excluded deliveries keep their existing pass-over semantics.
    """
    queued = sorted(
        (
            q
            for q in admission.queue
            if q.status == QueueStatus.QUEUED and q.parcel_id not in excluded_parcels
        ),
        key=admission_key,
    )
    auto_full = not auto_build_capacity_available(admission, config)
    eligible = [q for q in queued if not (q.auto and auto_full)]
    if not eligible:
        return None
    head = eligible[0]
    if pr_capacity_available(admission, config) or holds_pr_slot(
        admission, head.parcel_id, open_pr_parcels=open_pr_parcels
    ):
        return head
    return next(
        (
            q
            for q in eligible[1:]
            if holds_pr_slot(admission, q.parcel_id, open_pr_parcels=open_pr_parcels)
        ),
        None,
    )


def running_text(entries: tuple[QueueEntry, ...]) -> str:
    """``#673, #680`` for running build episodes (``?`` for an unknown issue)."""
    return ", ".join(f"#{q.issue_number}" if q.issue_number else "?" for q in entries)


def auto_build_capacity_available(admission: AdmissionSnapshot, config: TrustedConfig) -> bool:
    """Another auto-build may hold a building slot (``auto_build_concurrency``)."""
    return admission.auto_build_count < config.auto_build_concurrency


def building_capacity_available(admission: AdmissionSnapshot, config: TrustedConfig) -> bool:
    return admission.building_count < config.max_building


def pr_capacity_available(admission: AdmissionSnapshot, config: TrustedConfig) -> bool:
    return admission.prospective_pr_count < config.max_open_bot_prs


def startable_build(admission: AdmissionSnapshot, config: TrustedConfig) -> QueueEntry | None:
    """The queued build that capacity would admit right now, if any.

    Not paused, the queue head, a building slot free (for an auto-build also an auto-build
    slot) and an open-PR slot free unless the head already holds one: the repository-wide
    checks of ``CapacityAvailable`` (its per-parcel preconditions are the parcel's own).
    """
    head = admissible_build(admission, config)
    if head is None or admission.paused or not building_capacity_available(admission, config):
        return None
    return head
