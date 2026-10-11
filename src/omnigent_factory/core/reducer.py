"""The pure reducer ``transition(State, Event) -> TransitionResult`` (architecture §2).

No I/O, no clock, no environment, no randomness. Identifiers derive from the event ID,
the parcel version and an ordinal; nonces derive from the event's persisted ``entropy``.

Table families are applied in order:

1. envelope / deduplication (wrong repo or parcel, duplicate logical event ID);
2. current safety facts carried as fresh-read evidence;
3. control eligibility / authorization (owner, provenance, freshness after barrier);
4. the stage-specific handler for the event kind (§2.6 tables A-E);
5. guarded wake-ups of recorded successors, then the derived board projection.

A handler that finds no matching row raises :class:`Rejected`: the event becomes an
audited self-loop (state after family 2 is kept; handler changes are discarded) with no
work effect and at most one explanation. There is no "otherwise resume" path.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, replace
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.admission import admission_rejection
from omnigent_factory.core.canonical import (
    canonical_issue_snapshot,
    issue_snapshot_digest,
    resolve_hash,
    sha256_hex,
)
from omnigent_factory.core.effects import (
    EffectIntent,
    EffectKind,
    JsonValue,
    MessagePurpose,
    Preconditions,
    RetryClass,
    profile_for,
)
from omnigent_factory.core.events import Event, EventKind
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.predicates import (
    all_settled,
    approval_ok,
    authority_ok,
    board_pending,
    dispatchable,
    eligible,
    executable,
    gate_open,
    message_uncertain,
    only_checkpoint_fenced,
    plan_ok,
    settled,
    uncertain,
    work_allowed,
)
from omnigent_factory.core.projection import (
    NOTE_MAX,
    admissible_head,
    admission_key,
    auto_build_capacity_available,
    building_capacity_available,
    pr_capacity_available,
    project_bot,
    project_note,
    queue_head,
    ready_bot_ok,
    running_text,
    startable_build,
    sync_red_note,
    triage_queued_note,
    work_live,
)
from omnigent_factory.core.types import (
    AUTO_BUILD_QUEUED,
    AUTO_BUILD_STARTED,
    AUTOPILOT_LEVELS,
    BLOCKING_HOLDS,
    CONTROL_CLEARED_HOLDS,
    MICROS_PER_MINUTE,
    RELATIONS,
    STAGE_ORDER,
    AdmissionSnapshot,
    Approval,
    ApprovalKind,
    AutoBuildMark,
    AutoBuildStatus,
    AutopilotClaim,
    AutopilotGate,
    BotState,
    Contract,
    Decision,
    DecisionImpact,
    DecisionSource,
    DecisionStatus,
    EpicAutopilot,
    EpicPlan,
    FenceKind,
    Grant,
    HeldWake,
    Hold,
    InboxHold,
    InboxHoldReason,
    IssueSession,
    IssueSessionStatus,
    IssueSnapshot,
    Lifecycle,
    ObservedMove,
    Parcel,
    PendingMove,
    QueueEntry,
    QueueStatus,
    Readiness,
    RelatedMark,
    Reservation,
    ReservationKind,
    SessionKind,
    Size,
    Stage,
    StageAuthorization,
    StageSession,
    State,
    TrustedConfig,
    UnknownEffect,
    Via,
    WaitReason,
    blockers_text,
    is_leftward,
    issue_session_title,
)


class Rejected(Exception):
    """No transition row matched; carries the audit reason and optional explanation."""

    def __init__(
        self,
        reason: str,
        *,
        explain: bool = False,
        rollback_to: Stage | None = None,
        note: str | None = None,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.explain = explain
        self.rollback_to = rollback_to
        #: The owner-facing note instead of ``Command refused: <reason>``.
        self.note = note


@dataclass(frozen=True, slots=True)
class AuditRecord:
    event_id: str
    kind: str
    parcel_id: str | None
    accepted: bool
    reason: str
    before_version: int | None
    after_version: int | None
    dropped_effects: int = 0


@dataclass(frozen=True, slots=True)
class TransitionResult:
    """Reducer output. Iterable as ``(state, effects)`` for the §2.1 signature."""

    state: State
    effects: tuple[EffectIntent, ...]
    audit: AuditRecord
    duplicate: bool = False

    def __iter__(self) -> Iterator[Any]:
        yield self.state
        yield self.effects


def derive_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


_DEFAULT_RETRY: dict[EffectKind, RetryClass] = {
    EffectKind.MOVE_CARD: RetryClass.ADOPTABLE_WRITE,
    EffectKind.SET_BOT: RetryClass.ADOPTABLE_WRITE,
    EffectKind.SET_NOTE: RetryClass.ADOPTABLE_WRITE,
    EffectKind.SET_AUTO_BUILD: RetryClass.ADOPTABLE_WRITE,
    EffectKind.SET_AUTOPILOT: RetryClass.ADOPTABLE_WRITE,
    EffectKind.REACT_COMMENT: RetryClass.ADOPTABLE_WRITE,
    EffectKind.POST_COMMENT: RetryClass.ADOPTABLE_WRITE,
    EffectKind.PUBLISH_CONTRACT: RetryClass.ADOPTABLE_WRITE,
    EffectKind.PUBLISH_TRIAGE: RetryClass.ADOPTABLE_WRITE,
    EffectKind.PUBLISH_REPORT: RetryClass.ADOPTABLE_WRITE,
    EffectKind.EDIT_REPORT: RetryClass.READ,  # idempotent PATCH of a marked comment
    EffectKind.ENSURE_PROJECT_ITEM: RetryClass.ADOPTABLE_WRITE,
    EffectKind.FETCH_PR_EVIDENCE: RetryClass.READ,
    EffectKind.RECONCILE_PARCEL: RetryClass.READ,
    EffectKind.CREATE_SESSION: RetryClass.NEVER_BLIND,
    EffectKind.PREPARE_SESSION: RetryClass.LOCAL_IDEMPOTENT,
    EffectKind.SEND_MESSAGE: RetryClass.NEVER_BLIND,
    EffectKind.RESOLVE_ELICITATION: RetryClass.NEVER_BLIND,
    EffectKind.INTERRUPT_TREE: RetryClass.READ,
    EffectKind.SCAN_TREE: RetryClass.READ,
    EffectKind.RECONCILE_SESSION: RetryClass.READ,
    EffectKind.REPLACE_COST_POLICY: RetryClass.ADOPTABLE_WRITE,
    EffectKind.CLOSE_SESSION: RetryClass.READ,  # idempotent archive; safe to repeat
    EffectKind.RENAME_SESSION: RetryClass.READ,  # idempotent title PATCH
    EffectKind.VERIFY_POLICIES: RetryClass.READ,  # reads, or idempotent re-establishment
    EffectKind.DISABLE_ISSUANCE: RetryClass.LOCAL_IDEMPOTENT,
    EffectKind.ENABLE_ISSUANCE: RetryClass.LOCAL_IDEMPOTENT,
    EffectKind.CLEANUP_WORKSPACE: RetryClass.LOCAL_IDEMPOTENT,
    EffectKind.ARM_TIMER: RetryClass.LOCAL_IDEMPOTENT,
    EffectKind.WAKE_SCHEDULER: RetryClass.LOCAL_IDEMPOTENT,
}


class _Ctx:
    """Mutable scratch space for one pure transition. Never escapes the reducer."""

    def __init__(self, state: State, event: Event) -> None:
        self.event = event
        self.config: TrustedConfig = state.config
        self.admission: AdmissionSnapshot = state.admission
        self._parcel = state.parcel
        self.effects: list[EffectIntent] = []
        self.effects_dropped: list[EffectIntent] = []
        self._ordinal = 0
        self._ids = 0
        self.before_version = state.parcel.version if state.parcel else None
        #: Persisted column before this event's evidence (control allow-lists use it).
        self.origin_stage = state.parcel.stage if state.parcel else None
        #: This event set the parcel's status note (it wins over clearing).
        self.note_set = False

    # -- parcel access
    @property
    def p(self) -> Parcel:
        assert self._parcel is not None  # noqa: S101 - parcel handlers only
        return self._parcel

    @p.setter
    def p(self, value: Parcel) -> None:
        self._parcel = value

    @property
    def maybe_parcel(self) -> Parcel | None:
        return self._parcel

    def restore(
        self, parcel: Parcel | None, admission: AdmissionSnapshot, effects: list[EffectIntent]
    ) -> None:
        self._parcel = parcel
        self.admission = admission
        self.effects = effects

    @property
    def now(self) -> int:
        return self.event.source_time_us

    @property
    def next_version(self) -> int:
        return (self.before_version or 0) + 1

    # -- identifiers
    def new_id(self, prefix: str) -> str:
        self._ids += 1
        return derive_id(prefix, self.event.event_id, str(self.next_version), str(self._ids))

    def nonce(self) -> str | None:
        if not self.event.entropy:
            return None
        self._ids += 1
        return sha256_hex(f"{self.event.entropy}\x1f{self.event.event_id}\x1f{self._ids}".encode())[
            :32
        ]

    # -- mutation helpers
    def update(self, **changes: Any) -> None:
        self.p = replace(self.p, **changes)

    def put_session(self, s: StageSession) -> StageSession:
        sessions = tuple(s if x.session_id == s.session_id else x for x in self.p.sessions)
        self.update(sessions=sessions)
        return s

    def put_decision(self, d: Decision) -> None:
        self.update(
            decisions=tuple(d if x.decision_id == d.decision_id else x for x in self.p.decisions)
        )

    def put_approval(self, a: Approval) -> None:
        self.update(
            approvals=tuple(a if x.approval_id == a.approval_id else x for x in self.p.approvals)
        )

    def put_authorization(self, a: StageAuthorization) -> None:
        self.update(
            authorizations=tuple(
                a if x.authorization_id == a.authorization_id else x for x in self.p.authorizations
            )
        )

    def put_contract(self, c: Contract) -> None:
        self.update(
            contracts=tuple(c if x.contract_id == c.contract_id else x for x in self.p.contracts)
        )

    def hold(self, h: Hold) -> None:
        self.update(holds=self.p.holds | {h})

    def unhold(self, *hs: Hold) -> None:
        self.update(holds=self.p.holds - frozenset(hs))

    # -- effects
    def emit(
        self,
        kind: EffectKind,
        *,
        session: StageSession | None = None,
        target: str | None = None,
        args: Mapping[str, JsonValue] | None = None,
        dedupe: str | None = None,
    ) -> EffectIntent:
        self._ordinal += 1
        parcel = self._parcel
        effect_id = derive_id("ef", self.event.event_id, str(self.next_version), str(self._ordinal))
        approval_id: str | None = None
        authorization_id: str | None = None
        if session is not None and parcel is not None:
            authorization_id = session.authorization_id
            auth = parcel.authorization(session.authorization_id)
            approval_id = auth.approval_id if auth is not None else None
        pre = Preconditions(
            parcel_version=self.next_version,
            eligibility_epoch=parcel.eligibility_epoch if parcel is not None else 0,
            session_id=session.session_id if session is not None else None,
            authorization_id=authorization_id,
            approval_id=approval_id,
            grant_id=session.grant.grant_id if session is not None else None,
        )
        default_target = (
            session.session_id
            if session is not None
            else (parcel.parcel_id if parcel is not None else self.admission.repo_id)
        )
        effect = EffectIntent(
            effect_id=effect_id,
            kind=kind,
            parcel_id=parcel.parcel_id if parcel is not None else None,
            target=target or default_target,
            preconditions=pre,
            args=dict(args or {}),
            retry_class=_DEFAULT_RETRY[kind],
            dedupe_key=dedupe or effect_id,
        )
        self.effects.append(effect)
        if kind in (EffectKind.ENABLE_ISSUANCE, EffectKind.DISABLE_ISSUANCE) and session:
            current = self.p.session(session.session_id)
            if current is not None:
                enabled = kind == EffectKind.ENABLE_ISSUANCE
                self.update(
                    sessions=tuple(
                        replace(x, issuance_enabled=enabled)
                        if x.session_id == current.session_id
                        else x
                        for x in self.p.sessions
                    )
                )
        if (
            kind in (EffectKind.SEND_MESSAGE, EffectKind.RESOLVE_ELICITATION)
            and session is not None
            and parcel is not None
        ):
            self.update(sent_effects=(*self.p.sent_effects, (effect_id, session.session_id)))
        return effect

    def comment(self, template: str, **args: JsonValue) -> None:
        """An issue comment: only for what the owner must read or act on (it notifies)."""
        self.emit(EffectKind.POST_COMMENT, args={"template": template, **args})

    def note(self, text: str) -> None:
        """Informational status: shown on the card's "Factory note" field, no comment."""
        self.update(note=text[:NOTE_MAX])
        self.note_set = True


# ============================================================ generic helpers


def _control(ctx: _Ctx) -> None:
    """F ∧ O: authenticated, owner, actor-bearing, strictly after the latest barrier."""
    e = ctx.event
    if e.provenance not in ev.CONTROL_PROVENANCES:
        raise Rejected("control-without-authenticated-source")
    if e.actor_id is None or e.actor_id not in ctx.config.owners:
        raise Rejected("control-from-non-owner")
    if e.source_time_us <= ctx.p.barrier_time_us:
        raise Rejected("control-not-fresh-after-barrier", explain=True)


def _require_eligible(ctx: _Ctx) -> None:
    if not eligible(ctx.p):
        raise Rejected("parcel-not-eligible", explain=True)


def _grant_duration(ctx: _Ctx, requested: int | None, default_size: Size) -> int:
    if requested is not None:
        if requested <= 0 or requested > ctx.config.max_grant_us:
            raise Rejected("grant-duration-out-of-bounds", explain=True)
        return requested
    return ctx.config.block_us(ctx.p.size or default_size)


def _move(ctx: _Ctx, stage: Stage, via: Via | None = None) -> None:
    if ctx.p.stage == stage:
        return
    if via == Via.DRAG and not ctx.p.pending_moves:
        ctx.update(stage=stage, queued_move=None)  # the owner already put the card there
        return
    # A drag during an in-flight daemon write is the newest desired column: queue it so
    # the older write landing cannot leave the card where the owner did not put it.
    _emit_move(ctx, stage)


def _emit_move(ctx: _Ctx, stage: Stage, *, source: Stage | None = None) -> None:
    """Record ``stage`` as the desired column and write it to the board, serialised.

    Invariant: at most one daemon board write is in flight per parcel. While one is in
    flight a newer desired column is coalesced into ``queued_move`` (latest wins) and
    issued only after the in-flight write's trusted outcome (``_retire_move``), so an
    older outcome can never overwrite a newer intent. ``source`` overrides the expected
    source column (used when the persisted stage is not where the card is, e.g. the
    rollback of an owner's invalid drag).
    """
    if ctx.p.pending_moves:
        inflight = ctx.p.pending_moves[0]
        ctx.update(stage=stage, queued_move=None if stage == inflight.to_stage else stage)
        return
    _issue_move(ctx, stage, ctx.p.stage if source is None else source)


def _issue_move(ctx: _Ctx, stage: Stage, source: Stage | None) -> None:
    effect = ctx.emit(
        EffectKind.MOVE_CARD,
        args={"to": stage.value, "expected_from": source.value if source else None},
    )
    ctx.update(
        stage=stage,
        queued_move=None,
        pending_moves=(PendingMove(effect.effect_id, source, stage),),
    )


def _retire_move(ctx: _Ctx, move: PendingMove, *, landed: bool) -> None:
    """Resolve the in-flight board write from a trusted executor outcome.

    Landed: the card is at the exact target. The desired column is left untouched (it
    may already be newer); a queued target is issued from the landed column.
    Absent (cancelled, definitively failed, reconciled undelivered): the card is still
    in (or back at) the source column, which is reconciled like any fresh observation -
    leftward of the believed stage is a safety fact (which also drops a queued target).
    Otherwise the queued target is issued from the source column.
    """
    ctx.update(pending_moves=tuple(m for m in ctx.p.pending_moves if m.effect_id != move.effect_id))
    if landed:
        ctx.update(board_written_at_us=max(ctx.p.board_written_at_us, ctx.now))
    board = move.to_stage if landed else move.from_stage
    if not landed and move.from_stage is not None:
        _observe_stage(ctx, move.from_stage)
    queued = ctx.p.queued_move
    if queued is None:
        return
    if queued == board:
        ctx.update(queued_move=None)
        return
    _issue_move(ctx, queued, board)


def _new_authorization(
    ctx: _Ctx, kind: SessionKind, duration_us: int, approval_id: str | None = None
) -> StageAuthorization:
    auth = StageAuthorization(
        authorization_id=ctx.new_id("au"),
        kind=kind,
        generation=len(ctx.p.authorizations) + 1,
        source_event_id=ctx.event.event_id,
        source_time_us=ctx.now,
        revision=ctx.p.revision,
        eligibility_epoch=ctx.p.eligibility_epoch,
        grant_duration_us=duration_us,
        approval_id=approval_id,
    )
    ctx.update(authorizations=(*ctx.p.authorizations, auth))
    return auth


def _cancel_pending(ctx: _Ctx) -> None:
    auth = ctx.p.authorization(ctx.p.pending_authorization_id)
    if auth is not None:
        ctx.put_authorization(replace(auth, cancelled=True))
    ctx.update(pending_authorization_id=None)


def _set_queue(ctx: _Ctx, entry: QueueEntry | None, parcel_id: str) -> None:
    rest = tuple(q for q in ctx.admission.queue if q.parcel_id != parcel_id)
    ctx.admission = replace(ctx.admission, queue=rest if entry is None else (*rest, entry))


def _cancel_queue(ctx: _Ctx) -> None:
    entry = ctx.admission.queue_entry(ctx.p.parcel_id)
    if entry is not None and entry.status == QueueStatus.QUEUED:
        _set_queue(ctx, replace(entry, status=QueueStatus.CANCELLED), ctx.p.parcel_id)


def _release_build(ctx: _Ctx, *, keep_pr: bool) -> None:
    """End a build episode: release the building slot (and an unbound PR reservation)."""
    pid = ctx.p.parcel_id

    def ends(r: Reservation) -> bool:
        return (
            r.parcel_id == pid
            and r.live
            and (
                r.kind == ReservationKind.BUILDING
                or (r.kind == ReservationKind.OPEN_PR and r.pr_number is None and not keep_pr)
            )
        )

    ctx.admission = replace(
        ctx.admission,
        reservations=tuple(
            replace(r, live=False) if ends(r) else r for r in ctx.admission.reservations
        ),
    )
    entry = ctx.admission.queue_entry(pid)
    if entry is not None and (
        entry.status in (QueueStatus.RESERVED, QueueStatus.HELD)
        or (entry.status == QueueStatus.QUEUED and entry.resume)
    ):
        _set_queue(ctx, replace(entry, status=QueueStatus.RELEASED, resume=False), pid)
    if ctx.p.slot_parked or ctx.p.held_wakes:
        ctx.update(slot_parked=False, held_wakes=())


def _release_pr(ctx: _Ctx, pr_number: int | None) -> None:
    pid = ctx.p.parcel_id
    ctx.admission = replace(
        ctx.admission,
        reservations=tuple(
            replace(r, live=False)
            if r.parcel_id == pid
            and r.kind == ReservationKind.OPEN_PR
            and r.live
            and (r.pr_number is None or r.pr_number == pr_number)
            else r
            for r in ctx.admission.reservations
        ),
    )


def _begin_drain(
    ctx: _Ctx,
    s: StageSession,
    *,
    fences: frozenset[FenceKind] = frozenset(),
    interrupt: bool = True,
) -> StageSession:
    """Fence (optionally) and drain a session's whole tree. Never clears a fence."""
    new_fences = s.fences | fences
    if s.lifecycle == Lifecycle.RETIRED:
        return ctx.put_session(replace(s, fences=new_fences))
    if s.lifecycle == Lifecycle.FENCED:
        # FENCED is terminal for draining: a closed tree is never re-opened (not by new
        # fences, external activity or a successor's wake-up), so it never re-requires a
        # building slot. Observed external activity only extends quiescence waiting.
        s = ctx.put_session(replace(s, fences=new_fences))
        if new_fences & _HARD_FENCES:
            _end_build_episode(ctx, s)
        if s.external_active and s.root_id is not None:
            if new_fences:
                ctx.emit(EffectKind.INTERRUPT_TREE, session=s, args={"root_id": s.root_id})
            ctx.emit(EffectKind.SCAN_TREE, session=s, args={"root_id": s.root_id})
        return s
    target = Lifecycle.FENCED if new_fences else Lifecycle.RETIRED
    if s.lifecycle == Lifecycle.DRAINING and s.drain_target == Lifecycle.FENCED:
        target = Lifecycle.FENCED
    lifecycle = s.lifecycle
    if lifecycle not in (Lifecycle.UNKNOWN, Lifecycle.BLOCKED):
        lifecycle = Lifecycle.DRAINING
    s = ctx.put_session(
        replace(
            s,
            fences=new_fences,
            lifecycle=lifecycle,
            drain_target=target,
            quiescent=False,
            wait_reason=None,
            drain_started_us=s.drain_started_us or ctx.now,
        )
    )
    if new_fences:
        ctx.emit(EffectKind.DISABLE_ISSUANCE, session=s)
    if s.root_id is not None:
        if interrupt:
            ctx.emit(EffectKind.INTERRUPT_TREE, session=s, args={"root_id": s.root_id})
        ctx.emit(EffectKind.SCAN_TREE, session=s, args={"root_id": s.root_id})
    return s


_HARD_FENCES = frozenset({FenceKind.SAFETY, FenceKind.STOPPED, FenceKind.REVOKED})


def _terminally_fenced(ctx: _Ctx, lifecycle: Lifecycle, fences: frozenset[FenceKind]) -> bool:
    """A hard-fenced, stopped run of a completed parcel can never run again.

    Closing an issue makes its fresh read ineligible, which fences the live run (safety)
    before completion drains it, so the drain ends FENCED, not RETIRED. Only a checkpoint
    fence alone can resume a FENCED run, and a completed parcel restarts only through a
    fresh owner control (a new run): the run is closed like a retired one, so nothing
    keeps watching its tree.
    """
    return lifecycle == Lifecycle.FENCED and bool(fences & _HARD_FENCES) and _completed(ctx)


def _end_build_episode(ctx: _Ctx, s: StageSession) -> None:
    """A settled build that can never resume releases its admission slot."""
    if s.kind == SessionKind.BUILD and not s.restart_pending:
        _release_build(ctx, keep_pr=False)


def _finish_drain(ctx: _Ctx, s: StageSession) -> None:
    target = s.drain_target or (Lifecycle.FENCED if s.fences else Lifecycle.RETIRED)
    s = ctx.put_session(
        replace(
            s,
            lifecycle=target,
            drain_target=None,
            quiescent=True,
            execution_closed=s.execution_closed
            or target == Lifecycle.RETIRED
            or _terminally_fenced(ctx, target, s.fences),
            external_active=False,
            drain_started_us=0,
        )
    )
    if not any(x.lifecycle == Lifecycle.BLOCKED for x in ctx.p.sessions):
        ctx.unhold(Hold.STOP_UNVERIFIED)
    if _continue_pending(ctx, s):
        # A /continue accepted mid-drain: its PolicyReady may already have come and gone.
        # Re-read (never re-write) the policy set; PoliciesVerified then resumes the run.
        _emit_verify(ctx, s, reconcile=False)
    if target == Lifecycle.RETIRED or s.fences & _HARD_FENCES:
        _end_build_episode(ctx, s)
        _orphan_open_decisions(ctx, s.session_id)
    if s.restart_pending:
        _replace_after_crash(ctx, s)


def _orphan_open_decisions(ctx: _Ctx, session_id: str) -> None:
    """A finished (or hard-fenced) session can no longer receive answers: close its prompts."""
    for d in ctx.p.decisions:
        if d.session_id == session_id and d.status == DecisionStatus.OPEN:
            ctx.put_decision(replace(d, status=DecisionStatus.ORPHANED))


def _next_policy_generation(p: Parcel) -> int:
    """Cost-policy generations are unique per issue session, which runs share."""
    return max((s.grant.policy_generation for s in p.sessions), default=0) + 1


def _create_session(
    ctx: _Ctx,
    auth: StageAuthorization,
    *,
    attempt: int = 1,
    restart_count: int = 0,
    correction_count: int = 0,
    grant: Grant | None = None,
    fresh_root: bool = False,
) -> StageSession | None:
    """Start a stage run: in the parcel's live issue session, else in a new one.

    Callers have established Q (every earlier run settled), so the reused root is idle and
    no earlier run's authority is live. Its credential is still switched off explicitly
    before this run's preparation can enable its own.
    """
    nonce = ctx.nonce()
    if nonce is None:
        return None
    sid = ctx.new_id("ss")
    grant = grant or Grant(
        grant_id=ctx.new_id("gr"),
        source_event_id=auth.source_event_id,
        duration_us=auth.grant_duration_us,
        policy_generation=_next_policy_generation(ctx.p),
    )
    issue = ctx.p.issue_session
    reuse = issue if not fresh_root and issue is not None and issue.reusable else None
    s = StageSession(
        session_id=sid,
        kind=auth.kind,
        attempt=attempt,
        authorization_id=auth.authorization_id,
        nonce=nonce,
        revision=ctx.p.revision,
        policy_ready=False,  # until the prepared policy set is verified after its barrier
        lifecycle=Lifecycle.PREPARING if reuse is not None else Lifecycle.INTENT,
        grant=grant,
        restart_count=restart_count,
        correction_count=correction_count,
        root_id=reuse.root_id if reuse is not None else None,
    )
    ctx.update(sessions=(*ctx.p.sessions, s), current_session_id=sid)
    if reuse is not None:
        for old in ctx.p.sessions:
            if old.session_id != sid and old.issuance_enabled:
                ctx.emit(EffectKind.DISABLE_ISSUANCE, session=old)
        ctx.emit(
            EffectKind.PREPARE_SESSION,
            session=s,
            args={"root_id": reuse.root_id, "profile": profile_for(s.kind).value, "reuse": True},
        )
        return s
    _emit_create(ctx, s)
    return s


def _emit_create(ctx: _Ctx, s: StageSession) -> None:
    auth = ctx.p.authorization(s.authorization_id)
    ctx.emit(
        EffectKind.CREATE_SESSION,
        session=s,
        args={
            "nonce": s.nonce,
            "stage": s.kind.value,
            "attempt": s.attempt,
            "authorization_id": s.authorization_id,
            "revision": auth.revision if auth is not None else ctx.p.revision,
        },
        dedupe=f"create:{s.session_id}",
    )


def _retire_issue_session(ctx: _Ctx, root_id: str | None, reason: str) -> None:
    """The issue session behind ``root_id`` is dead or unusable: the next run replaces it."""
    issue = ctx.p.issue_session
    if issue is not None and issue.root_id == root_id and issue.reusable:
        ctx.update(
            issue_session=replace(issue, status=IssueSessionStatus.DEAD, reason=reason[:200])
        )


def _retire_settled_current(ctx: _Ctx) -> None:
    cur = ctx.p.current_session
    if cur is not None and settled(cur) and cur.lifecycle != Lifecycle.RETIRED:
        cur = ctx.put_session(replace(cur, lifecycle=Lifecycle.RETIRED, execution_closed=True))
        _end_build_episode(ctx, cur)


def _void_approval(ctx: _Ctx, reason: str) -> bool:
    """Invalidate the current approval; revoke its build; cancel its queue entry.

    A queued daemon board write predating the revocation is dropped; the caller may
    queue its own consequent move afterwards.
    """
    a = ctx.p.current_approval
    if a is None:
        return False
    _drop_queued_move(ctx)
    if a.valid:
        ctx.put_approval(replace(a, invalidated_reason=reason, invalidated_at_us=ctx.now))
    ctx.update(current_approval_id=None)
    for auth in ctx.p.authorizations:
        if auth.approval_id == a.approval_id and not auth.cancelled:
            ctx.put_authorization(replace(auth, cancelled=True))
    _cancel_queue(ctx)
    cur = ctx.p.current_session
    if cur is not None and cur.kind == SessionKind.BUILD and cur.lifecycle != Lifecycle.RETIRED:
        _begin_drain(ctx, cur, fences=frozenset({FenceKind.REVOKED}))
    return True


def _drop_queued_move(ctx: _Ctx) -> None:
    """Cancel a coalesced, not-yet-issued daemon board write (never the in-flight one).

    The desired column falls back to the in-flight write's target, where the card will be
    once that write's outcome arrives.
    """
    if ctx.p.queued_move is None:
        return
    inflight = ctx.p.pending_moves[0].to_stage if ctx.p.pending_moves else ctx.p.stage
    ctx.update(queued_move=None, stage=inflight)


def _safety(ctx: _Ctx, reason: str, *, fence: FenceKind = FenceKind.SAFETY) -> None:
    """New negative fact: barrier, cancel queued authority, fence and drain the tree."""
    ctx.update(
        barrier_time_us=max(ctx.p.barrier_time_us, ctx.now),
        eligibility_epoch=ctx.p.eligibility_epoch + 1,
    )
    _drop_queued_move(ctx)  # a queued daemon write must not outlive the barrier
    _cancel_pending(ctx)
    _cancel_queue(ctx)
    for s in ctx.p.sessions:
        if s.lifecycle != Lifecycle.RETIRED and (
            s.session_id == ctx.p.current_session_id or not settled(s)
        ):
            _begin_drain(ctx, s, fences=frozenset({fence}))
    ctx.hold(Hold.STOPPED if fence == FenceKind.STOPPED else Hold.SAFETY)
    ctx.unhold(Hold.AWAITING_OWNER)
    _ = reason


def _apply_evidence(ctx: _Ctx) -> None:
    snap = ctx.event.evidence
    if snap is None or ctx.maybe_parcel is None:
        return
    was_eligible = ctx.p.eligible
    was_in_project = ctx.p.in_project
    ctx.update(eligible=snap.eligible, in_project=snap.in_project)
    if snap.links is not None and snap.links != ctx.p.links:
        ctx.update(links=snap.links)
    _sync_session_title(ctx, snap)
    _observe_auto_build(ctx, snap)
    _observe_autopilot(ctx, snap)
    if snap.in_project:
        ctx.unhold(Hold.NO_PROJECT_ITEM)
    if was_eligible and not snap.eligible:
        _safety(ctx, "ineligible-snapshot")
    elif was_in_project and not snap.in_project:
        _safety(ctx, "project-item-missing")
    if not snap.open and Hold.COMPLETED not in ctx.p.holds:
        _complete(ctx)  # a fresh read of a closed issue is terminal even if the webhook was lost
    # Safety before control: the column in any fresh read (control envelopes included)
    # is reconciled first; a leftward observation advances the barrier, so the control
    # that carried it is then rejected as not fresh. Explicit move events carry their
    # own stage row. This runs before any evidence-derived action that can issue a
    # daemon board write (the waiver-text check below moves the card to Scoped), so
    # the observation is compared with the persisted column and can never be mistaken
    # for a pending write issued within this same event.
    # A read taken before the daemon's last landed board write shows the old column: it
    # is stale, never a move (a late pre-write read once looked like a leftward drag).
    if (
        snap.stage is not None
        and snap.read_at_us >= ctx.p.board_written_at_us
        and ctx.event.kind
        not in (
            EventKind.LEFTWARD_MOVE,
            EventKind.COLUMN_OBSERVED,
        )
    ):
        _observe_stage(ctx, snap.stage)
    # Issue text differing from a live waiver is an edit even if the webhook was lost.
    a = ctx.p.current_approval
    if (
        a is not None
        and a.kind == ApprovalKind.SKIP
        and a.valid
        and issue_snapshot_digest(snap.title, snap.body) != a.full_hash
    ):
        _waiver_edited(ctx)


def _observe_stage(ctx: _Ctx, observed: Stage) -> None:
    """Compare a trusted column observation with the persisted stage (§2.4, §3.3).

    While a daemon board write is pending, an observation of its target is consistent
    with it and one of its source may either predate it or be a human drag back; neither
    retires it. Work stays gated (``board_pending``) until the trusted executor
    acknowledges the exact target, or reports absence/cancellation, which reconciles the
    source column as a fresh observation (see ``_retire_move``). Any other
    leftward/Inbox observation is a safety fact from an unknown actor (negative half).
    """
    for m in ctx.p.pending_moves:
        if observed in (m.to_stage, m.from_stage):
            return
    if ctx.p.queued_move is not None and observed == ctx.p.queued_move:
        return
    current = ctx.p.stage
    if current is None or observed == current:
        ctx.update(stage=observed)
        return
    if is_leftward(current, observed):
        prior = ctx.p.barrier_time_us
        _leftward_negative(ctx, current, observed)
        _record_observed_move(ctx, current, observed, prior)
        return
    if board_pending(ctx.p):
        # A daemon write is in flight/queued: its target is the single desired column
        # and will be (re)asserted on the board; a rightward/unknown observation is not
        # authority and must not split the desired column from the queued target.
        return
    ctx.update(stage=observed)  # rightward/unknown observation: never authority
    queued = _queued(ctx)
    if not ready_bot_ok(ctx.p, project_bot(ctx.p, queued=queued)) or (
        observed == Stage.READY and work_live(ctx.p, queued=queued)
    ):
        # Ready never shows the bot working: work still running (or queued) puts the
        # card back where it was, with the reason.
        _emit_move(ctx, current, source=observed)
        ctx.note(f"Kept in {current.value}: the bot is still working")
        return
    if observed in _DRAG_TARGETS.values():
        _record_observed_move(ctx, current, observed, ctx.p.barrier_time_us)
    r = ctx.p.readiness
    if (
        current == Stage.BUILDING
        and observed == Stage.READY
        and r is not None
        and not r.ready
        and not _completed(ctx)
    ):
        # E.g. the owner drags a finished build to Ready: a fresh read accepts it (Bot
        # by the checks) or moves it back with the reason (see ``_h_readiness``).
        _fetch_evidence(ctx, r)


#: Owner drag controls and the column each one moves the card to.
_DRAG_TARGETS: dict[EventKind, Stage] = {
    EventKind.REQUEST_TRIAGE: Stage.TRIAGED,
    EventKind.REQUEST_PLAN: Stage.SCOPED,
    EventKind.REQUEST_REPLAN: Stage.SCOPED,
    EventKind.APPROVE_PLAN: Stage.BUILDING,
    EventKind.WAIVE_PLAN: Stage.BUILDING,
}


def _record_observed_move(
    ctx: _Ctx, from_stage: Stage, to_stage: Stage, prior_barrier_us: int
) -> None:
    """Remember a column change seen without an owner control (never authority)."""
    ctx.update(
        observed_move=ObservedMove(from_stage, to_stage, ctx.p.barrier_time_us, prior_barrier_us)
    )


def _owner_control(ctx: _Ctx) -> bool:
    e = ctx.event
    return (
        e.provenance in ev.CONTROL_PROVENANCES
        and e.actor_id is not None
        and e.actor_id in ctx.config.owners
    )


def _drag_origin(ctx: _Ctx) -> None:
    """Judge an owner drag by its own columns, not by a read that got there first.

    A reconcile read can land between the owner's drag and its webhook: it records the
    new column as a plain observation, and the drag would then look like a move from
    the column it went to (#729). When the parcel's last observed move is exactly the
    drag's from -> to and the card is still there, the drag's own source column is the
    origin its stage rules see. Authority still comes only from the drag (``_control``).
    """
    p = ctx.maybe_parcel
    body = ctx.event.body
    target = _DRAG_TARGETS.get(ctx.event.kind)
    if p is None or target is None or not _owner_control(ctx):
        return
    assert isinstance(  # noqa: S101 - _DRAG_TARGETS lists only these kinds
        body, ev.RequestTriage | ev.RequestPlan | ev.RequestReplan | ev.ApprovePlan | ev.WaivePlan
    )
    move = p.observed_move
    if (
        body.via == Via.DRAG
        and body.board_from is not None
        and move is not None
        and (move.from_stage, move.to_stage) == (body.board_from, target)
        and p.stage == target
    ):
        ctx.origin_stage = body.board_from


def _leftward_negative(
    ctx: _Ctx, from_stage: Stage | None, to_stage: Stage | None, *, owner_target: bool = False
) -> None:
    """Safety half of a leftward move; applies whoever (or whatever) caused it.

    A queued daemon board write predates the safety fact and is dropped (fail-closed).
    """
    _safety(ctx, "leftward-move")
    ctx.update(stage=to_stage)
    if from_stage == Stage.BUILDING and to_stage == Stage.SCOPED:
        cur = ctx.p.current_session
        if cur is not None and cur.kind == SessionKind.BUILD:
            _begin_drain(ctx, cur, fences=frozenset({FenceKind.REVOKED}))
        _void_approval(ctx, "building-to-scoped")
        ctx.update(revision_pending=True)
    if owner_target and to_stage is not None and ctx.p.pending_moves:
        # A fresh owner move is the newest desired column: queue it behind the older
        # in-flight daemon write (after the safety/revocation drops above) so that write
        # landing cannot leave the card elsewhere.
        _emit_move(ctx, to_stage)


def _stage_controllable(p: Parcel) -> bool:
    """No live admitted build: the parcel is idle, stopped or between stages."""
    cur = p.current_session
    entry_live = False
    if cur is not None and cur.kind == SessionKind.BUILD:
        entry_live = not (cur.fences or cur.lifecycle == Lifecycle.RETIRED)
    return not entry_live


def _start_stage(ctx: _Ctx, kind: SessionKind, duration_us: int) -> None:
    """Record owner stage authority as the pending action; drain the old tree first."""
    _cancel_pending(ctx)
    auth = _new_authorization(ctx, kind, duration_us)
    ctx.update(pending_authorization_id=auth.authorization_id)
    cur = ctx.p.current_session
    if cur is not None and not settled(cur) and cur.lifecycle != Lifecycle.DRAINING:
        _begin_drain(ctx, cur)
    if not ctx.p.in_project:
        ctx.hold(Hold.NO_PROJECT_ITEM)
        ctx.emit(EffectKind.ENSURE_PROJECT_ITEM, dedupe=f"ensure-item:{auth.authorization_id}")


def _start_plan(ctx: _Ctx, via: Via | None) -> None:
    ctx.unhold(*CONTROL_CLEARED_HOLDS)
    ctx.update(
        revision=ctx.p.revision + 1,
        revision_pending=True,
        revision_feedback=(ctx.event.event_id,),
    )
    _void_approval(ctx, "replan")
    _cancel_queue(ctx)
    _move(ctx, Stage.SCOPED, via)
    duration = ctx.config.block_us(ctx.p.size or Size.M)
    _start_stage(ctx, SessionKind.PLAN, duration)


def _try_activate_pending(ctx: _Ctx) -> None:
    """Wake a recorded triage/plan authority once E and Q hold (guarded row)."""
    auth = ctx.p.authorization(ctx.p.pending_authorization_id)
    if auth is None or auth.cancelled:
        return
    if auth.eligibility_epoch != ctx.p.eligibility_epoch or not dispatchable(ctx.p):
        return
    if not all_settled(ctx.p):
        cur = ctx.p.current_session
        if (
            cur is not None
            and not settled(cur)
            and cur.lifecycle
            not in (
                Lifecycle.DRAINING,
                Lifecycle.UNKNOWN,
                Lifecycle.BLOCKED,
                Lifecycle.FENCED,  # closed; waiting only on fresh quiescence evidence
            )
        ):
            _begin_drain(ctx, cur)
        return
    if auth.kind == SessionKind.TRIAGE and not _triage_slot_free(ctx):
        # Every triage slot is taken (owner and auto-triage runs alike): the request
        # waits, recorded, and starts on a later event once a slot is free.
        ctx.note(triage_queued_note())
        return
    if not ctx.event.entropy:
        return
    _retire_settled_current(ctx)
    if _create_session(ctx, auth) is not None:
        ctx.update(pending_authorization_id=None)


def _triage_slot_free(ctx: _Ctx) -> bool:
    """A repository triage slot is free for this parcel (``triage_concurrency``)."""
    others = ctx.admission.triage_runs - {ctx.p.parcel_id}
    return len(others) < ctx.config.triage_concurrency


def _maybe_start_prepared(ctx: _Ctx) -> None:
    s = ctx.p.current_session
    if s is None or s.lifecycle != Lifecycle.PREPARING or not s.prepared:
        return
    if not all_settled(ctx.p, except_id=s.session_id):
        return
    if not work_allowed(ctx.p, s):
        return
    if s.kind == SessionKind.BUILD and not approval_ok(ctx.p):
        return
    s = ctx.put_session(replace(s, lifecycle=Lifecycle.ACTIVE))
    ctx.emit(EffectKind.ENABLE_ISSUANCE, session=s, args={"profile": profile_for(s.kind).value})
    ctx.emit(
        EffectKind.SEND_MESSAGE,
        session=s,
        args={"purpose": MessagePurpose.FIRST.value, "revision": ctx.p.revision},
    )


def _maybe_ready(ctx: _Ctx) -> None:
    r = ctx.p.readiness
    if r is None or r.ready or not r.verified or ctx.p.stage != Stage.BUILDING:
        return
    if _completed(ctx):
        return
    if _in_review_grace(r):
        return  # the review bot still has time to comment on this head
    s = ctx.p.session(r.session_id)
    if s is None or s.session_id != ctx.p.current_session_id:
        return
    waiting = (
        s.lifecycle == Lifecycle.WAITING and s.wait_reason == WaitReason.CHECKS and s.quiescent
    )
    if (
        not (waiting or _wake_answered(ctx, s))
        or s.fences
        or ctx.p.open_decisions
        or Hold.REMEDIATION_EXHAUSTED in ctx.p.holds
        or uncertain(ctx.p)
        or message_uncertain(ctx.p, s)
        or board_pending(ctx.p)
    ):
        return
    ctx.update(readiness=replace(r, ready=True), pr_number=r.pr_number)
    s = ctx.put_session(replace(s, lifecycle=Lifecycle.RETIRED, execution_closed=True))
    ctx.emit(EffectKind.DISABLE_ISSUANCE, session=s)
    _release_build(ctx, keep_pr=True)
    _move(ctx, Stage.READY)
    _publish_ready_report(ctx, r.checks_summary)


def _replace_after_crash(ctx: _Ctx, old: StageSession) -> None:
    old = ctx.put_session(replace(old, restart_pending=False))
    auth = ctx.p.authorization(old.authorization_id)
    if (
        auth is None
        or not authority_ok(ctx.p, old)
        or old.grant.remaining_us <= 0
        or not all_settled(ctx.p)
    ):
        ctx.hold(Hold.RESTART_EXHAUSTED)
        if old.kind == SessionKind.BUILD:
            _release_build(ctx, keep_pr=False)
        return
    grant = Grant(
        grant_id=ctx.new_id("gr"),
        source_event_id=old.grant.source_event_id,
        duration_us=old.grant.remaining_us,
        policy_generation=old.grant.policy_generation,
    )
    created = _create_session(
        ctx,
        auth,
        attempt=old.attempt + 1,
        restart_count=old.restart_count + 1,
        correction_count=old.correction_count,
        grant=grant,
        fresh_root=True,
    )
    if created is None:
        ctx.hold(Hold.RESTART_EXHAUSTED)


def _session(ctx: _Ctx, session_id: str | None) -> StageSession:
    s = ctx.p.session(session_id)
    if s is None:
        raise Rejected("unknown-session")
    return s


def _current(ctx: _Ctx, session_id: str) -> StageSession:
    s = _session(ctx, session_id)
    if s.session_id != ctx.p.current_session_id:
        raise Rejected("stale-session")
    return s


def _enter_checkpoint(ctx: _Ctx, s: StageSession) -> None:
    was_executable = s.lifecycle == Lifecycle.ACTIVE
    deadline = s.grant.grace_deadline_us or (ctx.now + ctx.config.grace_us)
    s = ctx.put_session(
        replace(
            s,
            lifecycle=Lifecycle.CHECKPOINT_GRACE,
            wait_reason=None,
            grant=replace(s.grant, grace_deadline_us=deadline),
        )
    )
    if was_executable:
        ctx.emit(
            EffectKind.SEND_MESSAGE,
            session=s,
            args={"purpose": MessagePurpose.CHECKPOINT_CLEANUP.value},
        )
    ctx.emit(
        EffectKind.ARM_TIMER,
        session=s,
        args={"timer": "grace", "deadline_us": deadline, "grant_id": s.grant.grant_id},
        dedupe=f"grace:{s.grant.grant_id}",
    )
    ctx.comment("checkpoint", grant_id=s.grant.grant_id, deadline_us=deadline)


def _relay_answers(ctx: _Ctx, s: StageSession) -> None:
    """Relay recorded, still-unrelayed answers for ``s`` when a relay is permitted."""
    if not work_allowed(ctx.p, s) or s.lifecycle not in (Lifecycle.ACTIVE, Lifecycle.WAITING):
        return
    if s.kind == SessionKind.BUILD and not approval_ok(ctx.p):
        return
    for d in ctx.p.decisions:
        if d.session_id != s.session_id or d.status != DecisionStatus.ANSWERED:
            continue
        if d.prompt_lost or d.source == DecisionSource.MCP:
            ctx.emit(
                EffectKind.SEND_MESSAGE,
                session=s,
                args={"purpose": MessagePurpose.ANSWER_RELAY.value, "decision_id": d.decision_id},
            )
        else:
            ctx.emit(
                EffectKind.RESOLVE_ELICITATION,
                session=s,
                args={"elicitation_id": d.elicitation_id, "decision_id": d.decision_id},
                target=s.root_id or s.session_id,
            )
        ctx.put_decision(replace(d, status=DecisionStatus.RELAYED))
    s = ctx.p.session(s.session_id) or s
    if (
        s.lifecycle == Lifecycle.WAITING
        and s.wait_reason == WaitReason.DECISION
        and not any(d.session_id == s.session_id for d in ctx.p.open_decisions)
    ):
        ctx.put_session(replace(s, lifecycle=Lifecycle.ACTIVE, wait_reason=None))


def _enqueue_build(
    ctx: _Ctx,
    approval: Approval,
    duration_us: int,
    *,
    rework: bool = False,
    conflict: bool = False,
) -> None:
    auth = _new_authorization(ctx, SessionKind.BUILD, duration_us, approval.approval_id)
    if rework:
        ctx.put_authorization(replace(auth, rework=True, conflict=conflict))
    seq = ctx.admission.next_sequence
    ctx.update(slot_parked=False, held_wakes=())  # a new episode: any parked one is over
    _set_queue(
        ctx,
        QueueEntry(
            ctx.p.parcel_id,
            approval.approval_id,
            seq,
            QueueStatus.QUEUED,
            issue_number=ctx.p.issue_number,
        ),
        ctx.p.parcel_id,
    )
    ctx.admission = replace(ctx.admission, next_sequence=seq + 1)
    ctx.note(_queued_note(ctx))
    ctx.emit(EffectKind.WAKE_SCHEDULER, target=ctx.admission.repo_id)


def _new_approval(
    ctx: _Ctx,
    kind: ApprovalKind,
    full_hash: str,
    *,
    contract_id: str | None = None,
    snapshot: str | None = None,
    mark: AutoBuildMark | None = None,
) -> Approval:
    """A new current approval: the event's owner's, or (``mark``) the owner's auto-build
    mark the trusted clock starts (its owner, event and time are the approval's source)."""
    owner_id = ctx.event.actor_id if mark is None else mark.owner_id
    assert owner_id is not None  # noqa: S101 - guarded by _control or the mark
    a = Approval(
        approval_id=ctx.new_id("ap"),
        kind=kind,
        full_hash=full_hash,
        owner_id=owner_id,
        source_event_id=ctx.event.event_id if mark is None else mark.source_event_id,
        source_time_us=ctx.now if mark is None else mark.marked_at_us,
        sequence=ctx.admission.next_sequence,
        eligibility_epoch=ctx.p.eligibility_epoch,
        contract_id=contract_id,
        snapshot_canonical=snapshot,
        issue_edit_count=ctx.p.issue_edit_count,
    )
    ctx.update(approvals=(*ctx.p.approvals, a), current_approval_id=a.approval_id)
    return a


def _approval_active(ctx: _Ctx) -> bool:
    """A current approval already has a queued/admitted build episode."""
    a = ctx.p.current_approval
    if a is None or not a.valid:
        return False
    entry = ctx.admission.queue_entry(ctx.p.parcel_id)
    if (
        entry is not None
        and entry.approval_id == a.approval_id
        and entry.status
        in (
            QueueStatus.QUEUED,
            QueueStatus.RESERVED,
            QueueStatus.HELD,
        )
    ):
        return True
    cur = ctx.p.current_session
    return (
        cur is not None
        and cur.kind == SessionKind.BUILD
        and not cur.fences
        and cur.lifecycle != Lifecycle.RETIRED
    )


def _build_blocked_by_live(ctx: _Ctx) -> bool:
    cur = ctx.p.current_session
    return cur is not None and cur.kind == SessionKind.BUILD and gate_open(ctx.p, cur)


def _after_approval(ctx: _Ctx, approval: Approval, duration_us: int, via: Via | None) -> None:
    ctx.unhold(*CONTROL_CLEARED_HOLDS)
    ctx.update(readiness_wakes=0, findings_wakes=0)  # a new approved deliverable: own wakes
    _cancel_pending(ctx)
    _move(ctx, Stage.BUILDING, via)
    _enqueue_build(ctx, approval, duration_us)
    cur = ctx.p.current_session
    if cur is not None and not settled(cur) and cur.lifecycle != Lifecycle.DRAINING:
        _begin_drain(ctx, cur)


# ================================================================ control rows


_TRIAGE_STAGES = frozenset({None, Stage.INBOX})
_PLAN_STAGES = frozenset({None, Stage.INBOX, Stage.TRIAGED, Stage.SCOPED})
_NO_RECOVERY_STAGES = frozenset({Stage.READY, Stage.DONE})


def _stopped_recovery(ctx: _Ctx) -> bool:
    """§2.6B "any eligible stopped stage": a stop/safety barrier left no live work.

    Ready (and legacy Done) are excluded: owner feedback on a Ready card is rework.
    """
    p = ctx.p
    return (
        bool(p.holds & {Hold.STOPPED, Hold.SAFETY})
        and ctx.origin_stage not in _NO_RECOVERY_STAGES
        and _stage_controllable(p)
        and not _approval_active(ctx)
    )


def _equivalent_run_in_flight(ctx: _Ctx, kind: SessionKind) -> bool:
    """An owner request for ``kind`` would duplicate the run already pending or running.

    Equivalent: same stage, same eligibility epoch (no stop/safety barrier since), not
    cancelled, and not yet published (a plan whose contract the owner can see is
    finished work, so a later /plan is a deliberate new run). Card drag plus a /plan a
    minute later attach to the one run instead of interrupting and restarting it.
    """
    p = ctx.p
    if p.holds & CONTROL_CLEARED_HOLDS:
        return False  # the control has something to clear (stuck, stopped, invalid result)
    pending = p.authorization(p.pending_authorization_id)
    if pending is not None:
        return (
            pending.kind == kind
            and not pending.cancelled
            and pending.eligibility_epoch == p.eligibility_epoch
        )
    cur = p.current_session
    if cur is None or cur.kind != kind or cur.fences or cur.execution_closed:
        return False
    auth = p.authorization(cur.authorization_id)
    if auth is None or auth.cancelled or auth.eligibility_epoch != p.eligibility_epoch:
        return False
    if cur.lifecycle == Lifecycle.WAITING:
        if cur.wait_reason == WaitReason.PLAN_APPROVAL:
            # The plan is done but its contract is not yet visible to the owner: the
            # publication is still part of this run (also after a restart).
            return any(
                c.source_session_id == cur.session_id and not c.published and c.intact
                for c in p.contracts
            )
        return cur.wait_reason == WaitReason.DECISION
    return cur.lifecycle in (
        Lifecycle.INTENT,
        Lifecycle.CREATING,
        Lifecycle.PREPARING,
        Lifecycle.ACTIVE,
    )


def _h_request_triage(ctx: _Ctx, body: ev.RequestTriage) -> None:
    _control(ctx)
    _require_eligible(ctx)
    if ctx.origin_stage == Stage.TRIAGED and _equivalent_run_in_flight(ctx, SessionKind.TRIAGE):
        # Attach to the running triage: audited, no new authority, interrupt or restart.
        raise Rejected("equivalent-run-in-flight")
    in_list = ctx.origin_stage in _TRIAGE_STAGES and not _approval_active(ctx)
    if not (in_list or _stopped_recovery(ctx)):
        raise Rejected("triage-not-allowed-from-stage", explain=True)
    ctx.unhold(*CONTROL_CLEARED_HOLDS)
    _cancel_queue(ctx)
    _move(ctx, Stage.TRIAGED, body.via)
    _start_stage(ctx, SessionKind.TRIAGE, ctx.config.block_us(ctx.p.size or Size.S))


def _h_auto_triage(ctx: _Ctx, body: ev.AutoTriage) -> None:
    """Idle-time auto-triage: the operator's standing authorisation, from the clock.

    Starts triage exactly as an owner Inbox -> Triage drag does (the daemon moves the
    card), but only for an Inbox issue the factory never worked on: no authority, run,
    hold or pending write of any kind. The service picks the issue when the factory is
    idle; this re-checks what the parcel and admission can tell: not paused, no queued
    build that could start now (``startable_build``), a triage slot free. A build slot held
    with no run working does not count: whether a run works is the service's check. A
    barrier (stop, leftward move, safety fact) after the read refuses it like a stale owner
    control.
    """
    _ = body
    p = ctx.p
    if ctx.event.evidence is None:
        raise Rejected("auto-triage-without-fresh-read")
    if ctx.now <= p.barrier_time_us:
        raise Rejected("auto-triage-not-fresh-after-barrier")
    if ctx.admission.paused:
        raise Rejected("paused")
    if not dispatchable(p):
        raise Rejected("parcel-not-dispatchable")
    if ctx.origin_stage not in _TRIAGE_STAGES or p.stage != Stage.INBOX:
        raise Rejected("auto-triage-not-in-inbox")
    if (
        p.authorizations
        or p.sessions
        or p.holds
        or p.decisions
        or board_pending(p)
        or p.unknown_effects
        or p.pending_authorization_id is not None
    ):
        raise Rejected("auto-triage-issue-known")
    if startable_build(ctx.admission, ctx.config) is not None:
        raise Rejected("auto-triage-factory-busy")
    if not _triage_slot_free(ctx):
        raise Rejected("auto-triage-slot-busy")
    _move(ctx, Stage.TRIAGED)
    _start_stage(ctx, SessionKind.TRIAGE, ctx.config.block_us(p.size or Size.S))


#: Related marks kept per card (newest win); the note shows what fits on one line.
_MAX_RELATED_MARKS = 5


def _h_related_marked(ctx: _Ctx, body: ev.RelatedMarked) -> None:
    """Another issue's accepted triage named this one: remember it for the card's note."""
    if (
        body.relation not in RELATIONS
        or body.source_issue < 1
        or body.source_issue == ctx.p.issue_number
    ):
        raise Rejected("invalid-related-mark")
    mark = RelatedMark(body.source_issue, body.relation)
    if mark in ctx.p.related_marks:
        raise Rejected("already-marked")
    rest = tuple(m for m in ctx.p.related_marks if m.issue != body.source_issue)
    ctx.update(related_marks=(*rest, mark)[-_MAX_RELATED_MARKS:])


def _h_epic_progress(ctx: _Ctx, body: ev.EpicProgress) -> None:
    """The factory's board pass computed this epic's progress line (display only)."""
    text = " ".join(body.text.split())[:NOTE_MAX]
    if text == ctx.p.epic_note:
        raise Rejected("epic-progress-unchanged")
    ctx.update(epic_note=text)


EPIC_BUILD_NOTE = "Epic: build its sub-issues"


def _refuse_epic_build(ctx: _Ctx, rollback: Stage | None) -> None:
    """An epic (an issue with sub-issues) is never built: its sub-issues are."""
    links = ctx.p.links
    if links is not None and links.epic:
        raise Rejected("epic", explain=True, rollback_to=rollback, note=EPIC_BUILD_NOTE)


def _started_despite_blockers(ctx: _Ctx) -> None:
    """A manual build of an issue with open blockers proceeds; the note says so."""
    links = ctx.p.links
    blockers = links.open_blockers if links is not None else ()
    if blockers:
        ctx.note(f"Started despite open blocker {blockers_text(blockers)}")


# ------------------------------------------------------------------ auto-build


def _write_auto_build(ctx: _Ctx, value: str) -> None:
    """Write the board's "Auto-build" field (display; the owner's webhook is the control)."""
    if ctx.p.auto_build_field == value:
        return
    ctx.update(
        auto_build_field=value, auto_build_field_at_us=max(ctx.p.auto_build_field_at_us, ctx.now)
    )
    ctx.emit(EffectKind.SET_AUTO_BUILD, args={"value": value})


def _auto_build_live(p: Parcel) -> bool:
    """The build an auto-build mark started still runs under its approval."""
    mark = p.auto_build
    a = p.current_approval
    return (
        mark is not None
        and mark.status == AutoBuildStatus.STARTED
        and a is not None
        and a.valid
        and a.approval_id == mark.approval_id
    )


def _plan_revised(p: Parcel, mark: AutoBuildMark) -> bool:
    """The plan the mark approved is no longer the posted, current one.

    Any revision but the one an owner answer to the plan's question starts (owner
    feedback, ``/plan``, a replan) revises it. While that answer is folded into the
    plan, the mark waits; the re-posted plan must carry the same hash.
    """
    c = p.current_contract
    if p.revision != mark.revision or (c is not None and not c.intact):
        return True
    if p.revision_pending:
        return False  # an answer is being folded in: judged once the plan is posted
    return c is None or not c.published or c.superseded or c.full_hash != mark.full_hash


PLAN_REVISED_NOTE = "Plan revised: re-queue to approve"
AUTO_BUILD_UNCONFIRMED_NOTE = "Auto-build mark not confirmed: re-select it"


def _mark_refusal(ctx: _Ctx) -> str | None:
    """Why an owner's Queued cannot approve the plan posted now (None: it can)."""
    p = ctx.p
    if ctx.now <= p.barrier_time_us:
        return "Auto-build cleared: set before the latest stop; re-select Queued"
    if not eligible(p):
        return "Auto-build cleared: the issue is closed, assigned or unverified"
    if ctx.origin_stage != Stage.SCOPED or p.stage != Stage.SCOPED:
        return "Auto-build cleared: only a card in Planning with a posted plan can be queued"
    c = p.current_contract
    if c is None or not c.published or not c.intact or c.superseded:
        return "Auto-build cleared: there is no posted plan to approve"
    if p.revision_pending or c.posted_at_us is None or not c.posted_at_us < ctx.now:
        return PLAN_REVISED_NOTE
    if _build_blocked_by_live(ctx) or _approval_active(ctx):
        return "Auto-build cleared: a build is already approved"
    if p.links is not None and p.links.epic:
        return f"Auto-build cleared: {EPIC_BUILD_NOTE}"
    return None


def _h_auto_build_marked(ctx: _Ctx, body: ev.AutoBuildMarked) -> None:
    """The owner changed the card's "Auto-build" field (their own webhook; the factory's
    writes never arrive here).

    Queued approves the plan posted now, like ``/approve <hash>``: it is recorded as a
    mark bound to that plan's hash and started later by the trusted clock (``AutoBuild``)
    when a build slot is free. Anywhere a plan cannot be approved the factory clears the
    field and says why. An open owner question does not refuse it: the build waits for
    the answer. Clearing the field before the start drops the mark; after the start it
    changes nothing (``/stop`` or a leftward drag stops a build). "Started" is the
    factory's own option: the owner selecting it approves nothing.
    """
    option = body.option
    mark = ctx.p.auto_build
    ctx.update(
        auto_build_field=option,
        auto_build_field_at_us=max(ctx.p.auto_build_field_at_us, ctx.now),
    )
    if option == AUTO_BUILD_QUEUED:
        if _auto_build_live(ctx.p):
            _write_auto_build(ctx, AUTO_BUILD_STARTED)  # already started: nothing new
            return
        refusal = _mark_refusal(ctx)
        if refusal is not None:
            ctx.update(auto_build=None)
            _write_auto_build(ctx, "")
            ctx.note(refusal)
            return
        c = ctx.p.current_contract
        assert c is not None and ctx.event.actor_id is not None  # noqa: S101 - checked above
        ctx.update(
            auto_build=AutoBuildMark(
                status=AutoBuildStatus.QUEUED,
                full_hash=c.full_hash,
                contract_id=c.contract_id,
                owner_id=ctx.event.actor_id,
                source_event_id=ctx.event.event_id,
                marked_at_us=ctx.now,
                revision=ctx.p.revision,
            )
        )
        ctx.note(
            "Auto-build queued: starts once your open question is answered"
            if ctx.p.open_decisions
            else "Auto-build queued: starts when a build slot is free"
        )
        return
    queued = mark is not None and mark.status == AutoBuildStatus.QUEUED
    if option == "":
        if queued:
            ctx.update(auto_build=None)
            ctx.note("Auto-build mark cleared")
        return  # after the start: nothing (stop a build with /stop or a leftward drag)
    if option == AUTO_BUILD_STARTED and not _auto_build_live(ctx.p):
        # The factory's own option: never an approval.
        ctx.update(auto_build=None if queued else mark)
        _write_auto_build(ctx, "")
        ctx.note("Auto-build cleared: Started is set by the factory; select Queued to approve")


def _h_auto_build(ctx: _Ctx, body: ev.AutoBuild) -> None:
    """Start the build an owner's auto-build mark approved (the trusted clock).

    The mark is the owner's approval of exactly one posted plan; this re-checks
    everything that could have changed since: a fresh read, no barrier after the mark,
    the card still in Planning with that plan posted and approvable (no open question,
    no revision), no build live, and admission: not paused, no queued build waiting (a
    manual approval always goes first), a building slot, an auto-build slot and an
    open-PR slot free. Then it is an ordinary approval: the build is queued (as the queue's
    auto-build entry) and the card moves to Building; the field shows "Started".
    """
    _ = body
    p = ctx.p
    mark = p.auto_build
    if mark is None or mark.status != AutoBuildStatus.QUEUED:
        raise Rejected("no-auto-build-mark")
    if mark.not_before_us > ctx.now:
        raise Rejected("auto-build-not-due")
    if ctx.event.evidence is None:
        raise Rejected("auto-build-without-fresh-read")
    if ctx.now <= p.barrier_time_us or mark.marked_at_us <= p.barrier_time_us:
        raise Rejected("auto-build-not-fresh-after-barrier")
    if ctx.admission.paused:
        raise Rejected("paused")
    if not dispatchable(p):
        raise Rejected("parcel-not-dispatchable")
    if ctx.origin_stage != Stage.SCOPED or p.stage != Stage.SCOPED:
        raise Rejected("auto-build-not-in-planning")
    if p.open_decisions:
        raise Rejected("open-decisions")
    if not plan_ok(p) or _plan_revised(p, mark):
        raise Rejected("auto-build-plan-changed")
    links = ctx.event.evidence.links
    if links is None:
        # Fail closed for auto-build only: an unread link set could hide a blocker.
        raise Rejected("auto-build-links-unreadable")
    if links.epic:
        raise Rejected("auto-build-epic")
    if links.open_blockers:
        raise Rejected("auto-build-blocked")
    if _build_blocked_by_live(ctx) or _approval_active(ctx):
        raise Rejected("auto-build-build-live")
    if queue_head(ctx.admission) is not None:
        raise Rejected("auto-build-behind-queued-build")
    if not building_capacity_available(ctx.admission, ctx.config):
        raise Rejected("building-cap")
    if not auto_build_capacity_available(ctx.admission, ctx.config):
        raise Rejected("auto-build-cap")
    if not pr_capacity_available(ctx.admission, ctx.config):
        raise Rejected("open-pr-cap")
    latest = p.current_contract
    assert latest is not None  # noqa: S101 - plan_ok checks it
    approval = _new_approval(
        ctx, ApprovalKind.PLAN, latest.full_hash, contract_id=latest.contract_id, mark=mark
    )
    _after_approval(ctx, approval, ctx.config.block_us(latest.size), None)
    entry = ctx.admission.queue_entry(ctx.p.parcel_id)
    assert entry is not None  # noqa: S101 - _enqueue_build queued it
    _set_queue(ctx, replace(entry, auto=True), ctx.p.parcel_id)
    ctx.update(
        auto_build=replace(
            mark,
            status=AutoBuildStatus.STARTED,
            approval_id=approval.approval_id,
            sequence=entry.sequence,
        )
    )
    if not mark.autopilot_epic or ctx.p.auto_build_field:
        _write_auto_build(ctx, AUTO_BUILD_STARTED)  # an autopilot mark: only if shown


def _observe_auto_build(ctx: _Ctx, snap: IssueSnapshot) -> None:
    """A fresh read's "Auto-build" value (the webhook may have been lost).

    A read proves no actor: Queued seen without the owner's webhook is never acted on
    (the note asks the owner to select it again, once per value seen); an empty field
    while a mark waits is the owner clearing it (that only restricts: the mark drops).
    """
    value = snap.auto_build
    p = ctx.p
    if value == AUTO_BUILD_QUEUED and _auto_build_live(p):
        # Queued on a build its mark started: the read predates the factory's own
        # "Started" write (or that write was lost). Never a new mark: write Started again.
        ctx.update(auto_build_field=AUTO_BUILD_QUEUED)
        _write_auto_build(ctx, AUTO_BUILD_STARTED)
        return
    if (
        value is None
        or snap.read_at_us <= p.auto_build_field_at_us
        or value == p.auto_build_field
        or ctx.event.kind == EventKind.AUTO_BUILD_MARKED
    ):
        return
    ctx.update(auto_build_field=value, auto_build_field_at_us=snap.read_at_us)
    mark = p.auto_build
    if value == AUTO_BUILD_QUEUED:
        if mark is None or mark.status != AutoBuildStatus.QUEUED:
            ctx.note(AUTO_BUILD_UNCONFIRMED_NOTE)
    elif mark is not None and mark.status == AutoBuildStatus.QUEUED:
        ctx.update(auto_build=None)
        ctx.note("Auto-build mark cleared")


def _settle_auto_build(ctx: _Ctx) -> None:
    """After every event: a queued mark lapses as soon as it no longer approves the
    plan on the card (a revision, a barrier, the card leaving Planning); a finished
    parcel (Done, closed or merged) clears the field (a started mark stays as the record
    of its build)."""
    p = ctx.p
    mark = p.auto_build
    if Hold.COMPLETED in p.holds or p.stage == Stage.DONE:
        if mark is not None and mark.status == AutoBuildStatus.QUEUED:
            ctx.update(auto_build=None)
        if p.auto_build_field:
            _write_auto_build(ctx, "")
        return
    if mark is None or mark.status != AutoBuildStatus.QUEUED:
        return
    if p.stage != Stage.SCOPED:
        why = "Auto-build cleared: the card left Planning"
    elif not eligible(p):
        why = "Auto-build cleared: the issue is closed, assigned or unverified"
    elif mark.marked_at_us <= p.barrier_time_us:
        why = "Auto-build cleared: stopped after it was queued; re-select Queued"
    elif _plan_revised(p, mark):
        why = PLAN_REVISED_NOTE
    else:
        return
    ctx.update(auto_build=None)
    _write_auto_build(ctx, "")
    if not ctx.note_set:
        ctx.note(why)


# ------------------------------------------------------------------ epic autopilot


AUTOPILOT_UNCONFIRMED_NOTE = "Autopilot mark not confirmed: re-select it"
#: What each human step reads as on the epic's question comment template.
_AUTOPILOT_QUESTIONS = frozenset({"order"})


def _is_epic(p: Parcel) -> bool:
    return p.links is not None and p.links.epic


def _write_autopilot(ctx: _Ctx, value: str) -> None:
    """Write the epic's "Autopilot" field (the factory only clears it: display)."""
    if ctx.p.autopilot_field == value:
        return
    ctx.update(
        autopilot_field=value, autopilot_field_at_us=max(ctx.p.autopilot_field_at_us, ctx.now)
    )
    ctx.emit(EffectKind.SET_AUTOPILOT, args={"value": value})


def _autopilot_refusal(ctx: _Ctx) -> str | None:
    """Why an owner's Autopilot level cannot put this issue on autopilot (None: it can)."""
    p = ctx.p
    if not eligible(p):
        return "Autopilot cleared: the issue is closed, assigned or unverified"
    if p.links is None:
        return "Autopilot cleared: the issue's links could not be read; re-select it"
    if not p.links.epic:
        return "Autopilot cleared: only an epic (an issue with sub-issues) can run on autopilot"
    if ctx.origin_stage not in (None, Stage.INBOX, Stage.TRIAGED):
        return "Autopilot cleared: move the epic to Inbox or Triage first"
    return None


def _start_epic_plan(ctx: _Ctx) -> None:
    """The epic planning pass: the epic triage run, asked for the epic plan (the template
    follows ``Parcel.autopilot``). The card goes to Triage like an owner ``/triage``."""
    ctx.unhold(*CONTROL_CLEARED_HOLDS)
    _cancel_queue(ctx)
    _move(ctx, Stage.TRIAGED)
    _start_stage(ctx, SessionKind.TRIAGE, ctx.config.block_us(ctx.p.size or Size.S))


def _h_autopilot_marked(ctx: _Ctx, body: ev.AutopilotMarked) -> None:
    """The owner changed the epic's "Autopilot" field (their own webhook only).

    A level on an epic starts the epic planning pass; nothing advances until the owner
    approves the epic plan with ``/approve``. Changing the level keeps the plan and its
    approval. Clearing the field turns autopilot off: running builds continue, queued
    autopilot starts are withdrawn by the autopilot pass.
    """
    _control(ctx)
    option = body.option
    ctx.update(
        autopilot_field=option, autopilot_field_at_us=max(ctx.p.autopilot_field_at_us, ctx.now)
    )
    current = ctx.p.autopilot
    if option == "":
        if current is not None:
            ctx.update(autopilot=None)
            ctx.note("Autopilot off")
        return
    refusal = (
        "Autopilot cleared: unknown option; select Full, Delayed or Plan only"
        if option not in AUTOPILOT_LEVELS
        else _autopilot_refusal(ctx)
    )
    if refusal is not None:
        ctx.update(autopilot=None)
        _write_autopilot(ctx, "")
        ctx.note(refusal)
        return
    assert ctx.event.actor_id is not None  # noqa: S101 - _control
    if current is not None:
        ctx.update(autopilot=replace(current, level=option))
        ctx.note(f"Autopilot: {option}")
        return
    ctx.update(
        autopilot=EpicAutopilot(
            level=option,
            owner_id=ctx.event.actor_id,
            source_event_id=ctx.event.event_id,
            set_at_us=ctx.now,
        )
    )
    _start_epic_plan(ctx)
    ctx.note(f"Autopilot ({option}): writing the epic plan")


def _observe_autopilot(ctx: _Ctx, snap: IssueSnapshot) -> None:
    """A fresh read's "Autopilot" value (the webhook may have been lost).

    A read proves no actor: a level seen without the owner's webhook never enables or
    changes autopilot (the note asks the owner to select it again, once per value); an
    empty field while autopilot is on is the owner clearing it (that only restricts).
    """
    value = snap.autopilot
    p = ctx.p
    if (
        value is None
        or snap.read_at_us <= p.autopilot_field_at_us
        or value == p.autopilot_field
        or ctx.event.kind == EventKind.AUTOPILOT_MARKED
    ):
        return
    ctx.update(autopilot_field=value, autopilot_field_at_us=snap.read_at_us)
    if value == "":
        if p.autopilot is not None:
            ctx.update(autopilot=None)
            ctx.note("Autopilot off")
        return
    if p.autopilot is None or p.autopilot.level != value:
        ctx.note(AUTOPILOT_UNCONFIRMED_NOTE)


def _record_epic_plan(ctx: _Ctx, body: ev.ResultCandidate, s: StageSession, effect_id: str) -> None:
    """An epic triage with a plan on an autopilot epic: the epic plan to approve (posted
    once its comment is acknowledged)."""
    ap = ctx.p.autopilot
    if ap is None or not body.epic_plan_hash:
        return
    ctx.update(
        autopilot=replace(
            ap,
            plan=EpicPlan(body.epic_plan_hash, s.session_id, effect_id),
            revision_pending=False,
        )
    )


def _epic_plan_revision(ctx: _Ctx) -> None:
    """An owner comment on an autopilot epic with a plan: it revises the epic plan. The
    approval is void until the revised plan is approved; running builds continue."""
    ap = ctx.p.autopilot
    if ap is None or ap.plan is None:
        return
    ctx.update(
        autopilot=replace(
            ap,
            revision_pending=True,
            approved_hash="",
            approved_by=0,
            approval_event_id="",
            approved_at_us=0,
        )
    )


def _approve_epic_plan(ctx: _Ctx, body: ev.ApprovePlan) -> None:
    """``/approve [hash]`` on an autopilot epic approves its posted epic plan (hash-bound
    like a sub-issue plan). The approval event is the autopilot epoch."""
    ap = ctx.p.autopilot
    assert ap is not None  # noqa: S101 - checked by the caller
    plan = ap.plan
    if plan is None or plan.posted_at_us is None:
        raise Rejected("epic-plan-not-posted", explain=True)
    if ap.revision_pending:
        raise Rejected("epic-plan-revision-pending", explain=True)
    if ctx.p.open_decisions:
        raise Rejected("open-decisions", explain=True)
    if body.hash_text is not None:
        if resolve_hash(body.hash_text, [plan.full_hash]) != plan.full_hash:
            raise Rejected("hash-does-not-identify-latest", explain=True)
    elif not plan.posted_at_us < ctx.now:
        raise Rejected("epic-plan-not-posted-before-control", explain=True)
    if ap.approved:
        ctx.note("Epic plan already approved: autopilot is on")
        return
    assert ctx.event.actor_id is not None  # noqa: S101 - _control
    ctx.update(
        autopilot=replace(
            ap,
            approved_hash=plan.full_hash,
            approved_by=ctx.event.actor_id,
            approval_event_id=ctx.event.event_id,
            approved_at_us=ctx.now,
            paused="",
            asked=(),
        )
    )
    ctx.note(f"Epic plan approved: autopilot ({ap.level}) starts")


def _judge_epic_fit(ctx: _Ctx, body: ev.ResultCandidate, full_hash: str) -> None:
    """A plan of a sub-issue autopilot drives: keep the epic plan's word on its part."""
    claim = ctx.p.autopilot_claim
    if claim is None or not claim.active:
        return
    if body.epic_fit == "within":
        drift = ""
    elif body.epic_fit == "exceeds":
        drift = "its plan goes beyond its part of the epic plan"
    else:
        drift = "its plan does not say whether it stays within the epic plan"
    ctx.update(autopilot_claim=replace(claim, drift=drift, drift_hash=full_hash))


def _autopilot_fresh(ctx: _Ctx, what: str) -> None:
    """Shared checks of the trusted clock's autopilot steps."""
    p = ctx.p
    if ctx.event.evidence is None:
        raise Rejected(f"{what}-without-fresh-read")
    if ctx.now <= p.barrier_time_us:
        raise Rejected(f"{what}-not-fresh-after-barrier")
    if ctx.admission.paused:
        raise Rejected("paused")
    if not dispatchable(p):
        raise Rejected("parcel-not-dispatchable")


def _h_autopilot_plan(ctx: _Ctx, body: ev.AutopilotPlan) -> None:
    """Epic autopilot starts the plan of an unblocked sub-issue (the trusted clock).

    The service picked it (the epic's next sub-issue, its plan approved by the owner);
    this re-checks what the parcel and a fresh read can tell: a sub-issue of that epic,
    not an epic itself, every blocker closed, not claimed in this epoch (never twice), no
    run or build live. Unplanned issues go straight to planning (no triage); a plan the
    owner already has posted is taken as it is.
    """
    _autopilot_fresh(ctx, "autopilot")
    p = ctx.p
    if not body.epoch or body.epic <= 0:
        raise Rejected("autopilot-without-epoch")
    claim = p.autopilot_claim
    if claim is not None and claim.epoch == body.epoch:
        raise Rejected("autopilot-already-claimed")
    snap = ctx.event.evidence
    assert snap is not None  # noqa: S101 - _autopilot_fresh
    links = snap.links
    if links is None:
        raise Rejected("autopilot-links-unreadable")  # fail closed: a blocker may hide
    if links.parent is None or links.parent.repo or links.parent.number != body.epic:
        raise Rejected("autopilot-not-a-sub-issue")
    if links.epic:
        raise Rejected("autopilot-nested-epic")  # a blocker only, never recursed into
    if links.open_blockers:
        raise Rejected("autopilot-blocked")
    if _build_blocked_by_live(ctx) or _approval_active(ctx):
        raise Rejected("autopilot-build-live")
    if ctx.origin_stage not in (None, Stage.INBOX, Stage.TRIAGED, Stage.SCOPED):
        raise Rejected("autopilot-stage")
    cur = p.current_session
    if p.pending_authorization_id is not None or (cur is not None and not settled(cur)):
        raise Rejected("autopilot-run-live")
    if ctx.origin_stage == Stage.SCOPED and plan_ok(p):
        ctx.note("Autopilot: taking the posted plan")
    else:
        _start_plan(ctx, None)
        ctx.note(f"Autopilot: planning (epic #{body.epic})")
    ctx.update(
        autopilot_claim=AutopilotClaim(
            epic=body.epic, epoch=body.epoch, claimed_at_us=ctx.now, revision=ctx.p.revision
        )
    )


def _h_autopilot_queue(ctx: _Ctx, body: ev.AutopilotQueue) -> None:
    """Epic autopilot queues the build of a claimed sub-issue's posted plan.

    An auto-build mark bound to that plan's hash, carrying the owner's epic plan approval
    (``owner_id``, ``epoch``) and started by the auto-build queue (capacity, manual builds
    first) no earlier than ``delay_us`` after the plan was posted. Each plan is queued at
    most once; a plan that goes beyond its part of the epic is never queued.
    """
    _autopilot_fresh(ctx, "autopilot")
    p = ctx.p
    claim = p.autopilot_claim
    if claim is None or claim.epoch != body.epoch or claim.epic != body.epic:
        raise Rejected("autopilot-not-claimed")
    if not claim.active:
        raise Rejected("autopilot-released")
    if body.owner_id not in ctx.config.owners:
        raise Rejected("autopilot-owner-unknown")
    if ctx.origin_stage != Stage.SCOPED or p.stage != Stage.SCOPED:
        raise Rejected("autopilot-not-in-planning")
    if p.open_decisions:
        raise Rejected("open-decisions")
    if not plan_ok(p):
        raise Rejected("autopilot-no-posted-plan")
    c = p.current_contract
    assert c is not None  # noqa: S101 - plan_ok
    if c.posted_at_us is None or claim.marked_hash == c.full_hash:
        raise Rejected("autopilot-already-queued")
    if claim.drift and claim.drift_hash == c.full_hash:
        raise Rejected("autopilot-plan-drift")
    if claim.drift_hash != c.full_hash:
        raise Rejected("autopilot-plan-not-judged")
    if p.auto_build is not None or _build_blocked_by_live(ctx) or _approval_active(ctx):
        raise Rejected("autopilot-build-approved")
    if p.links is not None and (p.links.epic or p.links.open_blockers):
        raise Rejected("autopilot-blocked")
    due = max(ctx.now, c.posted_at_us + max(0, body.delay_us))
    ctx.update(
        auto_build=AutoBuildMark(
            status=AutoBuildStatus.QUEUED,
            full_hash=c.full_hash,
            contract_id=c.contract_id,
            owner_id=body.owner_id,
            source_event_id=body.approval_event_id or body.epoch,
            marked_at_us=ctx.now,
            revision=p.revision,
            autopilot_epic=body.epic,
            not_before_us=due if due > ctx.now else 0,
        ),
        autopilot_claim=replace(claim, marked_hash=c.full_hash),
    )
    if body.show_auto_build:
        _write_auto_build(ctx, AUTO_BUILD_QUEUED)
    if due > ctx.now:
        minutes = max(1, -(-(due - ctx.now) // MICROS_PER_MINUTE))
        ctx.note(f"Autopilot: build starts in {minutes} min unless you object")
    else:
        ctx.note("Autopilot: build queued")


def _h_autopilot_withdraw(ctx: _Ctx, body: ev.AutopilotWithdraw) -> None:
    """The epic no longer authorises autopilot here: drop its queued build mark and
    release the claim (a running build is never touched)."""
    p = ctx.p
    claim = p.autopilot_claim
    mark = p.auto_build
    queued = (
        mark is not None
        and mark.status == AutoBuildStatus.QUEUED
        and mark.autopilot_epic == body.epic
    )
    if not queued and (claim is None or claim.epic != body.epic or not claim.active):
        raise Rejected("autopilot-nothing-to-withdraw")
    reason = " ".join(body.reason.split())[:120] or "autopilot was turned off"
    if queued:
        ctx.update(auto_build=None)
        _write_auto_build(ctx, "")
    if claim is not None and claim.epic == body.epic and claim.active:
        ctx.update(autopilot_claim=replace(claim, released=reason))
    ctx.note(f"Autopilot stood down: {reason}")


def _h_autopilot_status(ctx: _Ctx, body: ev.AutopilotStatus) -> None:
    """The autopilot pass's note and pause state for this epic (display; a pause holds
    new starts)."""
    ap = ctx.p.autopilot
    if ap is None:
        raise Rejected("autopilot-off")
    text = " ".join(body.text.split())[:NOTE_MAX]
    paused = " ".join(body.paused.split())[:NOTE_MAX]
    if text == ctx.p.epic_note and paused == ap.paused:
        raise Rejected("autopilot-status-unchanged")
    ctx.update(epic_note=text, autopilot=replace(ap, paused=paused))
    if ctx.p.note.startswith(("Autopilot", "Epic plan")) and not ctx.note_set:
        ctx.update(note="")  # the pass's current status supersedes the acknowledgement


def _h_autopilot_gate_created(ctx: _Ctx, body: ev.AutopilotGateCreated) -> None:
    """A human gate of the epic plan exists as a sub-issue (recorded once per key)."""
    key = body.key.strip()
    if not key or body.number <= 0:
        raise Rejected("invalid-autopilot-gate")
    known = next((g for g in ctx.p.autopilot_gates if g.key == key), None)
    if known is not None:
        if known.number != body.number or known.linked or not body.linked:
            raise Rejected("autopilot-gate-known")
        gates = tuple(replace(g, linked=True) if g.key == key else g for g in ctx.p.autopilot_gates)
        ctx.update(autopilot_gates=gates)
        return
    gate = AutopilotGate(key, body.number, linked=body.linked)
    ctx.update(autopilot_gates=(*ctx.p.autopilot_gates, gate))


def _h_autopilot_question(ctx: _Ctx, body: ev.AutopilotQuestion) -> None:
    """Autopilot cannot go on without the owner: one comment per epoch and question."""
    ap = ctx.p.autopilot
    if ap is None or not ap.approved:
        raise Rejected("autopilot-off")
    if body.key not in _AUTOPILOT_QUESTIONS:
        raise Rejected("unknown-autopilot-question")
    if body.key in ap.asked:
        raise Rejected("autopilot-already-asked")
    ctx.update(autopilot=replace(ap, asked=(*ap.asked, body.key)))
    ctx.comment(f"autopilot-{body.key}")


def _settle_autopilot(ctx: _Ctx) -> None:
    """After every event: an epic's autopilot ends with the epic (closed, assigned,
    stopped, no longer an epic); a sub-issue's claim is released once the owner takes it
    over (a stop or leftward move, an assignment, a revision of the plan that is not an
    answer, or the autopilot build mark cleared without a build)."""
    p = ctx.p
    ap = p.autopilot
    if ap is not None:
        why = ""
        if Hold.COMPLETED in p.holds or p.stage == Stage.DONE:
            why = "Autopilot off: the epic is closed"
        elif not eligible(p):
            why = "Autopilot cleared: the epic is closed, assigned or unverified"
        elif p.barrier_time_us >= ap.set_at_us:
            why = "Autopilot cleared: stopped after it was set; re-select it"
        elif p.links is not None and not p.links.epic:
            why = "Autopilot cleared: the issue has no sub-issues"
        if why:
            ctx.update(autopilot=None)
            _write_autopilot(ctx, "")
            if not ctx.note_set:
                ctx.note(why)
    claim = p.autopilot_claim
    if claim is None or not claim.active or Hold.COMPLETED in p.holds:
        return
    released = ""
    if p.barrier_time_us >= claim.claimed_at_us:
        released = "stopped or moved back by the owner"
    elif not eligible(p):
        released = "assigned to a person"
    elif p.revision > claim.revision:
        released = "the owner revised the plan"
    elif claim.marked_hash:
        mark = p.auto_build
        kept = mark is not None and mark.full_hash == claim.marked_hash
        built = any(a.full_hash == claim.marked_hash for a in p.approvals)
        if not kept and not built:
            released = "the owner cleared the autopilot build"
    if released:
        ctx.update(autopilot_claim=replace(claim, released=released))


def _replan(ctx: _Ctx, via: Via | None) -> None:
    cur = ctx.p.current_session
    if cur is not None and cur.kind == SessionKind.BUILD and cur.lifecycle != Lifecycle.RETIRED:
        _begin_drain(ctx, cur, fences=frozenset({FenceKind.SAFETY, FenceKind.REVOKED}))
    ctx.update(barrier_time_us=max(ctx.p.barrier_time_us, ctx.now))
    _start_plan(ctx, via)


def _plan_control(ctx: _Ctx, via: Via | None) -> None:
    """§2.6B plan rows: Inbox/Triaged/Scoped or stopped recovery start a plan;
    Building replans (revoking any build); Ready takes rework, not a plan."""
    _control(ctx)
    _require_eligible(ctx)
    stage = ctx.origin_stage
    if stage in _NO_RECOVERY_STAGES:
        raise Rejected("plan-not-allowed-from-ready", explain=True)
    if (
        ctx.event.kind == EventKind.REQUEST_PLAN
        and stage == Stage.SCOPED
        and _equivalent_run_in_flight(ctx, SessionKind.PLAN)
    ):
        # Attach to the running plan: audited, no new revision, authority or interrupt.
        raise Rejected("equivalent-run-in-flight")
    if stage == Stage.BUILDING or not _stage_controllable(ctx.p) or _approval_active(ctx):
        _replan(ctx, via)
        return
    if stage in _PLAN_STAGES or _stopped_recovery(ctx):
        _start_plan(ctx, via)
        return
    raise Rejected("plan-not-allowed-from-stage", explain=True)  # pragma: no cover


def _h_request_plan(ctx: _Ctx, body: ev.RequestPlan) -> None:
    _plan_control(ctx, body.via)


def _h_request_replan(ctx: _Ctx, body: ev.RequestReplan) -> None:
    _plan_control(ctx, body.via)


def _h_plan_feedback(ctx: _Ctx, body: ev.PlanFeedback) -> None:
    """A plain owner comment (not a command): the owner steering the current stage.

    Every accepted comment is recorded (``factory_get_feedback`` serves them to all later
    runs). What it starts depends on the column: Triaged re-runs triage, Scoped revises
    the plan, Building nudges the build within its approval (or reworks a finished one),
    Ready reworks the build (see ``_rework``); elsewhere nothing starts.
    A run that is executing a turn gets no message: ``factory_submit_result`` refuses a
    result until the run has read every recorded comment, so a burst of comments folds
    into the run in progress instead of queuing several re-runs.
    """
    _control(ctx)
    _require_eligible(ctx)
    stage = ctx.origin_stage
    answered = _answer_questions(ctx)
    if not answered:
        _epic_plan_revision(ctx)
    if stage == Stage.BUILDING:
        _build_feedback(ctx, body, answered=answered)
    elif stage == Stage.READY and _feedback_rework(ctx):
        return
    elif answered:
        _deliver_answers(ctx)  # the comment was the answer, not also stage feedback
    elif stage == Stage.SCOPED:
        _plan_feedback(ctx, body)
    elif stage == Stage.TRIAGED:
        _triage_feedback(ctx)


def _answer_questions(ctx: _Ctx) -> bool:
    """While owner questions are open, the owner's next plain comment answers them.

    Each open question is answered with the whole comment (its text is served from the
    comment's delivery, see ``answer_event_id``) and relayed to its run like ``/decide``,
    except that a build is never revoked: the reply is taken within the approval.
    """
    open_ = ctx.p.open_decisions
    if not open_:
        return False
    for d in open_:
        ctx.put_decision(
            replace(d, status=DecisionStatus.ANSWERED, answer_event_id=ctx.event.event_id)
        )
    return True


def _deliver_answers(ctx: _Ctx) -> None:
    """Relay the answers this comment recorded to the runs that asked."""
    asked = [d.session_id for d in ctx.p.decisions if d.answer_event_id == ctx.event.event_id]
    for sid in dict.fromkeys(asked):
        s = ctx.p.session(sid)
        if s is not None:
            _deliver_answer(ctx, s)


def _deliver_answer(ctx: _Ctx, s: StageSession) -> None:
    """Relay recorded answers to ``s``; a plan answer is a plan revision."""
    if s.kind == SessionKind.PLAN:
        ctx.update(revision=ctx.p.revision + 1, revision_pending=True)
        mark = ctx.p.auto_build
        if mark is not None and mark.status == AutoBuildStatus.QUEUED:
            # The owner's own answer: the mark waits for the re-posted plan (same hash).
            ctx.update(auto_build=replace(mark, revision=ctx.p.revision))
        claim = ctx.p.autopilot_claim
        if claim is not None and claim.active and claim.revision == ctx.p.revision - 1:
            # The owner's answer to autopilot's plan run is no revision of theirs.
            ctx.update(autopilot_claim=replace(claim, revision=ctx.p.revision))
        _void_approval(ctx, "plan-decision")
        s = ctx.put_session(replace(s, revision=ctx.p.revision))
    _relay_answers(ctx, s)


def _fold_answers(ctx: _Ctx) -> None:
    """Answers taken by a rework: its run reads them with the comment (factory_get_feedback)."""
    for d in ctx.p.decisions:
        if d.status == DecisionStatus.ANSWERED and d.answer_event_id == ctx.event.event_id:
            ctx.put_decision(replace(d, status=DecisionStatus.RELAYED))


def _plan_feedback(ctx: _Ctx, body: ev.PlanFeedback) -> None:
    ctx.update(
        revision=ctx.p.revision + 1,
        revision_pending=True,
        revision_feedback=(*ctx.p.revision_feedback, ctx.event.event_id),
    )
    _void_approval(ctx, "feedback")
    cur = ctx.p.current_session
    if cur is not None and cur.kind == SessionKind.PLAN:
        if executable(ctx.p, cur) and work_allowed(ctx.p, cur):
            busy = cur.lifecycle == Lifecycle.ACTIVE and not _idle_run(cur)
            cur = ctx.put_session(
                replace(
                    cur,
                    revision=ctx.p.revision,
                    lifecycle=Lifecycle.ACTIVE,
                    wait_reason=None,
                    quiescent=cur.quiescent and busy,
                    comment_pending=busy,
                )
            )
            ctx.unhold(Hold.AWAITING_OWNER)
            if busy:
                return  # the running turn must read it before it can submit
            ctx.unhold(Hold.AGENT_BLOCKED, Hold.RESULT_INVALID)
            ctx.emit(
                EffectKind.SEND_MESSAGE,
                session=cur,
                args={
                    "purpose": MessagePurpose.FEEDBACK.value,
                    "revision": ctx.p.revision,
                    "text_digest": body.text_digest,
                },
            )
            return
        if cur.lifecycle in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT) or (
            cur.fences == frozenset({FenceKind.CHECKPOINT})
        ):
            return  # recorded as revision_pending; /continue required before execution
        if not cur.fences and cur.lifecycle in (
            Lifecycle.INTENT,
            Lifecycle.CREATING,
            Lifecycle.PREPARING,
            Lifecycle.UNKNOWN,
        ):
            ctx.put_session(replace(cur, revision=ctx.p.revision))
            return  # first message will carry the current revision
    # No usable current plan: fresh owner feedback is a new plan control.
    ctx.unhold(*CONTROL_CLEARED_HOLDS)
    _start_stage(ctx, SessionKind.PLAN, ctx.config.block_us(ctx.p.size or Size.M))


_TRIAGE_IN_PROGRESS = frozenset(
    {
        Lifecycle.INTENT,
        Lifecycle.CREATING,
        Lifecycle.PREPARING,
        Lifecycle.ACTIVE,
        Lifecycle.WAITING,
        Lifecycle.UNKNOWN,
    }
)


def _triage_feedback(ctx: _Ctx) -> None:
    """Triaged: re-run triage in the issue session unless a triage run will read it anyway."""
    pending = ctx.p.authorization(ctx.p.pending_authorization_id)
    if pending is not None and not pending.cancelled:
        return  # the pending run reads every comment before it submits
    cur = ctx.p.current_session
    if cur is not None and cur.kind == SessionKind.TRIAGE and not cur.fences:
        if cur.lifecycle in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT):
            return  # recorded; /continue required before execution
        finished = bool(ctx.p.holds & {Hold.AGENT_BLOCKED, Hold.RESULT_INVALID})
        if not (finished or cur.execution_closed) and cur.lifecycle in _TRIAGE_IN_PROGRESS:
            if _idle_run(cur) and work_allowed(ctx.p, cur):
                _relay_comment(ctx, cur)  # its turn ended without a result: nudge it
            elif cur.lifecycle == Lifecycle.ACTIVE:
                ctx.put_session(replace(cur, comment_pending=True))  # see _h_tree_quiescent
            return  # in progress: it must read the comment before it can submit
    ctx.unhold(*CONTROL_CLEARED_HOLDS)
    _start_stage(ctx, SessionKind.TRIAGE, ctx.config.block_us(ctx.p.size or Size.S))


def _build_feedback(ctx: _Ctx, body: ev.PlanFeedback, *, answered: bool = False) -> None:
    """Building: relay the comment to an idle build within its approval (never voids it).

    A comment that answers an open question is relayed as that answer. On Needs you with
    no fix attempt left (``readiness_failed``) or a closed run, it starts a rework instead
    (the answer then reaches the rework run with the comment).
    """
    cur = ctx.p.current_session
    build = cur if cur is not None and cur.kind == SessionKind.BUILD else None
    busy = build is not None and build.lifecycle == Lifecycle.ACTIVE and not _idle_run(build)
    if (
        not busy
        and (_build_ended(ctx) or Hold.READINESS_FAILED in ctx.p.holds)
        and _feedback_rework(ctx)  # e.g. Needs you with no fix attempt left
    ):
        return
    if answered:
        _deliver_answers(ctx)
        return
    if busy:
        assert build is not None  # noqa: S101 - busy implies a build
        ctx.put_session(replace(build, comment_pending=True))  # see _h_tree_quiescent
        return
    waiting = (
        build is not None
        and build.lifecycle == Lifecycle.WAITING
        and build.wait_reason == WaitReason.CHECKS
    )
    if (
        build is None
        or not (waiting or _idle_run(build))
        or ctx.p.open_decisions
        or not approval_ok(ctx.p)
        or not work_allowed(ctx.p, build)
    ):
        return  # recorded: a running, queued or paused build reads it before submitting
    build = ctx.put_session(
        replace(
            build,
            lifecycle=Lifecycle.ACTIVE,
            wait_reason=None,
            feedback_wakes=build.feedback_wakes + 1,
        )
    )
    _relay_comment(ctx, build, body.text_digest)


def _idle_run(s: StageSession) -> bool:
    """A live run whose turn ended without a result: its tree was observed idle."""
    return (
        s.lifecycle == Lifecycle.ACTIVE
        and s.quiescent
        and s.root_id is not None
        and not s.fences
        and not s.execution_closed
    )


def _ended_without_result(ctx: _Ctx, s: StageSession) -> bool:
    """The build run submitted build_ready (the readiness record is its own), was woken
    again (readiness wake or owner comment) and ended that turn idle with no new result.
    A run that reported blocked (or whose result was refused) is Bot Blocked already."""
    r = ctx.p.readiness
    return (
        r is not None
        and r.session_id == s.session_id
        and _idle_run(s)
        and not s.comment_pending
        and not ctx.p.holds & BLOCKING_HOLDS
    )


def _wake_answered(ctx: _Ctx, s: StageSession) -> bool:
    """The run's last wake was a readiness wake (check or findings; no owner comment relayed
    to it) and its turn ended with no new result: its build_ready stands and it has nothing
    left to do (#675: it named a red check outside the change on the PR; a re-submission of
    the same head was refused)."""
    return (
        _ended_without_result(ctx, s)
        and (ctx.p.readiness_wakes >= 1 or ctx.p.findings_wakes >= 1)
        and not s.feedback_wakes
    )


def _relay_comment(ctx: _Ctx, s: StageSession, digest: str = "") -> None:
    """Relay an owner comment to an idle run once; it is not re-sent until the tree has
    been seen idle again, so a burst of comments becomes one message."""
    ctx.unhold(Hold.AGENT_BLOCKED, Hold.RESULT_INVALID)  # the owner's steer answers it
    s = ctx.put_session(replace(s, quiescent=False, comment_pending=False))
    _ensure_issuance(ctx, s)
    ctx.emit(
        EffectKind.SEND_MESSAGE,
        session=s,
        args={"purpose": MessagePurpose.FEEDBACK.value, "text_digest": digest},
    )


def _build_ended(ctx: _Ctx) -> bool:
    """Building with no build running, queued or paused at a checkpoint (its run closed)."""
    cur = ctx.p.current_session
    if _approval_active(ctx):
        return False
    if cur is None or cur.kind != SessionKind.BUILD:
        return cur is None or settled(cur)
    return (
        settled(cur)
        and (cur.execution_closed or cur.lifecycle == Lifecycle.RETIRED)
        and not cur.fences & {FenceKind.CHECKPOINT, FenceKind.STOPPED}
    )


def _rework_refusal(ctx: _Ctx, *, live_ok: bool = False) -> str | None:
    """Why a rework cannot start now (None: it can). ``live_ok``: the current build run is
    idle at Needs you and is retired to make way."""
    if _completed(ctx):
        return "PR merged or issue closed"
    if not approval_ok(ctx.p):
        return "the approval is no longer valid"
    if live_ok:
        return None
    if _approval_active(ctx):
        return "a build is already queued or running"
    return None


def _feedback_rework(ctx: _Ctx) -> bool:
    """An owner comment or review on a finished build (Ready, or Needs you in Building:
    a closed run, or an idle run with no fix attempt left, which is retired for it).

    Not after ``/stop`` (a stopped parcel resumes only by an explicit control) nor once
    merged/closed. A comment during a rework already queued or running only steers it.
    """
    if Hold.STOPPED in ctx.p.holds or _completed(ctx) or _rework_queued(ctx):
        return False
    cur = ctx.p.current_session
    idle = cur is not None and cur.kind == SessionKind.BUILD and _approval_active(ctx)
    if idle and Hold.READINESS_FAILED not in ctx.p.holds:
        return False  # a live build only takes the comment as steering
    reason = _rework_refusal(ctx, live_ok=idle)
    if reason is not None:
        ctx.note(f"Rework refused: {reason}")
        return False
    _fold_answers(ctx)
    _rework(ctx, None)
    if idle and cur is not None and not settled(cur) and cur.lifecycle != Lifecycle.DRAINING:
        _begin_drain(ctx, cur)  # the idle run at Needs you makes way for the rework run
    return True


def _rework_queued(ctx: _Ctx) -> bool:
    """A build episode for the current approval is waiting for a slot (an admitted one has
    a live current run, see ``_approval_active``)."""
    a = ctx.p.current_approval
    entry = ctx.admission.queue_entry(ctx.p.parcel_id)
    return (
        a is not None
        and entry is not None
        and entry.approval_id == a.approval_id
        and entry.status == QueueStatus.QUEUED
    )


def _rework(ctx: _Ctx, via: Via | None, *, conflict: str = "") -> None:
    """Owner feedback on the built work: back to Building under the same approval.

    A new build episode on the parcel's branch and PR (admitted like any build, so it may
    queue for a slot) with a fresh time block and a fresh fix budget. The first message
    points the issue session at the feedback; Ready is then re-evaluated as usual.
    ``conflict`` (the base branch name): the episode instead resolves a merge conflict
    with that branch, and its first message says so.
    """
    a = ctx.p.current_approval
    assert a is not None  # noqa: S101 - approval_ok checked by the caller
    ctx.unhold(*CONTROL_CLEARED_HOLDS, Hold.REMEDIATION_EXHAUSTED)
    ctx.update(readiness=None, readiness_wakes=0, findings_wakes=0)
    _cancel_pending(ctx)
    _move(ctx, Stage.BUILDING, via)
    _enqueue_build(
        ctx, a, ctx.config.block_us(ctx.p.size or Size.M), rework=True, conflict=bool(conflict)
    )
    ctx.note(_conflict_note(conflict) if conflict else _REWORK_NOTE)


_REWORK_NOTE = "Rework: owner feedback"


def _h_approve_plan(ctx: _Ctx, body: ev.ApprovePlan) -> None:
    rollback = Stage.SCOPED if body.via == Via.DRAG else None
    _control(ctx)
    if not eligible(ctx.p):
        raise Rejected("parcel-not-eligible", explain=True, rollback_to=rollback)
    if body.via == Via.COMMAND and ctx.p.autopilot is not None and _is_epic(ctx.p):
        _approve_epic_plan(ctx, body)
        return
    latest = ctx.p.current_contract
    if latest is None:
        raise Rejected("no-published-contract", explain=True, rollback_to=rollback)
    a = ctx.p.current_approval
    if (
        a is not None
        and a.kind == ApprovalKind.PLAN
        and a.full_hash == latest.full_hash
        and _approval_active(ctx)
    ):
        ctx.note("Approved: build starts when capacity allows")
        return
    if _build_blocked_by_live(ctx) or ctx.origin_stage not in (Stage.SCOPED, Stage.BUILDING):
        raise Rejected("approval-stage-invalid", explain=True, rollback_to=rollback)
    if not plan_ok(ctx.p):
        raise Rejected("plan-not-approvable", explain=True, rollback_to=rollback)
    _refuse_epic_build(ctx, rollback)
    if body.hash_text is not None:
        digests = [c.full_hash for c in ctx.p.contracts if c.published]
        if resolve_hash(body.hash_text, digests) != latest.full_hash:
            raise Rejected("hash-does-not-identify-latest", explain=True, rollback_to=rollback)
    elif latest.posted_at_us is None or not latest.posted_at_us < ctx.now:
        raise Rejected("contract-not-posted-before-control", explain=True, rollback_to=rollback)
    size = latest.size
    duration = _grant_duration(ctx, body.duration_us, size)
    approval = _new_approval(
        ctx, ApprovalKind.PLAN, latest.full_hash, contract_id=latest.contract_id
    )
    _after_approval(ctx, approval, duration, body.via)
    _started_despite_blockers(ctx)


def _h_waive_plan(ctx: _Ctx, body: ev.WaivePlan) -> None:
    # A refused drag must not leave the card in Building with nothing running: roll it
    # back to the column it came from (design §B "guarded card rollback").
    origin = ctx.origin_stage
    rollback = (
        origin if body.via == Via.DRAG and origin is not None and origin != Stage.BUILDING else None
    )
    _control(ctx)
    snap = ctx.event.evidence
    if snap is None:
        raise Rejected("waiver-without-fresh-snapshot", explain=True, rollback_to=rollback)
    if not eligible(ctx.p):
        raise Rejected("parcel-not-eligible", explain=True, rollback_to=rollback)
    canonical = canonical_issue_snapshot(snap.title, snap.body)
    digest = sha256_hex(canonical)
    a = ctx.p.current_approval
    if (
        a is not None
        and a.kind == ApprovalKind.SKIP
        and a.full_hash == digest
        and (_approval_active(ctx))
    ):
        ctx.note("Approved: build starts when capacity allows")
        return
    if ctx.p.open_decisions:
        raise Rejected("open-decisions", explain=True, rollback_to=rollback)
    if ctx.p.revision_pending:
        raise Rejected("revision-in-flight", explain=True, rollback_to=rollback)
    stage_ok = ctx.origin_stage in (None, Stage.INBOX, Stage.TRIAGED) or body.via == Via.LABEL
    if not stage_ok or _build_blocked_by_live(ctx) or _approval_active(ctx):
        raise Rejected("waiver-stage-invalid", explain=True, rollback_to=rollback)
    _refuse_epic_build(ctx, rollback)
    duration = _grant_duration(ctx, body.duration_us, Size.M)
    approval = _new_approval(ctx, ApprovalKind.SKIP, digest, snapshot=canonical.decode("utf-8"))
    _after_approval(ctx, approval, duration, body.via)
    _started_despite_blockers(ctx)


def _h_decide(ctx: _Ctx, body: ev.Decide) -> None:
    _control(ctx)
    _require_eligible(ctx)
    open_ = ctx.p.open_decisions
    if body.decision_id is None:
        if len(open_) != 1:
            raise Rejected("decision-ambiguous", explain=True)
        d = open_[0]
    else:
        match = [x for x in open_ if x.decision_id == body.decision_id]
        if len(match) != 1:
            raise Rejected("decision-not-open", explain=True)
        d = match[0]
    d = replace(
        d, status=DecisionStatus.ANSWERED, answer=body.answer, answer_event_id=ctx.event.event_id
    )
    ctx.put_decision(d)
    s = _session(ctx, d.session_id)
    within = body.within_contract and d.impact == DecisionImpact.WITHIN_CONTRACT
    if s.kind == SessionKind.BUILD and not within:
        # Answer may change the contract: revoke the build and carry it into a replan.
        _replan(ctx, None)
        return
    _deliver_answer(ctx, s)


def _h_continue(ctx: _Ctx, body: ev.Continue) -> None:
    _control(ctx)
    _require_eligible(ctx)
    s = ctx.p.current_session
    if s is None:
        raise Rejected("nothing-to-continue", explain=True)
    in_checkpoint = s.lifecycle in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT) or (
        FenceKind.CHECKPOINT in s.fences
        and (s.lifecycle == Lifecycle.FENCED or _checkpoint_draining(s))
    )
    if not in_checkpoint:
        raise Rejected("not-at-checkpoint", explain=True)
    if not only_checkpoint_fenced(s):
        raise Rejected("continue-cannot-clear-hard-fence", explain=True)
    if s.lifecycle == Lifecycle.RETIRED or s.execution_closed:
        raise Rejected("session-retired", explain=True)
    if not s.grant.ready:
        if _continue_pending(ctx, s) and all_settled(ctx.p):
            # Retry the same grant (a resume preflight may have failed): re-read, no new grant.
            _emit_verify(ctx, s, reconcile=False)
            return
        raise Rejected("grant-already-preparing")
    if not authority_ok(ctx.p, s):
        raise Rejected("stage-authority-invalid", explain=True)
    if ctx.p.open_decisions:
        raise Rejected("open-decisions", explain=True)
    if s.lifecycle == Lifecycle.FENCED and not all_settled(ctx.p):
        raise Rejected("tree-not-quiescent", explain=True)
    default = ctx.p.size or (Size.M if s.kind != SessionKind.TRIAGE else Size.S)
    duration = _grant_duration(ctx, body.duration_us, default)
    grant = Grant(
        grant_id=ctx.new_id("gr"),
        source_event_id=ctx.event.event_id,
        duration_us=duration,
        ready=False,
        policy_generation=s.grant.policy_generation + 1,
    )
    s = ctx.put_session(replace(s, grant=grant))
    ctx.emit(
        EffectKind.REPLACE_COST_POLICY,
        session=s,
        args={
            "grant_id": grant.grant_id,
            "generation": grant.policy_generation,
            "granted_us": duration,
        },
        dedupe=f"policy:{grant.grant_id}",
    )


def _h_stop(ctx: _Ctx, body: ev.Stop) -> None:
    _control(ctx)
    _safety(ctx, "owner-stop", fence=FenceKind.STOPPED)
    ctx.note("Stopped: /stop")
    _ = body


def _h_request_rework(ctx: _Ctx, body: ev.RequestRework) -> None:
    _control(ctx)
    _require_eligible(ctx)
    if ctx.origin_stage != Stage.READY:
        raise Rejected("rework-only-from-ready", explain=True)
    reason = _rework_refusal(ctx)
    if reason is not None:
        raise Rejected(f"rework refused: {reason}", explain=True)
    _rework(ctx, Via.DRAG)
    _ = body


def _h_pause(ctx: _Ctx, body: ev.Pause | ev.Unpause) -> None:
    if ctx.event.provenance != ev.Provenance.OPERATOR:
        raise Rejected("pause-requires-operator")
    paused = isinstance(body, ev.Pause)
    ctx.admission = replace(ctx.admission, paused=paused)
    if not paused:
        ctx.emit(EffectKind.WAKE_SCHEDULER, target=ctx.admission.repo_id)


# ================================================================= safety rows


def _h_leftward(ctx: _Ctx, body: ev.LeftwardMove) -> None:
    e = ctx.event
    move = ctx.p.observed_move
    # A read that showed this very move first raised the barrier: the owner's drag is
    # judged against the barrier before it, unless anything newer raised it since.
    seen_first = (
        move is not None
        and (move.from_stage, move.to_stage) == (body.from_stage, body.to_stage)
        and ctx.p.stage == body.to_stage
        and ctx.p.barrier_time_us == move.barrier_us
    )
    barrier = move.prior_barrier_us if seen_first and move is not None else ctx.p.barrier_time_us
    owner_fresh = _owner_control(ctx) and e.source_time_us > barrier
    # A GitHub-origin move can never acknowledge a daemon write (daemon_effect_id is
    # ignored here). Only a non-owner/untrusted move to exactly the in-flight or queued
    # daemon target is consistent with it; a fresh owner control is always evaluated
    # in full (its target becomes the newest desired column, queued behind any
    # in-flight write).
    if not owner_fresh and (
        any(m.to_stage == body.to_stage for m in ctx.p.pending_moves)
        or (ctx.p.queued_move is not None and ctx.p.queued_move == body.to_stage)
    ):
        return
    from_stage = body.from_stage or ctx.p.stage
    if not is_leftward(from_stage, body.to_stage):
        raise Rejected("not-a-leftward-move")
    # Negative half, any actor: fence, and for Building->Scoped revoke/void/revise.
    _leftward_negative(ctx, from_stage, body.to_stage, owner_target=owner_fresh)
    if from_stage == Stage.BUILDING and body.to_stage == Stage.SCOPED:
        if owner_fresh and eligible(ctx.p):
            _start_plan(ctx, Via.DRAG)
    elif from_stage == Stage.READY and body.to_stage == Stage.BUILDING and owner_fresh:
        # The owner's drag back to Building asks for rework (the stop above stays if not).
        reason = _rework_refusal(ctx) if eligible(ctx.p) else "the issue is not eligible"
        if reason is None:
            _rework(ctx, Via.DRAG)
        else:
            ctx.hold(Hold.REWORK_CONTROL_REQUIRED)
            ctx.note(f"Rework refused: {reason}")


def _h_ineligible(
    ctx: _Ctx, body: ev.AssignedHuman | ev.Closed | ev.Transferred | ev.Deleted
) -> None:
    ctx.update(eligible=False)
    _safety(ctx, body.KIND.value)
    if isinstance(body, ev.Closed):
        _complete(ctx)
        ctx.emit(
            EffectKind.CLEANUP_WORKSPACE,
            args={"merged": False},
            dedupe=f"cleanup:{ctx.p.parcel_id}:closed:{ctx.event.event_id}",
        )


def _h_item_removed(ctx: _Ctx, body: ev.ItemRemoved) -> None:
    ctx.update(in_project=False)
    _safety(ctx, body.KIND.value)


def _h_inbox_hold_set(ctx: _Ctx, body: ev.InboxHoldSet) -> None:
    """Hold the parcel on an uninterpretable delivery; treat it as a possible safety fact.

    A stricter reason (``parked``) replaces a laxer one for the same delivery; the reverse
    is refused so a parked delivery can never be downgraded to inbox-releasable.
    """
    if not body.delivery_guid:
        raise Rejected("inbox-hold-without-delivery")
    existing = next((h for h in ctx.p.inbox_holds if h.delivery_guid == body.delivery_guid), None)
    if existing is not None:
        if existing.reason in (body.reason, InboxHoldReason.PARKED):
            raise Rejected("inbox-hold-already-set")
        ctx.update(
            inbox_holds=tuple(
                InboxHold(h.delivery_guid, body.reason)
                if h.delivery_guid == body.delivery_guid
                else h
                for h in ctx.p.inbox_holds
            )
        )
        return
    _safety(ctx, "inbox-hold")
    ctx.update(inbox_holds=(*ctx.p.inbox_holds, InboxHold(body.delivery_guid, body.reason)))
    ctx.hold(Hold.INBOX)


def _h_inbox_hold_released(ctx: _Ctx, body: ev.InboxHoldReleased) -> None:
    """Remove one inbox hold. Restores no authority and clears no fence."""
    held = next((h for h in ctx.p.inbox_holds if h.delivery_guid == body.delivery_guid), None)
    if held is None:
        raise Rejected("inbox-hold-not-found")
    releaser = (
        ev.Provenance.OPERATOR if held.reason == InboxHoldReason.PARKED else ev.Provenance.INBOX
    )
    if ctx.event.provenance != releaser:
        raise Rejected(f"inbox-hold-{held.reason.value}-release-requires-{releaser.value}")
    remaining = tuple(h for h in ctx.p.inbox_holds if h.delivery_guid != body.delivery_guid)
    ctx.update(inbox_holds=remaining)
    if not remaining:
        ctx.unhold(Hold.INBOX)


def _invalidate(ctx: _Ctx, reason: str, *, revision_pending: bool) -> None:
    had_build = _void_approval(ctx, reason)
    if revision_pending:
        ctx.update(revision_pending=True)
    if had_build:
        if ctx.p.pending_authorization_id is None:
            ctx.hold(Hold.APPROVAL_VOIDED)
        if ctx.p.stage == Stage.BUILDING:
            _move(ctx, Stage.SCOPED)
        ctx.note(f"Approval invalidated: {_words(reason)}; approve the current plan again")


def _h_approval_invalidated(ctx: _Ctx, body: ev.ApprovalInvalidated) -> None:
    if body.approval_id != ctx.p.current_approval_id:
        raise Rejected("not-current-approval")
    a = ctx.p.current_approval
    _invalidate(
        ctx,
        body.reason or "invalidated",
        revision_pending=a is not None and a.kind == ApprovalKind.PLAN,
    )


def _h_waiver_edited(ctx: _Ctx, body: ev.WaiverEdited) -> None:
    _waiver_edited(ctx)
    _ = body


def _waiver_edited(ctx: _Ctx) -> None:
    """A verified title/body edit: permanently voids a live waiver (§2.3, §2.6A).

    revision_pending is set, so recovery needs a published plan and fresh approval
    (or an owner /plan); a fresh waiver of the edited text is refused meanwhile.
    """
    ctx.update(issue_edit_count=ctx.p.issue_edit_count + 1)
    a = ctx.p.current_approval
    if a is not None and a.kind == ApprovalKind.SKIP and a.valid:
        _invalidate(ctx, "issue-edited-under-waiver", revision_pending=True)


def _h_contract_tampered(ctx: _Ctx, body: ev.ContractTampered) -> None:
    c = ctx.p.contract(body.contract_id)
    if c is None:
        raise Rejected("unknown-contract")
    ctx.put_contract(replace(c, intact=False))
    a = ctx.p.current_approval
    if a is not None and a.contract_id == c.contract_id:
        _invalidate(ctx, "contract-tampered", revision_pending=True)


# ========================================================== observation rows


def _h_snapshot(ctx: _Ctx, body: ev.GitHubSnapshot) -> None:
    evidence = ctx.event.evidence
    if evidence is None:
        raise Rejected("snapshot-without-evidence")
    _ = body
    # Drift correction from a fresh read: write Bot / Factory note only when the board
    # shows another value than the derived one (a change in this event is written by
    # the projection).
    derived = project_bot(ctx.p, queued=_queued(ctx))
    if (
        evidence.bot is not None
        and ctx.p.in_project
        and derived == ctx.p.bot
        and evidence.bot != derived.value
    ):
        ctx.emit(EffectKind.SET_BOT, args={"bot": derived.value})
    note = project_note(ctx.p, derived)
    if (
        evidence.note is not None
        and ctx.p.in_project
        and derived == ctx.p.bot
        and note == ctx.p.board_note
        and evidence.note != note
    ):
        ctx.emit(EffectKind.SET_NOTE, args={"note": note})


def _h_column_observed(ctx: _Ctx, body: ev.ColumnObserved) -> None:
    if body.daemon_effect_id is not None and ctx.event.provenance == ev.Provenance.ADAPTER:
        # Executor read-after-write: must name a pending write and its exact target.
        move = ctx.p.pending_move(body.daemon_effect_id)
        if move is None:
            raise Rejected("unknown-board-write")
        if body.stage != move.to_stage:
            raise Rejected("ack-target-mismatch")
        _retire_move(ctx, move, landed=True)
        if _late_findings_wake_due(ctx):
            r = ctx.p.readiness
            assert r is not None  # noqa: S101 - checked by _late_findings_wake_due
            _fetch_evidence(ctx, r)  # the read that wakes the re-opened run
        return
    if body.stage is None:
        raise Rejected("column-observation-without-stage")
    if ctx.now < ctx.p.board_written_at_us:
        return  # e.g. a late echo of an earlier daemon write: older than the board
    # Observation only (never authority): leftward routes through the safety half.
    _observe_stage(ctx, body.stage)


def _completed(ctx: _Ctx) -> bool:
    return Hold.COMPLETED in ctx.p.holds


def _complete(ctx: _Ctx) -> None:
    """Merged PR or closed issue: terminal before any readiness or stage transition.

    Drops a queued board write, drains live work and records the barrier; later checks,
    heads, reviews and evidence reads are audit only (never Building, never a comment).
    """
    ctx.hold(Hold.COMPLETED)
    ctx.unhold(Hold.CHECKS_FAILED, Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED)
    _drop_queued_move(ctx)
    _cancel_pending(ctx)
    cur = ctx.p.current_session
    if cur is not None and not settled(cur) and cur.lifecycle != Lifecycle.DRAINING:
        _begin_drain(ctx, cur)
    for s in ctx.p.sessions:
        if settled(s) and not s.execution_closed and _terminally_fenced(ctx, s.lifecycle, s.fences):
            ctx.put_session(replace(s, execution_closed=True))


def _merged(ctx: _Ctx, pr_number: int) -> None:
    """The linked PR merged: terminal even if the issue stays open (no closing reference).

    The open-PR slot is released and live build work drains, which frees the building
    slot once the tree is quiescent.
    """
    if _completed(ctx):
        return
    _release_pr(ctx, pr_number)
    _complete(ctx)
    # Finished: once its sessions have settled, remove the factory worktree/branch.
    ctx.emit(
        EffectKind.CLEANUP_WORKSPACE,
        args={"merged": True, "pr_number": pr_number},
        dedupe=f"cleanup:{ctx.p.parcel_id}:merged",
    )


def _h_pr_observed(ctx: _Ctx, body: ev.PRObserved) -> None:
    prs = set(ctx.admission.open_bot_prs)
    if body.bot_authored:
        if body.open:
            prs.add(body.pr_number)
        else:
            prs.discard(body.pr_number)
    ctx.admission = replace(ctx.admission, open_bot_prs=frozenset(prs))
    if not body.parcel_branch:
        return
    if body.open:
        ctx.update(pr_number=body.pr_number)
        ctx.admission = replace(
            ctx.admission,
            reservations=tuple(
                replace(r, pr_number=body.pr_number)
                if r.parcel_id == ctx.p.parcel_id
                and r.kind == ReservationKind.OPEN_PR
                and r.live
                and r.pr_number is None
                else r
                for r in ctx.admission.reservations
            ),
        )
        r = ctx.p.readiness
        if (
            not _completed(ctx)
            and r is not None
            and r.pr_number == body.pr_number
            and body.head_sha
            and r.head_sha != body.head_sha
        ):
            # A push (agent or owner merge-from-main) is a hint: a fresh read establishes
            # the current head, so a late old-head webhook can never roll it back.
            _fetch_evidence(ctx, r)
        return
    _release_pr(ctx, body.pr_number)
    if body.merged:
        _merged(ctx, body.pr_number)
        return
    if _completed(ctx):
        return
    ready = ctx.p.readiness
    if (
        ctx.p.stage == Stage.READY
        and ready is not None
        and (ready.ready or _refreshing(ctx))
        and (ready.pr_number == body.pr_number)
    ):
        _invalidate_ready(ctx, "pr-closed-unmerged")
    ctx.hold(Hold.PR_CLOSED)
    ctx.note(f"PR #{body.pr_number} closed without merging")


def _invalidate_ready(ctx: _Ctx, reason: str) -> None:
    if _completed(ctx):
        return  # merged/closed wins over any late negative evidence
    r = ctx.p.readiness
    if r is not None:
        ctx.update(
            readiness=replace(
                r,
                ready=False,
                verified=False,
                sync_red=False,
                red_checks="",
                report_effect_id="",  # a later Ready posts a new report
                report_key="",
            )
        )
    _move(ctx, Stage.BUILDING)
    ctx.hold(Hold.REWORK_CONTROL_REQUIRED)
    head = r.head_sha[:7] if r is not None and r.head_sha else ""
    ctx.note(f"No longer ready: {_words(reason)}" + (f" on {head}" if head else ""))


def _fetch_evidence(ctx: _Ctx, r: Readiness) -> None:
    """A fresh read of the linked PR: actual head, current checks, review, findings and
    whether it merges into its base.

    A review that predates a merge conflict seen on the PR never carries to a newer head
    as a base sync (``sync_carry`` false): the merge that resolved it is new work.
    """
    reviewed = r.reviewed_head or r.head_sha
    args: dict[str, JsonValue] = {
        "pr_number": r.pr_number,
        "head_sha": r.head_sha,
        "reviewed_head": reviewed,
        "session_id": r.session_id,
        "issue_number": ctx.p.issue_number,
    }
    if reviewed == ctx.p.conflict_reviewed_head:
        args["sync_carry"] = False
    ctx.emit(EffectKind.FETCH_PR_EVIDENCE, args=args)


def _readiness_for(ctx: _Ctx, pr_number: int, unknown: str) -> Readiness:
    """The readiness record a PR/check/review observation refers to (0 = unnamed PR)."""
    if _completed(ctx):
        raise Rejected("parcel-completed")
    r = ctx.p.readiness
    if r is None or pr_number not in (0, r.pr_number):
        # Before build_ready (or the PR link) is recorded: build_ready always issues a
        # fresh read, which covers this observation, so nothing is lost by not acting.
        raise Rejected(unknown)
    return r


def _h_checks_changed(ctx: _Ctx, body: ev.ChecksChanged) -> None:
    """A check-suite/workflow webhook is one suite on some head, not the aggregate: it
    only triggers a fresh current-head read, which alone decides readiness.

    A webhook received before the newest applied read began is already reflected in that
    read: no further read (#745: a burst of ~20 suite webhooks for one head, each applied
    after the previous read, re-read the PR every ~5s until the backlog drained).
    """
    r = _readiness_for(ctx, body.pr_number, "checks-before-readiness-recorded")
    if ctx.now <= r.read_started_us:
        return
    if ctx.p.stage == Stage.READY and r.ready:
        if body.head_sha == r.head_sha and body.state == ev.ChecksState.GREEN:
            return
    elif r.verified and body.head_sha == r.head_sha and body.state == ev.ChecksState.GREEN:
        return
    _fetch_evidence(ctx, r)


def _h_review_changed(ctx: _Ctx, body: ev.ReviewChanged) -> None:
    r = _readiness_for(ctx, body.pr_number, "review-for-unknown-pr")
    if ctx.p.stage == Stage.READY and body.changes_requested:
        _invalidate_ready(ctx, "review-changed")  # the owner asks for rework
    elif body.head_sha and body.head_sha != r.head_sha:
        _fetch_evidence(ctx, r)  # e.g. an approval of a newer head: re-read, never rework
    elif _awaiting_evidence(ctx):
        # E.g. the review bot's answer while it is waited for, or late findings on a Ready
        # card (#799): judged from a fresh read now, not at the next reconcile.
        _fetch_evidence(ctx, r)
    # Otherwise an observation only: a review never authorizes a new work episode.


def _h_readiness(ctx: _Ctx, body: ev.ReadinessEvidence) -> None:
    # A fresh evidence read supersedes earlier ambiguous reads (reads have no effects).
    if any(u.kind == EffectKind.FETCH_PR_EVIDENCE.value for u in ctx.p.unknown_effects):
        ctx.update(
            unknown_effects=tuple(
                u for u in ctx.p.unknown_effects if u.kind != EffectKind.FETCH_PR_EVIDENCE.value
            )
        )
    if _completed(ctx):
        return  # audit only: terminal state wins over a late read
    r = ctx.p.readiness
    if r is None or (r.session_id, r.pr_number, r.head_sha) != (
        body.session_id,
        body.pr_number,
        body.head_sha,
    ):
        raise Rejected("readiness-for-stale-report")
    if body.merged:
        _merged(ctx, r.pr_number)
        return
    observed = body.observed_head_sha or body.head_sha
    if observed != r.head_sha:
        _head_changed(ctx, r, observed)
        return
    if body.read_started_us > r.read_started_us:
        r = replace(r, read_started_us=body.read_started_us)
        ctx.update(readiness=r)
    r = _review_bot_window(
        ctx, r, body.review_bot_pending_since_us, body.review_bot_verdict, body.review_bot_eyes
    )
    if body.remediation_exhausted:
        ctx.hold(Hold.REMEDIATION_EXHAUSTED)
    if r.merge_unknown != (body.mergeable == ev.MERGE_UNKNOWN):
        r = replace(r, merge_unknown=body.mergeable == ev.MERGE_UNKNOWN)
        ctx.update(readiness=r)
    if body.mergeable == ev.MERGE_CONFLICT and body.pr_open:
        _merge_conflict(ctx, r, body)
        return
    r = _restore_withdrawn(ctx, r, body)
    in_ready = ctx.p.stage == Stage.READY and (r.ready or _refreshing(ctx))
    run = _open_run(ctx, r) if in_ready else None
    if run is not None:
        # The owner moved the card to Ready while its build run was still open.
        why = _run_unfinished(ctx, run, owner_moved=True) or _unfinished_reason(
            r, body, ctx.p.issue_number
        )
        if why:
            ctx.update(readiness=replace(r, verified=False))
            _move(ctx, Stage.BUILDING)
            ctx.note(f"Kept in Building: {why}")
            return
        _retire_ready_run(ctx, run, r)
        ctx.unhold(*_READY_CLEARED_HOLDS, Hold.AGENT_BLOCKED)
    if not body.verified:
        ctx.update(readiness=replace(r, verified=False))
        if in_ready:
            if body.checks == ev.ChecksState.PENDING and body.pr_open:
                # A re-run or a new head's checks: stay in Ready, waiting (Bot Working);
                # a later green read restores Ready with no owner action or comment.
                ctx.update(
                    readiness=replace(r, verified=False, ready=False, sync_red=False, red_checks="")
                )
                return
            if _red_only(body):
                # The bot's work is done; a red required check only decides Bot (Blocked):
                # no comment, no rework hold, no wake.
                _mark_red(ctx, r, body)
                _refresh_ready_report(ctx, body.checks_summary)
                return
            if _reopen_for_late_findings(ctx, r, body):
                return
            reason = (
                "readiness-unverified"
                if body.checks is None
                else _failure_reason(r, body, ctx.p.issue_number)
            )
            if Hold.READINESS_FAILED not in ctx.p.holds and body.checks is not None:
                _needs_you_comment(ctx, r, body, reason)
            ctx.hold(Hold.READINESS_FAILED)
            _invalidate_ready(ctx, reason)
            return
        if body.checks is None or not body.pr_open:
            ctx.hold(Hold.READINESS_FAILED)  # legacy read or closed PR: owner decides
            return
        if _reopen_for_late_findings(ctx, r, body):
            r = ctx.p.readiness or r  # the withdrawn card's run waits again: wake it below
        _not_ready(ctx, r, body)
        return
    if in_ready and body.remediation_exhausted:
        _invalidate_ready(ctx, "remediation-exhausted")
        return
    ctx.unhold(Hold.READINESS_FAILED, Hold.CHECKS_FAILED)
    ctx.update(
        readiness=replace(
            r,
            verified=True,
            ready=r.ready or in_ready,
            checks_summary=body.checks_summary[:200],
            verified_at_us=ctx.now,
            sync_red=False,
            red_checks="",
        )
    )
    _refresh_ready_report(ctx, body.checks_summary)
    run = None if in_ready else _open_run(ctx, r)
    if run is not None and _ended_without_result(ctx, run) and not _wake_answered(ctx, run):
        # Woken by an owner comment, it ended its turn without re-submitting: never left
        # as Working; the owner decides (a later build_ready clears this on a green read).
        ctx.hold(Hold.READINESS_FAILED)
        ctx.note(f"Needs you: PR #{r.pr_number} on {r.head_sha[:7]}: {_NO_NEW_RESULT}")


#: Why a woken build run that ended its turn idle is not taken as finished.
_NO_NEW_RESULT = "the build run ended its turn without submitting a new build_ready"

#: The review-bot line of a Ready report reached with the bot silent (grace passed).
_NO_BOT_RESPONSE = "no response within the grace window"
#: The review bot was (re-)asked and its grace is still running.
_BOT_ASKED = "asked, no response yet"

#: How long after the review grace a Ready card whose review-bot line is not final
#: (no response yet) is still re-read each reconcile: a +1 reaction sends no webhook.
_REPORT_POLL_CAP_US = 24 * 60 * 60 * 1_000_000


def _ready_report_args(r: Readiness, checks_summary: str) -> dict[str, JsonValue]:
    """What the Ready report's factory lines show: CI, red checks, the review bot."""
    args: dict[str, JsonValue] = {
        "report": "ready",
        "pr_number": r.pr_number,
        "head_sha": r.head_sha,
        "checks_summary": checks_summary[:200],
        "review_bot": r.review_bot,
    }
    if r.sync_red:
        args["red_checks"] = r.red_checks
    return args


def _report_key(args: Mapping[str, JsonValue]) -> str:
    keys = ("checks_summary", "red_checks", "review_bot")
    return "\x1f".join(f"{k}={args[k]}" if k in args else k for k in keys)


def _publish_ready_report(ctx: _Ctx, checks_summary: str) -> None:
    """Post the Ready report for the current head and remember it for in-place updates."""
    r = ctx.p.readiness
    assert r is not None  # noqa: S101 - callers hold a readiness record
    args = _ready_report_args(r, checks_summary)
    effect = ctx.emit(EffectKind.PUBLISH_REPORT, args=args)
    ctx.update(
        readiness=replace(r, report_effect_id=effect.effect_id, report_key=_report_key(args))
    )


def _refresh_ready_report(ctx: _Ctx, checks_summary: str) -> None:
    """Ready on the same head with a settled read (green, or parked red): when the
    report's factory lines would now read differently (a late bot verdict, the check
    summary, a red check gone or added), edit that comment in place (edits do not
    notify). Never a second comment; the agent's summary is not touched."""
    r = ctx.p.readiness
    if (
        r is None
        or not r.report_effect_id
        or ctx.p.stage != Stage.READY
        or not (r.ready or r.sync_red)
    ):
        return
    args = _ready_report_args(r, checks_summary)
    key = _report_key(args)
    if key == r.report_key:
        return
    ctx.emit(EffectKind.EDIT_REPORT, args={**args, "report_effect_id": r.report_effect_id})
    ctx.update(readiness=replace(r, report_key=key))


def _report_line_pending_at(p: Parcel, r: Readiness, now_us: int) -> bool:
    """A Ready card whose report still says the review bot gave no response: re-read on
    each reconcile (reactions send no webhook) for up to a day after the grace."""
    return (
        p.stage == Stage.READY
        and r.ready
        and bool(r.report_effect_id)
        and not r.review_bot_done
        and r.review_bot in (_NO_BOT_RESPONSE, _BOT_ASKED)
        and now_us < r.settle_at_us + _REPORT_POLL_CAP_US
    )


def _done_apart_from_checks(body: ev.ReadinessEvidence) -> bool:
    """Everything but the check colour says the bot's work is done: an open PR that
    closes the issue, an accepted cross-vendor review of this head (or carried to an
    owner/base sync of it) and an outcome for every review-bot finding."""
    return (
        body.pr_open
        and body.checks is not None
        and body.closes_issue
        and body.review_accepted
        and not body.findings_open
        and not body.remediation_exhausted
    )


def _red_only(body: ev.ReadinessEvidence) -> bool:
    """The only failure is a red required check."""
    return body.checks == ev.ChecksState.FAILED and _done_apart_from_checks(body)


def _unfinished_reason(r: Readiness, body: ev.ReadinessEvidence, issue: int | None) -> str:
    """Short reasons (for the card note) why the work is not done, check colour aside."""
    if not body.pr_open:
        return f"PR #{r.pr_number} is not open"
    if body.checks is None:
        return "the PR checks could not be read"
    parts = []
    if not body.closes_issue:
        parts.append(f"PR #{r.pr_number} does not close #{issue}")
    if not body.review_accepted:
        parts.append(f"no accepted cross-vendor review of {r.head_sha[:7]}")
    if body.findings_open:
        parts.append("review-bot findings have no outcome")
    if body.remediation_exhausted:
        parts.append("fix budget used up")
    return "; ".join(parts)


def _open_run(ctx: _Ctx, r: Readiness) -> StageSession | None:
    """The build run that reported ``r``, while it is the current run and not retired."""
    s = ctx.p.session(r.session_id)
    if s is None or s.session_id != ctx.p.current_session_id or s.lifecycle == Lifecycle.RETIRED:
        return None
    return s


def _run_unfinished(ctx: _Ctx, s: StageSession, *, owner_moved: bool = False) -> str:
    """Why the open build run still has something to do ("" = its turn is over: it
    submitted build_ready and waits on checks, or reported blocked and went idle; with
    no open question or unconfirmed write).

    ``owner_moved``: the owner put the card in Ready. A run waiting on checks after
    build_ready then counts as done even without a complete idle scan (#461: tree scans
    alternate complete/incomplete, and an incomplete one clears ``quiescent``).
    """
    waiting = s.lifecycle == Lifecycle.WAITING and s.wait_reason == WaitReason.CHECKS
    reported = (_idle_run(s) and Hold.AGENT_BLOCKED in ctx.p.holds) or _wake_answered(ctx, s)
    if s.fences or not ((waiting and (s.quiescent or owner_moved)) or reported):
        return "the build run is still working"
    if ctx.p.open_decisions:
        return "an open question needs your answer"
    if uncertain(ctx.p) or message_uncertain(ctx.p, s):
        return "a factory write is unconfirmed"
    return ""


def _retire_ready_run(ctx: _Ctx, s: StageSession, r: Readiness) -> None:
    """Close the finished build run for Ready (as ``_maybe_ready`` does), keeping the PR."""
    s = ctx.put_session(replace(s, lifecycle=Lifecycle.RETIRED, execution_closed=True))
    ctx.emit(EffectKind.DISABLE_ISSUANCE, session=s)
    _release_build(ctx, keep_pr=True)
    ctx.update(pr_number=r.pr_number)


#: Readiness holds that a card entering Ready with the bot's work done no longer needs.
_READY_CLEARED_HOLDS = (
    Hold.READINESS_FAILED,
    Hold.REWORK_CONTROL_REQUIRED,
    Hold.CHECKS_FAILED,
)


def _mark_red(ctx: _Ctx, r: Readiness, body: ev.ReadinessEvidence) -> None:
    """Ready with a red required check: Bot Blocked, the note names the checks.

    GitHub's check rollup for one head can differ from read to read (#461: 17 vs 18
    runs, one failure hidden in alternate reads), so the names accumulate while the
    same head stays red; a pending or green read or a new head starts afresh.
    """
    names = [n for n in body.failing_checks.split(_CHECK_SEP) if n]
    if r.sync_red:
        names += [n for n in r.red_checks.split(_CHECK_SEP) if n]
    red_checks = _CHECK_SEP.join(sorted(set(names)))[:200]
    ctx.update(
        readiness=replace(r, verified=False, ready=False, sync_red=True, red_checks=red_checks)
    )
    ctx.note(sync_red_note(red_checks))


#: Separator of check names (a name may contain commas: "web / Typecheck, test, lint").
_CHECK_SEP = "; "


def _park_red(ctx: _Ctx, r: Readiness, s: StageSession, body: ev.ReadinessEvidence) -> None:
    """Building, the bot's work done, only a required check red and no fix wake left (or
    none applicable): the card goes to Ready with Bot Blocked and no wake. The owner gets
    the Ready report once (to review and merge), naming the red checks; the agent's
    summary gives its cause. A card the old rule withdrew is restored without one."""
    report = not _withdrawn_for_checks(ctx)
    _retire_ready_run(ctx, s, r)
    ctx.unhold(*_READY_CLEARED_HOLDS)
    _move(ctx, Stage.READY)
    _mark_red(ctx, r, body)
    if report:
        # Once: parking retires the run it needs; retries adopt the effect marker.
        _publish_ready_report(ctx, body.checks_summary)


#: The withdrawal note of ``_invalidate_ready``, current and as earlier releases wrote it.
_WITHDRAWN_NOTES = ("No longer ready: ", "Ready withdrawn: ")

#: Holds that make a withdrawn Ready card more than the red-check misclassification.
_NOT_CHECKS_WITHDRAWN = frozenset(
    {
        Hold.SAFETY,
        Hold.STOPPED,
        Hold.COMPLETED,
        Hold.PR_CLOSED,
        Hold.REMEDIATION_EXHAUSTED,
        Hold.APPROVAL_VOIDED,
        Hold.UNSUPPORTED_REWORK,
        Hold.EXTERNAL_ACTIVITY,
        Hold.AGENT_BLOCKED,
    }
)


def _withdrawn_for_checks(ctx: _Ctx) -> bool:
    """The card left Ready only because a required check was red (the old rule): the
    withdrawal note names that one reason (several are "; "-joined)."""
    return (
        Hold.REWORK_CONTROL_REQUIRED in ctx.p.holds
        and ctx.p.note.startswith(_WITHDRAWN_NOTES)
        and "required checks failed" in ctx.p.note
        and "; " not in ctx.p.note
    )


def _restore_withdrawn(ctx: _Ctx, r: Readiness, body: ev.ReadinessEvidence) -> Readiness:
    """Self-correct a card the old rule withdrew from Ready for a red required check.

    Before this rule a red required check moved a Ready card to Building with
    ``readiness_failed`` + ``rework_control_required`` and the withdrawal note (#651:
    an owner base sync with its Ready run retired; #461: an owner drag to Ready with
    the build run still waiting on checks). When this read shows the bot's work done
    apart from the check colour, the card goes back to Ready (no session, no comment)
    and the read is judged there.
    """
    s = ctx.p.session(r.session_id)
    if not (
        ctx.p.stage == Stage.BUILDING
        and not r.ready
        and Hold.READINESS_FAILED in ctx.p.holds
        and _withdrawn_for_checks(ctx)
        and not ctx.p.holds & _NOT_CHECKS_WITHDRAWN
        and s is not None
        and s.session_id == ctx.p.current_session_id
        and (
            (s.lifecycle == Lifecycle.RETIRED and s.execution_closed)
            or not _run_unfinished(ctx, s, owner_moved=True)
        )
        and not ctx.p.open_decisions
        and not _rework_queued(ctx)
        and _done_apart_from_checks(body)
    ):
        return r
    ctx.unhold(*_READY_CLEARED_HOLDS)
    r = replace(r, ready=False, verified=False)
    ctx.update(readiness=r)
    _move(ctx, Stage.READY)
    return r


def _settle_at(ctx: _Ctx, head: str) -> int:
    """Review-bot grace for ``head``: a re-submission of the same head keeps its clock."""
    r = ctx.p.readiness
    if r is not None and r.head_sha == head and r.settle_at_us:
        return r.settle_at_us
    return ctx.now + ctx.config.review_grace_us


def _reopen_for_late_findings(ctx: _Ctx, r: Readiness, body: ev.ReadinessEvidence) -> bool:
    """Review-bot findings without an outcome arrived on the current head after the card
    reached Ready (#799: Codex answered 2 minutes after the grace), and this round's
    findings wake is unused: the build run retired for Ready is re-opened once, under its
    own authorization and approval (no new authority), and gets the findings wake.

    The card goes back to Building; the run waits on checks again with its building slot
    re-reserved, and the read after the move lands wakes it (``_not_ready``). A card the
    earlier rule already withdrew to Building for those findings (``readiness_failed`` +
    ``rework_control_required``) is re-opened in place and woken by this read. Anything
    else (the wake spent, the run re-opened before, no free building slot, a closed gate)
    stays with the owner as before. Returns whether the run was re-opened.
    """
    s = ctx.p.session(r.session_id)
    in_ready = ctx.p.stage == Stage.READY and (r.ready or _refreshing(ctx))
    # Only a failed read withdraws Ready with both holds (an owner review or a refused
    # rework sets ``rework_control_required`` alone); this read decides that findings are
    # the gap (the withdrawal note may be cut short or already cleared).
    withdrawn = (
        ctx.p.stage == Stage.BUILDING
        and not r.ready
        and {Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED} <= ctx.p.holds
    )
    if not (
        (in_ready or withdrawn)
        and body.findings_open
        and body.pr_open
        and body.checks not in (None, ev.ChecksState.PENDING)
        and body.closes_issue
        and body.review_accepted
        and not body.base_sync  # an owner sync head keeps its existing rule
        and not body.remediation_exhausted
        and ctx.p.findings_wakes < 1
        and s is not None
        and s.kind == SessionKind.BUILD
        and s.session_id == ctx.p.current_session_id
        and s.lifecycle == Lifecycle.RETIRED
        and s.execution_closed
        and not s.reopened
        and not s.fences
        and s.root_id is not None
        and not s.external_active
        and not s.restart_pending
        and s.policy_ready
        and s.grant.remaining_us > 0
        and not ctx.p.holds & (_NOT_CHECKS_WITHDRAWN | BLOCKING_HOLDS)
        and not ctx.p.open_decisions
        and not _rework_queued(ctx)
        and approval_ok(ctx.p)
        and authority_ok(ctx.p, s)
        and not uncertain(ctx.p)
        and building_capacity_available(ctx.admission, ctx.config)
    ):
        return False
    a = ctx.p.current_approval
    assert a is not None  # noqa: S101 - approval_ok checked above
    ctx.unhold(*_READY_CLEARED_HOLDS)
    ctx.put_session(
        replace(
            s,
            lifecycle=Lifecycle.WAITING,
            wait_reason=WaitReason.CHECKS,
            quiescent=True,
            execution_closed=False,
            reopened=True,
        )
    )
    building = Reservation(
        ctx.new_id("rs"), ctx.p.parcel_id, ReservationKind.BUILDING, a.approval_id
    )
    ctx.admission = replace(ctx.admission, reservations=(*ctx.admission.reservations, building))
    entry = ctx.admission.queue_entry(ctx.p.parcel_id)
    if entry is not None and entry.approval_id == a.approval_id:
        # The re-opened run holds the slot again (it is released when the run parks).
        _set_queue(ctx, replace(entry, status=QueueStatus.RESERVED, resume=False), entry.parcel_id)
    ctx.update(
        readiness=replace(
            r,
            ready=False,
            verified=False,
            sync_red=False,
            red_checks="",
            report_effect_id="",  # a later Ready posts a new report
            report_key="",
        ),
        pr_number=r.pr_number,
    )
    _move(ctx, Stage.BUILDING)
    ctx.note(f"Back to the build run: review-bot findings arrived on {r.head_sha[:7]} after Ready")
    return True


def _late_findings_wake_due(ctx: _Ctx) -> bool:
    """A run re-opened for late findings waits for its wake (see ``_not_ready``)."""
    r = ctx.p.readiness
    s = ctx.p.session(r.session_id) if r is not None else None
    return (
        r is not None
        and s is not None
        and s.reopened
        and not r.verified
        and ctx.p.stage == Stage.BUILDING
        and s.session_id == ctx.p.current_session_id
        and s.lifecycle == Lifecycle.WAITING
        and s.quiescent
        and ctx.p.findings_wakes < 1
        and not board_pending(ctx.p)
    )


def _review_bot_window(
    ctx: _Ctx,
    r: Readiness,
    since: int | None,
    verdict: str = "",
    eyes: ev.BotEyes | None = None,
) -> Readiness:
    """Wait for the review bot only while it can still respond to this head.

    ``since`` 0: it already answered this head and nothing re-pinged it, so readiness is
    judged on current evidence at once. A trigger time (push, PR open or re-ping) opens a
    wait from that trigger; None (unknown) keeps the wait: an unreadable bot state is
    never taken as answered.

    With the bot's 👀 readable (``eyes``), it decides the wait (#799: Ready was declared
    while Codex was visibly reviewing): 👀 at or after the trigger waits up to the cap,
    sticky for that trigger; no 👀 ends the wait ``review_ack_us`` after the trigger; an
    unreadable state waits up to the cap, from the trigger or (no trigger known) from the
    first such read. Without it (older reads, no bot configured) the grace runs from the
    later of the trigger and the build result.

    ``review_bot`` keeps the Ready report's review-bot line: the verdict as read, or no
    response (Ready is then only reached once the wait has passed).
    """
    cfg = ctx.config
    if since == 0:
        line = verdict[:200] or f"responded on `{r.head_sha[:7]}`"
        window = replace(r, review_bot_done=True, review_bot=line, unknown_since_us=0)
    elif since is None:
        window = replace(r, review_bot_done=False, review_bot="")
        if eyes == ev.BotEyes.UNKNOWN:
            start = r.unknown_since_us or ctx.now
            settle_at = start + cfg.review_cap_us
            window = replace(
                window,
                unknown_since_us=start,
                settle_at_us=settle_at,
                review_bot=_NO_BOT_RESPONSE if ctx.now >= settle_at else _BOT_ASKED,
            )
    else:
        trigger = min(since, ctx.now)
        eyes_trigger = r.eyes_trigger_us
        if eyes == ev.BotEyes.SEEN:
            eyes_trigger = since
        if eyes is None:
            settle_at = max(r.settle_at_us, trigger + cfg.review_grace_us)
        elif eyes_trigger == since or eyes == ev.BotEyes.UNKNOWN:
            settle_at = trigger + cfg.review_cap_us  # from the running config (reload)
        else:
            # No 👀 for this trigger: the ack window from the trigger replaces the grace.
            settle_at = trigger + cfg.review_ack_us
        window = replace(
            r,
            review_bot_done=False,
            settle_at_us=settle_at,
            eyes_trigger_us=eyes_trigger,
            unknown_since_us=0,
            review_bot=_NO_BOT_RESPONSE if ctx.now >= settle_at else _BOT_ASKED,
        )
    if window != r:
        ctx.update(readiness=window)
    return window


def _in_review_grace(r: Readiness) -> bool:
    """The review bot may still comment on ``head_sha``: no green read before
    ``settle_at_us`` makes the parcel Ready."""
    return not r.review_bot_done and r.verified_at_us < r.settle_at_us


def _refreshing(ctx: _Ctx) -> bool:
    """The card is in Ready while a new head or a check re-run is re-evaluated."""
    r = ctx.p.readiness
    return ctx.p.stage == Stage.READY and r is not None and not r.ready


def _head_changed(ctx: _Ctx, r: Readiness, head: str) -> None:
    """Fresh evidence shows a new PR head (agent push or owner "Update branch").

    Not an owner rework instruction: the old head's failure holds are retired and the new
    head is evaluated from a fresh read; a Ready card stays in Ready meanwhile. The
    accepted review stays bound to ``reviewed_head``; the read accepts it for the new head
    only when the new commits merely sync the base branch.
    """
    r = replace(
        r,
        head_sha=head,
        reviewed_head=r.reviewed_head or r.head_sha,
        verified=False,
        ready=False,
        checks_summary="",
        settle_at_us=ctx.now + ctx.config.review_grace_us,
        verified_at_us=0,
        review_bot_done=False,
        sync_red=False,
        red_checks="",
        review_bot="",
        report_effect_id="",  # the posted report is for the old head: never edited now
        report_key="",
        read_started_us=0,  # no read of the new head applied yet
        eyes_trigger_us=0,
        unknown_since_us=0,
    )
    ctx.update(readiness=r)
    ctx.unhold(Hold.CHECKS_FAILED, Hold.READINESS_FAILED)
    _fetch_evidence(ctx, r)


def _not_ready(ctx: _Ctx, r: Readiness, body: ev.ReadinessEvidence) -> None:
    """Current-head evidence is not green: wait, wake the idle build, or Needs you.

    Pending checks are waiting, never a failure. A genuine failure wakes the idle build
    session within its existing fix-batch/recheck allowance, at most once per approval
    (or rework) for each kind: the check wake for red checks and other gaps (no accepted
    review of this head, no closing reference), the findings wake for review-bot findings
    without an outcome. Findings that arrive after the check wake was spent still get
    their wake (#745). With the applicable wake spent the owner decides with the reason
    recorded. Only red required checks with the check wake spent (or not applicable: a
    base-sync head) put the card in Ready, Bot Blocked.
    """
    s = ctx.p.session(r.session_id)
    if body.checks == ev.ChecksState.PENDING:
        return
    if s is not None and s.session_id == ctx.p.current_session_id and not s.fences:
        busy = (s.lifecycle == Lifecycle.ACTIVE and not _ended_without_result(ctx, s)) or (
            s.lifecycle == Lifecycle.WAITING
            and s.wait_reason == WaitReason.CHECKS
            and not s.quiescent
        )
        if busy:
            return  # re-evaluated by the next reconcile read or a new result
    if (
        _red_only(body)
        and (ctx.p.readiness_wakes >= 1 or body.base_sync)
        and s is not None
        and s.session_id == ctx.p.current_session_id
        and s.lifecycle != Lifecycle.RETIRED
        and not _run_unfinished(ctx, s)
        and not ctx.p.holds & _NOT_CHECKS_WITHDRAWN
        and (Hold.REWORK_CONTROL_REQUIRED not in ctx.p.holds or _withdrawn_for_checks(ctx))
        and not board_pending(ctx.p)
    ):
        # The bot has nothing left to do: its one fix wake is spent, or the head only
        # syncs the base onto the reviewed head (nothing of its own to fix).
        if not r.review_bot_done and ctx.now < r.settle_at_us:
            return  # the review bot may still comment on this head
        _park_red(ctx, r, s, body)
        return
    reason = _failure_reason(r, body, ctx.p.issue_number)
    if s is not None and _ended_without_result(ctx, s) and not _wake_answered(ctx, s):
        reason += f"; {_NO_NEW_RESULT}"
    check_wake = _check_gap(body) and ctx.p.readiness_wakes < 1
    findings_wake = body.findings_open and ctx.p.findings_wakes < 1
    waiting = (
        (check_wake or findings_wake)
        and s is not None
        and s.session_id == ctx.p.current_session_id
        and s.lifecycle == Lifecycle.WAITING
        and s.wait_reason == WaitReason.CHECKS
        and s.quiescent
        and not ctx.p.open_decisions
        and approval_ok(ctx.p)
        and dispatchable(ctx.p)
    )
    if waiting and board_pending(ctx.p):
        return  # e.g. back to Building for late findings: woken once the move lands
    if waiting and s is not None and work_allowed(ctx.p, s):
        # One wake names every current gap; it spends the budget of each kind it covers.
        ctx.update(
            readiness_wakes=ctx.p.readiness_wakes + int(check_wake),
            findings_wakes=ctx.p.findings_wakes + int(findings_wake),
        )
        # The run works again: a Needs you from an earlier gap (the findings wake can come
        # after the check wake's Needs you) no longer stands.
        ctx.unhold(Hold.READINESS_FAILED)
        # Not quiescent until a scan sees this new turn end (as for a comment relay).
        s = ctx.put_session(
            replace(s, lifecycle=Lifecycle.ACTIVE, wait_reason=None, quiescent=False)
        )
        _ensure_issuance(ctx, s)
        ctx.emit(
            EffectKind.SEND_MESSAGE,
            session=s,
            args={
                "purpose": MessagePurpose.READINESS_WAKE.value,
                "reason": reason,
                "pr_number": r.pr_number,
                "head_sha": r.head_sha,
                # Findings only: the message asks for an outcome for each finding.
                "wake": "checks" if check_wake else "findings",
            },
        )
        return
    if Hold.READINESS_FAILED not in ctx.p.holds:
        ctx.hold(Hold.READINESS_FAILED)
        _needs_you_comment(ctx, r, body, reason)
        ctx.note(f"Needs you: PR #{r.pr_number} not ready on {r.head_sha[:7]}: {reason}")


def _needs_you_comment(ctx: _Ctx, r: Readiness, body: ev.ReadinessEvidence, reason: str) -> None:
    """The one comment when readiness puts the card at Needs you: the reason and, for
    review-bot findings, each open thread (path, severity, title, link). Posted only on
    entering the hold (callers check), so a restart or an unchanged re-read never posts
    it again; a retry adopts the comment by its effect marker."""
    args: dict[str, JsonValue] = {
        "pr_number": r.pr_number,
        "head_sha": r.head_sha,
        "reason": reason,
    }
    if body.findings_open and body.open_findings:
        args["findings"] = [
            {"path": f.path, "severity": f.severity, "title": f.title, "url": f.url}
            for f in body.open_findings
        ]
        args["further_round"] = body.findings_earlier_rounds
    ctx.comment("ready-blocked", **args)


def _h_base_pushed(ctx: _Ctx, body: ev.BasePushed) -> None:
    """The default branch moved: re-read the parcel's open PR, whose mergeability GitHub
    recomputes against the new base. Only a Building or Ready card with a recorded PR
    and build_ready; a build that has not submitted yet is read when it does."""
    _ = body
    r = ctx.p.readiness
    if (
        r is None
        or _completed(ctx)
        or ctx.p.stage not in (Stage.BUILDING, Stage.READY)
        or Hold.PR_CLOSED in ctx.p.holds
    ):
        return
    _fetch_evidence(ctx, r)


def _conflict_note(base: str) -> str:
    return f"Merge conflict with {base}"


def _merge_conflict(ctx: _Ctx, r: Readiness, body: ev.ReadinessEvidence) -> None:
    """GitHub reports the PR cannot merge into its base (the base moved under it).

    The review that covered the PR so far never carries past this point as a base sync.
    Once per (PR head, base head): a finished build run waiting on checks (or idle) is
    woken to merge the base in; a card whose build run closed (Ready, or Building with
    the build ended) goes back to Building with a conflict rework, admitted like any
    build (queued while no slot is free). A run mid-turn is not interrupted: the next
    read after its turn ends decides. After the wake for this pair, a run that ended
    without resolving it (or a conflict nothing may act on) is Needs you, with one
    comment.
    """
    base = body.base_ref or "main"
    key = f"{r.head_sha}:{body.base_head}"
    reason = f"merge conflict with {base}"
    ctx.update(
        conflict_reviewed_head=r.reviewed_head or r.head_sha,
        readiness=replace(r, verified=False, sync_red=False, red_checks=""),
    )
    r = ctx.p.readiness or r
    s = _open_run(ctx, r)
    if ctx.p.stage == Stage.READY and s is not None:
        # The owner moved the card to Ready while its build run was still open.
        _move(ctx, Stage.BUILDING)
    in_ready = ctx.p.stage == Stage.READY
    if ctx.p.conflict_wake != key:
        if (
            not in_ready
            and s is not None
            and s.kind == SessionKind.BUILD
            and not s.fences
            and (
                (
                    s.lifecycle == Lifecycle.WAITING
                    and s.wait_reason == WaitReason.CHECKS
                    and s.quiescent
                )
                or _idle_run(s)
            )
        ):
            if (
                ctx.p.open_decisions
                or not approval_ok(ctx.p)
                or not dispatchable(ctx.p)
                or not work_allowed(ctx.p, s)
                or board_pending(ctx.p)
            ):
                return  # recorded; a later read wakes it once the gate is open
            ctx.update(conflict_wake=key)
            ctx.unhold(Hold.READINESS_FAILED)
            # Not quiescent until a scan sees this new turn end (as for a comment relay).
            s = ctx.put_session(
                replace(s, lifecycle=Lifecycle.ACTIVE, wait_reason=None, quiescent=False)
            )
            _ensure_issuance(ctx, s)
            ctx.emit(
                EffectKind.SEND_MESSAGE,
                session=s,
                args={
                    "purpose": MessagePurpose.READINESS_WAKE.value,
                    "wake": "conflict",
                    "reason": reason,
                    "pr_number": r.pr_number,
                    "head_sha": r.head_sha,
                    "base_ref": base,
                },
            )
            ctx.note(_conflict_note(base))
            return
        if _conflict_run_live(ctx, s):
            return  # recorded: the run mid-turn merges the base itself or is read after
        if (
            (in_ready or _build_ended(ctx))
            and not ctx.p.holds
            & (_NOT_CHECKS_WITHDRAWN | BLOCKING_HOLDS | {Hold.REWORK_CONTROL_REQUIRED})
            and not ctx.p.open_decisions
            and not _rework_queued(ctx)
            and eligible(ctx.p)
            and _rework_refusal(ctx) is None
        ):
            ctx.update(conflict_wake=key)
            _rework(ctx, None, conflict=base)
            return
    elif _conflict_run_live(ctx, s):
        return  # woken for this pair: still working on it
    if in_ready:
        _invalidate_ready(ctx, reason)
    if Hold.READINESS_FAILED not in ctx.p.holds:
        ctx.hold(Hold.READINESS_FAILED)
        _needs_you_comment(ctx, r, body, reason)
        ctx.note(f"Needs you: PR #{r.pr_number} on {r.head_sha[:7]}: {reason}")


def _conflict_run_live(ctx: _Ctx, s: StageSession | None) -> bool:
    """The build episode is still running (mid-turn, queued, at a checkpoint, ...): it
    is not interrupted for a merge conflict."""
    if s is None:
        return _approval_active(ctx) or _rework_queued(ctx)
    if s.kind != SessionKind.BUILD or s.lifecycle == Lifecycle.RETIRED or s.execution_closed:
        return False
    return not _idle_run(s) and not (
        s.lifecycle == Lifecycle.WAITING and s.wait_reason == WaitReason.CHECKS and s.quiescent
    )


def _check_gap(body: ev.ReadinessEvidence) -> bool:
    """The read shows a gap besides review-bot findings (a red required check, no accepted
    review of this head, no closing reference, or any other failure): the check wake's
    kind. Findings alone are the findings wake's."""
    return (
        not body.findings_open
        or body.checks == ev.ChecksState.FAILED
        or not body.closes_issue
        or not body.review_accepted
    )


def _failure_reason(r: Readiness, body: ev.ReadinessEvidence, issue: int | None) -> str:
    parts = []
    if not body.closes_issue:
        parts.append(
            f"PR #{r.pr_number} does not close issue #{issue} (GitHub closing references); "
            f"its body must include `Closes #{issue}`"
        )
    if body.checks == ev.ChecksState.FAILED:
        parts.append(f"required checks failed ({body.checks_summary[:120]})")
    if body.findings_open:
        parts.append("review-bot findings have no outcome (fixed, follow-up or advisory)")
    if not body.review_accepted:
        parts.append(f"no accepted cross-vendor review of head {r.head_sha[:12]}")
    return "; ".join(parts) or "the PR does not meet the readiness checks"


def _h_contract_published(ctx: _Ctx, body: ev.ContractPublished) -> None:
    c = ctx.p.contract(body.contract_id)
    if c is None or c.published:
        raise Rejected("unknown-or-already-published-contract")
    if not body.verified:
        ctx.hold(Hold.PUBLICATION_FAILED)
        return
    ctx.unhold(Hold.PUBLICATION_FAILED)
    c = replace(c, published=True, comment_id=body.comment_id, posted_at_us=body.posted_at_us)
    if c.revision != ctx.p.revision:
        # Published but already superseded by newer feedback: never clears the revision.
        ctx.put_contract(replace(c, superseded=True))
        return
    ctx.update(
        contracts=tuple(
            c
            if x.contract_id == c.contract_id
            else (replace(x, superseded=True) if x.published else x)
            for x in ctx.p.contracts
        ),
        current_contract_id=c.contract_id,
        revision_pending=False,
    )
    _void_approval(ctx, "superseded-by-new-contract")
    ctx.hold(Hold.AWAITING_OWNER)


def _h_publication_acked(ctx: _Ctx, body: ev.PublicationAcked) -> None:
    """A triage/report/status comment is visible: clear pending/failed publication state."""
    if not body.comment_id:
        raise Rejected("publication-ack-without-comment")
    if ctx.p.unknown_effect(body.effect_id) is not None:
        ctx.update(
            unknown_effects=tuple(u for u in ctx.p.unknown_effects if u.effect_id != body.effect_id)
        )
    if body.effect_kind in _PUBLICATION_KINDS:
        ctx.unhold(Hold.PUBLICATION_FAILED)
    ap = ctx.p.autopilot
    if (
        ap is not None
        and ap.plan is not None
        and ap.plan.effect_id == body.effect_id
        and ap.plan.posted_at_us is None
    ):
        ctx.update(autopilot=replace(ap, plan=replace(ap.plan, posted_at_us=ctx.now)))
        ctx.note("Epic plan posted: approve it with /approve to start autopilot")
    if (
        body.effect_kind == EffectKind.PUBLISH_TRIAGE.value
        and Hold.PUBLICATION_PENDING in ctx.p.holds
    ):
        ctx.unhold(Hold.PUBLICATION_PENDING)
        ctx.hold(Hold.AWAITING_OWNER)


def _h_session_created(ctx: _Ctx, body: ev.SessionCreated) -> None:
    s = _session(ctx, body.session_id)
    _adopt(ctx, s, body.root_id, body.nonce)


def _adopt(ctx: _Ctx, s: StageSession, root_id: str, nonce: str) -> None:
    if s.root_id is not None:
        if s.root_id == root_id:
            raise Rejected("already-adopted")
        raise Rejected("conflicting-root")
    if nonce != s.nonce:
        raise Rejected("nonce-mismatch")
    if any(x.root_id == root_id for x in ctx.p.sessions):
        raise Rejected("root-already-bound")
    if s.lifecycle not in (
        Lifecycle.INTENT,
        Lifecycle.CREATING,
        Lifecycle.UNKNOWN,
        Lifecycle.DRAINING,
    ):
        raise Rejected("session-not-awaiting-create")
    s = replace(s, root_id=root_id)
    _bind_issue_session(ctx, s, root_id)
    if s.fences or s.lifecycle == Lifecycle.DRAINING or s.session_id != ctx.p.current_session_id:
        s = ctx.put_session(
            replace(
                s,
                lifecycle=Lifecycle.DRAINING,
                drain_started_us=s.drain_started_us or ctx.now,
            )
        )
        ctx.emit(EffectKind.INTERRUPT_TREE, session=s, args={"root_id": root_id})
        ctx.emit(EffectKind.SCAN_TREE, session=s, args={"root_id": root_id})
        return
    s = ctx.put_session(replace(s, lifecycle=Lifecycle.PREPARING))
    ctx.emit(
        EffectKind.PREPARE_SESSION,
        session=s,
        args={"root_id": root_id, "profile": profile_for(s.kind).value},
    )


def _bind_issue_session(ctx: _Ctx, s: StageSession, root_id: str) -> None:
    """A newly created root becomes the parcel's issue session (superseding any older)."""
    old = ctx.p.issue_session
    ctx.update(
        issue_session=IssueSession(
            root_id=root_id,
            nonce=s.nonce,
            created_by=s.session_id,
            generation=(old.generation + 1) if old is not None else 1,
            title=ctx.p.issue_title,  # the directory names a new session after this read
        )
    )
    if old is not None and old.root_id != root_id and old.status != IssueSessionStatus.CLOSED:
        _archive(ctx, old.root_id)  # the replaced conversation is kept, but archived


def _archive(ctx: _Ctx, root_id: str) -> None:
    ctx.emit(
        EffectKind.CLOSE_SESSION,
        target=root_id,
        args={"root_id": root_id},
        dedupe=f"close:{root_id}",
    )


def _sync_session_title(ctx: _Ctx, snap: IssueSnapshot) -> None:
    """Rename the live issue session when the issue title changes (a newer read wins)."""
    title = " ".join(snap.title.split())
    if title and title != ctx.p.issue_title and snap.read_at_us >= ctx.p.issue_title_read_at_us:
        ctx.update(issue_title=title, issue_title_read_at_us=snap.read_at_us)
    title = ctx.p.issue_title
    issue = ctx.p.issue_session
    if not title or issue is None or not issue.reusable or title == issue.title:
        return
    ctx.update(issue_session=replace(issue, title=title))
    if issue.title:  # "" = an older record: adopt the title without a write
        ctx.emit(
            EffectKind.RENAME_SESSION,
            target=issue.root_id,
            args={
                "root_id": issue.root_id,
                "title": issue_session_title(ctx.p.issue_number, title),
            },
        )


def _h_create_rejected(ctx: _Ctx, body: ev.CreateRejected) -> None:
    s = _session(ctx, body.session_id)
    if s.root_id is not None or s.lifecycle not in (
        Lifecycle.INTENT,
        Lifecycle.CREATING,
        Lifecycle.DRAINING,
    ):
        raise Rejected("session-not-awaiting-create")
    if not s.fences:
        ctx.hold(Hold.CREATE_REJECTED)
        ctx.comment("create-rejected", reason=body.reason)
        ctx.note("Blocked: Omnigent refused to create the session")
    _finish_drain(ctx, replace(s, drain_target=Lifecycle.FENCED if s.fences else Lifecycle.RETIRED))


def _h_adoption_result(ctx: _Ctx, body: ev.AdoptionResult) -> None:
    s = _session(ctx, body.session_id)
    if s.root_id is not None:
        raise Rejected("already-adopted")
    if body.matches == 1 and body.root_id is not None:
        _adopt(ctx, s, body.root_id, body.nonce)
        return
    if body.matches > 1:
        ctx.comment("adoption-ambiguous", session_id=s.session_id, matches=body.matches)
        ctx.note("Blocked: ambiguous Omnigent session adoption")
    # Zero matches is not proof of failure; remain UNKNOWN (no work, no blind retry).


def _h_effect_unknown(ctx: _Ctx, body: ev.EffectUnknown) -> None:
    s = ctx.p.session(body.session_id)
    if body.effect_kind == EffectKind.CREATE_SESSION.value and s is not None:
        if s.root_id is not None:
            raise Rejected("create-already-adopted")
        if s.lifecycle in (Lifecycle.INTENT, Lifecycle.CREATING):
            s = ctx.put_session(replace(s, lifecycle=Lifecycle.UNKNOWN))
        ctx.emit(EffectKind.RECONCILE_SESSION, session=s, args={"nonce": s.nonce})
        return
    if ctx.p.unknown_effect(body.effect_id) is None:
        ctx.update(
            unknown_effects=(
                *ctx.p.unknown_effects,
                UnknownEffect(body.effect_id, body.effect_kind, body.session_id),
            )
        )
    if s is not None and body.effect_kind in (
        EffectKind.SEND_MESSAGE.value,
        EffectKind.RESOLVE_ELICITATION.value,
    ):
        s = ctx.put_session(replace(s, message_unknown=True))
        ctx.emit(EffectKind.RECONCILE_SESSION, session=s, args={"effect_id": body.effect_id})
        return
    ctx.emit(EffectKind.RECONCILE_PARCEL, args={"effect_id": body.effect_id})


_PUBLICATION_KINDS = frozenset(
    {
        EffectKind.PUBLISH_TRIAGE.value,
        EffectKind.PUBLISH_CONTRACT.value,
        EffectKind.PUBLISH_REPORT.value,
    }
)


def _h_effect_cancelled(ctx: _Ctx, body: ev.EffectCancelled) -> None:
    if body.effect_kind == EffectKind.MOVE_CARD.value:
        move = ctx.p.pending_move(body.effect_id)
        if move is None:
            raise Rejected("unknown-board-write")
        _retire_move(ctx, move, landed=False)  # never happened: reconcile the source
        return
    if body.effect_kind in _PUBLICATION_KINDS:
        # Publications are never precondition-cancelled (not work-bearing): this is a
        # definitive failure. Blocked outranks Needs you; a pending triage publication
        # stays pending so a later successful retry still hands the card to the owner.
        ctx.hold(Hold.PUBLICATION_FAILED)
        return
    s = ctx.p.session(body.session_id)
    if body.failed and body.effect_kind == EffectKind.ENABLE_ISSUANCE.value and s is not None:
        # The stage cannot get its credential: fail visibly instead of running without it.
        ctx.hold(Hold.PREPARE_FAILED)
        if s.lifecycle not in (Lifecycle.RETIRED, Lifecycle.FENCED, Lifecycle.DRAINING):
            _begin_drain(ctx, s)
        return
    if body.effect_kind != EffectKind.CREATE_SESSION.value or s is None:
        return  # audit only
    if s.root_id is not None or s.lifecycle not in (
        Lifecycle.INTENT,
        Lifecycle.CREATING,
        Lifecycle.DRAINING,
    ):
        raise Rejected("create-not-cancellable")
    # Cancelled before any external call: nothing exists remotely.
    _finish_drain(ctx, replace(s, drain_target=Lifecycle.FENCED if s.fences else Lifecycle.RETIRED))


def _h_prepared(ctx: _Ctx, body: ev.Prepared) -> None:
    s = _session(ctx, body.session_id)
    if s.lifecycle != Lifecycle.PREPARING:
        raise Rejected("session-not-preparing")
    if body.unusable:
        issue = ctx.p.issue_session
        if s.root_id is None or issue is None or issue.root_id != s.root_id:
            raise Rejected("unusable-root-not-issue-session")
        if issue.created_by == s.session_id:
            # This run created the root: it is not a reused conversation to replace.
            ctx.hold(Hold.PREPARE_FAILED)
            _begin_drain(ctx, s)
            return
        _retire_issue_session(ctx, s.root_id, body.reason or "unusable")
        s = ctx.put_session(
            replace(
                s,
                root_id=None,
                lifecycle=Lifecycle.INTENT,
                prepared=False,
                policy_ready=False,
                policy_ready_at_us=0,
            )
        )
        _emit_create(ctx, s)
        return
    if body.unexpected_turn:
        ctx.hold(Hold.EXTERNAL_ACTIVITY)
        _begin_drain(ctx, s, fences=frozenset({FenceKind.SAFETY}))
        return
    if not body.ok:
        ctx.hold(Hold.PREPARE_FAILED)
        if body.note:
            ctx.note(f"Blocked: {body.note}")
        _begin_drain(ctx, s)
        return
    # Prepared is not open: the policy set must propagate (barrier) and be re-verified
    # exactly before any credential or work message (see _h_policies_verified).
    s = ctx.put_session(
        replace(
            s,
            prepared=True,
            policy_ready=False,
            policy_ready_at_us=body.policy_ready_at_us or ctx.now,
        )
    )
    _emit_verify(ctx, s, reconcile=False)


def _emit_verify(ctx: _Ctx, s: StageSession, *, reconcile: bool) -> None:
    ctx.emit(
        EffectKind.VERIFY_POLICIES,
        session=s,
        args={
            "root_id": s.root_id or "",
            "not_before_us": 0 if reconcile else s.policy_ready_at_us,
            "reconcile": reconcile,
        },
        dedupe=f"verify:{s.session_id}:{ctx.event.event_id}",
    )


def _h_policies_verified(ctx: _Ctx, body: ev.PoliciesVerified) -> None:
    s = _session(ctx, body.session_id)
    if _continue_pending(ctx, s) and not body.reconciled:
        if not body.ok:
            # Not yet this grant's generation: its own PolicyReady completes the resume.
            raise Rejected("continue-policy-not-yet-applied")
        _resume_checkpoint(ctx, s)
        return
    if s.policy_ready or s.root_id is None or s.fences or s.execution_closed:
        raise Rejected("no-pending-policy-verification")
    if body.reconciled:
        s = ctx.put_session(replace(s, policy_ready_at_us=body.ready_at_us or ctx.now))
        _emit_verify(ctx, s, reconcile=False)
        return
    if not body.ok:
        # Fail closed and visible: the run never opens with an unverified guard.
        ctx.hold(Hold.PREPARE_FAILED)
        if s.lifecycle not in (Lifecycle.RETIRED, Lifecycle.FENCED, Lifecycle.DRAINING):
            _begin_drain(ctx, s)
        return
    s = ctx.put_session(replace(s, policy_ready=True, policy_ready_at_us=0))
    if s.lifecycle != Lifecycle.PREPARING:
        _ensure_issuance(ctx, s)  # a boot hold lifted: re-enable once


def _h_policy_guard_failed(ctx: _Ctx, body: ev.PolicyGuardFailed) -> None:
    s = _current(ctx, body.session_id)
    if s.root_id is None or s.fences or s.execution_closed or not s.prepared:
        raise Rejected("no-live-prepared-run")
    s = ctx.put_session(replace(s, policy_ready=False, policy_ready_at_us=0))
    if s.issuance_enabled:
        ctx.emit(EffectKind.DISABLE_ISSUANCE, session=s)
    _emit_verify(ctx, s, reconcile=True)


def _h_message_ack(ctx: _Ctx, body: ev.MessageAck) -> None:
    """Our own item for exactly ``effect_id`` (a direct ack or marker adoption).

    Admitted only from the executor (admission table); must name a work-bearing effect
    this reducer issued to exactly that session, and carry the real item ID.
    """
    if not body.item_id:
        raise Rejected("ack-without-item")
    if (body.effect_id, body.session_id) not in ctx.p.sent_effects:
        raise Rejected("ack-for-unissued-effect")
    _resolve_unknown(ctx, body.effect_id, body.session_id, body.item_id)


def _h_effect_reconciled(ctx: _Ctx, body: ev.EffectReconciled) -> None:
    unknown = ctx.p.unknown_effect(body.effect_id)
    if unknown is None:
        raise Rejected("effect-not-unknown")
    if body.delivered and not body.item_id:
        raise Rejected("delivered-without-item")
    if unknown.kind == EffectKind.MOVE_CARD.value:
        move = ctx.p.pending_move(body.effect_id)
        _resolve_unknown(ctx, body.effect_id, body.session_id, "")
        if move is not None:
            _retire_move(ctx, move, landed=body.delivered)
        return
    _resolve_unknown(ctx, body.effect_id, body.session_id, body.item_id if body.delivered else "")


def _resolve_unknown(ctx: _Ctx, effect_id: str, session_id: str | None, item_id: str) -> None:
    """Clear an ambiguity only on a correlation-checked answer for that exact effect."""
    unknown = ctx.p.unknown_effect(effect_id)
    if unknown is not None and (unknown.session_id or None) != (session_id or None):
        raise Rejected("ack-correlation-mismatch")
    if unknown is not None:
        ctx.update(
            unknown_effects=tuple(u for u in ctx.p.unknown_effects if u.effect_id != effect_id)
        )
    s = ctx.p.session(session_id)
    if s is None:
        return
    items = s.own_items
    if item_id and item_id not in items:
        items = (*items, item_id)
    still = any(
        u.session_id == s.session_id
        and u.kind in (EffectKind.SEND_MESSAGE.value, EffectKind.RESOLVE_ELICITATION.value)
        for u in ctx.p.unknown_effects
    )
    ctx.put_session(replace(s, own_items=items, message_unknown=still))


def _root_owner(ctx: _Ctx, s: StageSession) -> StageSession:
    """The run that owns activity observed on ``s``'s root: successive runs share the
    issue session, so it is the current run when that one executes in the same root."""
    cur = ctx.p.current_session
    if cur is not None and s.root_id is not None and cur.root_id == s.root_id:
        return cur
    return s


def _h_runtime_activity(ctx: _Ctx, body: ev.RuntimeActivity) -> None:
    s = _session(ctx, body.session_id)
    if not body.busy:
        return  # idle is not quiescence evidence
    # A late observation labelled with an earlier run of the same root (e.g. read
    # before the build run was admitted) is the current run's own work, not external.
    s = _root_owner(ctx, s)
    if settled(s) or s.lifecycle in (Lifecycle.FENCED, Lifecycle.RETIRED):
        s = ctx.put_session(replace(s, external_active=True, quiescent=False))
        ctx.hold(Hold.EXTERNAL_ACTIVITY)
        if s.root_id is not None:
            if s.fences:
                ctx.emit(EffectKind.INTERRUPT_TREE, session=s, args={"root_id": s.root_id})
            ctx.emit(EffectKind.SCAN_TREE, session=s, args={"root_id": s.root_id})
        return
    s = ctx.put_session(replace(s, quiescent=False))
    if s.lifecycle in (Lifecycle.DRAINING, Lifecycle.BLOCKED) and s.root_id is not None:
        ctx.emit(EffectKind.INTERRUPT_TREE, session=s, args={"root_id": s.root_id})
        ctx.emit(EffectKind.SCAN_TREE, session=s, args={"root_id": s.root_id})


def _h_owner_direct(ctx: _Ctx, body: ev.OwnerDirectOmnigentMessage) -> None:
    s = _session(ctx, body.session_id)
    if s.fences or s.lifecycle in (Lifecycle.FENCED, Lifecycle.RETIRED):
        ctx.put_session(replace(s, external_active=True, quiescent=False))
        ctx.hold(Hold.EXTERNAL_ACTIVITY)


def _h_elicitation_opened(ctx: _Ctx, body: ev.ElicitationOpened) -> None:
    s = _session(ctx, body.session_id)
    if any(
        d.session_id == s.session_id and d.elicitation_id == body.elicitation_id
        for d in ctx.p.decisions
    ):
        raise Rejected("duplicate-elicitation")
    decision = Decision(
        decision_id=ctx.new_id("de"),
        session_id=s.session_id,
        elicitation_id=body.elicitation_id,
        revision=ctx.p.revision,
        impact=body.impact,
        status=DecisionStatus.OPEN,
        checkpoint_prompt=body.cost_ask,
    )
    live = s.session_id == ctx.p.current_session_id and not s.fences and not settled(s)
    if not live:
        ctx.update(decisions=(*ctx.p.decisions, replace(decision, status=DecisionStatus.ORPHANED)))
        return
    ctx.update(decisions=(*ctx.p.decisions, decision))
    if body.cost_ask:
        if s.lifecycle in (Lifecycle.ACTIVE, Lifecycle.WAITING):
            _enter_checkpoint(ctx, s)
        return
    if s.lifecycle == Lifecycle.ACTIVE:
        s = ctx.put_session(
            replace(s, lifecycle=Lifecycle.WAITING, wait_reason=WaitReason.DECISION)
        )
    ctx.comment(
        "decision",
        decision_id=decision.decision_id,
        impact=body.impact.value,
        contract_hash=ctx.p.current_contract.full_hash if ctx.p.current_contract else None,
        summary=body.summary[:300],
        node_id=body.node_id,
        root_id=s.root_id,
    )


def mcp_question_id(question_key: str) -> str:
    """The decision's ``elicitation_id`` slot for a ``factory_ask_owner`` question."""
    return f"mcp:{question_key}"


def _h_owner_question(ctx: _Ctx, body: ev.OwnerQuestion) -> None:
    """One structured owner question from the current run (``factory_ask_owner``)."""
    s = _current(ctx, body.session_id)
    if not body.question_key:
        raise Rejected("question-without-key")
    if s.fences or s.execution_closed or s.lifecycle not in (Lifecycle.ACTIVE, Lifecycle.WAITING):
        raise Rejected("question-through-closed-gate")
    slot = mcp_question_id(body.question_key)
    if any(d.session_id == s.session_id and d.elicitation_id == slot for d in ctx.p.decisions):
        raise Rejected("duplicate-question")
    decision = Decision(
        decision_id=ctx.new_id("de"),
        session_id=s.session_id,
        elicitation_id=slot,
        revision=ctx.p.revision,
        impact=body.impact,
        status=DecisionStatus.OPEN,
        source=DecisionSource.MCP,
    )
    ctx.update(decisions=(*ctx.p.decisions, decision))
    if s.lifecycle == Lifecycle.ACTIVE:
        ctx.put_session(replace(s, lifecycle=Lifecycle.WAITING, wait_reason=WaitReason.DECISION))
    ctx.comment(
        "decision",
        decision_id=decision.decision_id,
        impact=body.impact.value,
        contract_hash=ctx.p.current_contract.full_hash if ctx.p.current_contract else None,
        summary=body.summary[:4000],
        node_id=None,
        root_id=s.root_id,
    )


def _maybe_close_issue_session(ctx: _Ctx) -> None:
    """Archive the issue session once the parcel is terminal and its tree quiescent."""
    issue = ctx.p.issue_session
    if issue is None or issue.status != IssueSessionStatus.LIVE or not _completed(ctx):
        return
    if not all(settled(s) for s in ctx.p.sessions) or uncertain(ctx.p):
        return
    ctx.update(issue_session=replace(issue, status=IssueSessionStatus.CLOSING))
    _archive(ctx, issue.root_id)


def _h_issue_session_closed(ctx: _Ctx, body: ev.IssueSessionClosed) -> None:
    issue = ctx.p.issue_session
    if issue is not None and issue.root_id != body.root_id:
        return  # a replaced issue session was archived
    if issue is None:
        raise Rejected("not-the-issue-session")
    if issue.status == IssueSessionStatus.CLOSED:
        raise Rejected("already-closed")
    ctx.update(issue_session=replace(issue, status=IssueSessionStatus.CLOSED))


def _find_decision(ctx: _Ctx, session_id: str, elicitation_id: str) -> Decision:
    for d in ctx.p.decisions:
        if d.session_id == session_id and d.elicitation_id == elicitation_id:
            return d
    raise Rejected("unknown-elicitation")


def _h_elicitation_resolved(ctx: _Ctx, body: ev.ElicitationResolved) -> None:
    d = _find_decision(ctx, body.session_id, body.elicitation_id)
    if body.correlated:
        if d.status != DecisionStatus.RELAYED:
            raise Rejected("resolution-not-ours")
        return
    # Direct UI resolution: observation only; no approval or grant inferred.
    d = replace(d, externally_resolved=True)
    if d.status == DecisionStatus.OPEN:
        d = replace(d, status=DecisionStatus.RESOLVED_IN_OMNIGENT)
    ctx.put_decision(d)
    _resume_after_prompt(ctx, d)


def _h_elicitation_gone(ctx: _Ctx, body: ev.ElicitationGone) -> None:
    d = _find_decision(ctx, body.session_id, body.elicitation_id)
    s = ctx.p.session(d.session_id)
    if d.status == DecisionStatus.ANSWERED:
        # Answered here first: the relay still carries the owner's answer.
        ctx.put_decision(replace(d, prompt_lost=True))
        if s is not None:
            _relay_answers(ctx, s)
        return
    d = replace(d, prompt_lost=True)
    if d.status == DecisionStatus.OPEN:
        # Answered/cancelled in Omnigent: nothing waits on the owner any more.
        d = replace(d, status=DecisionStatus.RESOLVED_IN_OMNIGENT)
    ctx.put_decision(d)
    _resume_after_prompt(ctx, d)


def _resume_after_prompt(ctx: _Ctx, d: Decision) -> None:
    """A prompt answered or cancelled outside the factory no longer waits on the owner.

    The decision is closed (``resolved_in_omnigent``); a session parked in
    WAITING for decisions resumes ACTIVE once none of its prompts are still pending, so
    the board returns to Working. No approval, grant or answer is inferred.
    """
    s = ctx.p.session(d.session_id)
    if (
        s is None
        or s.lifecycle != Lifecycle.WAITING
        or s.wait_reason != WaitReason.DECISION
        or any(o.session_id == s.session_id for o in ctx.p.open_decisions)
    ):
        return
    s = ctx.put_session(replace(s, lifecycle=Lifecycle.ACTIVE, wait_reason=None))
    _ensure_issuance(ctx, s)


def _h_result(ctx: _Ctx, body: ev.ResultCandidate) -> None:
    s = _current(ctx, body.session_id)
    if s.root_id is None or body.root_id != s.root_id:
        raise Rejected("result-from-stale-root")
    if s.fences or s.lifecycle not in (
        Lifecycle.ACTIVE,
        Lifecycle.WAITING,
        Lifecycle.CHECKPOINT_GRACE,
    ):
        raise Rejected("result-through-closed-gate")
    if body.revision != s.revision:
        raise Rejected("result-for-stale-revision")
    kind_ok = {
        ev.ResultKind.TRIAGE: s.kind == SessionKind.TRIAGE,
        ev.ResultKind.PLAN: s.kind in (SessionKind.PLAN, SessionKind.BUILD),
        ev.ResultKind.BUILD_READY: s.kind == SessionKind.BUILD,
        ev.ResultKind.CHECKPOINT: s.lifecycle == Lifecycle.CHECKPOINT_GRACE,
        ev.ResultKind.BLOCKED: True,
    }[body.result_kind]
    decisions_ok = True
    if body.result_kind == ev.ResultKind.PLAN and s.kind == SessionKind.PLAN:
        open_ids = {d.decision_id for d in ctx.p.open_decisions}
        decisions_ok = (
            set(body.open_decision_ids) == open_ids
            and body.publication_kind == ev.PublicationKind.CONTRACT
            and body.contract_canonical is not None
            and body.size is not None
        )
    if s.kind == SessionKind.BUILD and body.result_kind == ev.ResultKind.PLAN:
        a = ctx.p.current_approval
        decisions_ok = (
            body.publication_kind == ev.PublicationKind.INFO
            and a is not None
            and a.kind == ApprovalKind.SKIP
        )
    if body.result_kind == ev.ResultKind.BUILD_READY:
        decisions_ok = body.pr_number is not None and body.head_sha is not None
    if not (body.valid and kind_ok and decisions_ok):
        _malformed(ctx, s)
        return
    if s.comment_pending:
        s = ctx.put_session(replace(s, comment_pending=False))
    if body.result_kind == ev.ResultKind.BLOCKED:
        # Honest "could not finish": Blocked with the agent's reason; nothing inferred.
        ctx.hold(Hold.AGENT_BLOCKED)
        ctx.emit(EffectKind.PUBLISH_REPORT, args={"report": "blocked", "session_id": s.session_id})
        return
    if body.result_kind == ev.ResultKind.TRIAGE:
        if body.size is not None:
            ctx.update(size=body.size)
        publish = ctx.emit(EffectKind.PUBLISH_TRIAGE, session=s, args={"session_id": s.session_id})
        _record_epic_plan(ctx, body, s, publish.effect_id)
        # Needs you only once the owner can see the outcome (PublicationAcked).
        ctx.hold(Hold.PUBLICATION_PENDING)
        _begin_drain(ctx, s, interrupt=False)
    elif body.result_kind == ev.ResultKind.PLAN and s.kind == SessionKind.PLAN:
        assert body.contract_canonical is not None and body.size is not None  # noqa: S101
        full_hash = sha256_hex(body.contract_canonical.encode("utf-8"))
        if any(c.revision == body.revision and c.full_hash == full_hash for c in ctx.p.contracts):
            raise Rejected("duplicate-contract-candidate")
        contract = Contract(
            contract_id=ctx.new_id("ct"),
            revision=body.revision,
            canonical=body.contract_canonical,
            full_hash=full_hash,
            source_session_id=s.session_id,
            size=body.size,
        )
        ctx.update(contracts=(*ctx.p.contracts, contract), size=body.size)
        _judge_epic_fit(ctx, body, full_hash)
        ctx.put_session(
            replace(s, lifecycle=Lifecycle.WAITING, wait_reason=WaitReason.PLAN_APPROVAL)
        )
        ctx.emit(
            EffectKind.PUBLISH_CONTRACT,
            args={"contract_id": contract.contract_id, "full_hash": contract.full_hash},
            dedupe=f"publish:{contract.contract_id}",
        )
    elif body.result_kind == ev.ResultKind.PLAN:
        ctx.emit(
            EffectKind.PUBLISH_REPORT, args={"report": "info-plan", "session_id": s.session_id}
        )
    elif body.result_kind == ev.ResultKind.BUILD_READY:
        assert body.pr_number is not None and body.head_sha is not None  # noqa: S101
        ctx.update(
            readiness=Readiness(
                s.session_id,
                body.pr_number,
                body.head_sha,
                reviewed_head=body.head_sha,
                settle_at_us=_settle_at(ctx, body.head_sha),
            )
        )
        s = ctx.put_session(
            replace(s, lifecycle=Lifecycle.WAITING, wait_reason=WaitReason.CHECKS, quiescent=False)
        )
        ctx.emit(
            EffectKind.FETCH_PR_EVIDENCE,
            args={
                "pr_number": body.pr_number,
                "head_sha": body.head_sha,
                "session_id": s.session_id,
                "issue_number": ctx.p.issue_number,
            },
        )
        if s.root_id is not None:
            ctx.emit(EffectKind.SCAN_TREE, session=s, args={"root_id": s.root_id})
    else:
        ctx.put_session(replace(s, lifecycle=Lifecycle.CHECKPOINT_WAIT, report_published=True))
        ctx.emit(
            EffectKind.PUBLISH_REPORT, args={"report": "checkpoint", "session_id": s.session_id}
        )


def _malformed(ctx: _Ctx, s: StageSession) -> None:
    """A result that does not fit the run. ``factory_submit_result`` validates in-turn and
    returns precise errors, so this only records a hard mismatch; there is no correction
    round-trip."""
    ctx.hold(Hold.RESULT_INVALID)
    ctx.note("Blocked: stage result invalid")


def _h_tree_quiescent(ctx: _Ctx, body: ev.TreeQuiescent) -> None:
    s = _session(ctx, body.session_id)
    if any(w.session_id == s.session_id for w in ctx.p.held_wakes):
        # Its message is held until it has a build slot: an idle tree is no turn ending.
        return
    if not body.complete or body.busy:
        s = ctx.put_session(replace(s, quiescent=False))
        if s.lifecycle in (Lifecycle.DRAINING, Lifecycle.BLOCKED) and s.root_id is not None:
            ctx.emit(EffectKind.INTERRUPT_TREE, session=s, args={"root_id": s.root_id})
            ctx.emit(EffectKind.SCAN_TREE, session=s, args={"root_id": s.root_id})
        return
    if s.root_id is None and s.lifecycle not in (Lifecycle.FENCED, Lifecycle.RETIRED):
        raise Rejected("no-root-to-scan")
    s = ctx.put_session(replace(s, quiescent=True, external_active=False))
    # A complete idle read of the root is quiescence for every run in it: an earlier
    # run's activity flag would otherwise never clear (only the current run is observed).
    for other in ctx.p.sessions:
        if other.external_active and other.root_id is not None and other.root_id == s.root_id:
            ctx.put_session(replace(other, external_active=False))
    if not any(x.external_active for x in ctx.p.sessions):
        ctx.unhold(Hold.EXTERNAL_ACTIVITY)
    if s.lifecycle == Lifecycle.DRAINING or (
        s.lifecycle == Lifecycle.BLOCKED and s.drain_target is not None
    ):
        _finish_drain(ctx, s)
    elif s.lifecycle == Lifecycle.CHECKPOINT_WAIT and not body.pending_waiter:
        ctx.put_session(
            replace(s, lifecycle=Lifecycle.FENCED, fences=s.fences | {FenceKind.CHECKPOINT})
        )
        ctx.emit(EffectKind.DISABLE_ISSUANCE, session=s)
    elif (
        s.comment_pending
        and _idle_run(s)
        and s.session_id == ctx.p.current_session_id
        and not ctx.p.open_decisions
        and work_allowed(ctx.p, s)
        and (s.kind != SessionKind.BUILD or approval_ok(ctx.p))
    ):
        _relay_comment(ctx, s)  # a comment came mid-turn and the turn ended without a result


def _h_stop_timeout(ctx: _Ctx, body: ev.StopTimeout) -> None:
    """The drain timed out without quiescence evidence.

    A hard-fenced drain (stop, safety, revocation) must not release work it could not
    verify stopped: it blocks and keeps scanning. Any other drain (checkpoint, retire,
    hand-over) finishes as if quiescent, exactly as a quiescent scan would finish it, so
    a stale busy node never parks the parcel forever (#627).
    """
    s = _session(ctx, body.session_id)
    if s.lifecycle != Lifecycle.DRAINING:
        raise Rejected("session-not-draining")
    if not s.fences & _HARD_FENCES:
        _finish_drain(ctx, s)
        return
    s = ctx.put_session(replace(s, lifecycle=Lifecycle.BLOCKED))
    ctx.hold(Hold.STOP_UNVERIFIED)
    ctx.comment("stop-unverified", session_id=s.session_id)
    ctx.note("Blocked: stop not verified")
    if s.root_id is not None:
        ctx.emit(EffectKind.SCAN_TREE, session=s, args={"root_id": s.root_id})


def _h_session_crashed(ctx: _Ctx, body: ev.SessionCrashed) -> None:
    s = _current(ctx, body.session_id)
    if s.lifecycle not in (Lifecycle.PREPARING, Lifecycle.ACTIVE, Lifecycle.WAITING):
        raise Rejected("crash-outside-executable-lifecycle")
    _retire_issue_session(ctx, s.root_id, "crashed")
    if (
        s.restart_count == 0
        and not s.fences
        and authority_ok(ctx.p, s)
        and s.grant.remaining_us > 0
    ):
        s = ctx.put_session(replace(s, restart_pending=True))
        _begin_drain(ctx, s)
        return
    ctx.hold(Hold.RESTART_EXHAUSTED)
    ctx.comment("restart-exhausted", session_id=s.session_id)
    ctx.note("Blocked: session stopped and could not be restarted")
    _begin_drain(ctx, s)


def _h_active_time(ctx: _Ctx, body: ev.ActiveTimeSample) -> None:
    s = _session(ctx, body.session_id)
    if s.grant.grant_id != body.grant_id:
        raise Rejected("stale-grant")
    if body.consumed_us > s.grant.consumed_us:
        ctx.put_session(replace(s, grant=replace(s.grant, consumed_us=body.consumed_us)))


def _h_cost_sample(ctx: _Ctx, body: ev.CostSample) -> None:
    _session(ctx, body.session_id)  # audit only; unknown cost is not zero


def _h_policy_ready(ctx: _Ctx, body: ev.PolicyReady) -> None:
    s = _current(ctx, body.session_id)
    if s.grant.grant_id != body.grant_id or s.grant.ready:
        raise Rejected("stale-or-ready-grant")
    if not only_checkpoint_fenced(s) or s.execution_closed:
        raise Rejected("hard-fence-present", explain=True)
    if _checkpoint_draining(s):
        raise Rejected("resume-deferred-until-tree-stops")  # see _finish_drain
    _resume_checkpoint(ctx, s)


def _checkpoint_draining(s: StageSession) -> bool:
    """A checkpoint fence's drain is still stopping the tree (it ends FENCED)."""
    return s.lifecycle in (Lifecycle.DRAINING, Lifecycle.BLOCKED) and (
        s.drain_target == Lifecycle.FENCED
    )


def _continue_pending(ctx: _Ctx, s: StageSession) -> bool:
    """A /continue grant awaits its resume on a settled checkpoint fence."""
    return (
        not s.grant.ready
        and s.lifecycle == Lifecycle.FENCED
        and s.fences == frozenset({FenceKind.CHECKPOINT})
        and not s.execution_closed
        and s.session_id == ctx.p.current_session_id
    )


def _resume_checkpoint(ctx: _Ctx, s: StageSession) -> None:
    if s.lifecycle not in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT, Lifecycle.FENCED):
        raise Rejected("not-at-checkpoint")
    if not dispatchable(ctx.p) or not authority_ok(ctx.p, s) or ctx.p.open_decisions:
        raise Rejected("continuation-preflight-failed", explain=True)
    if s.lifecycle == Lifecycle.FENCED and not all_settled(ctx.p):
        raise Rejected("tree-not-quiescent")
    s = ctx.put_session(
        replace(
            s,
            grant=replace(s.grant, ready=True),
            fences=s.fences - {FenceKind.CHECKPOINT},
            lifecycle=Lifecycle.ACTIVE,
            wait_reason=None,
        )
    )
    ctx.emit(EffectKind.ENABLE_ISSUANCE, session=s, args={"profile": profile_for(s.kind).value})
    prompts = [
        d
        for d in ctx.p.decisions
        if d.session_id == s.session_id
        and d.checkpoint_prompt
        and d.status == DecisionStatus.OPEN
        and not d.prompt_lost
    ]
    if prompts:
        d = prompts[-1]
        ctx.emit(
            EffectKind.RESOLVE_ELICITATION,
            session=s,
            args={
                "elicitation_id": d.elicitation_id,
                "decision_id": d.decision_id,
                "grant_id": s.grant.grant_id,
            },
            target=s.root_id or s.session_id,
        )
        ctx.put_decision(replace(d, status=DecisionStatus.RELAYED, answer_event_id=None))
    else:
        ctx.emit(
            EffectKind.SEND_MESSAGE,
            session=s,
            args={"purpose": MessagePurpose.CONTINUATION.value, "grant_id": s.grant.grant_id},
        )
    for d in ctx.p.decisions:
        if d.session_id == s.session_id and d.checkpoint_prompt and d.status == DecisionStatus.OPEN:
            ctx.put_decision(replace(d, status=DecisionStatus.CANCELLED))


def _h_active_limit(ctx: _Ctx, body: ev.ActiveLimitReached) -> None:
    s = _current(ctx, body.session_id)
    if s.grant.grant_id != body.grant_id:
        raise Rejected("stale-grant")
    if s.lifecycle not in (Lifecycle.ACTIVE, Lifecycle.WAITING) or s.fences:
        raise Rejected("not-executing")
    _enter_checkpoint(ctx, s)


def _h_grace_expired(ctx: _Ctx, body: ev.GraceExpired) -> None:
    s = _current(ctx, body.session_id)
    if s.grant.grant_id != body.grant_id:
        raise Rejected("stale-grant")
    if s.lifecycle not in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT):
        raise Rejected("not-in-grace")
    if s.lifecycle == Lifecycle.CHECKPOINT_WAIT and s.quiescent:
        ctx.put_session(
            replace(s, lifecycle=Lifecycle.FENCED, fences=s.fences | {FenceKind.CHECKPOINT})
        )
        ctx.emit(EffectKind.DISABLE_ISSUANCE, session=s)
        return
    _begin_drain(ctx, s, fences=frozenset({FenceKind.CHECKPOINT}))


def _h_capacity(ctx: _Ctx, body: ev.CapacityAvailable) -> None:
    entry = ctx.admission.queue_entry(ctx.p.parcel_id)
    if entry is None or entry.status != QueueStatus.QUEUED:
        raise Rejected("not-queued")
    a = ctx.p.current_approval
    auth = next(
        (
            x
            for x in reversed(ctx.p.authorizations)
            if x.approval_id == entry.approval_id and not x.cancelled
        ),
        None,
    )
    if a is None or a.approval_id != entry.approval_id or auth is None or not approval_ok(ctx.p):
        _set_queue(ctx, replace(entry, status=QueueStatus.CANCELLED), ctx.p.parcel_id)
        ctx.note("Queued build dropped: its approval is no longer valid")
        return
    if entry.resume:
        _admit_resume(ctx, entry)
        return
    if ctx.admission.paused:
        raise Rejected("paused")
    if not building_capacity_available(ctx.admission, ctx.config):
        raise Rejected("building-cap")
    if entry.auto and not auto_build_capacity_available(ctx.admission, ctx.config):
        raise Rejected("auto-build-cap")
    if admissible_head(ctx.admission, ctx.config) != entry:
        raise Rejected("not-queue-head")
    has_pr = ctx.p.pr_number is not None and ctx.p.pr_number in ctx.admission.open_bot_prs
    has_pr_reservation = any(
        r.parcel_id == ctx.p.parcel_id and r.kind == ReservationKind.OPEN_PR and r.live
        for r in ctx.admission.reservations
    )
    need_pr = not has_pr and not has_pr_reservation
    if need_pr and not pr_capacity_available(ctx.admission, ctx.config):
        raise Rejected("open-pr-cap")
    if not dispatchable(ctx.p) or not all_settled(ctx.p) or ctx.p.pending_authorization_id:
        raise Rejected("admission-preconditions")
    if auth.eligibility_epoch != ctx.p.eligibility_epoch:
        raise Rejected("authority-before-barrier")
    if not ctx.event.entropy:
        raise Rejected("missing-entropy")
    _ = body
    _retire_settled_current(ctx)
    s = _create_session(ctx, auth)
    if s is None:  # pragma: no cover - entropy checked above
        raise Rejected("missing-entropy")
    new = [Reservation(ctx.new_id("rs"), ctx.p.parcel_id, ReservationKind.BUILDING, a.approval_id)]
    if need_pr:
        new.append(
            Reservation(ctx.new_id("rs"), ctx.p.parcel_id, ReservationKind.OPEN_PR, a.approval_id)
        )
    ctx.admission = replace(ctx.admission, reservations=(*ctx.admission.reservations, *new))
    _set_queue(ctx, replace(entry, status=QueueStatus.RESERVED), ctx.p.parcel_id)
    if auth.rework:
        ctx.note(_REWORK_NOTE)  # Queued -> Working keeps the reason on the card


def _admit_resume(ctx: _Ctx, entry: QueueEntry) -> None:
    """A parked build re-acquires its slot (in-flight work: also while paused) and gets
    its held messages, exactly once."""
    s = ctx.p.current_session
    if s is None or not _resumable(s):
        raise Rejected("resume-run-gone")
    if not building_capacity_available(ctx.admission, ctx.config):
        raise Rejected("building-cap")
    if entry.auto and not auto_build_capacity_available(ctx.admission, ctx.config):
        raise Rejected("auto-build-cap")
    if admissible_head(ctx.admission, ctx.config) != entry:
        raise Rejected("not-queue-head")
    _acquire_slot(ctx, entry)
    _replay_held(ctx)


def _h_retry_due(ctx: _Ctx, body: ev.RetryDue) -> None:
    _ = body  # timers never approve, decide or reset an allowance


def _ensure_issuance(ctx: _Ctx, s: StageSession) -> None:
    """Re-enable the session's fixed-profile credential whenever its gate is open.

    The broker is default-deny after every restart and only re-checks at boot; a gate
    that reopens later (answered question, stale decision closed, operator resume) would
    otherwise leave a working session unable to push. Idempotent in the broker, and the
    effect's own precondition re-checks ``work_allowed`` at execution.
    """
    if (
        s.session_id == ctx.p.current_session_id
        and s.prepared
        and s.lifecycle in _RESUMABLE
        and work_allowed(ctx.p, s)
    ):
        ctx.emit(EffectKind.ENABLE_ISSUANCE, session=s, args={"profile": profile_for(s.kind).value})


def _close_stale_decisions(ctx: _Ctx) -> None:
    """Self-heal records left open by earlier versions: prompts already gone or answered
    in Omnigent, or belonging to sessions that have ended."""
    for d in ctx.p.decisions:
        if d.status != DecisionStatus.OPEN:
            continue
        owner = ctx.p.session(d.session_id)
        if d.source == DecisionSource.ELICITATION and (d.prompt_lost or d.externally_resolved):
            ctx.put_decision(replace(d, status=DecisionStatus.RESOLVED_IN_OMNIGENT))
        elif owner is None or owner.lifecycle in (Lifecycle.RETIRED, Lifecycle.FENCED):
            ctx.put_decision(replace(d, status=DecisionStatus.ORPHANED))
    for d in ctx.p.decisions:
        _resume_after_prompt(ctx, d)


_RESUMABLE = frozenset({Lifecycle.ACTIVE, Lifecycle.WAITING})


def _h_operator_resume(ctx: _Ctx, body: ev.OperatorResume) -> None:
    """Re-open the existing current session after a stale block and relay one note.

    Closes decisions already resolved in Omnigent and clears result/agent blocks, then
    requires the ordinary execution gate (authority, approval, grant, no open decision)
    to hold by itself: the operator adds no authority, approval or time.
    """
    s = ctx.p.current_session
    if s is None or s.root_id is None:
        raise Rejected("no-current-session")
    if s.fences or s.lifecycle not in _RESUMABLE or s.execution_closed:
        raise Rejected("session-not-resumable")
    if ctx.p.stage == Stage.READY:
        # Ready never shows the bot working (and a board move would gate the note):
        # the card goes back to Building first, e.g. by the owner.
        raise Rejected("card-in-ready", explain=True)
    if not body.text.strip():
        raise Rejected("empty-note")
    _close_stale_decisions(ctx)
    ctx.unhold(Hold.RESULT_INVALID, Hold.AGENT_BLOCKED)
    s = _session(ctx, s.session_id)
    if s.lifecycle == Lifecycle.WAITING and s.wait_reason == WaitReason.DECISION:
        s = ctx.put_session(replace(s, lifecycle=Lifecycle.ACTIVE, wait_reason=None))
    if not work_allowed(ctx.p, s):
        raise Rejected("work-not-allowed")
    _ensure_issuance(ctx, s)  # before the note: the session will need its credential
    ctx.emit(
        EffectKind.SEND_MESSAGE,
        session=s,
        args={"purpose": MessagePurpose.OPERATOR_NOTE.value, "text": body.text[:8000]},
    )


def _awaiting_evidence(ctx: _Ctx) -> bool:
    return awaiting_evidence(ctx.p, ctx.now)


def awaiting_evidence(p: Parcel, now_us: int) -> bool:
    """A linked, non-terminal PR whose readiness is not verified on its current head:
    Building, Ready (a new head or a re-run) or Needs you, live session or not.

    A reconcile of such a parcel issues a catch-up PR read (the service keeps reading
    it every reconcile interval)."""
    r = p.readiness
    return (
        r is not None
        and (
            not r.verified
            or (not r.ready and _in_review_grace(r))
            or _report_line_pending_at(p, r, now_us)
            or r.merge_unknown  # GitHub had not computed mergeability: ask again
        )
        and Hold.COMPLETED not in p.holds
        and Hold.PR_CLOSED not in p.holds
    )


def _h_reconcile_due(ctx: _Ctx, body: ev.ReconcileDue) -> None:
    _ = body
    ctx.emit(EffectKind.RECONCILE_PARCEL)
    _close_stale_decisions(ctx)
    cur = ctx.p.current_session
    if _awaiting_evidence(ctx):
        r = ctx.p.readiness
        assert r is not None  # noqa: S101 - checked by _awaiting_evidence
        _fetch_evidence(ctx, r)  # catch-up read: a missed or early webhook is not lost
    if cur is not None and not any(e.kind == EffectKind.ENABLE_ISSUANCE for e in ctx.effects):
        cur = ctx.p.current_session
        assert cur is not None  # noqa: S101 - unchanged above
        if not work_allowed(ctx.p, cur):
            # Closed gate: forget the last enable so the gate reopening re-enables once
            # (the broker is default-deny after a restart and only rechecks at boot).
            if cur.issuance_enabled:
                ctx.put_session(replace(cur, issuance_enabled=False))
        elif not cur.issuance_enabled:
            _ensure_issuance(ctx, cur)  # restart with the gate closed: re-enable once
    # Correct drift: the board's Bot field is re-asserted from derived state each cycle
    # (the adapter adopts without writing when it already matches).
    # Bot drift is corrected by the RECONCILE_PARCEL read (see ``_h_snapshot``).


# ================================================================== dispatch


_Handler = Callable[[_Ctx, Any], None]

HANDLERS: dict[EventKind, _Handler] = {
    EventKind.REQUEST_TRIAGE: _h_request_triage,
    EventKind.REQUEST_PLAN: _h_request_plan,
    EventKind.REQUEST_REPLAN: _h_request_replan,
    EventKind.PLAN_FEEDBACK: _h_plan_feedback,
    EventKind.APPROVE_PLAN: _h_approve_plan,
    EventKind.WAIVE_PLAN: _h_waive_plan,
    EventKind.DECIDE: _h_decide,
    EventKind.CONTINUE: _h_continue,
    EventKind.STOP: _h_stop,
    EventKind.REQUEST_REWORK: _h_request_rework,
    EventKind.PAUSE: _h_pause,
    EventKind.UNPAUSE: _h_pause,
    EventKind.LEFTWARD_MOVE: _h_leftward,
    EventKind.ASSIGNED_HUMAN: _h_ineligible,
    EventKind.CLOSED: _h_ineligible,
    EventKind.TRANSFERRED: _h_ineligible,
    EventKind.DELETED: _h_ineligible,
    EventKind.ITEM_REMOVED: _h_item_removed,
    EventKind.APPROVAL_INVALIDATED: _h_approval_invalidated,
    EventKind.WAIVER_EDITED: _h_waiver_edited,
    EventKind.CONTRACT_TAMPERED: _h_contract_tampered,
    EventKind.INBOX_HOLD_SET: _h_inbox_hold_set,
    EventKind.INBOX_HOLD_RELEASED: _h_inbox_hold_released,
    EventKind.GITHUB_SNAPSHOT: _h_snapshot,
    EventKind.COLUMN_OBSERVED: _h_column_observed,
    EventKind.PR_OBSERVED: _h_pr_observed,
    EventKind.CHECKS_CHANGED: _h_checks_changed,
    EventKind.REVIEW_CHANGED: _h_review_changed,
    EventKind.READINESS_EVIDENCE: _h_readiness,
    EventKind.BASE_PUSHED: _h_base_pushed,
    EventKind.OPERATOR_RESUME: _h_operator_resume,
    EventKind.AUTO_TRIAGE: _h_auto_triage,
    EventKind.AUTO_BUILD_MARKED: _h_auto_build_marked,
    EventKind.AUTO_BUILD: _h_auto_build,
    EventKind.AUTOPILOT_MARKED: _h_autopilot_marked,
    EventKind.AUTOPILOT_PLAN: _h_autopilot_plan,
    EventKind.AUTOPILOT_QUEUE: _h_autopilot_queue,
    EventKind.AUTOPILOT_WITHDRAW: _h_autopilot_withdraw,
    EventKind.AUTOPILOT_STATUS: _h_autopilot_status,
    EventKind.AUTOPILOT_GATE_CREATED: _h_autopilot_gate_created,
    EventKind.AUTOPILOT_QUESTION: _h_autopilot_question,
    EventKind.RELATED_MARKED: _h_related_marked,
    EventKind.EPIC_PROGRESS: _h_epic_progress,
    EventKind.CONTRACT_PUBLISHED: _h_contract_published,
    EventKind.PUBLICATION_ACKED: _h_publication_acked,
    EventKind.SESSION_CREATED: _h_session_created,
    EventKind.CREATE_REJECTED: _h_create_rejected,
    EventKind.ADOPTION_RESULT: _h_adoption_result,
    EventKind.EFFECT_UNKNOWN: _h_effect_unknown,
    EventKind.EFFECT_CANCELLED: _h_effect_cancelled,
    EventKind.PREPARED: _h_prepared,
    EventKind.MESSAGE_ACK: _h_message_ack,
    EventKind.EFFECT_RECONCILED: _h_effect_reconciled,
    EventKind.RUNTIME_ACTIVITY: _h_runtime_activity,
    EventKind.OWNER_DIRECT_MESSAGE: _h_owner_direct,
    EventKind.ELICITATION_OPENED: _h_elicitation_opened,
    EventKind.ELICITATION_RESOLVED: _h_elicitation_resolved,
    EventKind.ELICITATION_GONE: _h_elicitation_gone,
    EventKind.OWNER_QUESTION: _h_owner_question,
    EventKind.POLICIES_VERIFIED: _h_policies_verified,
    EventKind.POLICY_GUARD_FAILED: _h_policy_guard_failed,
    EventKind.RESULT_CANDIDATE: _h_result,
    EventKind.ISSUE_SESSION_CLOSED: _h_issue_session_closed,
    EventKind.TREE_QUIESCENT: _h_tree_quiescent,
    EventKind.STOP_TIMEOUT: _h_stop_timeout,
    EventKind.SESSION_CRASHED: _h_session_crashed,
    EventKind.ACTIVE_TIME_SAMPLE: _h_active_time,
    EventKind.COST_SAMPLE: _h_cost_sample,
    EventKind.POLICY_READY: _h_policy_ready,
    EventKind.ACTIVE_LIMIT_REACHED: _h_active_limit,
    EventKind.GRACE_EXPIRED: _h_grace_expired,
    EventKind.CAPACITY_AVAILABLE: _h_capacity,
    EventKind.RETRY_DUE: _h_retry_due,
    EventKind.RECONCILE_DUE: _h_reconcile_due,
}


def _explanation(ctx: _Ctx, rejection: Rejected) -> None:
    """At most one explanation (and guarded card rollback) for an owner's invalid control.

    The explanation is the card's "Factory note" (plus a reaction on a command comment),
    never a comment.
    """
    e = ctx.event
    if ctx.maybe_parcel is None or not rejection.explain:
        return
    if e.actor_id is None or e.actor_id not in ctx.config.owners:
        return
    note = rejection.note or f"Command refused: {rejection.reason}"
    if rejection.rollback_to is not None:
        note += f"; card moved back to {rejection.rollback_to.value}"
    ctx.note(note)
    if rejection.rollback_to is not None and ctx.p.stage != rejection.rollback_to:
        # The owner's drag already put the card in Building; the persisted stage is
        # unchanged, so the expected source is given explicitly. Serialised like every
        # daemon board write.
        _emit_move(ctx, rejection.rollback_to, source=Stage.BUILDING)


_COMMENT_EVENT_ID = re.compile(r"github:comment:(\d+):created")


def _react(ctx: _Ctx, accepted: bool) -> None:
    """React to an owner's command comment: +1 accepted, confused refused.

    Only slash commands (not free-form feedback). A replayed event is a duplicate and
    never reaches here, and a GitHub reaction is idempotent, so a retry cannot double it.
    """
    e = ctx.event
    if ctx.maybe_parcel is None or e.kind == EventKind.PLAN_FEEDBACK:
        return
    if e.actor_id is None or e.actor_id not in ctx.config.owners:
        return
    match = _COMMENT_EVENT_ID.fullmatch(e.event_id)
    if match is None:
        return
    ctx.emit(
        EffectKind.REACT_COMMENT,
        args={"comment_id": int(match.group(1)), "content": "+1" if accepted else "confused"},
    )


def _queued_note(ctx: _Ctx) -> str:
    """Why the parcel's queued build waits: an auto-build slot (naming the running
    auto-builds), else its place in line for a build slot (naming the running builds)."""
    entry = ctx.admission.queue_entry(ctx.p.parcel_id)
    if entry is None or entry.status != QueueStatus.QUEUED:
        return ""
    admission, config = ctx.admission, ctx.config
    if (
        entry.auto
        and building_capacity_available(admission, config)
        and not auto_build_capacity_available(admission, config)
    ):
        running = admission.running(auto=True)
        return (
            f"Waiting for an auto-build slot ({len(running)}/{config.auto_build_concurrency}"
            f" in use: {running_text(running)})"
        )
    if entry.resume:
        text = "Queued: waiting for a build slot to resume"
    else:
        key = admission_key(entry)
        ahead = sum(
            1 for q in admission.queue if q.status == QueueStatus.QUEUED and admission_key(q) < key
        )
        text = f"Queued: {_ordinal(ahead + 1)} in line"
    running = admission.running()
    if running and not building_capacity_available(admission, config):
        text += f" ({len(running)}/{config.max_building} running: {running_text(running)})"
    return text


def _queue_note_refreshable(note: str) -> bool:
    return not note or note.startswith(("Queued: ", "Waiting for an auto-build slot"))


def _queued(ctx: _Ctx) -> bool:
    entry = ctx.admission.queue_entry(ctx.p.parcel_id)
    return entry is not None and entry.status == QueueStatus.QUEUED


# ------------------------------------------------------------------ build slots

_WAKE_KINDS = (EffectKind.SEND_MESSAGE, EffectKind.RESOLVE_ELICITATION)


def build_parked(p: Parcel) -> bool:
    """The current build run holds no turn and waits on someone else: Blocked or Needs
    you with its tree seen idle, idle waiting on checks or the review bot, or a settled
    checkpoint. An unread or busy tree (``quiescent`` false) is running: it is counted."""
    s = p.current_session
    if s is None or s.kind != SessionKind.BUILD or s.restart_pending or s.execution_closed:
        return False
    if s.lifecycle == Lifecycle.FENCED:
        return s.fences == frozenset({FenceKind.CHECKPOINT})
    if (
        s.lifecycle not in (Lifecycle.ACTIVE, Lifecycle.WAITING)
        or s.fences
        or not s.quiescent
        or s.comment_pending
        or s.external_active
        or message_uncertain(p, s)
    ):
        return False
    if s.lifecycle == Lifecycle.WAITING and s.wait_reason == WaitReason.CHECKS:
        return True
    return project_bot(p) in (BotState.BLOCKED, BotState.NEEDS_YOU)


def _resumable(s: StageSession) -> bool:
    """A parked run that a message can wake (not stopping, replaced or closed)."""
    return (
        s.kind == SessionKind.BUILD
        and not s.fences
        and not s.execution_closed
        and s.lifecycle in (Lifecycle.ACTIVE, Lifecycle.WAITING)
    )


def _slot_free(ctx: _Ctx, entry: QueueEntry) -> bool:
    """A resume may take a building slot now (resumes go first; pause does not hold
    in-flight work)."""
    if not building_capacity_available(ctx.admission, ctx.config):
        return False
    if entry.auto and not auto_build_capacity_available(ctx.admission, ctx.config):
        return False
    key = admission_key(replace(entry, status=QueueStatus.QUEUED, resume=True))
    return not any(
        q.status == QueueStatus.QUEUED
        and q.resume
        and q.parcel_id != entry.parcel_id
        and admission_key(q) < key
        for q in ctx.admission.queue
    )


def _acquire_slot(ctx: _Ctx, entry: QueueEntry) -> None:
    building = Reservation(
        ctx.new_id("rs"), ctx.p.parcel_id, ReservationKind.BUILDING, entry.approval_id
    )
    ctx.admission = replace(ctx.admission, reservations=(*ctx.admission.reservations, building))
    _set_queue(ctx, replace(entry, status=QueueStatus.RESERVED, resume=False), ctx.p.parcel_id)
    ctx.update(slot_parked=False)
    if ctx.p.note.startswith(("Queued: ", "Waiting for an auto-build slot")):
        ctx.update(note="")


def _replay_held(ctx: _Ctx) -> None:
    """Send the held messages to the re-admitted run, once (fresh effects)."""
    held, s = ctx.p.held_wakes, ctx.p.current_session
    ctx.update(held_wakes=())
    for w in held:
        if s is None or w.session_id != s.session_id:
            continue
        ctx.emit(EffectKind(w.kind), session=s, target=w.target, args=json.loads(w.args_json))


def _hold_wakes(ctx: _Ctx, wakes: list[EffectIntent]) -> None:
    ids = {e.effect_id for e in wakes}
    ctx.effects = [e for e in ctx.effects if e.effect_id not in ids]
    held = tuple(
        HeldWake(
            kind=e.kind.value,
            session_id=e.preconditions.session_id or "",
            target=e.target,
            args_json=json.dumps(e.args, sort_keys=True, separators=(",", ":")),
        )
        for e in wakes
    )
    ctx.update(
        held_wakes=(*ctx.p.held_wakes, *held),
        sent_effects=tuple(x for x in ctx.p.sent_effects if x[0] not in ids),
    )


def _settle_build_slot(ctx: _Ctx) -> None:
    """After every event: a build holds a building slot only while its run works, winds
    down or is about to start. A parked run releases it; a message for a parked run takes
    a free slot at once, else it is held and the build queues to resume (ahead of new
    builds); a parked run seen busy again is counted at once (fail closed)."""
    p = ctx.p
    pid = p.parcel_id
    entry = ctx.admission.queue_entry(pid)
    s = p.current_session
    a = p.current_approval
    auth = p.authorization(s.authorization_id) if s is not None else None
    episode = (
        entry is not None
        and s is not None
        and s.kind == SessionKind.BUILD
        and a is not None
        and entry.approval_id == a.approval_id
        and auth is not None
        and auth.approval_id == entry.approval_id
    )
    parked_entry = entry is not None and (
        entry.status == QueueStatus.HELD or (entry.status == QueueStatus.QUEUED and entry.resume)
    )
    if not episode or entry is None or s is None:
        if parked_entry and entry is not None:
            status = (
                QueueStatus.CANCELLED
                if entry.status == QueueStatus.QUEUED
                else (QueueStatus.RELEASED)
            )
            _set_queue(ctx, replace(entry, status=status, resume=False), pid)
        if p.slot_parked or p.held_wakes:
            ctx.update(slot_parked=False, held_wakes=())
        return
    wakes = [
        e
        for e in ctx.effects
        if e.kind in _WAKE_KINDS and e.preconditions.session_id == s.session_id
    ]
    holds_slot = any(
        r.parcel_id == pid for r in ctx.admission.live_reservations(ReservationKind.BUILDING)
    )
    if entry.status == QueueStatus.RESERVED:
        if holds_slot and not wakes and build_parked(ctx.p):
            ctx.admission = replace(
                ctx.admission,
                reservations=tuple(
                    replace(r, live=False)
                    if r.parcel_id == pid and r.live and r.kind == ReservationKind.BUILDING
                    else r
                    for r in ctx.admission.reservations
                ),
            )
            _set_queue(ctx, replace(entry, status=QueueStatus.HELD, resume=True), pid)
            ctx.update(slot_parked=True)
        return
    if not parked_entry:
        return
    if not _resumable(s):
        # Stopping, fenced or settled at a checkpoint: no message may reach it now.
        if p.held_wakes:
            ctx.update(held_wakes=())
        if entry.status == QueueStatus.QUEUED:
            _set_queue(ctx, replace(entry, status=QueueStatus.HELD), pid)
        return
    if wakes or p.held_wakes:
        if _slot_free(ctx, entry):
            _acquire_slot(ctx, entry)
            _replay_held(ctx)
            return
        _hold_wakes(ctx, wakes)
        if entry.status != QueueStatus.QUEUED:
            _set_queue(ctx, replace(entry, status=QueueStatus.QUEUED, resume=True), pid)
            ctx.note(_queued_note(ctx))
        return
    if not build_parked(ctx.p):
        # Running again without a message (a busy read after a stale idle one): it holds
        # a slot whether or not one is free.
        _acquire_slot(ctx, entry)


def _settle_observed_move(ctx: _Ctx, accepted: bool) -> None:
    """An accepted owner drag explains the observed move; a card that moved on drops it."""
    move = ctx.p.observed_move
    if move is None:
        return
    drag = ctx.event.kind in _DRAG_TARGETS or ctx.event.kind == EventKind.LEFTWARD_MOVE
    if ctx.p.stage != move.to_stage or (accepted and drag and _owner_control(ctx)):
        ctx.update(observed_move=None)


def _project_board(ctx: _Ctx) -> None:
    """Derived Bot and "Factory note" values; each is written only when it changes.

    The status note is cleared when the card changes column or Bot state (a finished
    drain, Working -> Idle, keeps it: it explains why the card is idle), unless this
    event set it.
    """
    # The card moved on (a parcel first seen now counts as coming from Inbox): the marks
    # served their purpose.
    if ctx.p.related_marks and ctx.p.stage not in (None, ctx.origin_stage or Stage.INBOX):
        ctx.update(related_marks=())
    old_bot = ctx.p.bot
    bot = project_bot(ctx.p, queued=_queued(ctx))
    if ctx.p.note and not ctx.note_set:
        drained = old_bot == BotState.WORKING and bot == BotState.IDLE
        if ctx.p.stage != ctx.origin_stage or (bot != old_bot and not drained):
            ctx.update(note="")
    if bot == BotState.QUEUED and _queue_note_refreshable(ctx.p.note):
        # Builds ahead are admitted without an event here: every event (at least the
        # periodic reconcile) refreshes the position; the board is written on change.
        ctx.update(note=_queued_note(ctx) or ctx.p.note)
    if bot != old_bot:
        ctx.update(bot=bot)
        ctx.emit(EffectKind.SET_BOT, args={"bot": bot.value})
    note = project_note(ctx.p, bot)
    if note != ctx.p.board_note:
        ctx.update(board_note=note)
        ctx.emit(EffectKind.SET_NOTE, args={"note": note})


def _ordinal(n: int) -> str:
    suffix = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _words(reason: str) -> str:
    return reason.replace("-", " ").replace("_", " ")


def _reject(state: State, event: Event, reason: str) -> TransitionResult:
    parcel = state.parcel
    version = parcel.version if parcel else None
    return TransitionResult(
        state=state,
        effects=(),
        audit=AuditRecord(
            event.event_id, event.kind.value, event.parcel_id, False, reason, version, version
        ),
    )


def transition(state: State, event: Event) -> TransitionResult:
    """Apply one normalized event. Total: every input yields a result, never raises."""
    # ---- family 1: envelope / deduplication
    if event.repo_id != state.config.repo_id or event.repo_id != state.admission.repo_id:
        return _reject(state, event, "wrong-repository")
    is_global = event.kind in ev.GLOBAL_KINDS
    parcel = state.parcel
    if parcel is None and not is_global:
        return _reject(state, event, "parcel-required")
    if parcel is not None:
        if event.parcel_id != parcel.parcel_id or parcel.repo_id != event.repo_id:
            return _reject(state, event, "wrong-parcel")
        if event.event_id in parcel.applied_event_ids:
            return TransitionResult(
                state=state,
                effects=(),
                audit=AuditRecord(
                    event.event_id,
                    event.kind.value,
                    parcel.parcel_id,
                    False,
                    "duplicate",
                    parcel.version,
                    parcel.version,
                ),
                duplicate=True,
            )

    # ---- family 1b: central provenance/actor admission (before evidence or handlers)
    inadmissible = admission_rejection(
        event.kind, event.provenance, event.actor_id, state.config.owners
    )
    if inadmissible is not None:
        return _reject(state, event, inadmissible)

    ctx = _Ctx(state, event)
    _drag_origin(ctx)
    # ---- family 2: current safety facts (fresh-read evidence)
    _apply_evidence(ctx)
    checkpoint_parcel = ctx.maybe_parcel
    checkpoint_admission = ctx.admission
    checkpoint_effects = list(ctx.effects)

    # ---- families 3-4: control authorization + stage-specific row
    accepted = True
    reason = "accepted"
    try:
        HANDLERS[event.kind](ctx, event.body)
        if ctx.maybe_parcel is not None:
            # Guarded wake-ups of recorded successors (each re-checks its predicates).
            _try_activate_pending(ctx)
            _maybe_start_prepared(ctx)
            _maybe_ready(ctx)
            _maybe_close_issue_session(ctx)
    except Rejected as rejection:
        accepted = False
        reason = rejection.reason
        ctx.restore(checkpoint_parcel, checkpoint_admission, checkpoint_effects)
        ctx.note_set = False
        _explanation(ctx, rejection)
    _react(ctx, accepted)

    # ---- family 5: derived board projection and bookkeeping
    dropped = 0
    new_parcel = ctx.maybe_parcel
    if new_parcel is not None:
        _settle_observed_move(ctx, accepted)
        _settle_auto_build(ctx)
        _settle_autopilot(ctx)
        _settle_build_slot(ctx)
        _project_board(ctx)
        new_parcel = ctx.p
        kept: list[EffectIntent] = []
        dropped_ids: set[str] = set()
        for effect in ctx.effects:
            if effect.work_bearing and effect_still_valid(new_parcel, effect) is not None:
                dropped += 1
                dropped_ids.add(effect.effect_id)
                ctx.effects_dropped.append(effect)
                continue
            kept.append(effect)
        ctx.effects = kept
        if dropped_ids:
            ctx.update(sent_effects=tuple(x for x in ctx.p.sent_effects if x[0] not in dropped_ids))
            unsent = {
                e.preconditions.session_id
                for e in ctx.effects_dropped
                if e.kind == EffectKind.ENABLE_ISSUANCE
            }
            if unsent:
                ctx.update(
                    sessions=tuple(
                        replace(x, issuance_enabled=False) if x.session_id in unsent else x
                        for x in ctx.p.sessions
                    )
                )
        new_parcel = replace(
            new_parcel,
            version=ctx.next_version,
            applied_event_ids=new_parcel.applied_event_ids | {event.event_id},
        )
    new_state = State(parcel=new_parcel, admission=ctx.admission, config=state.config)
    return TransitionResult(
        state=new_state,
        effects=tuple(ctx.effects),
        audit=AuditRecord(
            event_id=event.event_id,
            kind=event.kind.value,
            parcel_id=event.parcel_id,
            accepted=accepted,
            reason=reason,
            before_version=ctx.before_version,
            after_version=new_parcel.version if new_parcel is not None else None,
            dropped_effects=dropped,
        ),
    )


__all__ = [
    "HANDLERS",
    "STAGE_ORDER",
    "AuditRecord",
    "Rejected",
    "TransitionResult",
    "awaiting_evidence",
    "derive_id",
    "mcp_question_id",
    "transition",
]
