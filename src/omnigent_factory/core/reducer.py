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
    building_capacity_available,
    pr_capacity_available,
    project_bot,
    project_note,
    queue_head,
)
from omnigent_factory.core.types import (
    CONTROL_CLEARED_HOLDS,
    STAGE_ORDER,
    AdmissionSnapshot,
    Approval,
    ApprovalKind,
    BotState,
    Contract,
    Decision,
    DecisionImpact,
    DecisionSource,
    DecisionStatus,
    FenceKind,
    Grant,
    Hold,
    InboxHold,
    InboxHoldReason,
    IssueSession,
    IssueSessionStatus,
    IssueSnapshot,
    Lifecycle,
    Parcel,
    PendingMove,
    QueueEntry,
    QueueStatus,
    Readiness,
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
    is_leftward,
    issue_session_title,
)


class Rejected(Exception):
    """No transition row matched; carries the audit reason and optional explanation."""

    def __init__(
        self, reason: str, *, explain: bool = False, rollback_to: Stage | None = None
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.explain = explain
        self.rollback_to = rollback_to


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
    EffectKind.REACT_COMMENT: RetryClass.ADOPTABLE_WRITE,
    EffectKind.POST_COMMENT: RetryClass.ADOPTABLE_WRITE,
    EffectKind.PUBLISH_CONTRACT: RetryClass.ADOPTABLE_WRITE,
    EffectKind.PUBLISH_TRIAGE: RetryClass.ADOPTABLE_WRITE,
    EffectKind.PUBLISH_REPORT: RetryClass.ADOPTABLE_WRITE,
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
    if entry is not None and entry.status == QueueStatus.RESERVED:
        _set_queue(ctx, replace(entry, status=QueueStatus.RELEASED), pid)


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
            execution_closed=s.execution_closed or target == Lifecycle.RETIRED,
            external_active=False,
        )
    )
    if not any(x.lifecycle == Lifecycle.BLOCKED for x in ctx.p.sessions):
        ctx.unhold(Hold.STOP_UNVERIFIED)
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
    _sync_session_title(ctx, snap)
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
        _leftward_negative(ctx, current, observed)
        return
    if board_pending(ctx.p):
        # A daemon write is in flight/queued: its target is the single desired column
        # and will be (re)asserted on the board; a rightward/unknown observation is not
        # authority and must not split the desired column from the queued target.
        return
    ctx.update(stage=observed)  # rightward/unknown observation: never authority


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
    if not ctx.event.entropy:
        return
    _retire_settled_current(ctx)
    if _create_session(ctx, auth) is not None:
        ctx.update(pending_authorization_id=None)


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
    if (
        s.lifecycle != Lifecycle.WAITING
        or s.wait_reason != WaitReason.CHECKS
        or not s.quiescent
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
    ctx.emit(
        EffectKind.PUBLISH_REPORT,
        args={"report": "ready", "pr_number": r.pr_number, "head_sha": r.head_sha},
    )


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
    ctx: _Ctx, approval: Approval, duration_us: int, *, rework: bool = False
) -> None:
    auth = _new_authorization(ctx, SessionKind.BUILD, duration_us, approval.approval_id)
    if rework:
        ctx.put_authorization(replace(auth, rework=True))
    seq = ctx.admission.next_sequence
    _set_queue(
        ctx,
        QueueEntry(ctx.p.parcel_id, approval.approval_id, seq, QueueStatus.QUEUED),
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
) -> Approval:
    assert ctx.event.actor_id is not None  # noqa: S101 - guarded by _control
    a = Approval(
        approval_id=ctx.new_id("ap"),
        kind=kind,
        full_hash=full_hash,
        owner_id=ctx.event.actor_id,
        source_event_id=ctx.event.event_id,
        source_time_us=ctx.now,
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


def _after_approval(ctx: _Ctx, approval: Approval, duration_us: int, via: Via) -> None:
    ctx.unhold(*CONTROL_CLEARED_HOLDS)
    ctx.update(readiness_wakes=0)  # a new approved deliverable gets its own wake
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


def _rework(ctx: _Ctx, via: Via | None) -> None:
    """Owner feedback on the built work: back to Building under the same approval.

    A new build episode on the parcel's branch and PR (admitted like any build, so it may
    queue for a slot) with a fresh time block and a fresh fix budget. The first message
    points the issue session at the feedback; Ready is then re-evaluated as usual.
    """
    a = ctx.p.current_approval
    assert a is not None  # noqa: S101 - approval_ok checked by the caller
    ctx.unhold(*CONTROL_CLEARED_HOLDS, Hold.REMEDIATION_EXHAUSTED)
    ctx.update(readiness=None, readiness_wakes=0)
    _cancel_pending(ctx)
    _move(ctx, Stage.BUILDING, via)
    _enqueue_build(ctx, a, ctx.config.block_us(ctx.p.size or Size.M), rework=True)
    ctx.note(_REWORK_NOTE)


_REWORK_NOTE = "Rework: owner feedback"


def _h_approve_plan(ctx: _Ctx, body: ev.ApprovePlan) -> None:
    rollback = Stage.SCOPED if body.via == Via.DRAG else None
    _control(ctx)
    if not eligible(ctx.p):
        raise Rejected("parcel-not-eligible", explain=True, rollback_to=rollback)
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
    duration = _grant_duration(ctx, body.duration_us, Size.M)
    approval = _new_approval(ctx, ApprovalKind.SKIP, digest, snapshot=canonical.decode("utf-8"))
    _after_approval(ctx, approval, duration, body.via)


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
    in_checkpoint = s.lifecycle in (
        Lifecycle.CHECKPOINT_GRACE,
        Lifecycle.CHECKPOINT_WAIT,
    ) or (FenceKind.CHECKPOINT in s.fences)
    if not in_checkpoint:
        raise Rejected("not-at-checkpoint", explain=True)
    if not only_checkpoint_fenced(s):
        raise Rejected("continue-cannot-clear-hard-fence", explain=True)
    if s.lifecycle == Lifecycle.RETIRED or s.execution_closed:
        raise Rejected("session-retired", explain=True)
    if not s.grant.ready:
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
    owner_fresh = (
        e.provenance in ev.CONTROL_PROVENANCES
        and e.actor_id is not None
        and e.actor_id in ctx.config.owners
        and e.source_time_us > ctx.p.barrier_time_us
    )
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
        ctx.update(readiness=replace(r, ready=False, verified=False))
    _move(ctx, Stage.BUILDING)
    ctx.hold(Hold.REWORK_CONTROL_REQUIRED)
    head = r.head_sha[:7] if r is not None and r.head_sha else ""
    ctx.note(f"Ready withdrawn: {_words(reason)}" + (f" on {head}" if head else ""))


def _fetch_evidence(ctx: _Ctx, r: Readiness) -> None:
    """A fresh read of the linked PR: actual head, current checks, review and findings."""
    ctx.emit(
        EffectKind.FETCH_PR_EVIDENCE,
        args={
            "pr_number": r.pr_number,
            "head_sha": r.head_sha,
            "reviewed_head": r.reviewed_head or r.head_sha,
            "session_id": r.session_id,
            "issue_number": ctx.p.issue_number,
        },
    )


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
    only triggers a fresh current-head read, which alone decides readiness."""
    r = _readiness_for(ctx, body.pr_number, "checks-before-readiness-recorded")
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
    r = _review_bot_window(ctx, r, body.review_bot_pending_since_us)
    if body.remediation_exhausted:
        ctx.hold(Hold.REMEDIATION_EXHAUSTED)
    in_ready = ctx.p.stage == Stage.READY and (r.ready or _refreshing(ctx))
    if not body.verified:
        ctx.update(readiness=replace(r, verified=False))
        if in_ready:
            if body.checks == ev.ChecksState.PENDING and body.pr_open:
                # A re-run or a new head's checks: stay in Ready, waiting (Bot Working);
                # a later green read restores Ready with no owner action or comment.
                ctx.update(readiness=replace(r, verified=False, ready=False))
                return
            ctx.hold(Hold.READINESS_FAILED)
            reason = (
                "readiness-unverified"
                if body.checks is None
                else _failure_reason(r, body, ctx.p.issue_number)
            )
            _invalidate_ready(ctx, reason)
            return
        if body.checks is None or not body.pr_open:
            ctx.hold(Hold.READINESS_FAILED)  # legacy read or closed PR: owner decides
            return
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
        )
    )


def _settle_at(ctx: _Ctx, head: str) -> int:
    """Review-bot grace for ``head``: a re-submission of the same head keeps its clock."""
    r = ctx.p.readiness
    if r is not None and r.head_sha == head and r.settle_at_us:
        return r.settle_at_us
    return ctx.now + ctx.config.review_grace_us


def _review_bot_window(ctx: _Ctx, r: Readiness, since: int | None) -> Readiness:
    """Wait for the review bot only while it can still respond to this head.

    ``since`` 0: it already answered this head and nothing re-pinged it, so readiness is
    judged on current evidence at once. A trigger time (push, PR open or re-ping) runs
    the grace from the later of that trigger and the build result. None (unknown) keeps
    the wait: an unreadable bot state is never taken as answered.
    """
    if since == 0:
        window = replace(r, review_bot_done=True)
    elif since is None:
        window = replace(r, review_bot_done=False)
    else:
        trigger_end = min(since, ctx.now) + ctx.config.review_grace_us
        window = replace(r, review_bot_done=False, settle_at_us=max(r.settle_at_us, trigger_end))
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
    )
    ctx.update(readiness=r)
    ctx.unhold(Hold.CHECKS_FAILED, Hold.READINESS_FAILED)
    _fetch_evidence(ctx, r)


def _not_ready(ctx: _Ctx, r: Readiness, body: ev.ReadinessEvidence) -> None:
    """Current-head evidence is not green: wait, wake the idle build once, or Needs you.

    Pending checks are waiting, never a failure. A genuine failure (red checks, open
    review-bot findings, no accepted review of this head) wakes the idle build session
    once per approval, within its existing fix-batch/recheck allowance; after that the
    owner decides with the reason recorded.
    """
    s = ctx.p.session(r.session_id)
    if body.checks == ev.ChecksState.PENDING:
        return
    if s is not None and s.session_id == ctx.p.current_session_id and not s.fences:
        busy = s.lifecycle == Lifecycle.ACTIVE or (
            s.lifecycle == Lifecycle.WAITING
            and s.wait_reason == WaitReason.CHECKS
            and not s.quiescent
        )
        if busy:
            return  # re-evaluated by the next reconcile read or a new result
    reason = _failure_reason(r, body, ctx.p.issue_number)
    if (
        ctx.p.readiness_wakes < 1
        and s is not None
        and s.session_id == ctx.p.current_session_id
        and s.lifecycle == Lifecycle.WAITING
        and s.wait_reason == WaitReason.CHECKS
        and s.quiescent
        and not ctx.p.open_decisions
        and approval_ok(ctx.p)
        and dispatchable(ctx.p)
        and work_allowed(ctx.p, s)
    ):
        ctx.update(readiness_wakes=ctx.p.readiness_wakes + 1)
        s = ctx.put_session(replace(s, lifecycle=Lifecycle.ACTIVE, wait_reason=None))
        _ensure_issuance(ctx, s)
        ctx.emit(
            EffectKind.SEND_MESSAGE,
            session=s,
            args={
                "purpose": MessagePurpose.READINESS_WAKE.value,
                "reason": reason,
                "pr_number": r.pr_number,
                "head_sha": r.head_sha,
            },
        )
        return
    if Hold.READINESS_FAILED not in ctx.p.holds:
        ctx.hold(Hold.READINESS_FAILED)
        ctx.comment("ready-blocked", pr_number=r.pr_number, head_sha=r.head_sha, reason=reason)
        ctx.note(f"Needs you: PR #{r.pr_number} not Ready on {r.head_sha[:7]}: {reason}")


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
        s = ctx.put_session(replace(s, lifecycle=Lifecycle.DRAINING))
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


def _h_runtime_activity(ctx: _Ctx, body: ev.RuntimeActivity) -> None:
    s = _session(ctx, body.session_id)
    if not body.busy:
        return  # idle is not quiescence evidence
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
        ctx.emit(EffectKind.PUBLISH_TRIAGE, session=s, args={"session_id": s.session_id})
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
    if not body.complete or body.busy:
        s = ctx.put_session(replace(s, quiescent=False))
        if s.lifecycle in (Lifecycle.DRAINING, Lifecycle.BLOCKED) and s.root_id is not None:
            ctx.emit(EffectKind.INTERRUPT_TREE, session=s, args={"root_id": s.root_id})
            ctx.emit(EffectKind.SCAN_TREE, session=s, args={"root_id": s.root_id})
        return
    if s.root_id is None and s.lifecycle not in (Lifecycle.FENCED, Lifecycle.RETIRED):
        raise Rejected("no-root-to-scan")
    s = ctx.put_session(replace(s, quiescent=True, external_active=False))
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
    s = _session(ctx, body.session_id)
    if s.lifecycle != Lifecycle.DRAINING:
        raise Rejected("session-not-draining")
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
    if ctx.admission.paused:
        raise Rejected("paused")
    if queue_head(ctx.admission) != entry:
        raise Rejected("not-queue-head")
    if not building_capacity_available(ctx.admission, ctx.config):
        raise Rejected("building-cap")
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
    """A linked, non-terminal PR whose readiness is not verified on its current head:
    Building, Ready (a new head or a re-run) or Needs you, live session or not."""
    r = ctx.p.readiness
    return (
        r is not None
        and (not r.verified or (not r.ready and _in_review_grace(r)))
        and not _completed(ctx)
        and Hold.PR_CLOSED not in ctx.p.holds
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
    EventKind.OPERATOR_RESUME: _h_operator_resume,
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
    note = f"Command refused: {rejection.reason}"
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
    """The parcel's current place in the build queue (1st = next to be admitted)."""
    entry = ctx.admission.queue_entry(ctx.p.parcel_id)
    if entry is None or entry.status != QueueStatus.QUEUED:
        return ""
    key = (entry.sequence, entry.parcel_id)
    ahead = sum(
        1
        for q in ctx.admission.queue
        if q.status == QueueStatus.QUEUED and (q.sequence, q.parcel_id) < key
    )
    return f"Queued: {_ordinal(ahead + 1)} in line"


def _queued(ctx: _Ctx) -> bool:
    entry = ctx.admission.queue_entry(ctx.p.parcel_id)
    return entry is not None and entry.status == QueueStatus.QUEUED


def _project_board(ctx: _Ctx) -> None:
    """Derived Bot and "Factory note" values; each is written only when it changes.

    The status note is cleared when the card changes column or Bot state (a finished
    drain, Working -> Idle, keeps it: it explains why the card is idle), unless this
    event set it.
    """
    old_bot = ctx.p.bot
    bot = project_bot(ctx.p, queued=_queued(ctx))
    if ctx.p.note and not ctx.note_set:
        drained = old_bot == BotState.WORKING and bot == BotState.IDLE
        if ctx.p.stage != ctx.origin_stage or (bot != old_bot and not drained):
            ctx.update(note="")
    if bot == BotState.QUEUED and (not ctx.p.note or ctx.p.note.startswith("Queued: ")):
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
    "derive_id",
    "mcp_question_id",
    "transition",
]
