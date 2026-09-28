"""Frozen domain types for the pure state kernel (architecture §2.2).

Every type here is an immutable value. The reducer (:mod:`omnigent_factory.core.reducer`)
never mutates an instance; it returns replacements built with :func:`dataclasses.replace`.
Times are UTC integer microseconds; durations are integer microseconds.

These types are the persisted aggregate: :mod:`omnigent_factory.core.codec` round-trips
them through JSON for the SQLite store, so field names are part of the storage contract.
Add fields with defaults; never rename or repurpose one without a migration.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field

MICROS_PER_SECOND = 1_000_000
MICROS_PER_MINUTE = 60 * MICROS_PER_SECOND
MICROS_PER_HOUR = 60 * MICROS_PER_MINUTE


class Stage(enum.StrEnum):
    """Board ``Status`` column. ``DONE`` is a retained legacy value, never a dispatch stage."""

    INBOX = "Inbox"
    TRIAGED = "Triaged"
    SCOPED = "Scoped"
    BUILDING = "Building"
    READY = "Ready"
    DONE = "Done"


#: Left-to-right order of the managed columns. ``DONE`` is deliberately absent.
STAGE_ORDER: dict[Stage, int] = {
    Stage.INBOX: 0,
    Stage.TRIAGED: 1,
    Stage.SCOPED: 2,
    Stage.BUILDING: 3,
    Stage.READY: 4,
}


def is_leftward(from_stage: Stage | None, to_stage: Stage | None) -> bool:
    """True when a move goes left, to Inbox, or off the managed columns."""
    if to_stage == Stage.INBOX and from_stage != Stage.INBOX:
        return True
    if from_stage is None or to_stage is None:
        return False
    if from_stage not in STAGE_ORDER or to_stage not in STAGE_ORDER:
        return False
    return STAGE_ORDER[to_stage] < STAGE_ORDER[from_stage]


class SessionKind(enum.StrEnum):
    TRIAGE = "triage"
    PLAN = "plan"
    BUILD = "build"


class Lifecycle(enum.StrEnum):
    """Stage-session lifecycle (§2.2). Observed runtime status is kept separately."""

    INTENT = "INTENT"
    CREATING = "CREATING"
    PREPARING = "PREPARING"
    ACTIVE = "ACTIVE"
    WAITING = "WAITING"
    CHECKPOINT_GRACE = "CHECKPOINT_GRACE"
    CHECKPOINT_WAIT = "CHECKPOINT_WAIT"
    DRAINING = "DRAINING"
    FENCED = "FENCED"
    RETIRED = "RETIRED"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"
    UNKNOWN = "UNKNOWN"


#: Lifecycles whose tree has been observed quiescent (or never existed remotely).
SETTLED_LIFECYCLES = frozenset({Lifecycle.FENCED, Lifecycle.RETIRED})

#: Lifecycles in which a daemon work-bearing effect may be sent when no fence exists.
EXECUTABLE_LIFECYCLES = frozenset({Lifecycle.ACTIVE, Lifecycle.WAITING})


class FenceKind(enum.StrEnum):
    SAFETY = "safety"
    STOPPED = "stopped"
    REVOKED = "revoked"
    CHECKPOINT = "checkpoint"


#: Bit values for the SQL ``fence_mask`` column.
FENCE_BITS: dict[FenceKind, int] = {
    FenceKind.SAFETY: 1,
    FenceKind.STOPPED: 2,
    FenceKind.REVOKED: 4,
    FenceKind.CHECKPOINT: 8,
}


class WaitReason(enum.StrEnum):
    DECISION = "decision"
    PLAN_APPROVAL = "plan_approval"
    CHECKS = "checks"


class ApprovalKind(enum.StrEnum):
    PLAN = "plan"
    SKIP = "skip"


class Via(enum.StrEnum):
    """Surface an owner control arrived on."""

    DRAG = "drag"
    COMMAND = "command"
    LABEL = "label"


class Size(enum.StrEnum):
    S = "S"
    M = "M"
    L = "L"


class QueueStatus(enum.StrEnum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    HELD = "HELD"
    RELEASED = "RELEASED"
    CANCELLED = "CANCELLED"


class ReservationKind(enum.StrEnum):
    BUILDING = "building"
    OPEN_PR = "open_pr"


class BotState(enum.StrEnum):
    """Derived display value for the board ``Bot`` field (§2.7). Never authority."""

    WORKING = "Working"
    NEEDS_YOU = "Needs you"
    CHECKPOINT = "Checkpoint"
    BLOCKED = "Blocked"
    IDLE = "Idle"


class Hold(enum.StrEnum):
    """Gates on a parcel. Holds restrict; none is a source of authorization."""

    SAFETY = "safety"
    STOPPED = "stopped"
    NO_PROJECT_ITEM = "no_project_item"
    AWAITING_OWNER = "awaiting_owner"
    STOP_UNVERIFIED = "stop_unverified"
    RESULT_INVALID = "result_invalid"
    CREATE_REJECTED = "create_rejected"
    PREPARE_FAILED = "prepare_failed"
    PUBLICATION_FAILED = "publication_failed"
    #: A stage result is accepted but its GitHub publication has not landed yet.
    PUBLICATION_PENDING = "publication_pending"
    #: The agent reported it could not finish (``blocked`` result); owner/operator acts.
    AGENT_BLOCKED = "agent_blocked"
    RESTART_EXHAUSTED = "restart_exhausted"
    CHECKS_FAILED = "checks_failed"
    READINESS_FAILED = "readiness_failed"
    REMEDIATION_EXHAUSTED = "remediation_exhausted"
    REWORK_CONTROL_REQUIRED = "rework_control_required"
    UNSUPPORTED_REWORK = "unsupported_rework"
    PR_CLOSED = "pr_closed"
    EXTERNAL_ACTIVITY = "external_activity"
    APPROVAL_VOIDED = "approval_voided"
    #: A durable inbox delivery for this parcel is parked or not yet identity-verified.
    INBOX = "inbox"
    #: Terminal: the linked PR merged or the issue closed. Late checks, heads, reviews and
    #: readiness reads are audit only; only a fresh owner stage control starts again.
    COMPLETED = "completed"


class InboxHoldReason(enum.StrEnum):
    """Why an inbox delivery holds its parcel (see ``Parcel.inbox_holds``)."""

    #: Deterministic poison: only the local operator may release it.
    PARKED = "parked"
    #: Identity not yet verified (§3.3(3)): released by the inbox once it resolves.
    UNRESOLVED = "unresolved"


@dataclass(frozen=True, slots=True)
class InboxHold:
    delivery_guid: str
    reason: InboxHoldReason


BLOCKING_HOLDS = frozenset(
    {
        Hold.STOP_UNVERIFIED,
        Hold.RESULT_INVALID,
        Hold.CREATE_REJECTED,
        Hold.PREPARE_FAILED,
        Hold.PUBLICATION_FAILED,
        Hold.RESTART_EXHAUSTED,
        Hold.INBOX,
        Hold.AGENT_BLOCKED,
    }
)

NEEDS_YOU_HOLDS = frozenset(
    {
        Hold.AWAITING_OWNER,
        Hold.CHECKS_FAILED,
        Hold.READINESS_FAILED,
        Hold.REMEDIATION_EXHAUSTED,
        Hold.REWORK_CONTROL_REQUIRED,
        Hold.UNSUPPORTED_REWORK,
        Hold.PR_CLOSED,
        Hold.NO_PROJECT_ITEM,
        Hold.APPROVAL_VOIDED,
        Hold.EXTERNAL_ACTIVITY,
    }
)

#: Holds cleared by any accepted fresh owner stage control.
CONTROL_CLEARED_HOLDS = frozenset(
    {
        Hold.SAFETY,
        Hold.STOPPED,
        Hold.AWAITING_OWNER,
        Hold.RESULT_INVALID,
        Hold.CREATE_REJECTED,
        Hold.PREPARE_FAILED,
        Hold.RESTART_EXHAUSTED,
        Hold.CHECKS_FAILED,
        Hold.READINESS_FAILED,
        Hold.REWORK_CONTROL_REQUIRED,
        Hold.UNSUPPORTED_REWORK,
        Hold.PR_CLOSED,
        Hold.APPROVAL_VOIDED,
        Hold.PUBLICATION_PENDING,
        Hold.AGENT_BLOCKED,
        Hold.COMPLETED,
    }
)


class DecisionStatus(enum.StrEnum):
    OPEN = "open"
    ANSWERED = "answered"
    RELAYED = "relayed"
    ORPHANED = "orphaned"
    CANCELLED = "cancelled"
    #: Answered, cancelled or gone in Omnigent itself (not via the factory): closed.
    RESOLVED_IN_OMNIGENT = "resolved_in_omnigent"


class DecisionSource(enum.StrEnum):
    """Where an owner question came from."""

    #: A native Omnigent elicitation (answered by resolving the prompt).
    ELICITATION = "elicitation"
    #: ``factory_ask_owner`` (answered by relaying a message to the issue session).
    MCP = "mcp"


class IssueSessionStatus(enum.StrEnum):
    """Lifetime of the parcel's Omnigent issue session (independent of any stage run)."""

    LIVE = "live"
    #: Crashed, deleted, archived elsewhere or otherwise unusable: the next run replaces it.
    DEAD = "dead"
    #: Terminal parcel: an archive request is pending.
    CLOSING = "closing"
    CLOSED = "closed"


class DecisionImpact(enum.StrEnum):
    WITHIN_CONTRACT = "within_contract"
    PLAN_REVISION = "plan_revision"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class IssueSnapshot:
    """Fresh GitHub read evidence for one issue (supplied by the GitHub adapter).

    ``identity_resolved`` is false when the repository/project/content identity could not
    be verified; that fails closed for work.
    """

    open: bool
    human_assigned: bool
    repo_matches: bool
    identity_resolved: bool
    in_project: bool
    stage: Stage | None
    title: str
    body: str | None
    read_at_us: int
    #: The board's current Bot value (display name) when read; None when not read.
    bot: str | None = None

    @property
    def eligible(self) -> bool:
        return (
            self.open and not self.human_assigned and self.repo_matches and (self.identity_resolved)
        )


@dataclass(frozen=True, slots=True)
class Contract:
    """An immutable plan contract publication candidate or accepted publication."""

    contract_id: str
    revision: int
    canonical: str
    full_hash: str
    source_session_id: str
    size: Size
    published: bool = False
    comment_id: str | None = None
    posted_at_us: int | None = None
    intact: bool = True
    superseded: bool = False

    @property
    def prefix(self) -> str:
        return self.full_hash[:12]


@dataclass(frozen=True, slots=True)
class Approval:
    """Owner build approval. Only invalidation metadata may change after creation."""

    approval_id: str
    kind: ApprovalKind
    full_hash: str
    owner_id: int
    source_event_id: str
    source_time_us: int
    sequence: int
    eligibility_epoch: int
    contract_id: str | None = None
    snapshot_canonical: str | None = None
    issue_edit_count: int = 0
    invalidated_reason: str | None = None
    invalidated_at_us: int | None = None

    @property
    def valid(self) -> bool:
        return self.invalidated_at_us is None


@dataclass(frozen=True, slots=True)
class StageAuthorization:
    """Persisted owner authority for one stage episode."""

    authorization_id: str
    kind: SessionKind
    generation: int
    source_event_id: str
    source_time_us: int
    revision: int
    eligibility_epoch: int
    grant_duration_us: int
    approval_id: str | None = None
    cancelled: bool = False


@dataclass(frozen=True, slots=True)
class Grant:
    """A bounded active-time grant for one stage session."""

    grant_id: str
    source_event_id: str
    duration_us: int
    consumed_us: int = 0
    ready: bool = True
    grace_deadline_us: int | None = None
    policy_generation: int = 0

    @property
    def remaining_us(self) -> int:
        return max(0, self.duration_us - self.consumed_us)


@dataclass(frozen=True, slots=True)
class StageSession:
    """One stage run (epoch) and its entire recursive worker tree.

    ``root_id`` is the parcel's issue session root the run executes in; successive runs
    share it (see :class:`IssueSession`). Fences, grant, credentials and lifecycle belong
    to the run only.
    """

    session_id: str
    kind: SessionKind
    attempt: int
    authorization_id: str
    nonce: str
    revision: int
    lifecycle: Lifecycle
    grant: Grant
    fences: frozenset[FenceKind] = frozenset()
    root_id: str | None = None
    drain_target: Lifecycle | None = None
    wait_reason: WaitReason | None = None
    execution_closed: bool = False
    quiescent: bool = False
    external_active: bool = False
    message_unknown: bool = False
    restart_count: int = 0
    correction_count: int = 0
    restart_pending: bool = False
    prepared: bool = False
    own_items: tuple[str, ...] = ()
    report_published: bool = False
    #: The reducer last emitted ENABLE_ISSUANCE for this session (reconcile skips a repeat).
    issuance_enabled: bool = False
    #: The root's exact factory policy set (caller identity included) was re-verified after
    #: its cross-replica propagation barrier. False closes the work gate: no credential,
    #: no work message. Runs recorded before this field existed default to verified.
    policy_ready: bool = True
    #: Not-before time of the pending post-barrier policy verification (0: none pending).
    policy_ready_at_us: int = 0


@dataclass(frozen=True, slots=True)
class IssueSession:
    """The parcel's one Omnigent root conversation, reused by successive stage runs.

    Stage runs (:class:`StageSession`) carry the fences, grant and credentials; the issue
    session carries none of them, so stopping or revoking a run never fences the
    conversation itself. It is replaced only when it is dead/unusable (``generation``
    increments) and archived once the parcel is terminal.
    """

    root_id: str
    #: The ``factory.dispatch`` label the root was created with (its creating run's nonce).
    nonce: str
    #: The stage run whose create produced this root.
    created_by: str
    generation: int = 1
    status: IssueSessionStatus = IssueSessionStatus.LIVE
    #: Why it was retired (dead/unusable reason); audit only.
    reason: str = ""

    @property
    def reusable(self) -> bool:
        return self.status == IssueSessionStatus.LIVE


@dataclass(frozen=True, slots=True)
class Decision:
    """An owner decision mirrored from a native Omnigent elicitation."""

    decision_id: str
    session_id: str
    elicitation_id: str
    revision: int
    impact: DecisionImpact
    status: DecisionStatus
    checkpoint_prompt: bool = False
    answer: str | None = None
    answer_event_id: str | None = None
    externally_resolved: bool = False
    prompt_lost: bool = False
    source: DecisionSource = DecisionSource.ELICITATION


@dataclass(frozen=True, slots=True)
class Readiness:
    """A build-ready attestation and the evidence verified against it."""

    session_id: str
    pr_number: int
    head_sha: str
    verified: bool = False
    ready: bool = False
    #: Human summary of CI at verification, e.g. "17 checks: 13 success, 4 skipped".
    checks_summary: str = ""
    #: The head the agent's accepted review attested ("" = ``head_sha``, older records).
    reviewed_head: str = ""


@dataclass(frozen=True, slots=True)
class UnknownEffect:
    """An external write whose outcome is ambiguous (§3.4).

    While any is recorded the parcel is Blocked: no successor tree, no Ready, and (for
    message/elicitation kinds) no further work-bearing relay. Only a correlation-checked
    acknowledgement or reconciliation of this exact ``effect_id`` clears it.
    """

    effect_id: str
    kind: str
    session_id: str | None = None


@dataclass(frozen=True, slots=True)
class PendingMove:
    """A daemon board write not yet observed landed (own-write acknowledgement).

    A snapshot showing ``from_stage`` predates the write; one showing ``to_stage`` (or
    the write's webhook echo) acknowledges it. Anything else is an external move.
    """

    effect_id: str
    from_stage: Stage | None
    to_stage: Stage


@dataclass(frozen=True, slots=True)
class QueueEntry:
    parcel_id: str
    approval_id: str
    sequence: int
    status: QueueStatus


@dataclass(frozen=True, slots=True)
class Reservation:
    reservation_id: str
    parcel_id: str
    kind: ReservationKind
    episode_id: str
    pr_number: int | None = None
    live: bool = True


@dataclass(frozen=True, slots=True)
class Parcel:
    """Per-issue aggregate. ``parcel_id`` is the stable issue node ID."""

    parcel_id: str
    repo_id: str
    issue_number: int | None = None
    version: int = 0
    stage: Stage | None = None
    eligible: bool = False
    in_project: bool = False
    eligibility_epoch: int = 0
    barrier_time_us: int = 0
    revision: int = 0
    revision_pending: bool = False
    revision_feedback: tuple[str, ...] = ()
    size: Size | None = None
    issue_edit_count: int = 0
    contracts: tuple[Contract, ...] = ()
    current_contract_id: str | None = None
    approvals: tuple[Approval, ...] = ()
    current_approval_id: str | None = None
    authorizations: tuple[StageAuthorization, ...] = ()
    pending_authorization_id: str | None = None
    sessions: tuple[StageSession, ...] = ()
    current_session_id: str | None = None
    decisions: tuple[Decision, ...] = ()
    holds: frozenset[Hold] = frozenset()
    readiness: Readiness | None = None
    #: Readiness fix wakes sent to the build session for the current approval (at most 1).
    readiness_wakes: int = 0
    pr_number: int | None = None
    bot: BotState = BotState.IDLE
    #: The in-flight daemon board write: at most one per parcel (serialised).
    pending_moves: tuple[PendingMove, ...] = ()
    #: Coalesced desired column awaiting the in-flight write's trusted outcome.
    queued_move: Stage | None = None
    unknown_effects: tuple[UnknownEffect, ...] = ()
    #: Work-bearing effects this reducer issued, as (effect_id, session_id): an
    #: acknowledgement must name one of these exactly.
    sent_effects: tuple[tuple[str, str], ...] = ()
    #: Inbox deliveries that could not be interpreted for this parcel. While any is
    #: present the parcel is not dispatchable; releasing one restores no authority.
    inbox_holds: tuple[InboxHold, ...] = ()
    #: The Omnigent conversation reused by every stage run of this issue (None before the
    #: first create is adopted).
    issue_session: IssueSession | None = None
    applied_event_ids: frozenset[str] = frozenset()

    def session(self, session_id: str | None) -> StageSession | None:
        if session_id is None:
            return None
        for s in self.sessions:
            if s.session_id == session_id:
                return s
        return None

    @property
    def current_session(self) -> StageSession | None:
        return self.session(self.current_session_id)

    def approval(self, approval_id: str | None) -> Approval | None:
        if approval_id is None:
            return None
        for a in self.approvals:
            if a.approval_id == approval_id:
                return a
        return None

    @property
    def current_approval(self) -> Approval | None:
        return self.approval(self.current_approval_id)

    def contract(self, contract_id: str | None) -> Contract | None:
        if contract_id is None:
            return None
        for c in self.contracts:
            if c.contract_id == contract_id:
                return c
        return None

    @property
    def current_contract(self) -> Contract | None:
        return self.contract(self.current_contract_id)

    def authorization(self, authorization_id: str | None) -> StageAuthorization | None:
        if authorization_id is None:
            return None
        for a in self.authorizations:
            if a.authorization_id == authorization_id:
                return a
        return None

    def decision(self, decision_id: str) -> Decision | None:
        for d in self.decisions:
            if d.decision_id == decision_id:
                return d
        return None

    def pending_move(self, effect_id: str | None) -> PendingMove | None:
        for m in self.pending_moves:
            if m.effect_id == effect_id:
                return m
        return None

    def unknown_effect(self, effect_id: str) -> UnknownEffect | None:
        for u in self.unknown_effects:
            if u.effect_id == effect_id:
                return u
        return None

    @property
    def open_decisions(self) -> tuple[Decision, ...]:
        """Unanswered material owner decisions (checkpoint prompts excluded)."""
        return tuple(
            d for d in self.decisions if d.status == DecisionStatus.OPEN and not d.checkpoint_prompt
        )


@dataclass(frozen=True, slots=True)
class AdmissionSnapshot:
    """Repository-wide admission state shared by all parcels of one repository."""

    repo_id: str
    paused: bool = False
    next_sequence: int = 1
    queue: tuple[QueueEntry, ...] = ()
    reservations: tuple[Reservation, ...] = ()
    open_bot_prs: frozenset[int] = frozenset()

    def queue_entry(self, parcel_id: str) -> QueueEntry | None:
        for q in self.queue:
            if q.parcel_id == parcel_id:
                return q
        return None

    def live_reservations(self, kind: ReservationKind) -> tuple[Reservation, ...]:
        return tuple(r for r in self.reservations if r.live and r.kind == kind)

    @property
    def building_count(self) -> int:
        return len(self.live_reservations(ReservationKind.BUILDING))

    @property
    def prospective_pr_count(self) -> int:
        """Open App-authored PRs plus live PR reservations not yet bound to one of them."""
        unbound = [
            r
            for r in self.live_reservations(ReservationKind.OPEN_PR)
            if r.pr_number is None or r.pr_number not in self.open_bot_prs
        ]
        return len(self.open_bot_prs) + len(unbound)


@dataclass(frozen=True, slots=True)
class TrustedConfig:
    """Host-trusted configuration snapshot (the trust root; §4 Configuration)."""

    repo_id: str
    owners: frozenset[int]
    max_building: int = 1
    max_open_bot_prs: int = 3
    block_hours: dict[Size, int] = field(default_factory=lambda: {Size.S: 2, Size.M: 4, Size.L: 6})
    grace_us: int = 15 * MICROS_PER_MINUTE
    max_grant_us: int = 12 * MICROS_PER_HOUR
    cost_usd_per_hour_micros: int = 35_000_000
    phase4_enabled: bool = False

    def block_us(self, size: Size) -> int:
        return self.block_hours[size] * MICROS_PER_HOUR


@dataclass(frozen=True, slots=True)
class State:
    """Reducer input/output: one parcel aggregate plus repository admission and config.

    ``parcel`` is ``None`` only for repository-global operator events (pause/unpause).
    """

    parcel: Parcel | None
    admission: AdmissionSnapshot
    config: TrustedConfig
