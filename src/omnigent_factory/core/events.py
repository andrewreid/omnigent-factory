"""Normalized event catalog (architecture §2.5).

An :class:`Event` is the persisted input envelope: a stable logical ID, provenance,
logical source time, optional fresh-read evidence, opaque entropy for nonce derivation,
and exactly one typed body. Adapters (GitHub normalizer, Omnigent observer, scheduler,
operator CLI) construct events; only the reducer interprets them.

Each body class declares its :class:`EventKind` and :class:`EventClass`. Adding a kind
requires a reducer handler; ``tests/test_reducer_tables.py`` enforces totality.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import ClassVar

from omnigent_factory.core.types import (
    DecisionImpact,
    InboxHoldReason,
    IssueSnapshot,
    SessionKind,
    Size,
    Stage,
    Via,
)


class EventClass(enum.StrEnum):
    CONTROL = "control"
    SAFETY = "safety"
    OBSERVATION = "observation"


class Provenance(enum.StrEnum):
    """Who vouches for an event. Controls require an authenticated GitHub source."""

    WEBHOOK = "webhook"  # verified signed delivery
    RECOVERY = "recovery"  # original bytes re-fetched via authenticated App delivery GET
    RECONCILER = "reconciler"  # current-state read by the daemon
    ADAPTER = "adapter"  # effect completion / Omnigent observation
    SCHEDULER = "scheduler"  # trusted clock/timer input
    OPERATOR = "operator"  # local protected CLI socket
    INBOX = "inbox"  # the daemon's durable delivery inbox (restrict-only holds)
    MCP = "mcp"  # a factory tool call from the current run's issue session (loopback MCP)


class EventKind(enum.StrEnum):
    # control
    REQUEST_TRIAGE = "RequestTriage"
    REQUEST_PLAN = "RequestPlan"
    REQUEST_REPLAN = "RequestReplan"
    PLAN_FEEDBACK = "PlanFeedback"
    APPROVE_PLAN = "ApprovePlan"
    WAIVE_PLAN = "WaivePlan"
    DECIDE = "Decide"
    CONTINUE = "Continue"
    STOP = "Stop"
    REQUEST_REWORK = "RequestRework"
    PAUSE = "Pause"
    UNPAUSE = "Unpause"
    OPERATOR_RESUME = "OperatorResume"
    # safety
    LEFTWARD_MOVE = "LeftwardMove"
    ASSIGNED_HUMAN = "AssignedHuman"
    CLOSED = "Closed"
    TRANSFERRED = "Transferred"
    DELETED = "Deleted"
    ITEM_REMOVED = "ItemRemoved"
    APPROVAL_INVALIDATED = "ApprovalInvalidated"
    WAIVER_EDITED = "WaiverEdited"
    CONTRACT_TAMPERED = "ContractTampered"
    INBOX_HOLD_SET = "InboxHoldSet"
    INBOX_HOLD_RELEASED = "InboxHoldReleased"
    # observation: GitHub
    GITHUB_SNAPSHOT = "GitHubSnapshot"
    COLUMN_OBSERVED = "ColumnObserved"
    PR_OBSERVED = "PRObserved"
    CHECKS_CHANGED = "ChecksChanged"
    REVIEW_CHANGED = "ReviewChanged"
    READINESS_EVIDENCE = "ReadinessEvidence"
    CONTRACT_PUBLISHED = "ContractPublished"
    PUBLICATION_ACKED = "PublicationAcked"
    # observation: Omnigent / effects
    SESSION_CREATED = "SessionCreated"
    CREATE_REJECTED = "CreateRejected"
    ADOPTION_RESULT = "AdoptionResult"
    EFFECT_UNKNOWN = "EffectUnknown"
    EFFECT_CANCELLED = "EffectCancelled"
    PREPARED = "Prepared"
    MESSAGE_ACK = "MessageAck"
    EFFECT_RECONCILED = "EffectReconciled"
    RUNTIME_ACTIVITY = "RuntimeActivity"
    OWNER_DIRECT_MESSAGE = "OwnerDirectOmnigentMessage"
    ELICITATION_OPENED = "ElicitationOpened"
    ELICITATION_RESOLVED = "ElicitationResolved"
    ELICITATION_GONE = "ElicitationGone"
    OWNER_QUESTION = "OwnerQuestion"
    POLICIES_VERIFIED = "PoliciesVerified"
    POLICY_GUARD_FAILED = "PolicyGuardFailed"
    RESULT_CANDIDATE = "ResultCandidate"
    ISSUE_SESSION_CLOSED = "IssueSessionClosed"
    TREE_QUIESCENT = "TreeQuiescent"
    STOP_TIMEOUT = "StopTimeout"
    SESSION_CRASHED = "SessionCrashed"
    ACTIVE_TIME_SAMPLE = "ActiveTimeSample"
    COST_SAMPLE = "CostSample"
    POLICY_READY = "PolicyReady"
    # observation: scheduler / derived safety
    ACTIVE_LIMIT_REACHED = "ActiveLimitReached"
    GRACE_EXPIRED = "GraceExpired"
    CAPACITY_AVAILABLE = "CapacityAvailable"
    RETRY_DUE = "RetryDue"
    RECONCILE_DUE = "ReconcileDue"


class ResultKind(enum.StrEnum):
    TRIAGE = "triage"
    PLAN = "plan"
    BUILD_READY = "build_ready"
    CHECKPOINT = "checkpoint"
    BLOCKED = "blocked"


class PublicationKind(enum.StrEnum):
    CONTRACT = "contract"
    INFO = "info"


class ChecksState(enum.StrEnum):
    """Automated required-check state for one head, human-review-gate excluded."""

    GREEN = "green"
    PENDING = "pending"
    FAILED = "failed"


class _Body:
    KIND: ClassVar[EventKind]
    CLASS: ClassVar[EventClass]


# ---------------------------------------------------------------- control bodies


@dataclass(frozen=True, slots=True)
class RequestTriage(_Body):
    KIND: ClassVar[EventKind] = EventKind.REQUEST_TRIAGE
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    via: Via = Via.COMMAND


@dataclass(frozen=True, slots=True)
class RequestPlan(_Body):
    KIND: ClassVar[EventKind] = EventKind.REQUEST_PLAN
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    via: Via = Via.COMMAND


@dataclass(frozen=True, slots=True)
class RequestReplan(_Body):
    KIND: ClassVar[EventKind] = EventKind.REQUEST_REPLAN
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    via: Via = Via.COMMAND


@dataclass(frozen=True, slots=True)
class PlanFeedback(_Body):
    """Plain non-command owner text: an issue comment, or on the parcel's PR a
    conversation comment or a review with text (``pr_number`` > 0). ``text_digest`` only."""

    KIND: ClassVar[EventKind] = EventKind.PLAN_FEEDBACK
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    text_digest: str = ""
    pr_number: int = 0


@dataclass(frozen=True, slots=True)
class ApprovePlan(_Body):
    """Drag Scoped→Building or ``/approve [H] [for D]``. ``hash_text`` is the typed H."""

    KIND: ClassVar[EventKind] = EventKind.APPROVE_PLAN
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    via: Via = Via.DRAG
    hash_text: str | None = None
    duration_us: int | None = None


@dataclass(frozen=True, slots=True)
class WaivePlan(_Body):
    """Stage-skip drag or owner ``factory:build``. Snapshot comes from ``Event.evidence``."""

    KIND: ClassVar[EventKind] = EventKind.WAIVE_PLAN
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    via: Via = Via.DRAG
    duration_us: int | None = None


@dataclass(frozen=True, slots=True)
class Decide(_Body):
    """``/decide`` or a quoting reply. ``within_contract`` is the owner choosing an offered
    in-contract option; anything else takes the plan-revision path."""

    KIND: ClassVar[EventKind] = EventKind.DECIDE
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    decision_id: str | None = None
    answer: str = ""
    within_contract: bool = False


@dataclass(frozen=True, slots=True)
class Continue(_Body):
    KIND: ClassVar[EventKind] = EventKind.CONTINUE
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    duration_us: int | None = None


@dataclass(frozen=True, slots=True)
class Stop(_Body):
    KIND: ClassVar[EventKind] = EventKind.STOP
    CLASS: ClassVar[EventClass] = EventClass.CONTROL


@dataclass(frozen=True, slots=True)
class RequestRework(_Body):
    """Explicit owner rework control from Ready (same approval, new build episode)."""

    KIND: ClassVar[EventKind] = EventKind.REQUEST_REWORK
    CLASS: ClassVar[EventClass] = EventClass.CONTROL


@dataclass(frozen=True, slots=True)
class Pause(_Body):
    KIND: ClassVar[EventKind] = EventKind.PAUSE
    CLASS: ClassVar[EventClass] = EventClass.CONTROL


@dataclass(frozen=True, slots=True)
class OperatorResume(_Body):
    """Local operator: re-open an existing stage session after a stale block and send it
    one note. Creates no authority, approval or grant; the gate must hold on its own."""

    KIND: ClassVar[EventKind] = EventKind.OPERATOR_RESUME
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    text: str = ""


@dataclass(frozen=True, slots=True)
class Unpause(_Body):
    KIND: ClassVar[EventKind] = EventKind.UNPAUSE
    CLASS: ClassVar[EventClass] = EventClass.CONTROL


# ----------------------------------------------------------------- safety bodies


@dataclass(frozen=True, slots=True)
class LeftwardMove(_Body):
    """A column move left or to Inbox. ``daemon_effect_id`` marks our own write's echo."""

    KIND: ClassVar[EventKind] = EventKind.LEFTWARD_MOVE
    CLASS: ClassVar[EventClass] = EventClass.SAFETY
    from_stage: Stage | None = None
    to_stage: Stage | None = None
    daemon_effect_id: str | None = None


@dataclass(frozen=True, slots=True)
class AssignedHuman(_Body):
    KIND: ClassVar[EventKind] = EventKind.ASSIGNED_HUMAN
    CLASS: ClassVar[EventClass] = EventClass.SAFETY


@dataclass(frozen=True, slots=True)
class Closed(_Body):
    KIND: ClassVar[EventKind] = EventKind.CLOSED
    CLASS: ClassVar[EventClass] = EventClass.SAFETY


@dataclass(frozen=True, slots=True)
class Transferred(_Body):
    KIND: ClassVar[EventKind] = EventKind.TRANSFERRED
    CLASS: ClassVar[EventClass] = EventClass.SAFETY


@dataclass(frozen=True, slots=True)
class Deleted(_Body):
    KIND: ClassVar[EventKind] = EventKind.DELETED
    CLASS: ClassVar[EventClass] = EventClass.SAFETY


@dataclass(frozen=True, slots=True)
class ItemRemoved(_Body):
    KIND: ClassVar[EventKind] = EventKind.ITEM_REMOVED
    CLASS: ClassVar[EventClass] = EventClass.SAFETY


@dataclass(frozen=True, slots=True)
class ApprovalInvalidated(_Body):
    KIND: ClassVar[EventKind] = EventKind.APPROVAL_INVALIDATED
    CLASS: ClassVar[EventClass] = EventClass.SAFETY
    approval_id: str = ""
    reason: str = ""


@dataclass(frozen=True, slots=True)
class WaiverEdited(_Body):
    """A verified issue title/body edit. Always counted, even if later reverted."""

    KIND: ClassVar[EventKind] = EventKind.WAIVER_EDITED
    CLASS: ClassVar[EventClass] = EventClass.SAFETY


@dataclass(frozen=True, slots=True)
class ContractTampered(_Body):
    KIND: ClassVar[EventKind] = EventKind.CONTRACT_TAMPERED
    CLASS: ClassVar[EventClass] = EventClass.SAFETY
    contract_id: str = ""


@dataclass(frozen=True, slots=True)
class InboxHoldSet(_Body):
    """A durable delivery for this parcel was parked or could not be verified.

    Uninterpretable input might have been a safety fact (a Stop, a leftward drag), so it
    is treated as one: barrier, queued authority cancelled, current tree fenced (safety)
    and interrupted, and no dispatch while the hold remains.
    """

    KIND: ClassVar[EventKind] = EventKind.INBOX_HOLD_SET
    CLASS: ClassVar[EventClass] = EventClass.SAFETY
    delivery_guid: str = ""
    reason: InboxHoldReason = InboxHoldReason.PARKED


@dataclass(frozen=True, slots=True)
class InboxHoldReleased(_Body):
    """The held delivery was released (operator) or resolved (inbox).

    Removes only that hold. It never clears a fence or restores authority: recovery needs
    a fresh owner stage control, as after any safety fact.
    """

    KIND: ClassVar[EventKind] = EventKind.INBOX_HOLD_RELEASED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    delivery_guid: str = ""


# ------------------------------------------------------------ observation bodies


@dataclass(frozen=True, slots=True)
class GitHubSnapshot(_Body):
    """Reconciler read. The snapshot itself travels in ``Event.evidence``."""

    KIND: ClassVar[EventKind] = EventKind.GITHUB_SNAPSHOT
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION


@dataclass(frozen=True, slots=True)
class ColumnObserved(_Body):
    """A non-control column observation (daemon echo, non-owner rightward move)."""

    KIND: ClassVar[EventKind] = EventKind.COLUMN_OBSERVED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    stage: Stage | None = None
    daemon_effect_id: str | None = None


@dataclass(frozen=True, slots=True)
class PRObserved(_Body):
    KIND: ClassVar[EventKind] = EventKind.PR_OBSERVED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    pr_number: int = 0
    head_sha: str = ""
    open: bool = True
    merged: bool = False
    bot_authored: bool = False
    parcel_branch: bool = False


@dataclass(frozen=True, slots=True)
class ChecksChanged(_Body):
    KIND: ClassVar[EventKind] = EventKind.CHECKS_CHANGED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    pr_number: int = 0
    head_sha: str = ""
    state: ChecksState = ChecksState.PENDING


@dataclass(frozen=True, slots=True)
class ReviewChanged(_Body):
    KIND: ClassVar[EventKind] = EventKind.REVIEW_CHANGED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    pr_number: int = 0
    head_sha: str = ""
    changes_requested: bool = False


@dataclass(frozen=True, slots=True)
class ReadinessEvidence(_Body):
    """Result of verifying a build-ready attestation against GitHub (§7.2, §8)."""

    KIND: ClassVar[EventKind] = EventKind.READINESS_EVIDENCE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    pr_number: int = 0
    head_sha: str = ""
    verified: bool = False
    remediation_exhausted: bool = False
    checks_summary: str = ""
    #: The PR head GitHub reported (``head_sha`` is the head the read was asked about).
    observed_head_sha: str = ""
    #: Current-head check state; None for reads recorded before it was carried.
    checks: ChecksState | None = None
    findings_open: bool = False
    review_accepted: bool = True
    pr_open: bool = True
    merged: bool = False
    #: GitHub's closingIssuesReferences of the PR include the parcel's issue.
    closes_issue: bool = True
    #: The review bot's latest unanswered trigger on this head (source time), 0 = it
    #: already answered this head, None = unknown (older reads or no bot configured).
    review_bot_pending_since_us: int | None = None
    #: The review was carried to this head only because every newer commit merely
    #: syncs the base branch (owner "Update branch"): a red check came from the base.
    base_sync: bool = False
    #: Names of the failing required checks, "; "-separated (names may contain commas).
    failing_checks: str = ""


@dataclass(frozen=True, slots=True)
class ContractPublished(_Body):
    """Bot comment created/adopted and its exact bytes/author/revision verified."""

    KIND: ClassVar[EventKind] = EventKind.CONTRACT_PUBLISHED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    contract_id: str = ""
    comment_id: str = ""
    verified: bool = False
    posted_at_us: int = 0


@dataclass(frozen=True, slots=True)
class PublicationAcked(_Body):
    """A triage/report/status comment was created or adopted by its effect marker."""

    KIND: ClassVar[EventKind] = EventKind.PUBLICATION_ACKED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    effect_id: str = ""
    effect_kind: str = ""
    session_id: str | None = None
    comment_id: str = ""


@dataclass(frozen=True, slots=True)
class SessionCreated(_Body):
    """Create acknowledged, or exactly one verified adoption of the recorded tuple."""

    KIND: ClassVar[EventKind] = EventKind.SESSION_CREATED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    root_id: str = ""
    nonce: str = ""


@dataclass(frozen=True, slots=True)
class CreateRejected(_Body):
    """Definitive pre-create rejection that the server proves had no side effect."""

    KIND: ClassVar[EventKind] = EventKind.CREATE_REJECTED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    reason: str = ""


@dataclass(frozen=True, slots=True)
class AdoptionResult(_Body):
    """Outcome of a nonce search after an ambiguous create."""

    KIND: ClassVar[EventKind] = EventKind.ADOPTION_RESULT
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    matches: int = 0
    root_id: str | None = None
    nonce: str = ""


@dataclass(frozen=True, slots=True)
class EffectUnknown(_Body):
    """An external write whose outcome is ambiguous (lost ack, restart during POST)."""

    KIND: ClassVar[EventKind] = EventKind.EFFECT_UNKNOWN
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    effect_id: str = ""
    effect_kind: str = ""
    session_id: str | None = None


@dataclass(frozen=True, slots=True)
class EffectCancelled(_Body):
    """The intent will not run: a stale precondition before any external call, or
    (``failed``) a definitive adapter failure that proved nothing happened."""

    KIND: ClassVar[EventKind] = EventKind.EFFECT_CANCELLED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    effect_id: str = ""
    effect_kind: str = ""
    session_id: str | None = None
    failed: bool = False


@dataclass(frozen=True, slots=True)
class Prepared(_Body):
    """``unusable``: the reused issue session is dead, archived, foreign or too full; the
    run is re-created in a fresh issue session instead of failing."""

    KIND: ClassVar[EventKind] = EventKind.PREPARED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    ok: bool = False
    unexpected_turn: bool = False
    unusable: bool = False
    reason: str = ""
    #: Cross-replica policy propagation barrier of the policy set just attached.
    policy_ready_at_us: int = 0


@dataclass(frozen=True, slots=True)
class MessageAck(_Body):
    """Acknowledged (or marker-adopted) own item for exactly ``effect_id``."""

    KIND: ClassVar[EventKind] = EventKind.MESSAGE_ACK
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    effect_id: str = ""
    item_id: str = ""


@dataclass(frozen=True, slots=True)
class EffectReconciled(_Body):
    """Resolution of an ambiguous write for exactly ``effect_id`` (§3.4).

    ``delivered`` true: our marked item/comment was found (``item_id``). False: a complete
    paginated history plus native pending-input scan proves absence, or an operator
    repaired it with evidence. Accepted only from the adapter or operator provenance.
    """

    KIND: ClassVar[EventKind] = EventKind.EFFECT_RECONCILED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    effect_id: str = ""
    session_id: str | None = None
    delivered: bool = False
    item_id: str = ""


@dataclass(frozen=True, slots=True)
class RuntimeActivity(_Body):
    KIND: ClassVar[EventKind] = EventKind.RUNTIME_ACTIVITY
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    busy: bool = False


@dataclass(frozen=True, slots=True)
class OwnerDirectOmnigentMessage(_Body):
    KIND: ClassVar[EventKind] = EventKind.OWNER_DIRECT_MESSAGE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    item_id: str = ""


@dataclass(frozen=True, slots=True)
class ElicitationOpened(_Body):
    KIND: ClassVar[EventKind] = EventKind.ELICITATION_OPENED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    elicitation_id: str = ""
    impact: DecisionImpact = DecisionImpact.UNKNOWN
    cost_ask: bool = False
    #: Sanitised, truncated description of what is asked (prompt text / tool + args).
    summary: str = ""
    #: Omnigent session (root or child) that holds the pending prompt.
    node_id: str | None = None


@dataclass(frozen=True, slots=True)
class ElicitationResolved(_Body):
    """``correlated`` is true only for the daemon's own recorded resolve."""

    KIND: ClassVar[EventKind] = EventKind.ELICITATION_RESOLVED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    elicitation_id: str = ""
    correlated: bool = False


@dataclass(frozen=True, slots=True)
class ElicitationGone(_Body):
    KIND: ClassVar[EventKind] = EventKind.ELICITATION_GONE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    elicitation_id: str = ""


@dataclass(frozen=True, slots=True)
class OwnerQuestion(_Body):
    """``factory_ask_owner`` from the current run: one structured owner question.

    ``question_key`` is stable per question within the run (idempotency); the answer
    arrives through the normal owner decision path and is relayed as a message.
    """

    KIND: ClassVar[EventKind] = EventKind.OWNER_QUESTION
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    question_key: str = ""
    summary: str = ""
    impact: DecisionImpact = DecisionImpact.UNKNOWN


@dataclass(frozen=True, slots=True)
class PoliciesVerified(_Body):
    """Outcome of a VERIFY_POLICIES effect for one run.

    ``reconciled``: the set was (re)established, and ``ready_at_us`` is its new barrier (a
    verification follows). Otherwise ``ok`` reports the post-barrier exact-set check.
    """

    KIND: ClassVar[EventKind] = EventKind.POLICIES_VERIFIED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    ok: bool = False
    reconciled: bool = False
    ready_at_us: int = 0


@dataclass(frozen=True, slots=True)
class PolicyGuardFailed(_Body):
    """Boot found a live run's factory policies (caller guard included) missing, altered
    or just repaired: its gate closes until reconciliation, barrier and verification."""

    KIND: ClassVar[EventKind] = EventKind.POLICY_GUARD_FAILED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""


@dataclass(frozen=True, slots=True)
class IssueSessionClosed(_Body):
    """The terminal parcel's issue session was archived (adapter acknowledgement)."""

    KIND: ClassVar[EventKind] = EventKind.ISSUE_SESSION_CLOSED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    root_id: str = ""


@dataclass(frozen=True, slots=True)
class ResultCandidate(_Body):
    """A stage result accepted from ``factory_submit_result``, already validated.

    ``valid`` false means the result failed schema/cross-record validation.
    ``contract_canonical`` is the canonical contract text computed by the daemon.
    """

    KIND: ClassVar[EventKind] = EventKind.RESULT_CANDIDATE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    root_id: str = ""
    revision: int = 0
    valid: bool = False
    result_kind: ResultKind = ResultKind.TRIAGE
    publication_kind: PublicationKind | None = None
    contract_canonical: str | None = None
    size: Size | None = None
    pr_number: int | None = None
    head_sha: str | None = None
    open_decision_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TreeQuiescent(_Body):
    """A recursive tree scan. Only ``complete and not busy`` is quiescence evidence."""

    KIND: ClassVar[EventKind] = EventKind.TREE_QUIESCENT
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    complete: bool = False
    busy: bool = True
    pending_waiter: bool = False


@dataclass(frozen=True, slots=True)
class StopTimeout(_Body):
    KIND: ClassVar[EventKind] = EventKind.STOP_TIMEOUT
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""


@dataclass(frozen=True, slots=True)
class SessionCrashed(_Body):
    KIND: ClassVar[EventKind] = EventKind.SESSION_CRASHED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""


@dataclass(frozen=True, slots=True)
class ActiveTimeSample(_Body):
    """Conservative (upper-bound) active-time consumption for a grant."""

    KIND: ClassVar[EventKind] = EventKind.ACTIVE_TIME_SAMPLE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    grant_id: str = ""
    consumed_us: int = 0


@dataclass(frozen=True, slots=True)
class CostSample(_Body):
    KIND: ClassVar[EventKind] = EventKind.COST_SAMPLE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    microdollars: int | None = None


@dataclass(frozen=True, slots=True)
class PolicyReady(_Body):
    """Replacement cost policy verified and its propagation barrier satisfied."""

    KIND: ClassVar[EventKind] = EventKind.POLICY_READY
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    grant_id: str = ""


@dataclass(frozen=True, slots=True)
class ActiveLimitReached(_Body):
    KIND: ClassVar[EventKind] = EventKind.ACTIVE_LIMIT_REACHED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    grant_id: str = ""


@dataclass(frozen=True, slots=True)
class GraceExpired(_Body):
    KIND: ClassVar[EventKind] = EventKind.GRACE_EXPIRED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    grant_id: str = ""


@dataclass(frozen=True, slots=True)
class CapacityAvailable(_Body):
    KIND: ClassVar[EventKind] = EventKind.CAPACITY_AVAILABLE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION


@dataclass(frozen=True, slots=True)
class RetryDue(_Body):
    KIND: ClassVar[EventKind] = EventKind.RETRY_DUE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    effect_id: str = ""


@dataclass(frozen=True, slots=True)
class ReconcileDue(_Body):
    KIND: ClassVar[EventKind] = EventKind.RECONCILE_DUE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION


EventBody = (
    RequestTriage
    | RequestPlan
    | RequestReplan
    | PlanFeedback
    | ApprovePlan
    | WaivePlan
    | Decide
    | Continue
    | Stop
    | RequestRework
    | Pause
    | OperatorResume
    | Unpause
    | LeftwardMove
    | AssignedHuman
    | Closed
    | Transferred
    | Deleted
    | ItemRemoved
    | ApprovalInvalidated
    | WaiverEdited
    | ContractTampered
    | InboxHoldSet
    | InboxHoldReleased
    | GitHubSnapshot
    | ColumnObserved
    | PRObserved
    | ChecksChanged
    | ReviewChanged
    | ReadinessEvidence
    | ContractPublished
    | PublicationAcked
    | SessionCreated
    | CreateRejected
    | AdoptionResult
    | EffectUnknown
    | EffectCancelled
    | Prepared
    | MessageAck
    | EffectReconciled
    | RuntimeActivity
    | OwnerDirectOmnigentMessage
    | ElicitationOpened
    | ElicitationResolved
    | ElicitationGone
    | OwnerQuestion
    | PoliciesVerified
    | PolicyGuardFailed
    | ResultCandidate
    | IssueSessionClosed
    | TreeQuiescent
    | StopTimeout
    | SessionCrashed
    | ActiveTimeSample
    | CostSample
    | PolicyReady
    | ActiveLimitReached
    | GraceExpired
    | CapacityAvailable
    | RetryDue
    | ReconcileDue
)

#: Every body class, keyed by kind. Used by the codec and totality tests.
BODY_TYPES: dict[EventKind, type[_Body]] = {
    cls.KIND: cls
    for cls in (
        RequestTriage,
        RequestPlan,
        RequestReplan,
        PlanFeedback,
        ApprovePlan,
        WaivePlan,
        Decide,
        Continue,
        Stop,
        RequestRework,
        Pause,
        OperatorResume,
        Unpause,
        LeftwardMove,
        AssignedHuman,
        Closed,
        Transferred,
        Deleted,
        ItemRemoved,
        ApprovalInvalidated,
        WaiverEdited,
        ContractTampered,
        InboxHoldSet,
        InboxHoldReleased,
        GitHubSnapshot,
        ColumnObserved,
        PRObserved,
        ChecksChanged,
        ReviewChanged,
        ReadinessEvidence,
        ContractPublished,
        PublicationAcked,
        SessionCreated,
        CreateRejected,
        AdoptionResult,
        EffectUnknown,
        EffectCancelled,
        Prepared,
        MessageAck,
        EffectReconciled,
        RuntimeActivity,
        OwnerDirectOmnigentMessage,
        ElicitationOpened,
        ElicitationResolved,
        ElicitationGone,
        OwnerQuestion,
        PoliciesVerified,
        PolicyGuardFailed,
        ResultCandidate,
        IssueSessionClosed,
        TreeQuiescent,
        StopTimeout,
        SessionCrashed,
        ActiveTimeSample,
        CostSample,
        PolicyReady,
        ActiveLimitReached,
        GraceExpired,
        CapacityAvailable,
        RetryDue,
        ReconcileDue,
    )
}

#: Kinds that act on repository admission rather than a parcel.
GLOBAL_KINDS = frozenset({EventKind.PAUSE, EventKind.UNPAUSE})

#: Provenances that may carry an owner GitHub control.
CONTROL_PROVENANCES = frozenset({Provenance.WEBHOOK, Provenance.RECOVERY})


@dataclass(frozen=True, slots=True)
class Event:
    """Persisted reducer input envelope.

    ``event_id`` is the stable logical key (comment ID, label-event identity, project
    field-change identity, effect completion ID...), not the delivery GUID: different
    deliveries of the same logical event share it. ``entropy`` is an opaque random value
    chosen at ingest and persisted with the event; the reducer derives unpredictable but
    replay-stable nonces from it.
    """

    event_id: str
    repo_id: str
    parcel_id: str | None
    source_time_us: int
    provenance: Provenance
    body: EventBody
    actor_id: int | None = None
    issue_number: int | None = None
    entropy: str = ""
    evidence: IssueSnapshot | None = None
    delivery_guid: str | None = None

    @property
    def kind(self) -> EventKind:
        return self.body.KIND

    @property
    def event_class(self) -> EventClass:
        return self.body.CLASS


__all__ = [
    "BODY_TYPES",
    "CONTROL_PROVENANCES",
    "GLOBAL_KINDS",
    "ActiveLimitReached",
    "ActiveTimeSample",
    "AdoptionResult",
    "ApprovalInvalidated",
    "ApprovePlan",
    "AssignedHuman",
    "CapacityAvailable",
    "ChecksChanged",
    "ChecksState",
    "Closed",
    "ColumnObserved",
    "Continue",
    "ContractPublished",
    "ContractTampered",
    "CostSample",
    "CreateRejected",
    "Decide",
    "DecisionImpact",
    "Deleted",
    "EffectCancelled",
    "EffectReconciled",
    "EffectUnknown",
    "ElicitationGone",
    "ElicitationOpened",
    "ElicitationResolved",
    "Event",
    "EventBody",
    "EventClass",
    "EventKind",
    "GitHubSnapshot",
    "GraceExpired",
    "InboxHoldReleased",
    "InboxHoldSet",
    "IssueSessionClosed",
    "ItemRemoved",
    "LeftwardMove",
    "MessageAck",
    "OperatorResume",
    "OwnerDirectOmnigentMessage",
    "OwnerQuestion",
    "PRObserved",
    "Pause",
    "PlanFeedback",
    "PoliciesVerified",
    "PolicyGuardFailed",
    "PolicyReady",
    "Prepared",
    "Provenance",
    "PublicationAcked",
    "PublicationKind",
    "ReadinessEvidence",
    "ReconcileDue",
    "RequestPlan",
    "RequestReplan",
    "RequestRework",
    "RequestTriage",
    "ResultCandidate",
    "ResultKind",
    "RetryDue",
    "ReviewChanged",
    "RuntimeActivity",
    "SessionCrashed",
    "SessionCreated",
    "SessionKind",
    "Size",
    "Stop",
    "StopTimeout",
    "Transferred",
    "TreeQuiescent",
    "Unpause",
    "WaivePlan",
    "WaiverEdited",
]
