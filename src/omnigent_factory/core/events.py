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
    AUTO_TRIAGE = "AutoTriage"
    AUTO_RETRIAGE = "AutoRetriage"
    CLOSE_CANDIDATE_REMOVED = "CloseCandidateRemoved"
    STALE_HOLD_CLEARED = "StaleHoldCleared"
    AUTO_BUILD_MARKED = "AutoBuildMarked"
    AUTO_BUILD = "AutoBuild"
    AUTOPILOT_MARKED = "AutopilotMarked"
    AUTOPILOT_PLAN = "AutopilotPlan"
    AUTOPILOT_QUEUE = "AutopilotQueue"
    AUTOPILOT_WITHDRAW = "AutopilotWithdraw"
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
    OBSERVATION_FAILED = "ObservationFailed"
    INBOX_HOLD_SET = "InboxHoldSet"
    INBOX_HOLD_RELEASED = "InboxHoldReleased"
    # observation: GitHub
    GITHUB_SNAPSHOT = "GitHubSnapshot"
    BOARD_FIELDS_OBSERVED = "BoardFieldsObserved"
    COLUMN_OBSERVED = "ColumnObserved"
    PR_OBSERVED = "PRObserved"
    OPEN_PR_CAPACITY_RECONCILED = "OpenPRCapacityReconciled"
    CHECKS_CHANGED = "ChecksChanged"
    REVIEW_CHANGED = "ReviewChanged"
    READINESS_EVIDENCE = "ReadinessEvidence"
    BASE_PUSHED = "BasePushed"
    CONTRACT_PUBLISHED = "ContractPublished"
    PUBLICATION_ACKED = "PublicationAcked"
    # observation: Omnigent / effects
    SESSION_CREATED = "SessionCreated"
    CREATE_REJECTED = "CreateRejected"
    ADOPTION_RESULT = "AdoptionResult"
    EFFECT_UNKNOWN = "EffectUnknown"
    EFFECT_RECOVERY_STARTED = "EffectRecoveryStarted"
    EFFECT_CANCELLED = "EffectCancelled"
    PREPARED = "Prepared"
    MESSAGE_ACK = "MessageAck"
    EFFECT_RECONCILED = "EffectReconciled"
    RUNTIME_ACTIVITY = "RuntimeActivity"
    OWNER_DIRECT_MESSAGE = "OwnerDirectOmnigentMessage"
    ELICITATION_OPENED = "ElicitationOpened"
    ELICITATION_RESOLVED = "ElicitationResolved"
    ELICITATION_GONE = "ElicitationGone"
    PERMISSION_PROMPT_DUE = "PermissionPromptDue"
    OWNER_QUESTION = "OwnerQuestion"
    FEEDBACK_READ = "FeedbackRead"
    POLICIES_VERIFIED = "PoliciesVerified"
    POLICY_GUARD_FAILED = "PolicyGuardFailed"
    RESULT_CANDIDATE = "ResultCandidate"
    ISSUE_SESSION_CLOSED = "IssueSessionClosed"
    RELATED_MARKED = "RelatedMarked"
    EPIC_PROGRESS = "EpicProgress"
    TRIAGE_STALE = "TriageStale"
    BUNDLE_HOLD = "BundleHold"
    AGENT_BLOCKED_ON = "AgentBlockedOn"
    AUTOPILOT_STATUS = "AutopilotStatus"
    AUTOPILOT_GATE_CREATED = "AutopilotGateCreated"
    AUTOPILOT_QUESTION = "AutopilotQuestion"
    TREE_QUIESCENT = "TreeQuiescent"
    STOP_TIMEOUT = "StopTimeout"
    SESSION_CRASHED = "SessionCrashed"
    STALL_OBSERVED = "StallObserved"
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


class BotEyes(enum.StrEnum):
    """The review bot's 👀 ("reviewing") reaction for its latest unanswered trigger."""

    #: 👀 on the PR or the re-ping comment, taken at or after the trigger.
    SEEN = "seen"
    #: None taken since the trigger.
    ABSENT = "absent"
    #: The bot is configured but its state could not be read: waited on, up to the cap.
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class FindingRef:
    """One review-bot thread without an outcome, for the owner's Needs you comment."""

    path: str = ""
    #: "P0".."P3" when the bot's badge names one ("" = none parsed).
    severity: str = ""
    title: str = ""
    url: str = ""


@dataclass(frozen=True, slots=True)
class CheckRef:
    """One failing required check, for the owner's Needs you comment."""

    name: str = ""
    #: The check run's (or commit status's) page on GitHub ("" = none reported).
    url: str = ""


class _Body:
    KIND: ClassVar[EventKind]
    CLASS: ClassVar[EventClass]


# ---------------------------------------------------------------- control bodies


@dataclass(frozen=True, slots=True)
class RequestTriage(_Body):
    KIND: ClassVar[EventKind] = EventKind.REQUEST_TRIAGE
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    via: Via = Via.COMMAND
    #: The column an owner drag left (from the webhook); None for a command or label.
    board_from: Stage | None = None


@dataclass(frozen=True, slots=True)
class RequestPlan(_Body):
    KIND: ClassVar[EventKind] = EventKind.REQUEST_PLAN
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    via: Via = Via.COMMAND
    #: The column an owner drag left (from the webhook); None for a command or label.
    board_from: Stage | None = None


@dataclass(frozen=True, slots=True)
class RequestReplan(_Body):
    KIND: ClassVar[EventKind] = EventKind.REQUEST_REPLAN
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    via: Via = Via.COMMAND
    #: The column an owner drag left (from the webhook); None for a command or label.
    board_from: Stage | None = None


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
    #: The column an owner drag left (from the webhook); None for a command or label.
    board_from: Stage | None = None


@dataclass(frozen=True, slots=True)
class WaivePlan(_Body):
    """Stage-skip drag or owner ``factory:build``. Snapshot comes from ``Event.evidence``."""

    KIND: ClassVar[EventKind] = EventKind.WAIVE_PLAN
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    via: Via = Via.DRAG
    duration_us: int | None = None
    #: The column an owner drag left (from the webhook); None for a command or label.
    board_from: Stage | None = None


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
    """Explicit owner rework control from Ready (same approval, new build episode).

    ``batch_id``: not a drag but the owner's ticks in the Ready report
    (``service.deferred``): the factory moves the card and the rework's brief is exactly
    the ticked items."""

    KIND: ClassVar[EventKind] = EventKind.REQUEST_REWORK
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    batch_id: str = ""


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


@dataclass(frozen=True, slots=True)
class AutoTriage(_Body):
    """Idle-time auto-triage of one Inbox issue (the trusted clock, never an owner).

    The standing authorisation is the operator's: host config ``auto_triage`` (or the
    ``auto-triage on`` CLI) and its daily budget. The service picks the issue and attaches
    a fresh read; the reducer starts triage exactly as an owner Inbox -> Triage drag would,
    for an issue the factory has never worked on. Nothing past triage is ever started.
    """

    KIND: ClassVar[EventKind] = EventKind.AUTO_TRIAGE
    CLASS: ClassVar[EventClass] = EventClass.CONTROL


@dataclass(frozen=True, slots=True)
class AutoRetriage(_Body):
    """Idle-time re-triage of one Triaged issue whose triage may be stale (the trusted
    clock, never an owner).

    The standing authorisation is the operator's: host config ``auto_retriage`` (or the
    ``auto-retriage on`` CLI) and its daily budget. The service picks the issue from
    deterministic signals (``signals``: what changed since the triage, for the run's
    brief) and attaches a fresh read; the reducer starts a triage run exactly as an
    owner's ``/triage`` on the Triaged card would, but never clears a hold: a held,
    questioned, bundled or busy card is refused.
    """

    KIND: ClassVar[EventKind] = EventKind.AUTO_RETRIAGE
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    signals: tuple[str, ...] = ()
    #: The triage run whose result was judged stale: refused unless it is still the
    #: card's newest triage ("" = unbound).
    triage_session: str = ""


@dataclass(frozen=True, slots=True)
class CloseCandidateRemoved(_Body):
    """An owner removed the ``factory:close-candidate`` label (their own ``issues``
    ``unlabeled`` webhook; never the factory's own write). The current triage
    generation's label work is superseded: the removal sticks until the next triage."""

    KIND: ClassVar[EventKind] = EventKind.CLOSE_CANDIDATE_REMOVED
    CLASS: ClassVar[EventClass] = EventClass.CONTROL


@dataclass(frozen=True, slots=True)
class StaleHoldCleared(_Body):
    """Startup repair (the trusted clock): drop a safety hold that fenced nothing.

    Only for an Inbox issue whose one hold is ``safety`` and that the factory never
    worked on (``predicates.stale_safety_hold``): the hold the board's own "item added"
    Status write once set. Restores no authority; any other parcel is refused.
    """

    KIND: ClassVar[EventKind] = EventKind.STALE_HOLD_CLEARED
    CLASS: ClassVar[EventClass] = EventClass.CONTROL


@dataclass(frozen=True, slots=True)
class AutoBuildMarked(_Body):
    """An owner changed the card's "Auto-build" field (their ``projects_v2_item`` webhook).

    ``option`` is the option now selected: ``"Queued"`` approves the plan posted now (an
    owner approval like ``/approve <hash>``), ``""`` cleared it, ``"Started"`` (the
    factory's own option) or ``"?"`` (unknown) approve nothing. The factory's own
    writes never become this event.
    """

    KIND: ClassVar[EventKind] = EventKind.AUTO_BUILD_MARKED
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    option: str = ""


@dataclass(frozen=True, slots=True)
class AutoBuild(_Body):
    """Start the build an owner's auto-build mark approved (the trusted clock).

    The owner's approval is the mark itself; the operator's standing authorisation
    (``auto_build``, its daily budget) decides only when. The service picks the mark when
    a build slot is free and attaches a fresh read; the reducer re-checks the mark against
    the parcel, the posted plan and admission, then queues the build like an approval.
    """

    KIND: ClassVar[EventKind] = EventKind.AUTO_BUILD
    CLASS: ClassVar[EventClass] = EventClass.CONTROL


@dataclass(frozen=True, slots=True)
class AutopilotMarked(_Body):
    """An owner changed the epic card's "Autopilot" field (their own webhook only).

    ``option`` is "Full", "Delayed" or "Plan only", ``""`` (cleared: autopilot off) or
    ``"?"`` (unknown). The factory's own writes and anyone else's never become this.
    """

    KIND: ClassVar[EventKind] = EventKind.AUTOPILOT_MARKED
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    option: str = ""


@dataclass(frozen=True, slots=True)
class AutopilotPlan(_Body):
    """Epic autopilot starts the plan of this unblocked sub-issue of ``epic`` (the
    trusted clock, with a fresh read). The authority is the owner's approval of the epic
    plan; ``epoch`` is the epic's autopilot (the owner's field event). The reducer
    re-checks the parcel and its links."""

    KIND: ClassVar[EventKind] = EventKind.AUTOPILOT_PLAN
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    epic: int = 0
    epoch: str = ""


@dataclass(frozen=True, slots=True)
class AutopilotQueue(_Body):
    """Epic autopilot queues the build of this claimed sub-issue's posted plan (an
    auto-build mark carrying the owner's epic plan approval: ``owner_id`` and
    ``approval_event_id``), started by the auto-build queue no earlier than ``delay_us``
    after the plan was posted. ``epoch`` is the epic's autopilot (its field event)."""

    KIND: ClassVar[EventKind] = EventKind.AUTOPILOT_QUEUE
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    epic: int = 0
    epoch: str = ""
    owner_id: int = 0
    approval_event_id: str = ""
    delay_us: int = 0
    #: The board has an "Auto-build" field: show the queued mark there (the owner may
    #: clear it to object).
    show_auto_build: bool = False


@dataclass(frozen=True, slots=True)
class AutopilotWithdraw(_Body):
    """The epic no longer authorises autopilot (turned off, re-approved, cleared): drop
    the queued autopilot build mark of this sub-issue and release the claim. Only ever
    restricts."""

    KIND: ClassVar[EventKind] = EventKind.AUTOPILOT_WITHDRAW
    CLASS: ClassVar[EventClass] = EventClass.CONTROL
    epic: int = 0
    reason: str = ""


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
class ObservationFailed(_Body):
    """A parcel observation violated a store invariant; hold and fence it for recovery."""

    KIND: ClassVar[EventKind] = EventKind.OBSERVATION_FAILED
    CLASS: ClassVar[EventClass] = EventClass.SAFETY


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
class BoardFieldsObserved(_Body):
    """The board diff's read of the card's Bot and "Factory note" values (None: not read).

    Display only: a value other than the one the factory last wrote is rewritten (drift
    correction); never a command, never any other transition.
    """

    KIND: ClassVar[EventKind] = EventKind.BOARD_FIELDS_OBSERVED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    bot: str | None = None
    note: str | None = None


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
class OpenPRCapacityReconciled(_Body):
    """A bounded repository read repaired open-PR accounting.

    ``open_prs`` and ``closed_prs`` are fresh GitHub facts. Reservation IDs are stale
    local records (terminal parcel, or unbound with no live build) selected by the
    service; the reducer still protects an unbound reservation if its build is live.
    """

    KIND: ClassVar[EventKind] = EventKind.OPEN_PR_CAPACITY_RECONCILED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    open_prs: tuple[int, ...] = ()
    closed_prs: tuple[int, ...] = ()
    expired_reservation_ids: tuple[str, ...] = ()


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
    #: A bundle PR: the issues (bundle issue and members, "#12, #13") its closing
    #: references lack; ``closes_issue`` is false while any is missing ("" = none).
    missing_closes: str = ""
    #: The review bot's latest unanswered trigger on this head (source time), 0 = it
    #: already answered this head, None = unknown (older reads or no bot configured).
    review_bot_pending_since_us: int | None = None
    #: The review was carried to this head only because every newer commit merely
    #: syncs the base branch (owner "Update branch"): a red check came from the base.
    base_sync: bool = False
    #: Names of the failing required checks, "; "-separated (names may contain commas).
    failing_checks: str = ""
    #: The failing required checks with their links (at most 20), for the Needs you
    #: comment; ``failing_checks`` stays the source of the names elsewhere.
    failing_check_links: tuple[CheckRef, ...] = ()
    #: How the review bot answered this head, when it did (``review_bot_pending_since_us``
    #: 0), e.g. "👍 on `f7493c8`" ("" = no answer or not told).
    review_bot_verdict: str = ""
    #: When the read began (daemon clock; 0 = unknown, older reads): it reflects every
    #: check webhook received before then.
    read_started_us: int = 0
    #: The review bot's 👀 for its latest unanswered trigger (None = not told: no bot
    #: configured, nothing pending or an older read).
    review_bot_eyes: BotEyes | None = None
    #: The bot threads without an outcome (at most 20), when ``findings_open``.
    open_findings: tuple[FindingRef, ...] = ()
    #: The review bot also opened threads on earlier commits of the PR: open findings are
    #: a further review round after a fix.
    findings_earlier_rounds: bool = False
    #: Whether the PR merges into its base: "clean", "conflict" or "unknown" (GitHub was
    #: still computing it after the read's bounded re-reads); "" = not told (older reads).
    mergeable: str = ""
    #: Reported PR base head (or the proven test-merge base for a constrained clean
    #: answer), and its name.
    base_head: str = ""
    base_ref: str = ""
    #: Base tip the read was required to verify ("" for older/unconstrained reads).
    expected_base_head: str = ""
    #: Review-bot rounds of the issue so far and its budget (0 = not counted: older
    #: reads, no bot configured); at or past the budget no further review is awaited.
    review_rounds_used: int = 0
    review_rounds_budget: int = 0
    #: The card's round line, e.g. "Codex round 2/2" ("" with no budget).
    review_rounds_note: str = ""


#: ``ReadinessEvidence.mergeable`` values.
MERGE_CLEAN = "clean"
MERGE_CONFLICT = "conflict"
MERGE_UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class BasePushed(_Body):
    """A push to the repository's default branch: open PRs may now conflict with it."""

    KIND: ClassVar[EventKind] = EventKind.BASE_PUSHED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    ref: str = ""
    head_sha: str = ""


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
    attempts: int = 0


@dataclass(frozen=True, slots=True)
class EffectRecoveryStarted(_Body):
    """A legacy/manual unknown safe write is returning to bounded automatic recovery."""

    KIND: ClassVar[EventKind] = EventKind.EFFECT_RECOVERY_STARTED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    effect_id: str = ""
    effect_kind: str = ""


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
    #: A newer persisted intent replaced this retry; the original remote outcome may
    #: still be ambiguous, so consumers must not treat it as proven absent.
    superseded: bool = False


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
    #: Owner-facing reason a preparation was refused (the card's "Factory note"), e.g.
    #: the worktree is off its recorded branch and cannot be switched back safely.
    note: str = ""
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
    #: A harness permission prompt (a worker asking to run a tool): ``summary`` is then
    #: the readable one-line ask and ``detail`` the tool's target; its comment waits for
    #: :class:`PermissionPromptDue` (most clear within seconds).
    permission: bool = False
    detail: str = ""


@dataclass(frozen=True, slots=True)
class PermissionPromptDue(_Body):
    """A permission prompt is still open after ``permission_prompt_grace_seconds``."""

    KIND: ClassVar[EventKind] = EventKind.PERMISSION_PROMPT_DUE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    elicitation_id: str = ""


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
    eli5: str | None = None
    impact: DecisionImpact = DecisionImpact.UNKNOWN


@dataclass(frozen=True, slots=True)
class FeedbackRead(_Body):
    """``factory_get_feedback`` served these owner comments (event IDs) to the run.

    Display only: the factory acknowledges each comment it marked 👀 with 👍 and clears
    the "Feedback received" card note. The submit gate does not use it (it reads the
    MCP read table directly).
    """

    KIND: ClassVar[EventKind] = EventKind.FEEDBACK_READ
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    event_ids: tuple[str, ...] = ()


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
class RelatedMarked(_Body):
    """Issue ``source_issue``'s accepted triage named this issue (``relation``).

    Display only: it sets the card's lowest-precedence "Factory note", never authority.
    """

    KIND: ClassVar[EventKind] = EventKind.RELATED_MARKED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    source_issue: int = 0
    relation: str = ""


@dataclass(frozen=True, slots=True)
class EpicProgress(_Body):
    """An epic's progress line for its card (``Epic · 1/8 done · next: #823``; "" clears).

    Display only, from the factory's board pass (sub-issue states, ranks and blockers
    span several issues, so the reducer cannot derive it); never authority.
    """

    KIND: ClassVar[EventKind] = EventKind.EPIC_PROGRESS
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    text: str = ""


@dataclass(frozen=True, slots=True)
class TriageStale(_Body):
    """The factory's stale-triage pass flagged this Triaged card ("" clears): the card's
    note says "Triage may be stale" with why. Display only; never authority."""

    KIND: ClassVar[EventKind] = EventKind.TRIAGE_STALE
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    text: str = ""


@dataclass(frozen=True, slots=True)
class BundleHold(_Body):
    """The bundling service's membership of this issue in bundle issue ``bundle``.

    ``bundle`` > 0 holds the issue in that bundle (an owner confirmed it; the issue is
    built only as part of the bundle's PR) and mirrors the bundle's ``bot`` value on the
    card; 0 releases it with ``note`` (ejected or released). ``to_triage``: an ejected
    member goes back to Triage. Restricts only; never authority to start anything.
    """

    KIND: ClassVar[EventKind] = EventKind.BUNDLE_HOLD
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    bundle: int = 0
    bot: str = ""
    note: str = ""
    to_triage: bool = False


@dataclass(frozen=True, slots=True)
class AgentBlockedOn(_Body):
    """The issues the current build run's stored ``blocked`` result names as its only
    obstacle (``ResultCandidate.blocked_on``), for a result accepted before the reducer
    kept them. Recovered once at boot from the stored result; never authority."""

    KIND: ClassVar[EventKind] = EventKind.AGENT_BLOCKED_ON
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    issues: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class AutopilotStatus(_Body):
    """The autopilot pass's view of this epic: its card note (``Epic · 3/9 done ·
    autopilot: planning #882, next #883``) and why it is paused ("" = not paused).
    Display and a hold on new starts only; never authority."""

    KIND: ClassVar[EventKind] = EventKind.AUTOPILOT_STATUS
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    text: str = ""
    paused: str = ""


@dataclass(frozen=True, slots=True)
class AutopilotGateCreated(_Body):
    """A human gate of the approved epic plan exists as sub-issue ``number`` (created, or
    found by its marker after a restart)."""

    KIND: ClassVar[EventKind] = EventKind.AUTOPILOT_GATE_CREATED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    key: str = ""
    number: int = 0
    #: Its blocked-by links to the sub-issues it comes before exist now.
    linked: bool = False


@dataclass(frozen=True, slots=True)
class AutopilotQuestion(_Body):
    """Autopilot cannot go on without the owner (``key``: e.g. "order", no build order):
    one comment on the epic per epoch and key."""

    KIND: ClassVar[EventKind] = EventKind.AUTOPILOT_QUESTION
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    key: str = ""


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
    #: An epic plan (an epic triage with a ``plan``): sha256 of its canonical result.
    epic_plan_hash: str | None = None
    #: A plan of an autopilot sub-issue: "within" its part of the epic plan, "exceeds",
    #: or None (not given).
    epic_fit: str | None = None
    #: A ``blocked`` result whose only obstacle is other issues (every ``related`` entry is
    #: ``depends_on``): those issues. The run waits instead of showing Blocked while each
    #: is an open native blocker of this issue.
    blocked_on: tuple[int, ...] = ()
    #: A build's ``blocked`` result with ``replan_required``: why the approved plan no
    #: longer holds and what the revised plan must change (None: no replan asked).
    replan_reason: str | None = None
    replan_changes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TreeQuiescent(_Body):
    """A recursive tree scan. Only ``complete and not busy`` is quiescence evidence."""

    KIND: ClassVar[EventKind] = EventKind.TREE_QUIESCENT
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    complete: bool = False
    busy: bool = True
    pending_waiter: bool = False


class StallAction(enum.StrEnum):
    """What the health sweep asks of a stalled run (``StallObserved``)."""

    #: Quiet without a result for ``stall_nudge_minutes``: one nudge message (or, past
    #: the run's loop guard, Needs you instead).
    NUDGE = "nudge"
    #: Nudged ``stall_escalate_minutes`` ago with no real progress since: Needs you.
    ESCALATE = "escalate"
    #: Busy with nothing new anywhere in its tree for ``stall_frozen_minutes``: Needs
    #: you, never a message.
    FROZEN = "frozen"
    #: An escalated run made progress again: it is watched afresh.
    PROGRESS = "progress"


@dataclass(frozen=True, slots=True)
class StallObserved(_Body):
    """The health sweep's reading of the current run (``service.health_sweep``).

    The reducer re-checks every gate (current, ACTIVE, unfenced, no open question or
    owner hold, the work gate and approval for a nudge) and the run's ``StallWatch``
    episode; the sweep only proposes. ``progress`` is the real-progress signature
    (workers and branch head, never the root's own items); ``idle_since_us`` the newest
    activity anywhere in the tree.
    """

    KIND: ClassVar[EventKind] = EventKind.STALL_OBSERVED
    CLASS: ClassVar[EventClass] = EventClass.OBSERVATION
    session_id: str = ""
    action: StallAction = StallAction.NUDGE
    idle_since_us: int = 0
    progress: str = ""
    #: The run's branch head ("" unknown): a new one resets the loop-guard budget.
    commit: str = ""
    #: Case B: the root's runner is gone (host online); the nudge relaunches it and is
    #: not counted against the run's budget.
    recovery: bool = False
    #: ESCALATE: the nudge must be at least this old (``stall_escalate_minutes``).
    escalate_after_us: int = 0
    #: NUDGE: the loop guard (``max_stall_nudges_per_run``).
    max_nudges: int = 3


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
    | AutoTriage
    | AutoRetriage
    | CloseCandidateRemoved
    | StaleHoldCleared
    | AutoBuildMarked
    | AutoBuild
    | AutopilotMarked
    | AutopilotPlan
    | AutopilotQueue
    | AutopilotWithdraw
    | LeftwardMove
    | AssignedHuman
    | Closed
    | Transferred
    | Deleted
    | ItemRemoved
    | ApprovalInvalidated
    | WaiverEdited
    | ContractTampered
    | ObservationFailed
    | InboxHoldSet
    | InboxHoldReleased
    | GitHubSnapshot
    | ColumnObserved
    | PRObserved
    | OpenPRCapacityReconciled
    | ChecksChanged
    | ReviewChanged
    | ReadinessEvidence
    | BasePushed
    | ContractPublished
    | PublicationAcked
    | SessionCreated
    | CreateRejected
    | AdoptionResult
    | EffectUnknown
    | EffectRecoveryStarted
    | EffectCancelled
    | Prepared
    | MessageAck
    | EffectReconciled
    | RuntimeActivity
    | OwnerDirectOmnigentMessage
    | ElicitationOpened
    | ElicitationResolved
    | ElicitationGone
    | PermissionPromptDue
    | OwnerQuestion
    | FeedbackRead
    | PoliciesVerified
    | PolicyGuardFailed
    | ResultCandidate
    | IssueSessionClosed
    | RelatedMarked
    | EpicProgress
    | TriageStale
    | BundleHold
    | AgentBlockedOn
    | AutopilotStatus
    | AutopilotGateCreated
    | AutopilotQuestion
    | TreeQuiescent
    | StallObserved
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
    | BoardFieldsObserved
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
        AutoTriage,
        AutoRetriage,
        CloseCandidateRemoved,
        StaleHoldCleared,
        AutoBuildMarked,
        AutoBuild,
        AutopilotMarked,
        AutopilotPlan,
        AutopilotQueue,
        AutopilotWithdraw,
        LeftwardMove,
        AssignedHuman,
        Closed,
        Transferred,
        Deleted,
        ItemRemoved,
        ApprovalInvalidated,
        WaiverEdited,
        ContractTampered,
        ObservationFailed,
        InboxHoldSet,
        InboxHoldReleased,
        GitHubSnapshot,
        ColumnObserved,
        PRObserved,
        OpenPRCapacityReconciled,
        ChecksChanged,
        ReviewChanged,
        ReadinessEvidence,
        BasePushed,
        ContractPublished,
        PublicationAcked,
        SessionCreated,
        CreateRejected,
        AdoptionResult,
        EffectUnknown,
        EffectRecoveryStarted,
        EffectCancelled,
        Prepared,
        MessageAck,
        EffectReconciled,
        RuntimeActivity,
        OwnerDirectOmnigentMessage,
        ElicitationOpened,
        ElicitationResolved,
        ElicitationGone,
        PermissionPromptDue,
        OwnerQuestion,
        FeedbackRead,
        PoliciesVerified,
        PolicyGuardFailed,
        ResultCandidate,
        IssueSessionClosed,
        RelatedMarked,
        EpicProgress,
        TriageStale,
        BundleHold,
        AgentBlockedOn,
        AutopilotStatus,
        AutopilotGateCreated,
        AutopilotQuestion,
        TreeQuiescent,
        StallObserved,
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
        BoardFieldsObserved,
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
    "MERGE_CLEAN",
    "MERGE_CONFLICT",
    "MERGE_UNKNOWN",
    "ActiveLimitReached",
    "ActiveTimeSample",
    "AdoptionResult",
    "AgentBlockedOn",
    "ApprovalInvalidated",
    "ApprovePlan",
    "AssignedHuman",
    "AutoBuild",
    "AutoBuildMarked",
    "AutoRetriage",
    "AutoTriage",
    "AutopilotGateCreated",
    "AutopilotMarked",
    "AutopilotPlan",
    "AutopilotQuestion",
    "AutopilotQueue",
    "AutopilotStatus",
    "AutopilotWithdraw",
    "BasePushed",
    "BoardFieldsObserved",
    "BotEyes",
    "BundleHold",
    "CapacityAvailable",
    "CheckRef",
    "ChecksChanged",
    "ChecksState",
    "CloseCandidateRemoved",
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
    "EffectRecoveryStarted",
    "EffectUnknown",
    "ElicitationGone",
    "ElicitationOpened",
    "ElicitationResolved",
    "EpicProgress",
    "Event",
    "EventBody",
    "EventClass",
    "EventKind",
    "FeedbackRead",
    "FindingRef",
    "GitHubSnapshot",
    "GraceExpired",
    "InboxHoldReleased",
    "InboxHoldSet",
    "IssueSessionClosed",
    "ItemRemoved",
    "LeftwardMove",
    "MessageAck",
    "ObservationFailed",
    "OpenPRCapacityReconciled",
    "OperatorResume",
    "OwnerDirectOmnigentMessage",
    "OwnerQuestion",
    "PRObserved",
    "Pause",
    "PermissionPromptDue",
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
    "RelatedMarked",
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
    "StaleHoldCleared",
    "StallAction",
    "StallObserved",
    "Stop",
    "StopTimeout",
    "Transferred",
    "TreeQuiescent",
    "TriageStale",
    "Unpause",
    "WaivePlan",
    "WaiverEdited",
]
