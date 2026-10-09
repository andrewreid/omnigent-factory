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
    """Board ``Status`` column identity. ``DONE`` is a retained legacy value, never a
    dispatch stage.

    Values are persisted identity keys (and the host config ``status_options`` keys), not
    display names: the live column names come from host config ``status_names`` and the
    board is read and written by option ID only. Never rename a value without a migration.
    """

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


class AutoBuildStatus(enum.StrEnum):
    """An owner's auto-build mark (the board's "Auto-build" field)."""

    #: Set to Queued by an owner: approval of the posted plan, waiting to be started.
    QUEUED = "queued"
    #: The factory started the build from the mark (it wrote "Started" on the board).
    STARTED = "started"


#: The board's "Auto-build" single-select option names (``auto_build_options`` keys).
AUTO_BUILD_QUEUED = "Queued"
AUTO_BUILD_STARTED = "Started"


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
    #: Approved and waiting for build capacity (an admission queue entry).
    QUEUED = "Queued"
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

#: The current stage cannot continue without the owner. ``AWAITING_OWNER`` (triage or
#: plan posted, the next move is the owner's) is not one of them: that card is Idle.
NEEDS_YOU_HOLDS = frozenset(
    {
        Hold.CHECKS_FAILED,
        Hold.READINESS_FAILED,
        Hold.REMEDIATION_EXHAUSTED,
        Hold.REWORK_CONTROL_REQUIRED,
        Hold.UNSUPPORTED_REWORK,
        Hold.PR_CLOSED,
        Hold.NO_PROJECT_ITEM,
        Hold.APPROVAL_VOIDED,
    }
)
#: ``EXTERNAL_ACTIVITY`` is in neither set: it clears by itself once the issue session is
#: observed idle, so it asks nothing of the owner (the derived note says it is waiting).

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


#: Linked issues kept per list (sub-issues, blockers, blocked); a longer list is cut.
MAX_LINKED_ISSUES = 50
#: Linked-issue titles are cut to this length (display only).
LINK_TITLE_MAX = 120


@dataclass(frozen=True, slots=True)
class LinkedIssue:
    """One GitHub-native issue link target (number, open/closed, title; untrusted title).

    ``repo`` is "" for an issue of the configured repository, else ``owner/name``.
    """

    number: int
    open: bool
    title: str = ""
    repo: str = ""

    @property
    def ref(self) -> str:
        return f"#{self.number}" if not self.repo else f"{self.repo}#{self.number}"


@dataclass(frozen=True, slots=True)
class IssueLinks:
    """GitHub-native links of one issue from a fresh read: parent, sub-issues, blocked-by
    and blocking. A read that could not see every blocker is never an ``IssueLinks``
    (it is None: unreadable, which fails closed for auto-build)."""

    parent: LinkedIssue | None = None
    #: Sub-issues (at most ``MAX_LINKED_ISSUES``); the totals are GitHub's summary.
    sub_issues: tuple[LinkedIssue, ...] = ()
    sub_total: int = 0
    sub_completed: int = 0
    blocked_by: tuple[LinkedIssue, ...] = ()
    blocking: tuple[LinkedIssue, ...] = ()

    @property
    def epic(self) -> bool:
        """An issue with at least one sub-issue is an epic."""
        return self.sub_total > 0 or bool(self.sub_issues)

    @property
    def open_blockers(self) -> tuple[LinkedIssue, ...]:
        return tuple(b for b in self.blocked_by if b.open)


def blockers_text(blockers: tuple[LinkedIssue, ...], limit: int = 3) -> str:
    """``#823, #822`` (``+N more`` past ``limit``)."""
    refs = [b.ref for b in blockers[:limit]]
    extra = len(blockers) - limit
    return ", ".join(refs) + (f" +{extra} more" if extra > 0 else "")


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
    #: The board's current "Factory note" text ("" when empty); None when not read.
    note: str | None = None
    #: sha256 of ``body`` when a stored read was superseded by a newer one and its body
    #: dropped (only the newest read's text is ever read back); None otherwise.
    body_sha256: str | None = None
    #: The board's "Auto-build" option name when read ("" = empty, "?" = an option the
    #: config does not know); None when not read (no field configured).
    auto_build: str | None = None
    #: GitHub-native links (parent, sub-issues, blocked-by, blocking); None when not read
    #: or unreadable (e.g. more blockers than one read returns).
    links: IssueLinks | None = None

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
    #: A build re-run on owner feedback after the work was built (same approval).
    rework: bool = False
    #: That re-run resolves a merge conflict with the base branch (no owner feedback).
    conflict: bool = False


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
    #: Owner comments relayed to this (build) run while it waited on checks; each one
    #: opens a fresh result slot so the run can re-submit the same head.
    feedback_wakes: int = 0
    #: An owner comment arrived mid-turn: relay it if the turn ends without a result.
    comment_pending: bool = False
    #: When the current drain began (0: not draining, or a drain recorded before this
    #: field existed). The scheduler ends a drain still waiting after the drain timeout.
    drain_started_us: int = 0
    #: A build run retired for Ready was re-opened once for review-bot findings that
    #: arrived after Ready (#799). Never again: a later late finding is the owner's.
    reopened: bool = False


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
    #: The issue title the session is named after ("" = unknown, older records).
    title: str = ""

    @property
    def reusable(self) -> bool:
        return self.status == IssueSessionStatus.LIVE


def issue_session_title(issue_number: int | None, title: str) -> str:
    """The Omnigent title of an issue session: ``#<n> · <issue title>``."""
    title = " ".join(title.split())
    return f"#{issue_number} · {title}"[:200] if title else f"#{issue_number}"


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
    #: Review bots get until this time to comment on ``head_sha``: only a green read
    #: taken at or after it can make the parcel Ready (0 = no wait, older records).
    settle_at_us: int = 0
    #: Source time of the read that set ``verified`` (0 = unknown, older records).
    verified_at_us: int = 0
    #: The latest read showed the review bot already answered ``head_sha`` with no
    #: re-ping since: nothing can still arrive, so there is no grace wait.
    review_bot_done: bool = False
    #: In Ready with the bot's work done and only a required check red (a base sync,
    #: a flake or main broken; or the one fix wake already spent): the card stays in
    #: Ready with Bot Blocked (no rework, no wake) until a pending/green read or a newer
    #: head. (Named for the first case, the owner's base sync; kept for stored state.)
    sync_red: bool = False
    #: The failing required checks seen red on this head, "; "-separated (a name may
    #: contain commas), e.g. "api / Dependency audit; web / Typecheck, test, lint".
    red_checks: str = ""
    #: The review bot's state on ``head_sha`` for the Ready report, e.g. "👍 on `f7493c8`",
    #: "reviewed `abc1234`, 2 findings, all with outcomes" or "no response within the
    #: grace window" ("" = no line: no bot configured or its state unknown).
    review_bot: str = ""
    #: The PUBLISH_REPORT effect of this head's Ready report ("" = none posted for it):
    #: its marker finds the comment whose factory lines are kept current in place.
    report_effect_id: str = ""
    #: The factory lines that report shows now (CI, red checks, review bot), as a key.
    report_key: str = ""
    #: When the newest applied evidence read of ``head_sha`` began (0 = none or older
    #: records): a check webhook received before then is already reflected in it.
    read_started_us: int = 0
    #: Source time of the latest review-bot trigger on which its 👀 was seen: the bot is
    #: reviewing it, so its wait runs to the cap even if the reaction is later gone.
    eyes_trigger_us: int = 0
    #: When the wait for a review bot whose state cannot be read began (0 = none): such
    #: a wait still ends at the cap.
    unknown_since_us: int = 0
    #: The latest read could not tell whether the PR merges cleanly into its base (GitHub
    #: was still computing it): the next catch-up read asks again.
    merge_unknown: bool = False


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
    #: Started from an owner's auto-build mark: admitted after every manually approved
    #: build and within ``auto_build_concurrency``. Derived on load from the parcel's
    #: mark (``AutoBuildMark.approval_id``/``sequence``), never stored in the queue table.
    auto: bool = False
    #: A parked build waiting to re-acquire its slot (admitted before new builds). Derived
    #: on load from the parcel's ``slot_parked``, never stored in the queue table.
    resume: bool = False
    #: The parcel's issue number (derived on load; display only).
    issue_number: int | None = None


@dataclass(frozen=True, slots=True)
class HeldWake:
    """A message to a parked build run, held until the run holds a build slot again.

    Replayed exactly once (as a fresh effect) when the run is re-admitted; dropped if the
    run is stopped, replaced or ends first.
    """

    kind: str
    session_id: str
    target: str
    #: The effect's args as canonical JSON (kept as text: the aggregate stays hashable).
    args_json: str


@dataclass(frozen=True, slots=True)
class AutoBuildMark:
    """An owner set the card's "Auto-build" field to Queued: approval of one posted plan.

    Accepted only from the owner's own ``projects_v2_item`` webhook. It approves exactly
    the plan posted when it was set (``full_hash``); a later plan revision, a barrier or
    the card leaving Planning lapses it before it starts. Once the factory starts the
    build (``STARTED``) it is an ordinary approved build.
    """

    status: AutoBuildStatus
    full_hash: str
    contract_id: str
    owner_id: int
    source_event_id: str
    marked_at_us: int
    #: The plan revision the mark approved; an owner answer to the plan's question moves
    #: it on (the re-posted plan must then carry the same hash), any other revision
    #: lapses the mark.
    revision: int = 0
    #: The approval the started build runs under ("" while queued) and the sequence of
    #: its queue entry (a later rework under the same approval is no auto-build).
    approval_id: str = ""
    sequence: int = 0


@dataclass(frozen=True, slots=True)
class Reservation:
    reservation_id: str
    parcel_id: str
    kind: ReservationKind
    episode_id: str
    pr_number: int | None = None
    live: bool = True


#: How a triage relates another issue to the one it triaged (``related`` in a triage result).
RELATIONS = ("duplicate", "overlaps", "conflicts", "depends_on", "blocks", "supersedes")


@dataclass(frozen=True, slots=True)
class RelatedMark:
    """Another issue's triage named this one: ``issue`` <relation> this issue."""

    issue: int
    relation: str


@dataclass(frozen=True, slots=True)
class ObservedMove:
    """A column change a read (or a non-owner webhook) showed, with no owner control yet.

    Never authority. An owner drag of exactly these columns that arrives after the read
    is judged by its own columns, as if it had come first (see the reducer's
    ``_drag_origin`` and ``_h_leftward``).
    """

    from_stage: Stage | None
    to_stage: Stage
    #: The parcel's barrier after the observation (a leftward one raises it).
    barrier_us: int
    #: The barrier before it: the drag must be fresh against this one.
    prior_barrier_us: int


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
    #: Readiness fix wakes sent to the build session for the current approval (at most 1)
    #: for red required checks and other gaps besides review-bot findings (the check wake;
    #: a wake recorded before the split counts as this one).
    readiness_wakes: int = 0
    #: Readiness fix wakes sent for review-bot findings without an outcome (at most 1 per
    #: approval or rework), separate so findings that arrive after the check wake was
    #: spent still reach the build once (#745).
    findings_wakes: int = 0
    pr_number: int | None = None
    #: "<PR head>:<base head>" of the last merge-conflict wake (a wake of the build run or
    #: a conflict rework): at most one per pair, never re-sent after a restart.
    conflict_wake: str = ""
    #: The reviewed head current when a merge conflict with the base was seen: its review
    #: never carries to a later head as a base sync (resolving a conflict is new work
    #: that needs a fresh cross-vendor review).
    conflict_reviewed_head: str = ""
    bot: BotState = BotState.IDLE
    #: Latest informational status reason (replaces status comments; "" when none).
    note: str = ""
    #: The "Factory note" value last written to the board (derived, like ``bot``).
    board_note: str = ""
    #: The in-flight daemon board write: at most one per parcel (serialised).
    pending_moves: tuple[PendingMove, ...] = ()
    #: When the last daemon board write landed: a column read taken before it is stale.
    board_written_at_us: int = 0
    #: The issue title from the newest read (whitespace-normalised) and that read's time.
    issue_title: str = ""
    issue_title_read_at_us: int = 0
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
    #: Other issues whose accepted triage named this one (newest last). Shown as the
    #: lowest-precedence "Factory note"; cleared when the card changes column.
    related_marks: tuple[RelatedMark, ...] = ()
    #: The latest column change observed without an owner control (None once a control
    #: explains it or the card moves on).
    observed_move: ObservedMove | None = None
    #: The owner's auto-build mark (None: none, or it lapsed or finished).
    auto_build: AutoBuildMark | None = None
    #: The "Auto-build" option the factory believes the board shows ("" = empty): the
    #: factory's own last write, or the owner's change as seen by a webhook or a read.
    auto_build_field: str = ""
    #: When ``auto_build_field`` last changed: a read taken before then is stale.
    auto_build_field_at_us: int = 0
    #: GitHub-native links from the newest read that carried them (None: never read).
    links: IssueLinks | None = None
    #: The build episode's run is parked (Blocked, Needs you, a settled checkpoint, or idle
    #: waiting on checks): its building slot is released until it must work again.
    slot_parked: bool = False
    #: Messages for the parked run, held until it is re-admitted (``HeldWake``).
    held_wakes: tuple[HeldWake, ...] = ()
    #: An epic's progress note (``Epic · 1/8 done · next: #823``), set by the factory's
    #: board pass only when it changes ("" = none).
    epic_note: str = ""
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
    #: Derived on load, never persisted: parcels whose triage run holds a triage slot
    #: (``predicates.holds_triage_slot``), shared by every triage start in the repository.
    triage_runs: frozenset[str] = frozenset()

    @property
    def auto_build_count(self) -> int:
        """Builds started from an auto-build mark that hold their building slot (running;
        a parked one has released it)."""
        return sum(1 for q in self.queue if q.auto and q.status == QueueStatus.RESERVED)

    def running(self, *, auto: bool | None = None) -> tuple[QueueEntry, ...]:
        """Build episodes holding their building slot (``auto``: only auto-builds or only
        manual ones; None: all), oldest first."""
        return tuple(
            q
            for q in self.queue
            if q.status == QueueStatus.RESERVED and (auto is None or q.auto == auto)
        )

    def parked(self) -> tuple[QueueEntry, ...]:
        """Build episodes whose run is parked: slot released, held or waiting to resume."""
        return tuple(
            q
            for q in self.queue
            if q.status == QueueStatus.HELD or (q.status == QueueStatus.QUEUED and q.resume)
        )

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
    #: How long review bots get to comment on a new PR head before it can be Ready.
    review_grace_us: int = 0
    #: With the review bot's 👀 readable: how long after a trigger it gets to show 👀 (or
    #: answer) before the wait ends.
    review_ack_us: int = 0
    #: How long after a trigger a review bot showing 👀 (or whose state cannot be read)
    #: is waited for at most.
    review_cap_us: int = 0
    #: Triage runs that may hold a slot at once, repository-wide (owner and auto-triage
    #: starts alike); a triage request beyond it waits for a slot.
    triage_concurrency: int = 1
    #: Builds started from an owner's auto-build mark that may run at once (within
    #: ``max_building``; manually approved builds are always admitted first).
    auto_build_concurrency: int = 1

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
