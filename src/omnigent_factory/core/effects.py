"""Effect intents and adapter outcomes (architecture §2.1, §3.1, §3.4).

The reducer emits :class:`EffectIntent` values; the store persists them in the outbox in
the same transaction as the event. An executor later claims an intent, re-checks its
preconditions against current persisted state (:func:`omnigent_factory.core.preconditions.
effect_still_valid`) under the parcel lease, calls exactly one adapter, and turns the
:data:`AdapterOutcome` into a new observation event.

The effect vocabulary is closed. There is deliberately no kind that merges a pull
request, closes an issue, deletes a branch, edits rulesets or mints a token with a
profile broader than the session kind's (§2.8 invariant 16).
"""

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass, field

from omnigent_factory.core.types import SessionKind

JsonScalar = str | int | bool | None
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class EffectKind(enum.StrEnum):
    # GitHub (daemon installation credential)
    MOVE_CARD = "move_card"
    SET_BOT = "set_bot"
    #: Write the board's "Factory note" text field (display only, never authority).
    SET_NOTE = "set_note"
    #: Write the board's "Auto-build" field (``args.value``: "Queued", "Started" or ""
    #: to clear; display only, the owner's own webhook is the control).
    SET_AUTO_BUILD = "set_auto_build"
    #: Clear the epic's "Autopilot" field (``args.value`` ""; the factory only ever clears
    #: it, the owner's own webhook is the control).
    SET_AUTOPILOT = "set_autopilot"
    #: React to an owner comment: a command (+1 accepted, confused refused), or plain
    #: feedback (eyes taken, +1 once a run has read it; ``args.supersedes`` "eyes" then
    #: removes the bot's own eyes after the +1 is up).
    REACT_COMMENT = "react_comment"
    POST_COMMENT = "post_comment"
    PUBLISH_CONTRACT = "publish_contract"
    PUBLISH_TRIAGE = "publish_triage"
    PUBLISH_REPORT = "publish_report"
    #: Keep the factory lines of a posted Ready report current: edit the comment its
    #: PUBLISH_REPORT marker finds (``args.report_effect_id``) in place; never posts.
    EDIT_REPORT = "edit_report"
    #: Converge the issue's ``factory:close-candidate`` label (idempotent add/remove):
    #: ``args.action`` "triage" applies the published triage ``args.session_id``'s
    #: recommendation (add for a closure candidate, else remove); "remove" drops it when
    #: the card left Triage. Keyed to that triage generation (``Parcel.close_candidate_
    #: session``): an older one is superseded, so an owner's removal sticks until the next
    #: triage. Never closes the issue.
    SET_CLOSE_CANDIDATE = "set_close_candidate"
    ENSURE_PROJECT_ITEM = "ensure_project_item"
    FETCH_PR_EVIDENCE = "fetch_pr_evidence"
    RECONCILE_PARCEL = "reconcile_parcel"
    # Omnigent
    CREATE_SESSION = "create_session"
    PREPARE_SESSION = "prepare_session"
    SEND_MESSAGE = "send_message"
    RESOLVE_ELICITATION = "resolve_elicitation"
    INTERRUPT_TREE = "interrupt_tree"
    SCAN_TREE = "scan_tree"
    RECONCILE_SESSION = "reconcile_session"
    REPLACE_COST_POLICY = "replace_cost_policy"
    #: Archive the terminal parcel's issue session (idempotent PATCH archived=true).
    CLOSE_SESSION = "close_session"
    #: Retitle the live issue session after the issue title changed (idempotent PATCH).
    RENAME_SESSION = "rename_session"
    #: After ``args.not_before_us``: re-read and verify the run's exact policy set
    #: (``args.reconcile``: first re-establish it, then report a new barrier).
    VERIFY_POLICIES = "verify_policies"
    # local credentials
    DISABLE_ISSUANCE = "disable_issuance"
    ENABLE_ISSUANCE = "enable_issuance"
    # local workspace (factory-owned clone only)
    CLEANUP_WORKSPACE = "cleanup_workspace"
    # scheduler
    ARM_TIMER = "arm_timer"
    WAKE_SCHEDULER = "wake_scheduler"


#: Effects that can make an agent do work or give it write capability. Each must cite
#: persisted stage authority and, for a build, a current approval and grant.
WORK_BEARING_KINDS = frozenset(
    {
        EffectKind.SEND_MESSAGE,
        EffectKind.RESOLVE_ELICITATION,
        EffectKind.ENABLE_ISSUANCE,
    }
)


#: Effects that only read (GitHub issue/PR reads, Omnigent tree scans): routine, so their
#: start/done log lines are DEBUG.
READ_ONLY_KINDS = frozenset(
    {
        EffectKind.RECONCILE_PARCEL,
        EffectKind.FETCH_PR_EVIDENCE,
        EffectKind.SCAN_TREE,
        EffectKind.RECONCILE_SESSION,
    }
)


#: Remote writes whose requested end state can be read back or safely applied again.
#: An ambiguous outcome is retried with a bounded backoff instead of immediately
#: blocking the parcel.  This is also the operator ``retry-effect`` allow-list.
VERIFIABLE_WRITE_KINDS = frozenset(
    {
        EffectKind.MOVE_CARD,
        EffectKind.SET_BOT,
        EffectKind.SET_NOTE,
        EffectKind.SET_AUTO_BUILD,
        EffectKind.SET_AUTOPILOT,
        EffectKind.REACT_COMMENT,
        EffectKind.EDIT_REPORT,
        EffectKind.SET_CLOSE_CANDIDATE,
        EffectKind.ENSURE_PROJECT_ITEM,
        EffectKind.INTERRUPT_TREE,
        EffectKind.REPLACE_COST_POLICY,
        EffectKind.CLOSE_SESSION,
        EffectKind.RENAME_SESSION,
        EffectKind.VERIFY_POLICIES,
    }
)

#: Creates carrying a stable hidden marker.  Re-execution first looks the marker up and
#: adopts the existing object, so a lost acknowledgement cannot create a duplicate.
MARKER_ADOPTABLE_CREATE_KINDS = frozenset(
    {
        EffectKind.POST_COMMENT,
        EffectKind.PUBLISH_CONTRACT,
        EffectKind.PUBLISH_TRIAGE,
        EffectKind.PUBLISH_REPORT,
    }
)

#: Ambiguous remote writes which may start or answer agent work.  They retain their
#: dedicated reconciliation/fencing paths and are never automatically re-applied.
NON_IDEMPOTENT_AMBIGUOUS_KINDS = frozenset(
    {
        EffectKind.CREATE_SESSION,
        EffectKind.SEND_MESSAGE,
        EffectKind.RESOLVE_ELICITATION,
    }
)

AUTOMATIC_RECOVERY_KINDS = VERIFIABLE_WRITE_KINDS | MARKER_ADOPTABLE_CREATE_KINDS


class MessagePurpose(enum.StrEnum):
    FIRST = "first"
    FEEDBACK = "feedback"
    CHECKPOINT_CLEANUP = "checkpoint_cleanup"
    CONTINUATION = "continuation"
    ANSWER_RELAY = "answer_relay"
    #: One local-operator note to an existing session (``OperatorResume``).
    OPERATOR_NOTE = "operator_note"
    #: One wake after fresh current-head evidence shows a failure the build can fix.
    READINESS_WAKE = "readiness_wake"
    #: One wake of a build run that waited on open native blockers: all are closed now.
    BLOCKERS_CLOSED = "blockers_closed"
    #: One health-sweep nudge to a run whose turn ended without a result (stalled).
    STALL_NUDGE = "stall_nudge"


class RetryClass(enum.StrEnum):
    READ = "read"  # safe to repeat
    ADOPTABLE_WRITE = "adoptable_write"  # lost ack resolved by read-back/marker adoption
    LOCAL_IDEMPOTENT = "local_idempotent"  # local config/timer, safe to reapply
    NEVER_BLIND = "never_blind"  # ambiguity becomes UNKNOWN, never a blind retry


class CredentialProfile(enum.StrEnum):
    """Fixed permission profile, derived from session kind only (§6.1)."""

    READ_ONLY = "read_only"
    BUILD = "build"


def profile_for(kind: SessionKind) -> CredentialProfile:
    return CredentialProfile.BUILD if kind == SessionKind.BUILD else CredentialProfile.READ_ONLY


@dataclass(frozen=True, slots=True)
class Preconditions:
    """Facts that must still hold when the executor claims the intent."""

    parcel_version: int
    eligibility_epoch: int
    session_id: str | None = None
    authorization_id: str | None = None
    approval_id: str | None = None
    grant_id: str | None = None


@dataclass(frozen=True, slots=True)
class EffectIntent:
    """One durable outbox entry.

    ``effect_id`` is derived deterministically from the event ID, parcel version and
    ordinal. ``dedupe_key`` is the semantic uniqueness key persisted with a UNIQUE index.
    ``args`` must be JSON-serializable and contain no secrets.
    """

    effect_id: str
    kind: EffectKind
    parcel_id: str | None
    target: str
    preconditions: Preconditions
    args: Mapping[str, JsonValue] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    retry_class: RetryClass = RetryClass.NEVER_BLIND
    dedupe_key: str = ""

    @property
    def work_bearing(self) -> bool:
        return self.kind in WORK_BEARING_KINDS


# ----------------------------------------------------------------- adapter outcomes


@dataclass(frozen=True, slots=True)
class Ack:
    """The remote operation definitely happened. ``remote_id`` is e.g. item/comment ID."""

    remote_id: str | None = None
    detail: Mapping[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DefinitiveFailure:
    """The remote operation definitely did not happen (server-proven, no side effect)."""

    reason: str


@dataclass(frozen=True, slots=True)
class RetryableReadFailure:
    """A read failed transiently; retry after ``retry_after_us`` if given."""

    reason: str
    retry_after_us: int | None = None


@dataclass(frozen=True, slots=True)
class AmbiguousWrite:
    """A write may or may not have happened.

    Verifiable/idempotent kinds recover by read/re-apply; genuinely non-idempotent kinds
    become UNKNOWN and use their correlation-specific reconciliation (§3.4).
    """

    reason: str


AdapterOutcome = Ack | DefinitiveFailure | RetryableReadFailure | AmbiguousWrite


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Versioned context handed to an adapter together with the claimed intent."""

    boot_id: str
    lease_epoch: int
    parcel_version: int
    attempt: int
