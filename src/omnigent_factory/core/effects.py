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
    POST_COMMENT = "post_comment"
    PUBLISH_CONTRACT = "publish_contract"
    PUBLISH_TRIAGE = "publish_triage"
    PUBLISH_REPORT = "publish_report"
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
    # local credentials
    DISABLE_ISSUANCE = "disable_issuance"
    ENABLE_ISSUANCE = "enable_issuance"
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


class MessagePurpose(enum.StrEnum):
    FIRST = "first"
    FEEDBACK = "feedback"
    CORRECTION = "correction"
    CHECKPOINT_CLEANUP = "checkpoint_cleanup"
    CONTINUATION = "continuation"
    ANSWER_RELAY = "answer_relay"


class RetryClass(enum.StrEnum):
    READ = "read"  # safe to repeat
    ADOPTABLE_WRITE = "adoptable_write"  # lost ack resolved by marker/nonce adoption
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
    """A write may or may not have happened. Never retried blindly (§3.4)."""

    reason: str


AdapterOutcome = Ack | DefinitiveFailure | RetryableReadFailure | AmbiguousWrite


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Versioned context handed to an adapter together with the claimed intent."""

    boot_id: str
    lease_epoch: int
    parcel_version: int
    attempt: int
