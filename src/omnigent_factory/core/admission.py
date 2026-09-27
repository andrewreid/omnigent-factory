"""Central provenance/actor admission table (the reducer's front door).

Every :class:`~omnigent_factory.core.events.EventKind` declares, in one place, which
provenances may carry it and which actor class it requires. ``transition`` rejects any
other combination as an audited no-op *before* evidence or any handler runs.

Trust classes:

* GitHub-origin (``WEBHOOK``/``RECOVERY``, and the daemon's own ``RECONCILER`` reads) may
  carry owner controls, safety facts and GitHub observations. They can never clear an
  ambiguous effect, acknowledge a daemon board write, or stand in for a daemon outcome.
* ``ADAPTER`` is the daemon's executor/observer: effect outcomes, session/stream
  observations, read-after-write board acknowledgements.
* ``SCHEDULER`` is the trusted clock; ``OPERATOR`` the local protected CLI socket.
* ``INBOX`` is the durable delivery inbox. It may only restrict (hold a parcel whose
  delivery it could not interpret) or retire its own ``unresolved`` hold once the
  delivery resolved; a ``parked`` hold is released only by the operator (handler check).
"""

from __future__ import annotations

import enum
from dataclasses import dataclass

from omnigent_factory.core.events import EventKind, Provenance


class ActorRule(enum.StrEnum):
    ANY = "any"
    OWNER = "owner"  # numeric actor must be a configured owner


@dataclass(frozen=True, slots=True)
class Admission:
    provenances: frozenset[Provenance]
    actor: ActorRule = ActorRule.ANY


_GITHUB = frozenset({Provenance.WEBHOOK, Provenance.RECOVERY})
_GITHUB_OR_READ = _GITHUB | {Provenance.RECONCILER}
_OBSERVED = _GITHUB_OR_READ | {Provenance.ADAPTER}
_ADAPTER = frozenset({Provenance.ADAPTER})
_SCHEDULER = frozenset({Provenance.SCHEDULER})
_OPERATOR = frozenset({Provenance.OPERATOR})
_INBOX = frozenset({Provenance.INBOX})

_OWNER_CONTROL = Admission(_GITHUB, ActorRule.OWNER)

ADMISSION: dict[EventKind, Admission] = {
    # owner controls: signed GitHub input only, numeric owner actor
    EventKind.REQUEST_TRIAGE: _OWNER_CONTROL,
    EventKind.REQUEST_PLAN: _OWNER_CONTROL,
    EventKind.REQUEST_REPLAN: _OWNER_CONTROL,
    EventKind.PLAN_FEEDBACK: _OWNER_CONTROL,
    EventKind.APPROVE_PLAN: _OWNER_CONTROL,
    EventKind.WAIVE_PLAN: _OWNER_CONTROL,
    EventKind.DECIDE: _OWNER_CONTROL,
    EventKind.CONTINUE: _OWNER_CONTROL,
    EventKind.STOP: _OWNER_CONTROL,
    EventKind.REQUEST_REWORK: _OWNER_CONTROL,
    # operator
    EventKind.PAUSE: Admission(_OPERATOR),
    EventKind.UNPAUSE: Admission(_OPERATOR),
    # safety facts: any actor, GitHub input or a daemon read (they only restrict)
    EventKind.LEFTWARD_MOVE: Admission(_GITHUB_OR_READ),
    EventKind.ASSIGNED_HUMAN: Admission(_GITHUB_OR_READ),
    EventKind.CLOSED: Admission(_GITHUB_OR_READ),
    EventKind.TRANSFERRED: Admission(_GITHUB_OR_READ),
    EventKind.DELETED: Admission(_GITHUB_OR_READ),
    EventKind.ITEM_REMOVED: Admission(_GITHUB_OR_READ),
    EventKind.WAIVER_EDITED: Admission(_GITHUB_OR_READ),
    EventKind.APPROVAL_INVALIDATED: Admission(_OBSERVED),
    EventKind.CONTRACT_TAMPERED: Admission(_OBSERVED),
    EventKind.INBOX_HOLD_SET: Admission(_INBOX),
    EventKind.INBOX_HOLD_RELEASED: Admission(_INBOX | _OPERATOR),
    # GitHub observations
    EventKind.GITHUB_SNAPSHOT: Admission(frozenset({Provenance.RECONCILER, Provenance.ADAPTER})),
    # A daemon_effect_id is honoured only from ADAPTER (checked in the handler).
    EventKind.COLUMN_OBSERVED: Admission(_OBSERVED),
    EventKind.PR_OBSERVED: Admission(_OBSERVED),
    EventKind.CHECKS_CHANGED: Admission(_OBSERVED),
    EventKind.REVIEW_CHANGED: Admission(_OBSERVED),
    EventKind.READINESS_EVIDENCE: Admission(_ADAPTER),
    EventKind.CONTRACT_PUBLISHED: Admission(_ADAPTER),
    EventKind.PUBLICATION_ACKED: Admission(_ADAPTER),
    # daemon effect outcomes and Omnigent observations: executor/observer only
    EventKind.SESSION_CREATED: Admission(_ADAPTER),
    EventKind.CREATE_REJECTED: Admission(_ADAPTER),
    EventKind.ADOPTION_RESULT: Admission(_ADAPTER),
    EventKind.EFFECT_UNKNOWN: Admission(_ADAPTER),
    EventKind.EFFECT_CANCELLED: Admission(_ADAPTER),
    EventKind.EFFECT_RECONCILED: Admission(_ADAPTER | _OPERATOR),
    EventKind.PREPARED: Admission(_ADAPTER),
    EventKind.MESSAGE_ACK: Admission(_ADAPTER),
    EventKind.RUNTIME_ACTIVITY: Admission(_ADAPTER),
    EventKind.OWNER_DIRECT_MESSAGE: Admission(_ADAPTER),
    EventKind.ELICITATION_OPENED: Admission(_ADAPTER),
    EventKind.ELICITATION_RESOLVED: Admission(_ADAPTER),
    EventKind.ELICITATION_GONE: Admission(_ADAPTER),
    EventKind.RESULT_CANDIDATE: Admission(_ADAPTER),
    EventKind.TREE_QUIESCENT: Admission(_ADAPTER),
    EventKind.STOP_TIMEOUT: Admission(_ADAPTER | _SCHEDULER),
    EventKind.SESSION_CRASHED: Admission(_ADAPTER),
    EventKind.ACTIVE_TIME_SAMPLE: Admission(_ADAPTER),
    EventKind.COST_SAMPLE: Admission(_ADAPTER),
    EventKind.POLICY_READY: Admission(_ADAPTER),
    # trusted clock
    EventKind.ACTIVE_LIMIT_REACHED: Admission(_SCHEDULER),
    EventKind.GRACE_EXPIRED: Admission(_SCHEDULER),
    EventKind.CAPACITY_AVAILABLE: Admission(_SCHEDULER),
    EventKind.RETRY_DUE: Admission(_SCHEDULER),
    EventKind.RECONCILE_DUE: Admission(_SCHEDULER),
}


def admission_rejection(
    kind: EventKind, provenance: Provenance, actor_id: int | None, owners: frozenset[int]
) -> str | None:
    """Return the rejection reason for an inadmissible event, else ``None``."""
    rule = ADMISSION[kind]
    if provenance not in rule.provenances:
        return "provenance-not-admitted"
    if rule.actor == ActorRule.OWNER and (actor_id is None or actor_id not in owners):
        return "control-from-non-owner"
    return None
