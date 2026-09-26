"""Effect precondition re-check (architecture §2.1, §3.4).

An executor calls :func:`effect_still_valid` against the *current persisted* parcel under
the parcel lease immediately before any external operation. A non-``None`` result is a
reason to cancel the intent; cancellation never creates replacement authority.
"""

from __future__ import annotations

from omnigent_factory.core.effects import EffectIntent, EffectKind, MessagePurpose
from omnigent_factory.core.predicates import (
    authority_ok,
    board_pending,
    dispatchable,
    message_uncertain,
    work_allowed,
)
from omnigent_factory.core.types import Lifecycle, Parcel, SessionKind


def effect_still_valid(parcel: Parcel | None, effect: EffectIntent) -> str | None:
    """Return ``None`` if ``effect`` may execute now, else the cancellation reason."""
    pre = effect.preconditions
    if effect.kind == EffectKind.CREATE_SESSION:
        return _create_valid(parcel, effect)
    if effect.kind == EffectKind.PREPARE_SESSION:
        if parcel is None:
            return "unknown-parcel"
        s = parcel.session(pre.session_id)
        if s is None or s.lifecycle != Lifecycle.PREPARING or s.fences:
            return "session-not-preparing"
        return None
    if not effect.work_bearing:
        return None
    if parcel is None:
        return "unknown-parcel"
    s = parcel.session(pre.session_id)
    if s is None:
        return "unknown-session"
    if pre.eligibility_epoch != parcel.eligibility_epoch:
        return "eligibility-barrier-advanced"
    if pre.authorization_id != s.authorization_id or pre.grant_id != s.grant.grant_id:
        return "stale-authority-or-grant"
    if s.kind == SessionKind.BUILD and pre.approval_id != parcel.current_approval_id:
        return "stale-approval"
    purpose = effect.args.get("purpose")
    if purpose == MessagePurpose.CHECKPOINT_CLEANUP.value:
        # Protocol-only cleanup allowed inside checkpoint grace, never through a fence.
        if (
            s.session_id == parcel.current_session_id
            and s.lifecycle == Lifecycle.CHECKPOINT_GRACE
            and not s.fences
            and authority_ok(parcel, s)
            and not message_uncertain(parcel, s)
            and not board_pending(parcel)
        ):
            return None
        return "checkpoint-cleanup-not-permitted"
    if not dispatchable(parcel):
        return "not-dispatchable"
    if not work_allowed(parcel, s):
        return "execution-gate-closed"
    return None


def _create_valid(parcel: Parcel | None, effect: EffectIntent) -> str | None:
    if parcel is None:
        return "unknown-parcel"
    pre = effect.preconditions
    s = parcel.session(pre.session_id)
    if s is None:
        return "unknown-session"
    if s.lifecycle not in (Lifecycle.INTENT, Lifecycle.CREATING) or s.fences:
        return "session-not-creatable"
    if s.session_id != parcel.current_session_id:
        return "session-not-current"
    if pre.eligibility_epoch != parcel.eligibility_epoch:
        return "eligibility-barrier-advanced"
    auth = parcel.authorization(s.authorization_id)
    if auth is None or auth.cancelled:
        return "authority-cancelled"
    if not dispatchable(parcel):
        return "not-dispatchable"
    return None
