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
from omnigent_factory.core.types import (
    SETTLED_LIFECYCLES,
    Hold,
    Lifecycle,
    Parcel,
    SessionKind,
    Stage,
    issue_session_title,
)

#: Comments that would regress a merged/closed parcel if posted after the fact.
_REGRESSION_COMMENTS = frozenset({"ready-blocked"})


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
    if (
        effect.kind == EffectKind.SET_BOT
        and parcel is not None
        and effect.args.get("bot") != parcel.bot.value
    ):
        return "bot-superseded"  # a newer derived Bot value is queued; latest wins
    if (
        effect.kind == EffectKind.SET_NOTE
        and parcel is not None
        and effect.args.get("note") != parcel.board_note
    ):
        return "note-superseded"  # a newer note write is queued; latest wins
    if (
        effect.kind == EffectKind.SET_AUTO_BUILD
        and parcel is not None
        and effect.args.get("value") != parcel.auto_build_field
    ):
        return "auto-build-superseded"  # the field changed since: latest wins
    if (
        effect.kind == EffectKind.SET_AUTOPILOT
        and parcel is not None
        and effect.args.get("value") != parcel.autopilot_field
    ):
        return "autopilot-superseded"  # the owner set it again since: latest wins
    if effect.kind == EffectKind.SET_CLOSE_CANDIDATE and parcel is not None:
        generation = effect.args.get("session_id")
        if generation != parcel.close_candidate_session:
            return "close-candidate-superseded"  # a newer triage decides the label
        if effect.args.get("action") == "triage" and (
            parcel.stage != Stage.TRIAGED or parcel.close_candidate_removed == generation
        ):
            # Left Triage since, or the owner removed this triage's label: never re-add.
            return "close-candidate-superseded"
    if parcel is not None and Hold.COMPLETED in parcel.holds:
        completed = _completed_invalid(effect)
        if completed is not None:
            return completed
    if (
        effect.kind == EffectKind.MOVE_CARD
        and parcel is not None
        and effect.args.get("to") != (parcel.stage.value if parcel.stage is not None else None)
    ):
        return "move-superseded"  # a newer owner/factory target is the desired column
    if effect.kind == EffectKind.REACT_COMMENT and parcel is not None:
        comment_id = effect.args.get("comment_id")
        if (
            effect.args.get("content") == "eyes"
            and f"github:comment:{comment_id}:created" not in parcel.unread_feedback
        ):
            return "reaction-superseded"  # the later thumbs-up removed these eyes
    if effect.kind == EffectKind.INTERRUPT_TREE:
        superseded = _interrupt_superseded(parcel, effect)
        if superseded:
            return superseded
    if effect.kind == EffectKind.REPLACE_COST_POLICY:
        superseded = _cost_policy_superseded(parcel, effect)
        if superseded:
            return superseded
    if effect.kind == EffectKind.RENAME_SESSION:
        superseded = _rename_superseded(parcel, effect)
        if superseded:
            return superseded
    if effect.kind == EffectKind.VERIFY_POLICIES:
        superseded = _policy_verification_superseded(parcel, effect)
        if superseded:
            return superseded
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


def _interrupt_superseded(parcel: Parcel | None, effect: EffectIntent) -> str | None:
    """Do not let an old stage's root-wide stop interrupt a newer run on that root."""
    if parcel is None:
        return "interrupt-superseded"
    session = parcel.session(effect.preconditions.session_id)
    root_id = effect.args.get("root_id")
    if session is None or not root_id or session.root_id != root_id:
        return "interrupt-superseded"
    current = parcel.current_session
    if (
        current is not None
        and current.session_id != session.session_id
        and current.root_id == root_id
        and current.lifecycle not in SETTLED_LIFECYCLES
    ):
        return "interrupt-superseded"
    return None


def _cost_policy_superseded(parcel: Parcel | None, effect: EffectIntent) -> str | None:
    """A shared root's cost policy belongs to the current run's current grant."""
    if parcel is None:
        return "cost-policy-superseded"
    session = parcel.session(effect.preconditions.session_id)
    if (
        session is None
        or session.session_id != parcel.current_session_id
        or effect.preconditions.grant_id != session.grant.grant_id
        or effect.args.get("grant_id") != session.grant.grant_id
        or effect.args.get("generation") != session.grant.policy_generation
    ):
        return "cost-policy-superseded"
    return None


def _rename_superseded(parcel: Parcel | None, effect: EffectIntent) -> str | None:
    """Only the current issue title may rename the current reusable conversation."""
    if parcel is None or parcel.issue_session is None:
        return "rename-superseded"
    issue = parcel.issue_session
    expected = issue_session_title(parcel.issue_number, parcel.issue_title)
    if (
        not issue.reusable
        or effect.args.get("root_id") != issue.root_id
        or effect.args.get("title") != expected
    ):
        return "rename-superseded"
    return None


def _policy_verification_superseded(parcel: Parcel | None, effect: EffectIntent) -> str | None:
    """Verification/reconciliation is meaningful only for the latest run and grant."""
    if parcel is None:
        return "policy-verification-superseded"
    session = parcel.session(effect.preconditions.session_id)
    if (
        session is None
        or session.session_id != parcel.current_session_id
        or session.root_id != effect.args.get("root_id")
        or effect.preconditions.grant_id != session.grant.grant_id
        or session.policy_ready
        or session.execution_closed
    ):
        return "policy-verification-superseded"
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


def _completed_invalid(effect: EffectIntent) -> str | None:
    """Re-check the terminal barrier at the call boundary: a regression queued before the
    merge/close landed is cancelled instead of posted."""
    if effect.work_bearing:
        return "parcel-completed"
    if effect.kind == EffectKind.MOVE_CARD and effect.args.get("to") != Stage.DONE.value:
        return "parcel-completed"
    if (
        effect.kind == EffectKind.POST_COMMENT
        and effect.args.get("template") in _REGRESSION_COMMENTS
    ):
        return "parcel-completed"
    return None
