"""Authorization predicates and execution-gate helpers (architecture §2.2-§2.3).

E = fresh eligible snapshot, D = no unresolved material decision, Q = every previous
execution tree observed quiescent. PlanOK / SkipOK / BuildOK are evaluated at approval,
queue admission, dispatch-intent commit and immediately before every work-bearing effect.
"""

from __future__ import annotations

from omnigent_factory.core.types import (
    EXECUTABLE_LIFECYCLES,
    SETTLED_LIFECYCLES,
    Approval,
    ApprovalKind,
    FenceKind,
    Lifecycle,
    Parcel,
    SessionKind,
    StageSession,
)

#: A triage run in one of these lifecycles is running and holds a triage slot. A run
#: waiting on the owner (an open question, a plan approval, a checkpoint) or
#: blocked/closed is not. (Idle-time auto-triage has its own, narrower rule for plan and
#: build runs: ``service.auto_triage.busy_reason``.)
RUNNING_LIFECYCLES = frozenset(
    {
        Lifecycle.INTENT,
        Lifecycle.CREATING,
        Lifecycle.PREPARING,
        Lifecycle.ACTIVE,
        Lifecycle.CHECKPOINT_GRACE,
        Lifecycle.DRAINING,
        Lifecycle.UNKNOWN,
    }
)


def holds_triage_slot(p: Parcel) -> bool:
    """A triage run of ``p`` occupies one of the repository's triage slots.

    The store derives ``AdmissionSnapshot.triage_runs`` with the same rule in SQL.
    """
    return any(
        s.kind == SessionKind.TRIAGE and s.lifecycle in RUNNING_LIFECYCLES for s in p.sessions
    )


def eligible(p: Parcel) -> bool:
    """E for recording authority: open, unassigned, identity-verified issue."""
    return p.eligible


def dispatchable(p: Parcel) -> bool:
    """E for dispatch: eligible, the configured project item exists (§3.3 step 4) and no
    durable delivery for the parcel is parked or unverified."""
    return p.eligible and p.in_project and not p.inbox_holds


def no_open_decisions(p: Parcel) -> bool:
    """D."""
    return not p.open_decisions


def settled(s: StageSession) -> bool:
    """The session's tree was observed quiescent (or never existed remotely)."""
    return s.lifecycle in SETTLED_LIFECYCLES and not s.external_active


#: Ambiguous effect kinds that may have started (or queued) agent work.
_WORK_UNKNOWN_KINDS = frozenset({"send_message", "resolve_elicitation", "create_session"})


def uncertain(p: Parcel) -> bool:
    """Any external write with an ambiguous outcome is outstanding."""
    return bool(p.unknown_effects)


def message_uncertain(p: Parcel, s: StageSession) -> bool:
    """A work-bearing write to ``s``'s tree may or may not have been delivered."""
    return s.message_unknown or any(
        u.kind in _WORK_UNKNOWN_KINDS and u.session_id in (s.session_id, None)
        for u in p.unknown_effects
    )


def board_pending(p: Parcel) -> bool:
    """A daemon board write is in flight (not yet trusted-retired) or queued behind it."""
    return bool(p.pending_moves) or p.queued_move is not None


def all_settled(p: Parcel, *, except_id: str | None = None) -> bool:
    """Q: every other tree settled and no ambiguous external write outstanding.

    An unreconciled message could be a queued input that relaunches a tree, so any
    ambiguity makes quiescence unknown (§5.3).
    """
    return (
        not uncertain(p)
        and not board_pending(p)
        and all(settled(s) for s in p.sessions if s.session_id != except_id)
    )


def gate_open(p: Parcel, s: StageSession) -> bool:
    """Effective execution gate: current, unfenced, not closed/terminal/draining."""
    return (
        s.session_id == p.current_session_id
        and not s.fences
        and not s.execution_closed
        and s.lifecycle
        not in (
            Lifecycle.RETIRED,
            Lifecycle.FENCED,
            Lifecycle.FAILED,
            Lifecycle.BLOCKED,
            Lifecycle.DRAINING,
            Lifecycle.UNKNOWN,
            Lifecycle.CHECKPOINT_GRACE,
            Lifecycle.CHECKPOINT_WAIT,
        )
    )


def executable(p: Parcel, s: StageSession) -> bool:
    return gate_open(p, s) and s.lifecycle in EXECUTABLE_LIFECYCLES


def only_checkpoint_fenced(s: StageSession) -> bool:
    return s.fences <= frozenset({FenceKind.CHECKPOINT})


def plan_ok(p: Parcel, approval: Approval | None = None) -> bool:
    """PlanOK for ``approval`` (or for the latest contract when checking a candidate)."""
    if not (eligible(p) and no_open_decisions(p)) or p.revision_pending:
        return False
    latest = p.current_contract
    if latest is None or not latest.published or not latest.intact or latest.superseded:
        return False
    if approval is None:
        return True
    return (
        approval.kind == ApprovalKind.PLAN
        and approval.valid
        and approval.contract_id == latest.contract_id
        and approval.full_hash == latest.full_hash
    )


def skip_ok(p: Parcel, approval: Approval) -> bool:
    """SkipOK: waiver snapshot unchanged since approval (edit history, not just digest)."""
    return (
        eligible(p)
        and no_open_decisions(p)
        and not p.revision_pending
        and approval.kind == ApprovalKind.SKIP
        and approval.valid
        and approval.issue_edit_count == p.issue_edit_count
    )


def approval_ok(p: Parcel) -> bool:
    a = p.current_approval
    if a is None:
        return False
    return plan_ok(p, a) if a.kind == ApprovalKind.PLAN else skip_ok(p, a)


def authority_ok(p: Parcel, s: StageSession) -> bool:
    """Stage authority for ``s`` is still persisted and, for a build, approval-backed."""
    auth = p.authorization(s.authorization_id)
    if auth is None or auth.cancelled:
        return False
    if auth.eligibility_epoch != p.eligibility_epoch or not dispatchable(p):
        return False
    if s.kind == SessionKind.BUILD:
        return (
            auth.approval_id is not None
            and auth.approval_id == p.current_approval_id
            and (approval_ok(p))
        )
    return True


def build_ok(p: Parcel, s: StageSession) -> bool:
    """BuildOK for a work-bearing build relay (grant and gate included)."""
    return (
        s.kind == SessionKind.BUILD
        and authority_ok(p, s)
        and s.grant.ready
        and s.grant.remaining_us > 0
        and gate_open(p, s)
        and not message_uncertain(p, s)
        and not board_pending(p)
    )


def work_allowed(p: Parcel, s: StageSession) -> bool:
    """Common preflight for any work-bearing relay to ``s``.

    Includes the run's verified policy barrier: until the root's exact policy set (with
    the caller-identity guard) is verified after propagation, nothing may run.
    """
    if not s.policy_ready:
        return False
    if not (gate_open(p, s) and authority_ok(p, s)) or message_uncertain(p, s):
        return False
    if board_pending(p):  # the column (and so the authority it implies) is unresolved
        return False
    return s.grant.ready and s.grant.remaining_us > 0
