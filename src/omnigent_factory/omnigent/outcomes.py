"""Map Omnigent adapter outcomes to core observation bodies (architecture §3.4).

The executor (Task 4) owns the store transition; this pure helper tells it which
normalized events an outcome implies, so the adapter's ``Ack.detail`` contract is not
re-interpreted ad hoc. A ``RetryableReadFailure`` yields no event (retry later); an
``AmbiguousWrite`` yields ``EffectUnknown`` and never a blind retry.

``PolicyReady`` is *not* produced here: the executor emits it once the returned
``ready_at_us`` propagation barrier has passed (§5.2 cross-replica cache TTL).
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    AmbiguousWrite,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
)
from omnigent_factory.omnigent.adapter import ELICITATION_NOT_PENDING


def observations(effect: EffectIntent, outcome: AdapterOutcome) -> tuple[ev.EventBody, ...]:
    sid = effect.preconditions.session_id or ""
    kind = effect.kind
    if isinstance(outcome, AmbiguousWrite):
        return (ev.EffectUnknown(effect.effect_id, kind.value, sid or None),)
    if isinstance(outcome, DefinitiveFailure):
        if kind == EffectKind.CREATE_SESSION:
            return (ev.CreateRejected(session_id=sid, reason=outcome.reason),)
        if kind == EffectKind.RESOLVE_ELICITATION and outcome.reason == ELICITATION_NOT_PENDING:
            eid = effect.args.get("elicitation_id")
            return (ev.ElicitationGone(session_id=sid, elicitation_id=str(eid)),)
        return ()
    if not isinstance(outcome, Ack):
        return ()
    d = outcome.detail
    if kind == EffectKind.CREATE_SESSION and outcome.remote_id:
        return (
            ev.SessionCreated(session_id=sid, root_id=outcome.remote_id, nonce=str(d["nonce"])),
        )
    if kind == EffectKind.PREPARE_SESSION:
        return (
            ev.Prepared(
                session_id=sid,
                ok=bool(d.get("ok")),
                unexpected_turn=bool(d.get("unexpected_turn")),
                unusable=bool(d.get("unusable")),
                reason=str(d.get("reason") or "")[:200] if d.get("unusable") else "",
                note=str(d.get("note") or "")[:200],
                policy_ready_at_us=_int(d.get("policy_ready_at_us")),
            ),
        )
    if kind in (EffectKind.SEND_MESSAGE, EffectKind.RESOLVE_ELICITATION) and outcome.remote_id:
        return (
            ev.MessageAck(session_id=sid, effect_id=effect.effect_id, item_id=outcome.remote_id),
        )
    if kind == EffectKind.VERIFY_POLICIES:
        return (
            ev.PoliciesVerified(
                session_id=sid,
                ok=d.get("ok") is True,
                reconciled=d.get("reconciled") is True,
                ready_at_us=_int(d.get("ready_at_us")),
            ),
        )
    if kind == EffectKind.CLOSE_SESSION:
        return (ev.IssueSessionClosed(root_id=str(effect.args.get("root_id") or "")),)
    if kind == EffectKind.SCAN_TREE:
        return (
            ev.TreeQuiescent(
                session_id=sid,
                complete=bool(d.get("complete")),
                busy=bool(d.get("busy", True)),
                pending_waiter=bool(d.get("pending_waiter")),
            ),
        )
    if kind == EffectKind.RECONCILE_SESSION:
        return _reconciled(effect, sid, outcome)
    return ()


def _reconciled(effect: EffectIntent, sid: str, outcome: Ack) -> tuple[ev.EventBody, ...]:
    d = outcome.detail
    if d.get("kind") == "adoption":
        root = d.get("root_id")
        matches = d.get("matches")
        return (
            ev.AdoptionResult(
                session_id=sid,
                matches=matches if isinstance(matches, int) else 0,
                root_id=root if isinstance(root, str) else None,
                nonce=str(d.get("nonce") or ""),
            ),
        )
    target = effect.args.get("effect_id")
    if not isinstance(target, str):
        return ()
    state = d.get("state")
    if state == "delivered":
        return (
            ev.EffectReconciled(
                effect_id=target, session_id=sid or None, delivered=True, item_id=str(d["item_id"])
            ),
        )
    if state in ("absent", "still_pending"):
        return (ev.EffectReconciled(effect_id=target, session_id=sid or None, delivered=False),)
    # pending_input / ambiguous / gone: the outcome stays UNKNOWN (no proof either way).
    return ()


def _int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0
