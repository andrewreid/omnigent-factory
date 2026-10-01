"""Hypothesis stateful model: §2.8 invariants over generated event sequences.

Two parcels share one repository admission snapshot with ``max_building=1`` and
``max_open_bot_prs=2``. Every step draws an event (controls from the owner or another
actor, safety facts, adapter observations, scheduler inputs, stale/forged inputs and
verbatim replays) and applies it through the real reducer. Invariants are checked on
every transition (``_check_transition``) and on every state (``@invariant``).
"""

from __future__ import annotations

import hashlib

from hypothesis import event as hyp_event
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from omnigent_factory.core import events as ev
from omnigent_factory.core.admission import admission_rejection
from omnigent_factory.core.effects import (
    WORK_BEARING_KINDS,
    EffectKind,
    MessagePurpose,
    profile_for,
)
from omnigent_factory.core.events import EventClass, EventKind, Provenance
from omnigent_factory.core.predicates import (
    approval_ok,
    gate_open,
    message_uncertain,
    settled,
)
from omnigent_factory.core.projection import project_bot, ready_bot_ok
from omnigent_factory.core.reducer import TransitionResult
from omnigent_factory.core.types import (
    ApprovalKind,
    DecisionImpact,
    FenceKind,
    Hold,
    InboxHoldReason,
    Lifecycle,
    Parcel,
    ReservationKind,
    SessionKind,
    Size,
    Stage,
    Via,
    is_leftward,
)
from omnigent_factory.testing.builders import (
    OTHER_USER_ID,
    OWNER_ID,
    config,
    contract_text,
    snapshot,
)
from omnigent_factory.testing.harness import HEAD, Harness

PIDS = ("I_parcel_1", "I_parcel_2")
OWNERS = frozenset({OWNER_ID})
CAP_BUILD = 1
CAP_PR = 2
HEADS = (HEAD, "b" * 40)


def draw_body(data: st.DataObject, p: Parcel) -> tuple[ev.EventBody, dict[str, object]]:
    """Draw one event body relevant to ``p`` (sequential draws keep steps cheap)."""
    d = data.draw
    cur = p.current_session
    all_sids = [s.session_id for s in p.sessions] or ["missing"]
    sid = d(st.sampled_from([cur.session_id] * 3 + all_sids if cur else all_sids))
    s = p.session(sid)
    gid = d(st.sampled_from([s.grant.grant_id, "gr-x"] if s else ["gr-x"]))
    nonce = d(st.sampled_from([s.nonce, s.nonce, "forged"] if s else ["forged"]))
    root = d(st.sampled_from([(s.root_id or "root-x") if s else "root-x", "root-stale"]))
    elicit = d(st.sampled_from([x.elicitation_id for x in p.decisions] or ["el-x"]))
    contract_id = d(st.sampled_from([c.contract_id for c in p.contracts] or ["ct-x"]))
    stage = d(st.sampled_from([*Stage, None]))
    stage2 = d(st.sampled_from([*Stage, None]))
    via = d(st.sampled_from(list(Via)))
    flag = d(st.booleans())
    flag2 = d(st.booleans())
    pr = d(st.sampled_from([7, 8, 9]))
    head = d(st.sampled_from(HEADS))
    actor = d(st.sampled_from([OWNER_ID, OWNER_ID, OWNER_ID, OTHER_USER_ID]))
    evidence = d(
        st.sampled_from(
            [
                None,
                snapshot(),
                snapshot(),
                snapshot(title="T2"),
                snapshot(human_assigned=True),
                snapshot(open=False),
                snapshot(in_project=False),
                snapshot(stage=stage),
                snapshot(stage=stage2),
                snapshot(stage=p.stage),
                snapshot(stage=stage, title="T2"),
            ]
        )
    )
    decision_ids = [x.decision_id for x in p.decisions]
    builders = [
        # controls
        lambda: ev.RequestTriage(via=via),
        lambda: ev.RequestPlan(via=via),
        lambda: ev.RequestReplan(),
        lambda: ev.PlanFeedback(text_digest="t"),
        lambda: ev.ApprovePlan(
            via=via,
            hash_text=(p.current_contract.prefix if p.current_contract and flag else None),
        ),
        lambda: ev.WaivePlan(via=via),
        lambda: ev.Decide(
            decision_id=(d(st.sampled_from(decision_ids)) if decision_ids and flag else None),
            answer="a",
            within_contract=flag2,
        ),
        lambda: ev.Continue(duration_us=3_600_000_000 if flag else None),
        lambda: ev.Stop(),
        lambda: ev.RequestRework(),
        # safety
        lambda: ev.LeftwardMove(from_stage=stage, to_stage=stage2),
        lambda: ev.AssignedHuman(),
        lambda: ev.Closed(),
        lambda: ev.ItemRemoved(),
        lambda: ev.WaiverEdited(),
        lambda: ev.ApprovalInvalidated(approval_id=p.current_approval_id or "ap-x", reason="x"),
        lambda: ev.ContractTampered(contract_id=contract_id),
        # inbox holds (parked / unverified deliveries)
        lambda: ev.InboxHoldSet(
            delivery_guid=d(st.sampled_from(["g1", "g2"])),
            reason=d(st.sampled_from(list(InboxHoldReason))),
        ),
        lambda: ev.InboxHoldReleased(delivery_guid=d(st.sampled_from(["g1", "g2", "g-x"]))),
        # adapter
        lambda: ev.SessionCreated(
            session_id=sid, root_id=d(st.sampled_from(["r1", "r2", "r3"])), nonce=nonce
        ),
        lambda: ev.Prepared(session_id=sid, ok=flag or flag2, unexpected_turn=flag and flag2),
        lambda: ev.PoliciesVerified(session_id=sid, ok=flag or flag2, reconciled=flag and flag2),
        lambda: ev.PolicyGuardFailed(session_id=sid),
        lambda: ev.TreeQuiescent(
            session_id=sid,
            complete=flag or flag2,
            busy=flag and flag2,
            pending_waiter=flag and not flag2,
        ),
        lambda: ev.TreeQuiescent(session_id=sid, complete=True, busy=False),
        lambda: ev.StopTimeout(session_id=sid),
        lambda: ev.RuntimeActivity(session_id=sid, busy=flag),
        lambda: ev.OwnerDirectOmnigentMessage(session_id=sid),
        lambda: ev.EffectUnknown(
            effect_id="e1",
            effect_kind=d(st.sampled_from(["create_session", "send_message", "post_comment"])),
            session_id=sid,
        ),
        lambda: ev.EffectCancelled(effect_id="e2", effect_kind="create_session", session_id=sid),
        lambda: ev.AdoptionResult(
            session_id=sid,
            matches=d(st.integers(0, 2)),
            root_id="r9" if flag else None,
            nonce=nonce,
        ),
        lambda: ev.CreateRejected(session_id=sid),
        lambda: ev.MessageAck(session_id=sid, effect_id="e1", item_id="i"),
        lambda: ev.EffectReconciled(effect_id="e1", session_id=sid, delivered=flag, item_id="i"),
        lambda: ev.SessionCrashed(session_id=sid),
        lambda: ev.ElicitationOpened(
            session_id=sid,
            elicitation_id=d(st.sampled_from(["q1", "q2", "q3"])),
            impact=d(st.sampled_from(list(DecisionImpact))),
            cost_ask=flag and flag2,
        ),
        lambda: ev.ElicitationResolved(session_id=sid, elicitation_id=elicit, correlated=flag),
        lambda: ev.ElicitationGone(session_id=sid, elicitation_id=elicit),
        lambda: ev.ContractPublished(
            contract_id=contract_id, comment_id="c", verified=flag or flag2
        ),
        lambda: ev.ActiveTimeSample(
            session_id=sid, grant_id=gid, consumed_us=d(st.integers(0, 8 * 3_600_000_000))
        ),
        lambda: ev.PolicyReady(session_id=sid, grant_id=gid),
        lambda: ev.ResultCandidate(
            session_id=sid,
            root_id=root,
            revision=d(st.sampled_from([s.revision if s else p.revision, max(0, p.revision - 1)])),
            valid=flag or flag2,
            result_kind=d(st.sampled_from(list(ev.ResultKind))),
            publication_kind=d(st.sampled_from(list(ev.PublicationKind))),
            contract_canonical=contract_text("M", d(st.sampled_from(["g1", "g2", "g3"]))),
            size=d(st.sampled_from(list(Size))),
            pr_number=pr,
            head_sha=head,
            open_decision_ids=tuple(x.decision_id for x in p.open_decisions),
        ),
        # github
        lambda: ev.GitHubSnapshot(),
        lambda: ev.ColumnObserved(stage=stage),
        lambda: ev.PRObserved(
            pr_number=pr,
            head_sha=head,
            open=flag,
            merged=flag2,
            bot_authored=d(st.booleans()),
            parcel_branch=d(st.booleans()),
        ),
        lambda: ev.ChecksChanged(
            pr_number=pr, head_sha=head, state=d(st.sampled_from(list(ev.ChecksState)))
        ),
        lambda: ev.ReviewChanged(pr_number=pr, head_sha=head, changes_requested=flag),
        lambda: ev.ReadinessEvidence(
            session_id=sid,
            pr_number=pr,
            head_sha=head,
            verified=flag or flag2,
            remediation_exhausted=flag and flag2,
        ),
        # scheduler
        lambda: ev.CapacityAvailable(),
        lambda: ev.ActiveLimitReached(session_id=sid, grant_id=gid),
        lambda: ev.GraceExpired(session_id=sid, grant_id=gid),
        lambda: ev.ReconcileDue(),
    ]
    body = d(st.sampled_from(builders))()
    return body, {"actor": actor, "evidence": evidence}


class FactoryModel(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        # Board writes stay pending until a rule plays the executor's outcome.
        self.h = Harness(
            cfg=config(max_building=CAP_BUILD, max_open_bot_prs=CAP_PR), auto_ack_moves=False
        )
        for pid in PIDS:
            self.h.eligible(pid)
        self.events: list[ev.Event] = []
        self.created: set[str] = set()
        self.roots = 0

    # ------------------------------------------------------------------ rules

    @rule(data=st.data(), pid=st.sampled_from(PIDS))
    def step(self, data: st.DataObject, pid: str) -> None:
        p = self.h.p(pid)
        body, extras = draw_body(data, p)
        f = self.h.f(pid)
        kw: dict[str, object] = {}
        if body.CLASS == EventClass.CONTROL:
            kw["actor"] = extras["actor"]
            if extras["evidence"] is not None:
                kw["evidence"] = extras["evidence"]
        elif isinstance(body, ev.GitHubSnapshot):
            kw["evidence"] = extras["evidence"] or snapshot()
            kw["provenance"] = Provenance.RECONCILER
        elif body.CLASS == EventClass.SAFETY:
            kw["actor"] = extras["actor"]
        if isinstance(body, ev.InboxHoldReleased):
            # the operator releases parked holds; the inbox releases unresolved ones
            kw["provenance"] = data.draw(st.sampled_from([Provenance.INBOX, Provenance.OPERATOR]))
        if isinstance(body, ev.ContractPublished):
            body = ev.ContractPublished(
                contract_id=body.contract_id,
                comment_id=f"c{len(self.events)}",
                verified=body.verified,
                posted_at_us=f.now,
            )
        # Untrusted variants: any event may arrive with any provenance.
        forced = data.draw(st.sampled_from([None, None, None, *Provenance]))
        if forced is not None:
            kw["provenance"] = forced
        event = f.make(body, **kw)  # type: ignore[arg-type]
        self._apply(event)

    def _next_action(self, p: Parcel) -> str | None:
        s = p.current_session
        entry = self.h.admission.queue_entry(p.parcel_id)
        if p.pending_moves:
            return "ack_move"
        if any(
            x.root_id is not None and x.lifecycle in (Lifecycle.DRAINING, Lifecycle.BLOCKED)
            for x in p.sessions
        ):
            return "quiesce"
        if s is not None and s.lifecycle == Lifecycle.INTENT:
            return "create"
        if any(not c.published for c in p.contracts):
            return "publish"
        if s is not None and s.lifecycle in (Lifecycle.ACTIVE, Lifecycle.CHECKPOINT_GRACE):
            return "result"
        if s is not None and s.lifecycle == Lifecycle.CHECKPOINT_WAIT:
            return "quiesce"
        if p.readiness is not None and not p.readiness.ready:
            return "ready" if not p.readiness.verified else "quiesce"
        if entry is not None and entry.status.value == "QUEUED":
            return "admit"
        if p.current_contract is not None and not p.revision_pending and p.stage == Stage.SCOPED:
            return "approve"
        if s is None or settled(s):
            return "start"
        return None

    @rule(pid=st.sampled_from(PIDS), steps=st.integers(1, 8))
    def drive(self, pid: str, steps: int) -> None:
        """Guided happy-path progress so generated traces reach deep states."""
        for _ in range(steps):
            p = self.h.p(pid)
            action = self._next_action(p)
            if action is None:
                return
            self._do(pid, p, action)

    def _do(self, pid: str, p: Parcel, action: str) -> None:
        s = p.current_session
        f = self.h.f(pid)
        bodies: list[ev.EventBody] = []
        if action == "create" and s is not None:
            self.roots += 1
            bodies = [
                ev.SessionCreated(session_id=s.session_id, root_id=f"g{self.roots}", nonce=s.nonce),
                ev.Prepared(session_id=s.session_id, ok=True),
                ev.PoliciesVerified(session_id=s.session_id, ok=True),
            ]
        elif action == "result" and s is not None and s.root_id is not None:
            kind = {
                SessionKind.TRIAGE: ev.ResultKind.TRIAGE,
                SessionKind.PLAN: ev.ResultKind.PLAN,
                SessionKind.BUILD: ev.ResultKind.BUILD_READY,
            }[s.kind]
            if s.lifecycle == Lifecycle.CHECKPOINT_GRACE:
                kind = ev.ResultKind.CHECKPOINT
            bodies = [
                ev.ResultCandidate(
                    session_id=s.session_id,
                    root_id=s.root_id,
                    revision=s.revision,
                    valid=True,
                    result_kind=kind,
                    publication_kind=ev.PublicationKind.CONTRACT,
                    contract_canonical=contract_text("M", f"goal-{s.revision}-{len(p.contracts)}"),
                    size=Size.M,
                    pr_number=7,
                    head_sha=HEAD,
                    open_decision_ids=tuple(x.decision_id for x in p.open_decisions),
                )
            ]
        elif action == "publish":
            pending = [c for c in p.contracts if not c.published]
            bodies = [
                ev.ContractPublished(
                    contract_id=pending[-1].contract_id,
                    comment_id=f"c-{pending[-1].contract_id}",
                    verified=True,
                    posted_at_us=f.now,
                )
            ]
        elif action == "ack_move":
            m = p.pending_moves[0]
            bodies = [ev.ColumnObserved(stage=m.to_stage, daemon_effect_id=m.effect_id)]
        elif action == "approve":
            bodies = [ev.ApprovePlan(via=Via.DRAG)]
        elif action == "admit":
            bodies = [ev.CapacityAvailable()]
        elif action == "ready" and p.readiness is not None:
            r = p.readiness
            bodies = [
                ev.PRObserved(
                    pr_number=r.pr_number,
                    head_sha=r.head_sha,
                    open=True,
                    bot_authored=True,
                    parcel_branch=True,
                ),
                ev.ReadinessEvidence(
                    session_id=r.session_id,
                    pr_number=r.pr_number,
                    head_sha=r.head_sha,
                    verified=True,
                ),
            ]
        elif action == "quiesce":
            bodies = [
                ev.TreeQuiescent(session_id=x.session_id, complete=True, busy=False)
                for x in p.sessions
                if x.root_id is not None and not settled(x)
            ][:1]
        elif action == "start":
            bodies = [
                ev.RequestPlan(via=Via.DRAG)
                if p.stage == Stage.TRIAGED
                else ev.RequestTriage(via=Via.DRAG)
            ]
        for body in bodies:
            self._apply(f.make(body))

    @rule(
        pid=st.sampled_from(PIDS),
        what=st.sampled_from(
            [
                "limit",
                "grace",
                "continue",
                "policy",
                "stop",
                "assign",
                "feedback",
                "replan",
                "elicit",
                "cost_ask",
                "decide",
                "crash",
                "unknown_msg",
                "restore",
                "waive",
                "edit",
                "ack_exact",
                "ack_wrong",
                "reconcile_absent",
                "snap_left",
                "snap_own",
                "move_cancel",
                "move_unknown",
                "forged_move_ack",
                "forged_msg_ack",
                "forged_reconcile",
                "stale_continue",
                "stale_approve",
                "owner_scoped_drag",
            ]
        ),
    )
    def disrupt(self, pid: str, what: str) -> None:
        """Well-formed disruptive inputs aimed at the *current* session."""
        p = self.h.p(pid)
        s = p.current_session
        sid = s.session_id if s else "missing"
        gid = s.grant.grant_id if s else "missing"
        unknown = p.unknown_effects[0] if p.unknown_effects else None
        move = p.pending_moves[0] if p.pending_moves else None
        sent = p.sent_effects[-1] if p.sent_effects else None
        body: ev.EventBody = {
            "limit": ev.ActiveLimitReached(session_id=sid, grant_id=gid),
            "grace": ev.GraceExpired(session_id=sid, grant_id=gid),
            "continue": ev.Continue(),
            "policy": ev.PolicyReady(session_id=sid, grant_id=gid),
            "stop": ev.Stop(),
            "assign": ev.AssignedHuman(),
            "feedback": ev.PlanFeedback(text_digest="fb"),
            "replan": ev.RequestReplan(),
            "elicit": ev.ElicitationOpened(
                session_id=sid,
                elicitation_id=f"q{len(self.events)}",
                impact=DecisionImpact.WITHIN_CONTRACT,
            ),
            "cost_ask": ev.ElicitationOpened(
                session_id=sid, elicitation_id=f"a{len(self.events)}", cost_ask=True
            ),
            "decide": ev.Decide(answer="yes", within_contract=True),
            "crash": ev.SessionCrashed(session_id=sid),
            "unknown_msg": ev.EffectUnknown(
                effect_id=f"m{len(self.events)}", effect_kind="send_message", session_id=sid
            ),
            "restore": ev.GitHubSnapshot(),
            "waive": ev.WaivePlan(via=Via.LABEL),
            "edit": ev.WaiverEdited(),
            "ack_exact": ev.MessageAck(
                session_id=unknown.session_id if unknown else sid,
                effect_id=unknown.effect_id if unknown else "none",
                item_id=f"it{len(self.events)}",
            ),
            "ack_wrong": ev.MessageAck(
                session_id="wrong",
                effect_id=unknown.effect_id if unknown else "none",
                item_id="x",
            ),
            "reconcile_absent": ev.EffectReconciled(
                effect_id=unknown.effect_id if unknown else "none",
                session_id=unknown.session_id if unknown else sid,
            ),
            "snap_left": ev.GitHubSnapshot(),
            "snap_own": ev.GitHubSnapshot(),
            "move_cancel": ev.EffectCancelled(
                effect_id=move.effect_id if move else "none", effect_kind="move_card"
            ),
            "move_unknown": ev.EffectUnknown(
                effect_id=move.effect_id if move else "none", effect_kind="move_card"
            ),
            "forged_move_ack": ev.ColumnObserved(
                stage=move.to_stage if move else Stage.BUILDING,
                daemon_effect_id=move.effect_id if move else "none",
            ),
            "forged_msg_ack": ev.MessageAck(
                session_id=sent[1] if sent else sid,
                effect_id=sent[0] if sent else "none",
                item_id="forged",
            ),
            "forged_reconcile": ev.EffectReconciled(
                effect_id=unknown.effect_id if unknown else "none",
                session_id=unknown.session_id if unknown else sid,
            ),
            "stale_continue": ev.Continue(),
            "stale_approve": ev.ApprovePlan(via=Via.COMMAND),
            "owner_scoped_drag": ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.SCOPED),
        }[what]
        kw: dict[str, object] = {}
        fresh = self.h.f(pid).now + 1  # read for this event, after any landed write
        if what == "restore":
            kw = {"evidence": snapshot(read_at_us=fresh), "provenance": Provenance.RECONCILER}
        if what == "snap_left":
            order = [Stage.INBOX, Stage.TRIAGED, Stage.SCOPED, Stage.BUILDING, Stage.READY]
            left = order[max(0, order.index(p.stage) - 1)] if p.stage in order else Stage.INBOX
            kw = {
                "evidence": snapshot(stage=left, read_at_us=fresh),
                "provenance": Provenance.RECONCILER,
            }
        if what.startswith("forged"):
            kw = {"provenance": Provenance.WEBHOOK, "actor": OTHER_USER_ID}
        if what.startswith("stale"):
            order = [Stage.INBOX, Stage.TRIAGED, Stage.SCOPED, Stage.BUILDING, Stage.READY]
            left = order[max(0, order.index(p.stage) - 1)] if p.stage in order else Stage.INBOX
            kw = {"evidence": snapshot(stage=left, read_at_us=fresh)}
        if what == "snap_own":
            move = p.pending_moves[-1] if p.pending_moves else None
            seen = (move.from_stage or move.to_stage) if move else p.stage
            kw = {
                "evidence": snapshot(stage=seen, read_at_us=fresh),
                "provenance": Provenance.RECONCILER,
            }
        if what == "assign":
            kw = {"actor": OTHER_USER_ID}
        if what == "owner_scoped_drag":
            kw = {"actor": OWNER_ID}
        self._apply(self.h.f(pid).make(body, **kw))  # type: ignore[arg-type]

    @rule(
        pid=st.sampled_from(PIDS),
        kind=st.sampled_from(["send_message", "resolve_elicitation", "post_comment"]),
        follow=st.sampled_from(["feedback", "decide", "continue", "drive", "ready"]),
    )
    def ambiguity_then_work(self, pid: str, kind: str, follow: str) -> None:
        """An ambiguous write on the current tree followed by input that would do work."""
        p = self.h.p(pid)
        s = p.current_session
        if s is None:
            return
        f = self.h.f(pid)
        self._apply(
            f.make(
                ev.EffectUnknown(
                    effect_id=f"amb{len(self.events)}", effect_kind=kind, session_id=s.session_id
                )
            )
        )
        if follow == "feedback":
            self._apply(f.make(ev.PlanFeedback(text_digest="amb")))
        elif follow == "decide":
            self._apply(
                f.make(
                    ev.ElicitationOpened(
                        session_id=s.session_id,
                        elicitation_id=f"aq{len(self.events)}",
                        impact=DecisionImpact.WITHIN_CONTRACT,
                    )
                )
            )
            self._apply(f.make(ev.Decide(answer="y", within_contract=True)))
        elif follow == "continue":
            cur = self.h.p(pid).current_session
            if cur is not None:
                self._apply(
                    f.make(
                        ev.ActiveLimitReached(
                            session_id=cur.session_id, grant_id=cur.grant.grant_id
                        )
                    )
                )
                self._apply(f.make(ev.Continue()))
                cur = self.h.p(pid).current_session
                if cur is not None:
                    self._apply(
                        f.make(
                            ev.PolicyReady(session_id=cur.session_id, grant_id=cur.grant.grant_id)
                        )
                    )
        elif follow == "drive":
            self.drive(pid, 6)
        else:
            for action in ("result", "ready", "quiesce"):
                self._do(pid, self.h.p(pid), action)

    @rule(
        pid=st.sampled_from(PIDS),
        cmd=st.sampled_from(["triage", "plan", "approve", "replan"]),
        steps=st.integers(1, 6),
    )
    def command_then_work(self, pid: str, cmd: str, steps: int) -> None:
        """A command whose daemon board write stays unacknowledged while progress
        (creation, preparation, results, feedback) is attempted."""
        f = self.h.f(pid)
        body: ev.EventBody = {
            "triage": ev.RequestTriage(via=Via.COMMAND),
            "plan": ev.RequestPlan(via=Via.COMMAND),
            "approve": ev.ApprovePlan(via=Via.COMMAND),
            "replan": ev.RequestReplan(via=Via.COMMAND),
        }[cmd]
        self._apply(f.make(body))
        for _ in range(steps):
            p = self.h.p(pid)
            action = self._next_action(p)
            if action == "ack_move":  # withhold the executor's acknowledgement
                s = p.current_session
                if s is not None and s.lifecycle == Lifecycle.INTENT:
                    action = "create"
                elif s is not None and s.root_id is not None:
                    action = "result"
                else:
                    action = "admit"
            if action is None:
                return
            self._do(pid, p, action)
            self._apply(f.make(ev.PlanFeedback(text_digest="w")))

    @rule(
        pid=st.sampled_from(PIDS),
        pick=st.integers(0, 7),
        outcome=st.sampled_from(["landed", "cancelled", "absent", "delivered"]),
    )
    def board_outcome(self, pid: str, pick: int, outcome: str) -> None:
        """Trusted executor outcome for ANY pending board write, in any order."""
        p = self.h.p(pid)
        if not p.pending_moves:
            return
        m = p.pending_moves[pick % len(p.pending_moves)]
        f = self.h.f(pid)
        if outcome == "landed":
            self._apply(f.make(ev.ColumnObserved(stage=m.to_stage, daemon_effect_id=m.effect_id)))
        elif outcome == "cancelled":
            self._apply(f.make(ev.EffectCancelled(effect_id=m.effect_id, effect_kind="move_card")))
        else:
            self._apply(f.make(ev.EffectUnknown(effect_id=m.effect_id, effect_kind="move_card")))
            self._apply(
                f.make(
                    ev.EffectReconciled(
                        effect_id=m.effect_id,
                        delivered=outcome == "delivered",
                        item_id="item" if outcome == "delivered" else "",
                    )
                )
            )

    @rule(pid=st.sampled_from(PIDS))
    def owner_drag_to_daemon_target(self, pid: str) -> None:
        """A fresh owner Building->Scoped drag while Scoped is the in-flight or queued
        daemon target (it must still record replan authority)."""
        p = self.h.p(pid)
        targets = {m.to_stage for m in p.pending_moves} | (
            {p.queued_move} if p.queued_move is not None else set()
        )
        if Stage.SCOPED not in targets:
            return
        self._apply(
            self.h.f(pid).make(
                ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.SCOPED),
                actor=OWNER_ID,
            )
        )

    @rule(data=st.data())
    def replay(self, data: st.DataObject) -> None:
        # Global operator events are deduplicated by the store's logical-key index.
        parcel_events = [e for e in self.events if e.parcel_id is not None]
        if not parcel_events:
            return
        event = data.draw(st.sampled_from(parcel_events))
        pid = event.parcel_id
        assert pid is not None
        before = (self.h.p(pid), self.h.admission)
        result = self.h.apply(event)
        # invariant 9: a replay is a duplicate, or (never admitted) rejected again unchanged
        admission_rejected = result.audit.reason in (
            "provenance-not-admitted",
            "control-from-non-owner",
        )
        assert (result.duplicate or admission_rejected) and result.effects == ()
        assert (self.h.p(pid), self.h.admission) == before

    @rule(paused=st.booleans())
    def operator_pause(self, paused: bool) -> None:
        body = ev.Pause() if paused else ev.Unpause()
        self._apply(self.h.f(PIDS[0]).make(body, parcel_id=None))

    # ---------------------------------------------------------- transition checks

    def _apply(self, event: ev.Event) -> None:
        pid = event.parcel_id
        before = self.h.p(pid) if pid else None
        adm_before = self.h.admission
        result = self.h.apply(event)  # invariant 17: never raises
        self.events.append(event)
        after = self.h.p(pid) if pid else None
        self._check_transition(event, before, after, adm_before, result)

    def _check_transition(
        self,
        event: ev.Event,
        before: Parcel | None,
        after: Parcel | None,
        adm_before: object,
        result: TransitionResult,
    ) -> None:
        effects = result.effects
        if not result.audit.accepted:
            assert not [e for e in effects if e.kind in WORK_BEARING_KINDS]
        if before is None or after is None:
            return
        # (F1) ambiguity is an execution and readiness gate, cleared only by an exact answer.
        for e in effects:
            if e.kind in WORK_BEARING_KINDS:
                s = after.session(e.preconditions.session_id)
                assert s is not None and not message_uncertain(after, s)
        was_ready = before.readiness is not None and before.readiness.ready
        if after.readiness is not None and after.readiness.ready and not was_ready:
            assert not after.unknown_effects
        removed = {u.effect_id for u in before.unknown_effects} - {
            u.effect_id for u in after.unknown_effects
        }
        if removed:
            assert event.kind in (EventKind.MESSAGE_ACK, EventKind.EFFECT_RECONCILED)
            assert removed == {event.body.effect_id}  # type: ignore[union-attr]
        # (r2 F2/F3) nothing untrusted is admitted: no state change at all.
        if admission_rejection(event.kind, event.provenance, event.actor_id, OWNERS):
            assert not result.audit.accepted and effects == ()
            assert after == before and self.h.admission == adm_before
            return
        # (r2 F2) no work while a daemon board write is unresolved.
        if after.pending_moves:
            assert not [e for e in effects if e.kind in WORK_BEARING_KINDS]
        # (r4 F2) a non-column safety/stop fact drops any queued daemon board write.
        if result.audit.accepted and event.kind in (
            EventKind.ASSIGNED_HUMAN,
            EventKind.CLOSED,
            EventKind.TRANSFERRED,
            EventKind.DELETED,
            EventKind.ITEM_REMOVED,
            EventKind.STOP,
        ):
            assert after.queued_move is None
            assert EffectKind.MOVE_CARD not in [e.kind for e in effects]
        # (r4 F1) a fresh owner Building->Scoped drag always records replan authority,
        # whatever daemon target is in flight or queued.
        if (
            result.audit.accepted
            and event.kind == EventKind.LEFTWARD_MOVE
            and event.actor_id == OWNER_ID
            and event.provenance in (Provenance.WEBHOOK, Provenance.RECOVERY)
            and (event.body.from_stage or before.stage) == Stage.BUILDING  # type: ignore[union-attr]
            and event.body.to_stage == Stage.SCOPED  # type: ignore[union-attr]
            and event.source_time_us > before.barrier_time_us
            and before.eligible
        ):
            assert any(
                a.source_event_id == event.event_id and a.kind == SessionKind.PLAN
                for a in after.authorizations
            )
        # (r3 F1) board writes are serialised and a trusted "landed" outcome never
        # changes the desired column (it may already be newer).
        assert len(after.pending_moves) <= 1
        assert after.queued_move is None or after.pending_moves
        retired = {m.effect_id for m in before.pending_moves} - {
            m.effect_id for m in after.pending_moves
        }
        landed = event.provenance == Provenance.ADAPTER and (
            (event.kind == EventKind.COLUMN_OBSERVED and event.body.daemon_effect_id in retired)  # type: ignore[union-attr]
            or (
                event.kind == EventKind.EFFECT_RECONCILED
                and event.body.delivered  # type: ignore[union-attr]
                and event.body.effect_id in retired  # type: ignore[union-attr]
            )
        )
        if landed:
            assert after.stage == before.stage
        # (r2 F2) a pending write is retired only by a trusted executor/operator outcome;
        # absence reconciles its source column (leftward => safety).
        gone = [m for m in before.pending_moves if after.pending_move(m.effect_id) is None]
        for m in gone:
            assert event.provenance in (Provenance.ADAPTER, Provenance.OPERATOR)
            assert event.kind in (
                EventKind.COLUMN_OBSERVED,
                EventKind.EFFECT_CANCELLED,
                EventKind.EFFECT_RECONCILED,
            )
            absent = event.kind == EventKind.EFFECT_CANCELLED or (
                event.kind == EventKind.EFFECT_RECONCILED and not event.body.delivered  # type: ignore[union-attr]
            )
            if absent and is_leftward(before.stage, m.from_stage) and not before.pending_moves[1:]:
                assert after.barrier_time_us >= event.source_time_us
        # (F2, r2 F1) a fresh leftward column observation - control envelopes included -
        # is a safety fact applied before the control; the control cannot resume past it.
        # A source-column read during a pending write is not treated as safe: the write
        # stays pending (work gated) until a trusted outcome.
        snap = event.evidence
        sources = {m.from_stage for m in before.pending_moves}
        targets = {m.to_stage for m in before.pending_moves} | (
            {before.queued_move} if before.queued_move is not None else set()
        )
        if (
            snap is not None
            and snap.stage is not None
            and event.kind not in (EventKind.LEFTWARD_MOVE, EventKind.COLUMN_OBSERVED)
            and before.stage is not None
        ):
            if snap.stage in sources:
                assert all(after.pending_move(m.effect_id) for m in before.pending_moves) or (
                    event.provenance == Provenance.ADAPTER
                )
            elif (
                snap.stage not in targets
                and is_leftward(before.stage, snap.stage)
                and snap.read_at_us >= before.board_written_at_us  # older reads are stale
            ):
                assert after.barrier_time_us >= event.source_time_us
                if event.event_class == EventClass.CONTROL:
                    assert not result.audit.accepted
                cur = before.current_session
                if cur is not None and cur.lifecycle != Lifecycle.RETIRED:
                    s2 = after.session(cur.session_id)
                    assert s2 is not None and FenceKind.SAFETY in s2.fences
        # (F3) stage allow-lists; Ready takes rework (a build), never triage or a plan.
        if result.audit.accepted and event.kind == EventKind.REQUEST_TRIAGE:
            assert before.stage in (None, Stage.INBOX) or (
                before.holds & {Hold.STOPPED, Hold.SAFETY}
                and before.stage not in (Stage.READY, Stage.DONE)
            )
        if result.audit.accepted and event.kind in (
            EventKind.REQUEST_PLAN,
            EventKind.REQUEST_REPLAN,
        ):
            assert before.stage not in (Stage.READY, Stage.DONE)
        # (F4) a waiver edit forces a published plan; no waiver while a revision is pending.
        a0 = before.current_approval
        if a0 is not None and a0.kind == ApprovalKind.SKIP and a0.valid:
            a1 = after.approval(a0.approval_id)
            if a1 is not None and not a1.valid and after.issue_edit_count > before.issue_edit_count:
                assert after.revision_pending
        new_skips = [
            a
            for a in after.approvals
            if a.kind == ApprovalKind.SKIP and before.approval(a.approval_id) is None
        ]
        if new_skips:
            assert not before.revision_pending
        # (2) every work-bearing effect cites persisted, current authority.
        for e in effects:
            if e.kind not in WORK_BEARING_KINDS:
                continue
            s = after.session(e.preconditions.session_id)
            assert s is not None
            auth = after.authorization(s.authorization_id)
            assert auth is not None and not auth.cancelled
            assert e.preconditions.authorization_id == s.authorization_id
            assert e.preconditions.grant_id == s.grant.grant_id
            assert s.session_id == after.current_session_id
            if e.args.get("purpose") == MessagePurpose.CHECKPOINT_CLEANUP.value:
                assert s.lifecycle == Lifecycle.CHECKPOINT_GRACE and not s.fences
                continue
            assert gate_open(after, s) and s.grant.ready and s.grant.remaining_us > 0
            if s.kind == SessionKind.BUILD:
                assert approval_ok(after) and auth.approval_id == after.current_approval_id
            if e.kind == EffectKind.ENABLE_ISSUANCE:
                assert e.args["profile"] == profile_for(s.kind).value  # (16)
        # (1)/(10) a new root is created only when every other tree is settled, once.
        for e in effects:
            if e.kind == EffectKind.CREATE_SESSION:
                sid = e.preconditions.session_id
                assert sid not in self.created
                self.created.add(sid)  # type: ignore[arg-type]
                assert all(settled(s) for s in after.sessions if s.session_id != sid)
            if e.kind == EffectKind.PREPARE_SESSION:
                s = after.session(e.preconditions.session_id)
                assert s is not None and e.args["profile"] == profile_for(s.kind).value
        # (3) safety/observation events never create authority.
        positive_half = isinstance(event.body, ev.LeftwardMove) and event.actor_id == OWNER_ID
        if event.event_class != EventClass.CONTROL and not positive_half:
            assert len(after.authorizations) == len(before.authorizations)
            assert len(after.approvals) == len(before.approvals)
        # (4) fences are only ever removed by PolicyReady, and only checkpoint.
        for s in before.sessions:
            s2 = after.session(s.session_id)
            assert s2 is not None
            removed = s.fences - s2.fences
            if removed:
                assert removed == {FenceKind.CHECKPOINT}
                assert event.kind == EventKind.POLICY_READY and not s2.fences
            # retired/closed gates never reopen
            if s.lifecycle == Lifecycle.RETIRED:
                assert s2.lifecycle == Lifecycle.RETIRED
            if s.execution_closed:
                assert s2.execution_closed
        # (6) revision_pending clears only on publication of that exact revision.
        if before.revision_pending and not after.revision_pending:
            assert event.kind == EventKind.CONTRACT_PUBLISHED
            assert after.current_contract is not None
            assert after.current_contract.revision == after.revision
        if after.revision > before.revision:
            assert after.revision_pending
        # (8) invalidated approvals never become valid again.
        for a in before.approvals:
            a2 = after.approval(a.approval_id)
            assert a2 is not None
            if not a.valid:
                assert not a2.valid
            assert (a2.kind, a2.full_hash, a2.owner_id) == (a.kind, a.full_hash, a.owner_id)
        # (15) stale-root / non-current results never alter contract/approval/readiness.
        if isinstance(event.body, ev.ResultCandidate) and not result.audit.accepted:
            assert after.contracts == before.contracts
            assert after.approvals == before.approvals
            assert after.readiness == before.readiness
        # (13) new reservations never exceed caps.
        adm = self.h.admission
        if adm.building_count > adm_before.building_count:  # type: ignore[attr-defined]
            assert adm.building_count <= CAP_BUILD
        new_pr = len(adm.live_reservations(ReservationKind.OPEN_PR))
        old_pr = len(adm_before.live_reservations(ReservationKind.OPEN_PR))  # type: ignore[attr-defined]
        if new_pr > old_pr:
            assert adm.prospective_pr_count <= CAP_PR
        # (Task 5a) inbox holds: set only from the inbox, released only by the matching
        # releaser, fence the current tree on arrival and block all dispatch/work.
        held_before = {h.delivery_guid: h.reason for h in before.inbox_holds}
        held_after = {h.delivery_guid: h.reason for h in after.inbox_holds}
        if held_after.keys() - held_before.keys():
            assert event.kind == EventKind.INBOX_HOLD_SET
            assert event.provenance == Provenance.INBOX
            cur = before.current_session
            if cur is not None and cur.lifecycle != Lifecycle.RETIRED:
                s2 = after.session(cur.session_id)
                assert s2 is not None and FenceKind.SAFETY in s2.fences
        for guid, reason in held_before.items():
            if guid not in held_after:
                assert event.kind == EventKind.INBOX_HOLD_RELEASED
                assert event.provenance == (
                    Provenance.OPERATOR if reason == InboxHoldReason.PARKED else Provenance.INBOX
                )
            elif reason == InboxHoldReason.PARKED:
                assert held_after[guid] == InboxHoldReason.PARKED  # never downgraded
        if after.inbox_holds:
            assert not [
                e
                for e in effects
                if e.kind in WORK_BEARING_KINDS or e.kind == EffectKind.CREATE_SESSION
            ]
        assert (Hold.INBOX in after.holds) == bool(after.inbox_holds)
        # (16) closed vocabulary; nothing merges/closes/bypasses.
        for e in effects:
            assert e.kind in EffectKind
            assert "merge" not in e.kind.value
            # Archiving an Omnigent session is the only "close": never an issue or PR.
            assert "close" not in e.kind.value or e.kind == EffectKind.CLOSE_SESSION

    # ------------------------------------------------------------ state invariants

    @invariant()
    def coverage_labels(self) -> None:
        """Label reached states for ``--hypothesis-show-statistics`` (no assertions)."""
        for pid in PIDS:
            p = self.h.p(pid)
            hyp_event(f"stage={p.stage}")
            if p.unknown_effects:
                hyp_event("ambiguous-effect-outstanding")
            if p.pending_moves:
                hyp_event("own-board-write-pending")
            if p.inbox_holds:
                hyp_event("inbox-hold")
            if p.revision_pending and any(a.kind == ApprovalKind.SKIP for a in p.approvals):
                hyp_event("revision-pending-after-waiver")
            for x in p.sessions:
                hyp_event(f"{x.kind.value}:{x.lifecycle.value}")
                for fence in x.fences:
                    hyp_event(f"fence={fence.value}")

    @invariant()
    def at_most_one_open_gate(self) -> None:  # (1)
        for pid in PIDS:
            p = self.h.p(pid)
            assert sum(1 for s in p.sessions if gate_open(p, s)) <= 1
            live = [s for s in p.sessions if not settled(s)]
            if len(live) > 1:
                # Several unsettled trees may coexist only while all but one are
                # fenced/draining towards closure; never two open gates.
                # (A retired/fenced root with observed external activity is not a gate.)
                assert (
                    sum(
                        1
                        for s in live
                        if not s.fences
                        and s.lifecycle
                        not in (
                            Lifecycle.DRAINING,
                            Lifecycle.UNKNOWN,
                            Lifecycle.BLOCKED,
                            Lifecycle.RETIRED,
                            Lifecycle.FENCED,
                        )
                    )
                    <= 1
                )

    @invariant()
    def approvals_bind_published_bytes(self) -> None:  # (7)
        for pid in PIDS:
            p = self.h.p(pid)
            for c in p.contracts:
                assert c.full_hash == hashlib.sha256(c.canonical.encode()).hexdigest()
            a = p.current_approval
            if a is not None and a.valid and a.kind == ApprovalKind.PLAN:
                c = p.contract(a.contract_id)
                assert c is not None and c.published and c.full_hash == a.full_hash
            if a is not None and a.kind == ApprovalKind.SKIP:
                assert a.contract_id is None and a.snapshot_canonical is not None

    @invariant()
    def reservations_within_caps(self) -> None:  # (13)
        adm = self.h.admission
        assert adm.building_count <= CAP_BUILD
        per_parcel: dict[tuple[str, ReservationKind], int] = {}
        for r in adm.reservations:
            if r.live:
                key = (r.parcel_id, r.kind)
                per_parcel[key] = per_parcel.get(key, 0) + 1
        assert all(v == 1 for v in per_parcel.values())

    @invariant()
    def ready_binds_verified_head(self) -> None:  # (14)
        for pid in PIDS:
            p = self.h.p(pid)
            if p.readiness is not None and p.readiness.ready:
                assert p.readiness.verified
                s = p.session(p.readiness.session_id)
                assert s is not None and s.lifecycle == Lifecycle.RETIRED

    @invariant()
    def ready_is_never_working(self) -> None:
        """Ready and Bot Working/Queued/Checkpoint are mutually exclusive (#461)."""
        for pid in PIDS:
            p = self.h.p(pid)
            assert ready_bot_ok(p, p.bot), (p.stage, p.bot)
            entry = self.h.admission.queue_entry(pid)
            queued = entry is not None and entry.status.value == "QUEUED"
            assert ready_bot_ok(p, project_bot(p, queued=queued)), (p.stage, p.bot)

    @invariant()
    def building_slot_matches_live_build(self) -> None:
        for pid in PIDS:
            p = self.h.p(pid)
            live_build = [
                s
                for s in p.sessions
                if s.kind == SessionKind.BUILD
                and s.lifecycle not in (Lifecycle.RETIRED,)
                and not (s.lifecycle == Lifecycle.FENCED and s.fences - {FenceKind.CHECKPOINT})
            ]
            has_slot = any(
                r.live and r.parcel_id == pid and r.kind == ReservationKind.BUILDING
                for r in self.h.admission.reservations
            )
            if live_build:
                # A live (or checkpoint-parked) build always holds its building slot.
                assert has_slot


TestFactoryModel = FactoryModel.TestCase
