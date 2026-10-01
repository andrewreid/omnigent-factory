"""In-memory reducer harness: several parcels sharing one repository admission snapshot.

It applies events through the real pure reducer exactly as the store does (one parcel
aggregate + shared admission per transition) and offers scripted happy-path flows so
tests can reach any stage quickly.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent, EffectKind
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.projection import ready_bot_ok
from omnigent_factory.core.reducer import TransitionResult, transition
from omnigent_factory.core.types import (
    AdmissionSnapshot,
    Parcel,
    Size,
    StageSession,
    State,
    TrustedConfig,
    Via,
)
from omnigent_factory.testing.builders import (
    REPO_ID,
    T0,
    EventFactory,
    config,
    contract_text,
    result_candidate,
    snapshot,
)

HEAD = "a" * 40


@dataclass
class Harness:
    cfg: TrustedConfig = field(default_factory=config)
    admission: AdmissionSnapshot = field(
        default_factory=lambda: AdmissionSnapshot(repo_id=REPO_ID, paused=False)
    )
    parcels: dict[str, Parcel] = field(default_factory=dict)
    factories: dict[str, EventFactory] = field(default_factory=dict)
    log: list[tuple[Event, TransitionResult]] = field(default_factory=list)
    #: Simulate the executor acknowledging every daemon board write immediately.
    auto_ack_moves: bool = True
    _roots: int = 0

    # ----------------------------------------------------------------- core

    def f(self, pid: str = "I_parcel_1") -> EventFactory:
        if pid not in self.factories:
            n = len(self.factories) + 1
            self.factories[pid] = EventFactory(pid, start_us=T0, issue_number=n)
        return self.factories[pid]

    def apply(self, event: Event) -> TransitionResult:
        """Apply ``event``; then, if ``auto_ack_moves``, play the executor's trusted
        read-after-write acknowledgement for every pending daemon board write."""
        result = self._apply_one(event)
        if self.auto_ack_moves and event.parcel_id is not None:
            self.ack_moves(event.parcel_id)
        return result

    def ack_moves(self, pid: str) -> None:
        for _ in range(8):
            p = self.parcels.get(pid)
            if p is None or not p.pending_moves:
                return
            move = p.pending_moves[0]
            self._apply_one(
                self.f(pid).make(
                    ev.ColumnObserved(stage=move.to_stage, daemon_effect_id=move.effect_id),
                    provenance=Provenance.ADAPTER,
                )
            )

    def _apply_one(self, event: Event) -> TransitionResult:
        parcel: Parcel | None = None
        if event.parcel_id is not None:
            parcel = self.parcels.get(event.parcel_id) or Parcel(
                parcel_id=event.parcel_id, repo_id=event.repo_id, issue_number=event.issue_number
            )
        result = transition(State(parcel, self.admission, self.cfg), event)
        if result.state.parcel is not None:
            # Projection invariant on every transition: Ready is never Working/Queued.
            after = result.state.parcel
            assert ready_bot_ok(after, after.bot), (event.kind, after.stage, after.bot)
            self.parcels[result.state.parcel.parcel_id] = result.state.parcel
        self.admission = result.state.admission
        self.log.append((event, result))
        return result

    def send(self, pid: str, body: ev.EventBody, **kw: object) -> TransitionResult:
        return self.apply(self.f(pid).make(body, **kw))  # type: ignore[arg-type]

    def p(self, pid: str = "I_parcel_1") -> Parcel:
        return self.parcels[pid]

    def cur(self, pid: str = "I_parcel_1") -> StageSession:
        s = self.p(pid).current_session
        assert s is not None
        return s

    @staticmethod
    def of(result: TransitionResult, kind: EffectKind) -> list[EffectIntent]:
        return [e for e in result.effects if e.kind == kind]

    # ---------------------------------------------------------------- flows

    def eligible(self, pid: str = "I_parcel_1") -> TransitionResult:
        f = self.f(pid)
        return self.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now)))

    def create_ok(self, pid: str = "I_parcel_1") -> StageSession:
        """Create (or, in the parcel's live issue session, just prepare) the current run."""
        s = self.cur(pid)
        if s.root_id is None:
            self._roots += 1
            root = f"root-{self._roots}"
            self.send(pid, ev.SessionCreated(session_id=s.session_id, root_id=root, nonce=s.nonce))
        self.send(pid, ev.Prepared(session_id=s.session_id, ok=True))
        self.verify_policies(pid)
        return self.cur(pid)

    def verify_policies(self, pid: str = "I_parcel_1") -> TransitionResult:
        """The post-barrier exact policy-set verification of the current run succeeds."""
        return self.send(pid, ev.PoliciesVerified(session_id=self.cur(pid).session_id, ok=True))

    def quiesce(self, pid: str, session_id: str) -> TransitionResult:
        return self.send(pid, ev.TreeQuiescent(session_id=session_id, complete=True, busy=False))

    def triage(self, pid: str = "I_parcel_1", size: Size = Size.M) -> StageSession:
        self.eligible(pid)
        self.send(pid, ev.RequestTriage(via=Via.DRAG))
        s = self.create_ok(pid)
        assert s.root_id is not None
        r = self.send(
            pid,
            result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.TRIAGE, size=size),
        )
        self.quiesce(pid, s.session_id)
        for publish in self.of(r, EffectKind.PUBLISH_TRIAGE):
            self.send(
                pid,
                ev.PublicationAcked(
                    effect_id=publish.effect_id,
                    effect_kind=publish.kind.value,
                    session_id=s.session_id,
                    comment_id=f"c-{publish.effect_id}",
                ),
            )
        return s

    def plan_published(self, pid: str = "I_parcel_1", *, goal: str = "Ship it") -> StageSession:
        if pid not in self.parcels:
            self.eligible(pid)
        self.send(pid, ev.RequestPlan(via=Via.DRAG))
        s = self.create_ok(pid)
        self.publish_plan(pid, goal=goal)
        return s

    def publish_plan(self, pid: str = "I_parcel_1", *, goal: str = "Ship it") -> None:
        s = self.cur(pid)
        assert s.root_id is not None
        p = self.p(pid)
        r = self.send(
            pid,
            result_candidate(
                s.session_id,
                s.root_id,
                p.revision,
                ev.ResultKind.PLAN,
                publication_kind=ev.PublicationKind.CONTRACT,
                contract_canonical=contract_text("M", goal),
                size=Size.M,
                open_decision_ids=tuple(d.decision_id for d in p.open_decisions),
            ),
        )
        [publish] = self.of(r, EffectKind.PUBLISH_CONTRACT)
        cid = str(publish.args["contract_id"])
        f = self.f(pid)
        self.send(
            pid,
            ev.ContractPublished(
                contract_id=cid, comment_id=f"c-{cid}", verified=True, posted_at_us=f.now
            ),
        )

    def approve(self, pid: str = "I_parcel_1") -> TransitionResult:
        plan = self.cur(pid)
        r = self.send(pid, ev.ApprovePlan(via=Via.DRAG))
        self.quiesce(pid, plan.session_id)
        return r

    def admit(self, pid: str = "I_parcel_1") -> StageSession:
        self.send(pid, ev.CapacityAvailable())
        return self.create_ok(pid)

    def to_building(self, pid: str = "I_parcel_1") -> StageSession:
        self.plan_published(pid)
        self.approve(pid)
        return self.admit(pid)

    def build_ready(self, pid: str = "I_parcel_1", pr: int = 7, head: str = HEAD) -> None:
        s = self.cur(pid)
        assert s.root_id is not None
        self.send(
            pid,
            result_candidate(
                s.session_id,
                s.root_id,
                s.revision,
                ev.ResultKind.BUILD_READY,
                pr_number=pr,
                head_sha=head,
            ),
        )
        self.send(
            pid,
            ev.PRObserved(
                pr_number=pr, head_sha=head, open=True, bot_authored=True, parcel_branch=True
            ),
        )
        self.send(
            pid,
            ev.ReadinessEvidence(
                session_id=s.session_id, pr_number=pr, head_sha=head, verified=True
            ),
        )
        self.quiesce(pid, s.session_id)
