"""A realistic parcel population and counting fakes for the quiet-factory tests.

``populate`` writes ~140 parcels into a fresh store the way the daemon would have: most
idle in Inbox, finished triages, Done parcels (retired runs and the legacy FENCED kind),
and a handful with live work.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    EffectIntent,
    EffectKind,
    ExecutionContext,
)
from omnigent_factory.core.types import DecisionImpact, IssueSnapshot, Lifecycle, Parcel, Via
from omnigent_factory.omnigent.tree import NodeState, TreeObservation
from omnigent_factory.ports.github import GITHUB_EFFECT_KINDS, BoardCard, IssueRef
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.harness import Harness


@dataclass(frozen=True, slots=True)
class Population:
    parcels: dict[str, Parcel]
    #: Expected to be live (per-issue reads every reconcile interval).
    live: frozenset[str]
    #: Done parcels whose (stored) run is FENCED with a hard fence.
    fenced_done: frozenset[str]


def build_population(
    *, inbox: int = 70, triaged: int = 50, done: int = 8, fenced_done: int = 6
) -> tuple[Harness, Population]:
    h = Harness()
    for i in range(inbox):
        h.eligible(f"I_inbox_{i:03d}")
    for i in range(triaged):
        h.triage(f"I_triaged_{i:03d}")
    for i in range(done):
        pid = f"I_done_{i:03d}"
        h.triage(pid)
        h.send(pid, ev.Closed())
        issue = h.p(pid).issue_session
        if issue is not None:
            h.send(pid, ev.IssueSessionClosed(root_id=issue.root_id))
    fenced: set[str] = set()
    for i in range(fenced_done):
        # A plan run when its issue is closed: the closed read fences it (safety)
        # before completion drains it, so it ends FENCED.
        pid = f"I_fenced_{i:03d}"
        h.eligible(pid)
        h.send(pid, ev.RequestPlan(via=Via.DRAG))
        s = h.create_ok(pid)
        f = h.f(pid)
        h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(open=False, read_at_us=f.now)))
        h.quiesce(pid, s.session_id)
        assert h.cur(pid).lifecycle == Lifecycle.FENCED
        issue = h.p(pid).issue_session
        if issue is not None:
            h.send(pid, ev.IssueSessionClosed(root_id=issue.root_id))
        fenced.add(pid)
    live: set[str] = set()
    building = "I_live_build"
    h.to_building(building)
    live.add(building)
    for i in range(3):
        pid = f"I_live_plan_{i}"
        h.plan_published(pid)
        live.add(pid)
    triage = "I_live_triage"
    h.eligible(triage)
    h.send(triage, ev.RequestTriage(via=Via.DRAG))
    h.create_ok(triage)
    live.add(triage)
    question = "I_live_question"
    h.eligible(question)
    h.send(question, ev.RequestPlan(via=Via.DRAG))
    s = h.create_ok(question)
    h.send(
        question,
        ev.ElicitationOpened(
            session_id=s.session_id,
            elicitation_id="e-1",
            impact=DecisionImpact.WITHIN_CONTRACT,
            summary="Which export format?",
        ),
    )
    assert h.p(question).open_decisions
    live.add(question)
    return h, Population(dict(h.parcels), frozenset(live), frozenset(fenced))


def populate(store: SqliteStore, harness: Harness) -> None:
    """Apply the harness's whole event log to ``store`` (the daemon's own history)."""
    for event, _ in harness.log:
        store.apply_event(event, harness.cfg)


def card_for(parcel: Parcel, *, labels: Sequence[str] = (), open_: bool | None = None) -> BoardCard:
    """The board card a parcel's issue shows (values the tests change to simulate edits)."""
    return BoardCard(
        node_id=parcel.parcel_id,
        number=parcel.issue_number or 0,
        open=parcel.eligible if open_ is None else open_,
        status_option=parcel.stage.value if parcel.stage else "",
        bot_option=parcel.bot.value,
        assignees=(),
        labels=tuple(labels),
        title="Add export button",
        updated_at="2026-10-01T00:00:00Z",
    )


@dataclass
class CountingGitHub:
    """GitHub effect adapter: per-issue reads answer with the scripted (or default) issue.

    ``requests`` models HTTP requests: a per-issue read is a REST issue read plus a
    GraphQL project-item read; a board read is one GraphQL request per 100 cards.
    """

    snapshots: dict[str, IssueSnapshot] = field(default_factory=dict)
    cards: list[BoardCard] = field(default_factory=list)
    reads: Counter[str] = field(default_factory=Counter)
    requests: int = 0
    board_reads: int = 0

    @property
    def handled_kinds(self) -> frozenset[EffectKind]:
        return GITHUB_EFFECT_KINDS

    def _snapshot(self, parcel_id: str, now_us: int) -> IssueSnapshot:
        return self.snapshots.get(parcel_id) or snapshot(read_at_us=now_us)

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        del ctx
        if effect.kind == EffectKind.RECONCILE_PARCEL and effect.parcel_id is not None:
            self.reads[effect.parcel_id] += 1
            self.requests += 2
            snap = self._snapshot(effect.parcel_id, 0)
            return Ack(
                detail={
                    "open": snap.open,
                    "human_assigned": snap.human_assigned,
                    "repo_matches": True,
                    "identity_resolved": True,
                    "in_project": snap.in_project,
                    "stage": snap.stage.value if snap.stage else None,
                    "title": snap.title,
                    "body": snap.body,
                    "read_at_us": 1,
                }
            )
        self.requests += 1
        return Ack(remote_id=f"{effect.kind.value}-{effect.effect_id}")

    async def issue_snapshot(self, ref: IssueRef) -> IssueSnapshot:
        self.reads[ref.parcel_id] += 1
        self.requests += 2
        return self._snapshot(ref.parcel_id, 1)

    async def board_cards(self) -> list[BoardCard]:
        self.board_reads += 1
        self.requests += max(1, -(-len(self.cards) // 100))
        return list(self.cards)


@dataclass
class CountingOmnigent:
    """Observer adapter: tree scans and inventory refreshes, as Omnigent requests.

    A scan reads a root's child list and its snapshot (2 requests); an inventory refresh
    is one incremental page (1 request). ``fail_inventory`` makes every refresh fail.
    """

    busy_roots: set[str] = field(default_factory=set)
    scans: Counter[str] = field(default_factory=Counter)
    inventory_reads: int = 0
    fail_inventory: bool = False

    @property
    def requests(self) -> int:
        return 2 * sum(self.scans.values()) + self.inventory_reads

    def _observation(self, root_id: str, inventory_ok: bool) -> TreeObservation:
        status = "running" if root_id in self.busy_roots else "idle"
        nodes = {root_id: NodeState(root_id, None, status)}
        errors = () if inventory_ok else ("inventory: down",)
        return TreeObservation(root_id, inventory_ok, nodes, errors)

    async def observe_tree(self, root_id: str) -> TreeObservation:
        self.scans[root_id] += 1
        self.inventory_reads += 1
        return self._observation(root_id, not self.fail_inventory)

    async def observe_trees(self, root_ids: Iterable[str]) -> list[Any]:
        roots = list(root_ids)
        for root_id in roots:
            self.scans[root_id] += 1
        self.inventory_reads += 1
        if self.fail_inventory:
            return [self._observation(root_id, False) for root_id in roots]
        return [self._observation(root_id, True) for root_id in roots]

    def busy_nodes(self, root_id: str) -> tuple[str, ...]:
        del root_id
        return ()


class LineCounter(logging.Handler):
    """Counts INFO+ records (what the journal keeps at the daemon's INFO level)."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.info = 0
        self.debug = 0
        self.messages: Counter[str] = Counter()

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.INFO:
            self.info += 1
            self.messages[record.getMessage()[:90]] += 1
        else:
            self.debug += 1


__all__ = [
    "CountingGitHub",
    "CountingOmnigent",
    "LineCounter",
    "Population",
    "build_population",
    "card_for",
    "populate",
]
