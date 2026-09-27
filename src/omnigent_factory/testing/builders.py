"""Fixture builders shared by every task's tests."""

from __future__ import annotations

import itertools
from collections.abc import Mapping
from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.canonical import canonical_contract
from omnigent_factory.core.events import Event, EventBody, EventClass, Provenance
from omnigent_factory.core.types import (
    MICROS_PER_SECOND,
    IssueSnapshot,
    Stage,
    TrustedConfig,
)

OWNER_ID = 114979
OTHER_USER_ID = 4242
REPO_ID = "R_kgDOTC12Fg"
T0 = 1_800_000_000_000_000


def config(**overrides: object) -> TrustedConfig:
    base = TrustedConfig(repo_id=REPO_ID, owners=frozenset({OWNER_ID}))
    return replace(base, **overrides)  # type: ignore[arg-type]


def snapshot(
    *,
    open: bool = True,
    human_assigned: bool = False,
    in_project: bool = True,
    stage: Stage | None = None,
    title: str = "Add export button",
    body: str | None = "Body text",
    read_at_us: int = T0,
    repo_matches: bool = True,
    identity_resolved: bool = True,
    bot: str | None = None,
) -> IssueSnapshot:
    return IssueSnapshot(
        open=open,
        human_assigned=human_assigned,
        repo_matches=repo_matches,
        identity_resolved=identity_resolved,
        in_project=in_project,
        stage=stage,
        title=title,
        body=body,
        read_at_us=read_at_us,
        bot=bot,
    )


def contract(size: str = "M", goal: str = "Ship the export button") -> dict[str, object]:
    return {
        "goal": goal,
        "acceptance_criteria": [
            {"id": "AC1", "criterion": "Button exports CSV", "verification": "unit test"}
        ],
        "non_goals": ["PDF export"],
        "size": size,
        "resolved_decisions": [],
    }


def contract_text(size: str = "M", goal: str = "Ship the export button") -> str:
    return canonical_contract(contract(size, goal)).decode("utf-8")


class EventFactory:
    """Builds events for one parcel with increasing IDs and source times.

    Controls default to the owner via a signed webhook; safety facts to a webhook from
    ``OTHER_USER_ID``; observations to the adapter. Every event carries fresh entropy and,
    for controls, a fresh eligible snapshot unless ``evidence`` is overridden.
    """

    def __init__(
        self,
        parcel_id: str = "I_parcel_1",
        repo_id: str = REPO_ID,
        *,
        start_us: int = T0,
        step_us: int = MICROS_PER_SECOND,
        issue_number: int = 1,
    ) -> None:
        self.parcel_id = parcel_id
        self.repo_id = repo_id
        self.issue_number = issue_number
        self._time = start_us
        self._step = step_us
        self._ids = itertools.count(1)

    @property
    def now(self) -> int:
        return self._time

    def tick(self, us: int | None = None) -> int:
        self._time += self._step if us is None else us
        return self._time

    def make(
        self,
        body: EventBody,
        *,
        actor: int | None = None,
        provenance: Provenance | None = None,
        evidence: IssueSnapshot | str | None = "default",
        event_id: str | None = None,
        time_us: int | None = None,
        parcel_id: str | str | None = "default",
        entropy: str | None = None,
    ) -> Event:
        n = next(self._ids)
        t = self.tick() if time_us is None else time_us
        cls = body.CLASS
        if provenance is None:
            provenance = {
                EventClass.CONTROL: Provenance.WEBHOOK,
                EventClass.SAFETY: Provenance.WEBHOOK,
                EventClass.OBSERVATION: Provenance.ADAPTER,
            }[cls]
            if isinstance(body, ev.Pause | ev.Unpause):
                provenance = Provenance.OPERATOR
            if isinstance(
                body,
                ev.ActiveLimitReached
                | ev.GraceExpired
                | ev.CapacityAvailable
                | ev.RetryDue
                | ev.ReconcileDue,
            ):
                provenance = Provenance.SCHEDULER
            if isinstance(body, ev.InboxHoldSet | ev.InboxHoldReleased):
                provenance = Provenance.INBOX
        if actor is None and cls == EventClass.CONTROL and provenance != Provenance.OPERATOR:
            actor = OWNER_ID
        if isinstance(evidence, str):
            evidence = snapshot(read_at_us=t) if cls == EventClass.CONTROL else None
        return Event(
            event_id=event_id or f"{self.parcel_id}:e{n}",
            repo_id=self.repo_id,
            parcel_id=self.parcel_id if parcel_id == "default" else parcel_id,
            source_time_us=t,
            provenance=provenance,
            body=body,
            actor_id=actor,
            issue_number=self.issue_number,
            entropy=f"entropy-{self.parcel_id}-{n}" if entropy is None else entropy,
            evidence=evidence,
        )


def result_candidate(
    session_id: str,
    root_id: str,
    revision: int,
    kind: ev.ResultKind,
    **fields: object,
) -> ev.ResultCandidate:
    base: Mapping[str, object] = {
        "session_id": session_id,
        "root_id": root_id,
        "revision": revision,
        "valid": True,
        "result_kind": kind,
    }
    return ev.ResultCandidate(**{**base, **fields})  # type: ignore[arg-type]
