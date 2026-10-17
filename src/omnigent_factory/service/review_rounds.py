"""Review rounds: how many review-bot reviews an issue's PR gets before the factory stops
waiting for (and waking the run on) another one.

* **Budget.** The owner's board "Review rounds" NUMBER field on the issue, else on its
  parent epic, else ``review_rounds_default`` (hot-reloadable); plus one per "One more
  review round" box the owner ticked in a Ready report. Only the owner's own board edit
  counts (their ``projects_v2_item`` webhook, numeric sender id), as for Auto-build and
  Autopilot: a value seen on the board without such a webhook (someone else's edit, or a
  lost delivery) is not trusted and the default applies. A value below 1 is ignored.
* **Rounds.** One round is one review-bot review of a PR head (a review of that commit,
  a "Reviewed commit" verdict naming it, or its +1 on the PR after the push), counted per
  issue across runs and reworks; owner comments and reworks do not reset it. A head that
  only syncs the base branch (the owner's "Update branch") is not counted.
* **Exhausted** (rounds used >= budget): readiness no longer waits for a further review,
  findings the bot leaves on a head reviewed beyond the budget neither wake the run nor
  hold Ready, and the run is told to wrap up (``core.reducer``, the wake templates).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from omnigent_factory.core.types import Parcel
from omnigent_factory.github.client import GitHubAPIError
from omnigent_factory.service.follow_ups import _time_us
from omnigent_factory.service.ranking import owner_edit
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import discipline as ds

LOG = logging.getLogger(__name__)

#: Reads the item's current value of a NUMBER field when an owner webhook omits it
#: (item node id, field id) -> value (None: empty or unreadable).
ItemNumberReader = Callable[[str, str], Awaitable[float | None]]


@dataclass(frozen=True, slots=True)
class RoundsView:
    """Review rounds of one issue as of a PR read."""

    used: int
    budget: int
    #: Reviewed commits beyond the budget (their findings are not waited for).
    over_budget: frozenset[str] = field(default_factory=frozenset)

    @property
    def exhausted(self) -> bool:
        return self.budget > 0 and self.used >= self.budget

    @property
    def strict_from(self) -> int:
        """From this round on only P1 findings with real impact must be fixed."""
        return max(2, self.budget)

    def as_status(self) -> dict[str, Any]:
        return {
            "used": self.used,
            "budget": self.budget,
            "exhausted": self.exhausted,
            "strict_from_round": self.strict_from,
        }


def distinct_rounds(rounds: Sequence[tuple[str, int]]) -> list[tuple[str, int]]:
    """Rounds in review order, one per commit: a verdict naming a commit by its prefix
    and a review of the full SHA are the same round."""
    kept: list[tuple[str, int]] = []
    for key, at in sorted(rounds, key=lambda r: (r[1], r[0])):
        if any(key.startswith(k) or k.startswith(key) for k, _ in kept):
            continue
        kept.append((key, at))
    return kept


def view(rounds: Sequence[tuple[str, int]], budget: int) -> RoundsView:
    ordered = distinct_rounds(rounds)
    over = frozenset(key for key, _ in ordered[budget:]) if budget > 0 else frozenset()
    return RoundsView(used=len(ordered), budget=budget, over_budget=over)


class ReviewRounds:
    def __init__(
        self, service: FactoryService, read_number: ItemNumberReader | None = None
    ) -> None:
        self.service = service
        self.read_number = read_number

    @property
    def _repo_id(self) -> str:
        return self.service.config.repo_id

    # ------------------------------------------------------------------ owner field

    async def note_owner_field(self, body: bytes) -> None:
        """A ``projects_v2_item`` delivery: record the owner's "Review rounds" edit."""
        field_id = self.service.config.review_rounds_field_node_id
        if not field_id:
            return
        try:
            payload: Any = json.loads(body)
        except ValueError:
            return
        edit = owner_edit(payload, self.service.config)
        if edit is None or edit[1] != field_id:
            return
        node_id, _field, change = edit
        sender = payload.get("sender") if isinstance(payload, Mapping) else None
        actor = sender.get("id") if isinstance(sender, Mapping) else None
        item = payload.get("projects_v2_item")
        if "to" in change:
            value = _number(change.get("to"))
        elif self.read_number is not None and isinstance(item, Mapping):
            value = _number(await self.read_number(str(item.get("node_id") or ""), field_id))
        else:
            LOG.warning("review rounds edit without a value ignored node=%s", node_id)
            return
        at_us = _time_us(item.get("updated_at") if isinstance(item, Mapping) else None)
        changed = await self.service.db.call(
            partial(
                ds.set_round_budget,
                repo_id=self._repo_id,
                issue_node_id=node_id,
                value=value,
                actor_id=int(actor) if isinstance(actor, int) else 0,
                at_us=at_us or self.service.clock.now_utc_us(),
            )
        )
        if changed:
            LOG.info("review rounds set by owner node=%s value=%s", node_id, value)

    # ------------------------------------------------------------------ budget

    async def budget(self, parcel: Parcel) -> int:
        """The issue's round budget: its owner value, else its epic's, else the default;
        plus the owner's "one more round" grants."""
        repo_id = self._repo_id
        own = await self.service.db.call(
            partial(ds.round_budget, repo_id=repo_id, issue_node_id=parcel.parcel_id)
        )
        value = own if own is not None and own >= 1 else None
        parent = parcel.links.parent if parcel.links is not None else None
        if value is None and parent is not None and not parent.repo:
            epic_id = await self._parcel_id(parent.number)
            if epic_id is not None:
                epic = await self.service.db.call(
                    partial(ds.round_budget, repo_id=repo_id, issue_node_id=epic_id)
                )
                value = epic if epic is not None and epic >= 1 else None
        if value is None:
            value = self.service.config.review_rounds_default
        grants = await self.service.db.call(
            partial(ds.round_grants, repo_id=repo_id, parcel_id=parcel.parcel_id)
        )
        return value + grants

    async def _parcel_id(self, number: int) -> str | None:
        repo_id = self._repo_id
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT parcel_id FROM parcels WHERE repo_id = ? AND issue_number = ?",
                (repo_id, number),
            )
        )
        return str(rows[0][0]) if rows else None

    # ------------------------------------------------------------------ rounds

    async def observe(
        self, parcel_id: str, pr_number: int, rounds: Sequence[tuple[str, int]]
    ) -> RoundsView | None:
        """Record the review-bot rounds a PR read saw and return the issue's rounds (None:
        no such parcel)."""
        parcel = await self.service.db.call(lambda store: store.load_parcel(parcel_id))
        if parcel is None:
            return None
        repo_id = self._repo_id
        if rounds:
            added = await self.service.db.call(
                partial(
                    ds.record_rounds,
                    repo_id=repo_id,
                    parcel_id=parcel_id,
                    pr_number=pr_number,
                    rounds=list(rounds),
                )
            )
            if added:
                LOG.info(
                    "review rounds recorded issue=#%s pr=#%s new=%s",
                    parcel.issue_number,
                    pr_number,
                    added,
                )
        return await self.current(parcel)

    async def current(self, parcel: Parcel) -> RoundsView:
        stored = await self.service.db.call(
            partial(ds.review_rounds, repo_id=self._repo_id, parcel_id=parcel.parcel_id)
        )
        return view(stored, await self.budget(parcel))

    async def status(self, parcel_id: str) -> RoundsView | None:
        parcel = await self.service.db.call(lambda store: store.load_parcel(parcel_id))
        return None if parcel is None else await self.current(parcel)

    async def grant(self, parcel_id: str, grant_key: str) -> bool:
        """One more round for the issue (once per ``grant_key``)."""
        added: bool = await self.service.db.call(
            partial(
                ds.grant_round,
                repo_id=self._repo_id,
                parcel_id=parcel_id,
                grant_key=grant_key,
                at_us=self.service.clock.now_utc_us(),
            )
        )
        if added:
            LOG.info("review round granted parcel=%s key=%s", parcel_id, grant_key)
        return added


async def read_item_number(client: Any, item_id: str, field_id: str) -> float | None:  # noqa: ANN401 - GitHubClient
    """The project item's value of the NUMBER field ``field_id`` (None: empty or unread)."""
    if not item_id:
        return None
    query = """
    query($id: ID!) { node(id: $id) { ... on ProjectV2Item {
      fieldValues(first: 50) { nodes {
        ... on ProjectV2ItemFieldNumberValue { number field { ... on ProjectV2Field { id } } }
      } }
    } } }
    """
    try:
        data = await client.graphql(query, {"id": item_id})
    except GitHubAPIError:
        return None
    node = data.get("node") if isinstance(data, dict) else None
    values = node.get("fieldValues") if isinstance(node, dict) else None
    for value in (values.get("nodes") if isinstance(values, dict) else None) or []:
        field = value.get("field") if isinstance(value, dict) else None
        if isinstance(field, dict) and field.get("id") == field_id:
            number = value.get("number")
            return float(number) if isinstance(number, int | float) else None
    return None


def _number(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


__all__ = ["ReviewRounds", "RoundsView", "distinct_rounds", "read_item_number", "view"]
