"""Mark the issues a published triage names as related (their card's "Factory note").

After a triage comment is published, each related issue that is open on the board (Inbox
to Ready) gets a ``RelatedMarked`` event: its card then reads e.g. ``Related: #12
(overlap)`` unless a more important note (any status reason, Blocked, Needs you,
Checkpoint, Queued, ...) is showing. No comment is posted. Best effort: it runs in the
background (never inside the publishing effect's parcel lock), and a failed board read
only logs; each mark's event ID is stable, so a retried publication marks once.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import RELATIONS, STAGE_ORDER
from omnigent_factory.service.board_index import BoardIndex, BoardUnavailable
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.runtime import FactoryService

LOG = logging.getLogger(__name__)


class RelatedMarker:
    def __init__(
        self, service: FactoryService, board: BoardIndex, directory: ServiceDispatchDirectory
    ) -> None:
        self.service = service
        self.board = board
        self.directory = directory
        self._tasks: set[asyncio.Task[int]] = set()

    def schedule(self, effect: EffectIntent) -> None:
        """Called by the GitHub adapter once a triage comment exists (created or adopted)."""
        task = asyncio.create_task(self.mark(effect), name="related-marks")
        self._tasks.add(task)
        task.add_done_callback(self._done)

    def _done(self, task: asyncio.Task[int]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            LOG.warning("related-issue marking failed")

    async def mark(self, effect: EffectIntent) -> int:
        """Apply one ``RelatedMarked`` per related board issue; returns how many applied."""
        session_id = str(effect.args.get("session_id") or effect.preconditions.session_id or "")
        if effect.parcel_id is None or not session_id:
            return 0
        related = _related(self.directory.latest_result(session_id))
        if not related:
            return 0
        parcel_id = effect.parcel_id
        source = await self.service.db.call(lambda store: store.load_parcel(parcel_id))
        if source is None or source.issue_number is None:
            return 0
        try:
            board = {issue.number: issue for issue in await self.board.issues()}
        except BoardUnavailable as exc:
            LOG.warning("related-issue marking skipped: board unavailable reason=%s", exc)
            return 0
        applied = 0
        for number, relation in related:
            target = board.get(number)
            if target is None or target.stage not in STAGE_ORDER or number == source.issue_number:
                continue  # not open on the board (Inbox..Ready): nothing to mark
            result = await self.service.apply_event(
                Event(
                    event_id=f"related:{effect.effect_id}:{number}",
                    repo_id=self.service.config.repo_id,
                    parcel_id=target.node_id,
                    source_time_us=self.service.clock.now_utc_us(),
                    provenance=Provenance.ADAPTER,
                    body=ev.RelatedMarked(source_issue=source.issue_number, relation=relation),
                    issue_number=number,
                )
            )
            applied += int(result.accepted and not result.duplicate)
        if applied:
            LOG.info("related issues marked source=#%s count=%s", source.issue_number, applied)
        return applied


def _related(stored: dict[str, Any] | None) -> list[tuple[int, str]]:
    record = stored.get("factory_result") if stored is not None else None
    body = record.get("result") if isinstance(record, dict) else None
    items = body.get("related") if isinstance(body, dict) and body.get("kind") == "triage" else None
    out: list[tuple[int, str]] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        number, relation = item.get("issue"), item.get("relation")
        if isinstance(number, int) and not isinstance(number, bool) and relation in RELATIONS:
            out.append((number, str(relation)))
    return out
