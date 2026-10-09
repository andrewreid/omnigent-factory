"""Board-wide diff: one Projects read finds the parcels whose GitHub state changed.

Parcels with live work are read per issue on the reconcile cadence; every other parcel
changes only on GitHub, and its webhooks normally carry the change. To bound a missed
webhook without a per-issue read of every parcel each cycle, the service reads the whole
board every ``board_diff_interval_minutes`` and compares each parcel's card (column,
Bot, open/closed, assignees, labels, title, ``updatedAt``: :class:`BoardCard.digest`)
with the values stored when it was last compared. Only a parcel whose card changed (or
left the board) gets a per-issue read. A parcel never compared before is *unknown*: it
is read once (the service spreads those reads) and then compared like the others.

A failed or partial board read reports nothing, never "no change".
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.ports.github import BoardCard

if TYPE_CHECKING:
    from omnigent_factory.service.runtime import FactoryService

LOG = logging.getLogger(__name__)

#: Digest of a parcel whose card is not on the board (removed, or never added).
ABSENT = "absent"

BoardRead = Callable[[], Awaitable[list[BoardCard] | RetryableReadFailure]]


@dataclass(frozen=True, slots=True)
class DiffResult:
    #: Parcels whose card differs from the stored values, with the new digest.
    changed: Mapping[str, str] = field(default_factory=dict)
    #: Parcels never compared before, with their current digest.
    unknown: Mapping[str, str] = field(default_factory=dict)


class BoardDiff:
    def __init__(self, service: FactoryService, read_board: BoardRead) -> None:
        self.service = service
        self.read_board = read_board

    async def run_once(self) -> DiffResult | None:
        """Compare the board with the stored digests; None when the board is unreadable."""
        cards = await self.read_board()
        if isinstance(cards, RetryableReadFailure):
            LOG.warning("board diff read failed reason=%s", cards.reason)
            return None
        repo_id = self.service.config.repo_id
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT parcel_id FROM parcels WHERE repo_id = ? AND issue_number IS NOT NULL "
                "ORDER BY parcel_id",
                (repo_id,),
            )
        )
        stored = await self.service.db.call(lambda store: store.board_digests())
        by_id = {card.node_id: card for card in cards}
        changed: dict[str, str] = {}
        unknown: dict[str, str] = {}
        for row in rows:
            parcel_id = str(row[0])
            card = by_id.get(parcel_id)
            digest = card.digest if card is not None else ABSENT
            previous = stored.get(parcel_id)
            if previous is None:
                unknown[parcel_id] = digest
            elif previous != digest:
                changed[parcel_id] = digest
        LOG.log(
            logging.INFO if changed else logging.DEBUG,
            "board diff cards=%s parcels=%s changed=%s unknown=%s",
            len(cards),
            len(rows),
            len(changed),
            len(unknown),
        )
        return DiffResult(changed=changed, unknown=unknown)
