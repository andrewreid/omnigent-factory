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

A changed or unknown card that shows a ``factory:*`` command label also gets a label
check (``service.label_recovery``): the per-issue read carries no label actor, so a label
whose webhook was lost is recovered from the issue's timeline before the card's digest is
stored.

The same read carries each card's Bot and "Factory note" values (not part of the digest).
A parcel card showing another value than the one the factory last wrote gets a
``BoardFieldsObserved`` event, whose handler rewrites the field (drift correction: a lost,
cancelled or overwritten display write). A parcel with a Bot/note write in flight, of
unknown outcome or settled less than ``DRIFT_SETTLE_US`` ago is skipped: the board may not
show that write yet.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, RetryableReadFailure
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.ports.github import BoardCard
from omnigent_factory.service.label_recovery import LabelCheck, label_check

if TYPE_CHECKING:
    from omnigent_factory.core.types import TrustedConfig
    from omnigent_factory.service.runtime import FactoryService
    from omnigent_factory.store.sqlite import ApplyResult, SqliteStore

LOG = logging.getLogger(__name__)

#: Digest of a parcel whose card is not on the board (removed, or never added).
ABSENT = "absent"

#: A Bot/note write settled this recently may not show in a board read yet
#: (read-after-write lag): its card is not corrected until a later diff.
DRIFT_SETTLE_US = 120 * 1_000_000

_DISPLAY_KINDS = (EffectKind.SET_BOT.value, EffectKind.SET_NOTE.value)

BoardRead = Callable[[], Awaitable[list[BoardCard] | RetryableReadFailure]]


@dataclass(frozen=True, slots=True)
class DiffResult:
    #: Parcels whose card differs from the stored values, with the new digest.
    changed: Mapping[str, str] = field(default_factory=dict)
    #: Parcels never compared before, with their current digest.
    unknown: Mapping[str, str] = field(default_factory=dict)
    #: Changed or unknown parcels whose card shows a command label.
    label_checks: Mapping[str, LabelCheck] = field(default_factory=dict)
    #: Labels of each changed or unknown card (``service.follow_ups`` checks its holds).
    labels: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


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
        checks: dict[str, LabelCheck] = {}
        labels: dict[str, tuple[str, ...]] = {}
        for row in rows:
            parcel_id = str(row[0])
            card = by_id.get(parcel_id)
            digest = card.digest if card is not None else ABSENT
            previous = stored.get(parcel_id)
            if previous is None:
                unknown[parcel_id] = digest
            elif previous != digest:
                changed[parcel_id] = digest
            else:
                continue
            if card is not None:
                labels[parcel_id] = tuple(card.labels)
            check = label_check(card.labels, card.updated_at) if card is not None else None
            if check is not None:
                checks[parcel_id] = check
        LOG.log(
            logging.INFO if changed else logging.DEBUG,
            "board diff cards=%s parcels=%s changed=%s unknown=%s",
            len(cards),
            len(rows),
            len(changed),
            len(unknown),
        )
        tracked = {pid: by_id[pid] for pid in (str(row[0]) for row in rows) if pid in by_id}
        try:
            await self._correct_drift(tracked)
        except Exception:
            # Display only: the diff's findings still count (the next diff tries again).
            LOG.warning("board drift correction failed", exc_info=True)
        return DiffResult(changed=changed, unknown=unknown, label_checks=checks, labels=labels)

    async def _correct_drift(self, cards: Mapping[str, BoardCard]) -> None:
        """Rewrite each card's Bot / "Factory note" that shows another value than the one
        the factory last wrote (at most one correction per field and diff)."""
        trusted = self.service.config.trusted
        for parcel_id, card in sorted(cards.items()):
            if card.bot is None and card.note is None:
                continue
            now_us = self.service.clock.now_utc_us()
            event = Event(
                event_id=f"board-drift:{parcel_id}:{now_us}",
                repo_id=self.service.config.repo_id,
                parcel_id=parcel_id,
                source_time_us=now_us,
                provenance=Provenance.RECONCILER,
                body=ev.BoardFieldsObserved(),
            )

            def correct(
                store: SqliteStore,
                card: BoardCard = card,
                event: Event = event,
                trusted: TrustedConfig = trusted,
            ) -> tuple[ApplyResult, dict[str, tuple[str, str]]] | None:
                # One DB call under the parcel lock: no display write can be issued
                # between this check and the event.
                drift = _drift(store, card, event.parcel_id or "", event.source_time_us)
                if not drift:
                    return None
                body = ev.BoardFieldsObserved(
                    bot=drift["bot"][0] if "bot" in drift else None,
                    note=drift["note"][0] if "note" in drift else None,
                )
                return store.apply_event(replace(event, body=body), trusted), drift

            async with self.service.serializers.lock(parcel_id):
                outcome = await self.service.db.call(correct)
            if outcome is None:
                continue
            result, drift = outcome
            for effect in result.effects:
                field_name = {EffectKind.SET_BOT: "bot", EffectKind.SET_NOTE: "note"}.get(
                    effect.kind
                )
                if field_name is None or field_name not in drift:
                    continue
                LOG.info(
                    "board drift corrected issue=#%s field=%s seen=%r expected=%r",
                    card.number,
                    field_name,
                    drift[field_name][0],
                    effect.args.get(field_name),
                )


def _drift(
    store: SqliteStore, card: BoardCard, parcel_id: str, now_us: int
) -> dict[str, tuple[str, str]]:
    """{field: (seen, last written)} for the card's Bot/note values that differ from the
    factory's; empty when they match, nothing was written, or a write may be in flight."""
    parcel = store.load_parcel(parcel_id)
    if parcel is None or not parcel.in_project:
        return {}
    drift: dict[str, tuple[str, str]] = {}
    # An empty Bot is "never written" (as on a per-issue read), not drift.
    if card.bot and card.bot != parcel.bot.value:
        drift["bot"] = (card.bot, parcel.bot.value)
    # Compared without surrounding whitespace: a value the board trims is no drift
    # (rewriting it would never converge).
    if card.note is not None and card.note.strip() != parcel.board_note.strip():
        drift["note"] = (card.note, parcel.board_note)
    if not drift:
        return {}
    busy = store.query(
        "SELECT 1 FROM effects WHERE parcel_id = ? AND kind IN (?, ?) "
        "AND (state IN ('pending', 'claimed', 'unknown') OR updated_at_us >= ?) LIMIT 1",
        (parcel_id, *_DISPLAY_KINDS, now_us - DRIFT_SETTLE_US),
    )
    return {} if busy else drift
