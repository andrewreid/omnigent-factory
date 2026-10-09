"""Owner label commands whose webhook was lost, recovered from the issue's timeline.

A ``factory:*`` label is an owner command only through its webhook, which names the
actor. When that webhook is lost the board diff still sees the card's labels change, but
the per-issue read it schedules carries no label actor. So for a changed card that shows
a command label, the board diff asks :class:`LabelRecovery` to read the issue's label
timeline (one GraphQL request, :meth:`GitHubAPIAdapter.label_events`). Only when the
newest event of that label is a ``labeled`` event by a configured owner's numeric ID does
it apply the same control the webhook would have carried: ``Provenance.RECOVERY``, the
timeline actor, the label time as source time. Anyone else, or an unknown actor, is
ignored with a log line; a label removed again is never acted on.

Webhook and recovery share one logical event ID per timeline event
(:func:`label_event_id`), so a label command applies at most once, across restarts and
whichever of the two comes first. A command applied before that ID existed (under its
delivery's ID) is recognised by parcel, kind and time (:func:`_applied_before`).

The check is re-runnable: it returns False when a read failed, and the board diff then
keeps the card's old digest so the next diff checks it again.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import IssueSnapshot, Via
from omnigent_factory.github.webhook import LABEL_CONTROLS
from omnigent_factory.ports.github import IssueRef, LabelEvent

if TYPE_CHECKING:
    from omnigent_factory.service.runtime import FactoryService

LOG = logging.getLogger(__name__)

#: A webhook's source time is the issue's ``updated_at`` in its payload: at or after the
#: label time. A second of slack for GitHub's second-precision timestamps.
_LEGACY_SLACK_US = 1_000_000

ReadLabelEvents = Callable[[IssueRef], Awaitable[tuple[LabelEvent, ...] | RetryableReadFailure]]
ReadSnapshot = Callable[[IssueRef], Awaitable[IssueSnapshot | RetryableReadFailure]]


def label_event_id(event: LabelEvent) -> str:
    """Logical event ID of the label command one timeline event carries."""
    return f"github:label:{event.event_id}"


def newest_label_event(
    events: Iterable[LabelEvent], label: str, *, actor_id: int | None = None
) -> LabelEvent | None:
    """The newest timeline event of ``label`` (``labeled`` only when ``actor_id`` is
    given: that actor's newest ``labeled`` event)."""
    found: LabelEvent | None = None
    for event in events:  # oldest first; the later of equal times wins
        if event.label != label:
            continue
        if actor_id is not None and (not event.labeled or event.actor_id != actor_id):
            continue
        if found is None or event.created_at_us >= found.created_at_us:
            found = event
    return found


@dataclass(frozen=True, slots=True)
class LabelCheck:
    """The command labels a board card shows, with the card's ``updatedAt`` (adding a
    label moves it; a column or field change does not)."""

    labels: tuple[str, ...]
    updated_at: str


def label_check(labels: Iterable[str], updated_at: str) -> LabelCheck | None:
    """The check a card needs, or None when it shows no command label.

    A card with more labels than one board read lists (``#<total>`` marker beyond the
    names read) may hide one: every command label is checked.
    """
    names = tuple(labels)
    commands = sorted({name for name in names if name in LABEL_CONTROLS})
    read = [name for name in names if not name.startswith("#")]
    totals = [name[1:] for name in names if name.startswith("#") and name[1:].isdigit()]
    if totals and int(totals[-1]) > len(read):
        commands = sorted(LABEL_CONTROLS)
    return LabelCheck(tuple(commands), updated_at) if commands else None


class LabelRecovery:
    def __init__(
        self,
        service: FactoryService,
        read_events: ReadLabelEvents,
        read_snapshot: ReadSnapshot,
    ) -> None:
        self.service = service
        self.read_events = read_events
        self.read_snapshot = read_snapshot
        #: The last check settled per parcel: the same labels and ``updatedAt`` again
        #: (e.g. only the column moved) cannot carry a new label event.
        self._settled: dict[str, LabelCheck] = {}

    async def check(self, parcel_id: str, check: LabelCheck) -> bool:
        """Apply any lost owner label command on the parcel's issue.

        True when settled (nothing to recover, recovered, or ignored); False when a read
        failed and the check must run again.
        """
        if self._settled.get(parcel_id) == check:
            return True
        parcel = await self.service.db.call(lambda store: store.load_parcel(parcel_id))
        if parcel is None or parcel.issue_number is None:
            return True
        ref = IssueRef(self.service.config.repo_id, parcel.issue_number, parcel_id)
        events = await self.read_events(ref)
        if isinstance(events, RetryableReadFailure):
            LOG.warning(
                "label recovery read failed issue=%s reason=%s", ref.issue_number, events.reason
            )
            return False
        for label in check.labels:
            if not await self._recover(ref, label, events):
                return False
        self._settled[parcel_id] = check
        return True

    async def _recover(self, ref: IssueRef, label: str, events: tuple[LabelEvent, ...]) -> bool:
        newest = newest_label_event(events, label)
        if newest is None or not newest.labeled:
            # Removed again, or older than the label events one read returns.
            LOG.debug("label recovery: %s not currently added issue=%s", label, ref.issue_number)
            return True
        event_id = label_event_id(newest)
        if await self.service.db.call(lambda store: store.has_event(event_id)):
            return True
        owners = self.service.config.owners
        if newest.actor_id is None or newest.actor_id not in owners:
            LOG.info(
                "label recovery ignored: not an owner issue=%s label=%s actor=%s",
                ref.issue_number,
                label,
                newest.actor_id if newest.actor_id is not None else "unknown",
            )
            return True
        body = LABEL_CONTROLS[label]
        if await self._applied_before(ref.parcel_id, body, newest.created_at_us):
            return True
        snapshot = await self.read_snapshot(ref)
        if isinstance(snapshot, RetryableReadFailure):
            LOG.warning(
                "label recovery issue read failed issue=%s reason=%s",
                ref.issue_number,
                snapshot.reason,
            )
            return False
        result = await self.service.apply_event(
            Event(
                event_id=event_id,
                repo_id=self.service.config.repo_id,
                parcel_id=ref.parcel_id,
                source_time_us=newest.created_at_us,
                provenance=Provenance.RECOVERY,
                body=body,
                actor_id=newest.actor_id,
                issue_number=ref.issue_number,
                evidence=snapshot,
            )
        )
        LOG.info(
            "label command recovered issue=%s label=%s actor=%s accepted=%s reason=%s",
            ref.issue_number,
            label,
            newest.actor_id,
            result.accepted,
            result.reason,
        )
        return True

    async def _applied_before(self, parcel_id: str, body: ev.EventBody, labeled_us: int) -> bool:
        """A label control of this kind already reached the parcel from GitHub at or after
        the label time (a webhook processed under its delivery's ID)."""
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT 1 FROM events WHERE parcel_id = ? AND kind = ? "
                "AND provenance IN (?, ?) AND json_extract(payload_json, '$.body.via') = ? "
                "AND source_time_us >= ? LIMIT 1",
                (
                    parcel_id,
                    body.KIND.value,
                    Provenance.WEBHOOK.value,
                    Provenance.RECOVERY.value,
                    Via.LABEL.value,
                    labeled_us - _LEGACY_SLACK_US,
                ),
            )
        )
        return bool(rows)
