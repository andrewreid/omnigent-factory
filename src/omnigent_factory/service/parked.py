"""Operator-releasable parked deliveries, persisted in the store (replaces the Task-4 file).

The ``parked_deliveries`` table is the authority; this class keeps a synchronous mirror
so the executor's per-effect gate needs no database round trip. Ordering keeps the mirror
at least as restrictive as the database at every instant:

* park: mirror first, then one transaction (scope row + ``rejected`` status + parcel
  hold event); a failed commit restores the mirror from the database;
* release: one transaction (drop row + ``pending`` + hold release event), then mirror.

A parcel-scoped park also applies :class:`~omnigent_factory.core.events.InboxHoldSet`
(``INBOX`` provenance) in that transaction: the parcel is fenced (safety), its running
tree interrupted and drained, queued authority cancelled, and nothing dispatches until
the operator releases the delivery. Release removes only the hold; recovery then needs a
fresh owner stage control, as after any safety fact. A repository-wide park (unknown
scope) gates all new work through :meth:`blocks` and has no single parcel to fence.

On first start after the upgrade, a Task-4 ``parked-deliveries.json`` is imported with
its scopes (and scoped holds) in the same startup transaction, then renamed to
``parked-deliveries.json.migrated``. A corrupt legacy file aborts startup (fail closed).
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import InboxHoldReason, TrustedConfig
from omnigent_factory.ports.clock import Clock
from omnigent_factory.service.db import StoreWorker

LEGACY_REGISTRY = "parked-deliveries.json"


def read_legacy_registry(path: Path) -> tuple[tuple[str, str | None], ...]:
    """Parse a Task-4 registry file; raise on anything but ``{guid: scope|null}``."""
    if not path.exists():
        return ()
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or any(
        not isinstance(guid, str) or (scope is not None and not isinstance(scope, str))
        for guid, scope in raw.items()
    ):
        raise RuntimeError("invalid parked delivery registry")
    return tuple(sorted(raw.items()))


def inbox_hold_event(
    config: TrustedConfig,
    clock: Clock,
    delivery_guid: str,
    parcel_id: str,
    reason: InboxHoldReason,
    *,
    event_id: str | None = None,
) -> Event:
    return Event(
        event_id=event_id or f"inbox-hold:{delivery_guid}:{reason.value}:{uuid.uuid4()}",
        repo_id=config.repo_id,
        parcel_id=parcel_id,
        source_time_us=clock.now_utc_us(),
        provenance=Provenance.INBOX,
        body=ev.InboxHoldSet(delivery_guid=delivery_guid, reason=reason),
    )


def inbox_release_event(
    config: TrustedConfig,
    clock: Clock,
    delivery_guid: str,
    parcel_id: str,
    provenance: Provenance,
    *,
    event_id: str | None = None,
) -> Event:
    return Event(
        event_id=event_id or f"inbox-release:{delivery_guid}:{uuid.uuid4()}",
        repo_id=config.repo_id,
        parcel_id=parcel_id,
        source_time_us=clock.now_utc_us(),
        provenance=provenance,
        body=ev.InboxHoldReleased(delivery_guid=delivery_guid),
    )


class ParkedDeliveries:
    def __init__(
        self, db: StoreWorker, state_dir: Path, config: TrustedConfig, clock: Clock
    ) -> None:
        self._db = db
        self._config = config
        self._clock = clock
        self.legacy_path = state_dir / LEGACY_REGISTRY
        self._items: dict[str, str | None] = {}

    def update_config(self, config: TrustedConfig) -> None:
        """Adopt a hot-reloaded trusted config (same repository)."""
        self._config = config

    async def load(self) -> None:
        """Startup: import any legacy registry, reconcile statuses, fill the mirror."""
        legacy = read_legacy_registry(self.legacy_path)
        holds = tuple(
            inbox_hold_event(
                self._config,
                self._clock,
                guid,
                scope,
                InboxHoldReason.PARKED,
                event_id=f"inbox-hold:{guid}:legacy-import",
            )
            for guid, scope in legacy
            if scope is not None
        )
        config = self._config
        await self._db.call(
            lambda store: store.sync_parked_deliveries(legacy, holds=holds, config=config)
        )
        if self.legacy_path.exists():
            os.replace(self.legacy_path, self.legacy_path.with_name(LEGACY_REGISTRY + ".migrated"))
            _fsync_dir(self.legacy_path.parent)
        await self._refresh()

    def contains(self, delivery_guid: str) -> bool:
        return delivery_guid in self._items

    def blocks(self, parcel_id: str | None) -> bool:
        return any(scope is None or scope == parcel_id for scope in self._items.values())

    def records(self) -> tuple[tuple[str, str | None], ...]:
        return tuple(sorted(self._items.items()))

    async def park(
        self, delivery_guid: str, parcel_id: str | None, *, reason: str | None = None
    ) -> None:
        prior = self._items.get(delivery_guid, parcel_id)
        # Mirror first: never less restrictive than the database, even mid-commit.
        self._items[delivery_guid] = parcel_id if prior == parcel_id else None
        hold = (
            inbox_hold_event(
                self._config, self._clock, delivery_guid, parcel_id, InboxHoldReason.PARKED
            )
            if parcel_id is not None
            else None
        )
        config = self._config
        try:
            await self._db.call(
                lambda store: store.park_delivery(
                    delivery_guid, parcel_id, hold=hold, config=config, reason=reason
                )
            )
        finally:
            await self._refresh(keep=delivery_guid)

    async def release(self, delivery_guid: str) -> None:
        if delivery_guid not in self._items:
            raise ValueError("delivery is not parked")
        scope = self._items[delivery_guid]
        release = (
            inbox_release_event(
                self._config, self._clock, delivery_guid, scope, Provenance.OPERATOR
            )
            if scope is not None
            else None
        )
        config = self._config
        released = await self._db.call(
            lambda store: store.release_parked_delivery(
                delivery_guid, release=release, config=config
            )
        )
        await self._refresh()
        if not released:
            raise ValueError("delivery is not parked")

    async def _refresh(self, *, keep: str | None = None) -> None:
        rows = dict(await self._db.call(lambda store: store.parked_delivery_rows()))
        if keep is not None and keep not in rows and keep in self._items:
            # The park did not commit: stay closed repository-wide until startup sync.
            rows[keep] = None
        self._items = rows


def _fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
