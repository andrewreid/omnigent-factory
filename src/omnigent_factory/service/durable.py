"""Store-backed implementations of the adapter-side durability protocols (Task 5a).

Each class funnels its reads and writes through the service's serialized
:class:`~omnigent_factory.service.db.StoreWorker`, so no SQLite work runs on the event
loop and every write is committed before the protocol method returns:

* :class:`StoreOwnItemLedger` - Omnigent own-send/resolve intents written BEFORE each POST
  (``own_sends``), replacing ``MemoryOwnItemLedger`` in production;
* :class:`StoreCapabilityStore` - capability hashes, generations and worker bindings;
* :class:`StoreWorkerGrantStore` - daemon-recorded ``WorkerGrant`` tuples.

:func:`reenable_issuance_after_boot` is the service's post-restart issuance recheck:
issuance stays default-deny until each executing stage's current gate is confirmed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path

from omnigent_factory.core.effects import CredentialProfile, profile_for
from omnigent_factory.core.types import Lifecycle, Parcel
from omnigent_factory.credentials.broker import LocalCredentialBroker
from omnigent_factory.credentials.capabilities import CapabilityRecord
from omnigent_factory.credentials.server import WorkerGrant
from omnigent_factory.omnigent.directory import OwnSend
from omnigent_factory.service.db import StoreWorker
from omnigent_factory.store.sqlite import CapabilityRow, OwnSendRow, WorkerGrantRow


class StoreOwnItemLedger:
    """Durable :class:`~omnigent_factory.omnigent.directory.OwnItemLedger`."""

    def __init__(self, db: StoreWorker) -> None:
        self._db = db

    async def record_intent(self, send: OwnSend) -> None:
        row = OwnSendRow(
            effect_id=send.effect_id,
            session_id=send.session_id,
            node_id=send.node_id,
            kind=send.kind,
            text_sha256=send.text_sha256,
            elicitation_id=send.elicitation_id,
            item_id=None,
        )
        await self._db.call(lambda store: store.record_own_send(row))

    async def record_item(self, effect_id: str, item_id: str) -> None:
        await self._db.call(lambda store: store.record_own_item(effect_id, item_id))

    async def lookup(self, effect_id: str) -> OwnSend | None:
        row = await self._db.call(lambda store: store.own_send(effect_id))
        if row is None:
            return None
        return OwnSend(
            effect_id=row.effect_id,
            session_id=row.session_id,
            node_id=row.node_id,
            kind=row.kind,
            text_sha256=row.text_sha256,
            elicitation_id=row.elicitation_id,
            item_id=row.item_id,
        )

    async def own_item_ids(self, session_id: str) -> frozenset[str]:
        return await self._db.call(lambda store: store.own_item_ids(session_id))


class StoreCapabilityStore:
    """Durable :class:`~omnigent_factory.credentials.capabilities.CapabilityStore`."""

    def __init__(self, db: StoreWorker) -> None:
        self._db = db

    async def save(self, record: CapabilityRecord) -> None:
        row = CapabilityRow(
            session_key=record.session_id,
            stage_session_id=record.worker_of or record.session_id,
            worker_id=(
                record.session_id.removeprefix(f"{record.worker_of}~worker~")
                if record.worker_of is not None
                else None
            ),
            worker_profile=record.worker_profile.value if record.worker_profile else None,
            capability_id=record.capability_id,
            secret_sha256=record.secret_sha256,
            generation=record.generation,
            path=str(record.path),
            revoked=False,
        )
        await self._db.call(lambda store: store.save_capability(row))

    async def revoke(self, session_id: str) -> None:
        await self._db.call(lambda store: store.revoke_capability(session_id))

    async def load(self) -> tuple[CapabilityRecord, ...]:
        rows = await self._db.call(lambda store: store.capability_rows())
        return tuple(
            CapabilityRecord(
                capability_id=r.capability_id,
                session_id=r.session_key,
                secret_sha256=r.secret_sha256,
                generation=r.generation,
                path=Path(r.path),
                worker_of=r.stage_session_id if r.worker_id is not None else None,
                worker_profile=(
                    CredentialProfile(r.worker_profile) if r.worker_profile is not None else None
                ),
                revoked=r.revoked,
            )
            for r in rows
        )


class StoreWorkerGrantStore:
    """Durable :class:`~omnigent_factory.credentials.server.WorkerGrantStore`."""

    def __init__(self, db: StoreWorker) -> None:
        self._db = db

    async def save(self, stage_session_id: str, grant: WorkerGrant) -> None:
        row = WorkerGrantRow(
            stage_session_id,
            grant.worker_id,
            str(grant.path),
            grant.branch,
            grant.profile.value,
        )
        await self._db.call(lambda store: store.save_worker_grant(row))

    async def delete(self, stage_session_id: str) -> None:
        await self._db.call(lambda store: store.delete_worker_grants(stage_session_id))

    async def load(self) -> Mapping[str, Mapping[str, WorkerGrant]]:
        rows = await self._db.call(lambda store: store.worker_grant_rows())
        out: dict[str, dict[str, WorkerGrant]] = {}
        for r in rows:
            out.setdefault(r.stage_session_id, {})[r.worker_id] = WorkerGrant(
                r.worker_id, Path(r.path), r.branch, CredentialProfile(r.profile)
            )
        return out


#: Lifecycles in which an ``ENABLE_ISSUANCE`` may already have been applied.
_ISSUING = frozenset({Lifecycle.ACTIVE, Lifecycle.WAITING, Lifecycle.CHECKPOINT_GRACE})


async def reenable_issuance_after_boot(
    broker: LocalCredentialBroker, parcels: Iterable[Parcel]
) -> tuple[str, ...]:
    """After :meth:`LocalCredentialBroker.restore`, re-enable only rechecked stages.

    Candidates are each parcel's current, unfenced, executing stage session; the broker
    still consults the persisted execution gate for each before enabling its fixed
    profile. Everything else (fenced, draining, retired, workers) stays denied.
    """
    enabled: list[str] = []
    for parcel in parcels:
        s = parcel.current_session
        if s is None or s.fences or s.execution_closed or s.lifecycle not in _ISSUING:
            continue
        if await broker.reenable_after_recheck(s.session_id, profile_for(s.kind)):
            enabled.append(s.session_id)
    return tuple(enabled)
