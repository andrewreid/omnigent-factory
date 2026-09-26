"""Durable capability hashes/generations, worker bindings and worker grants (Task 5a).

After a restart every persisted capability still verifies, but issuance is denied until
the service re-enables a stage whose current persisted gate allows its fixed profile.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import CredentialProfile, EffectKind
from omnigent_factory.core.types import Via
from omnigent_factory.credentials.broker import LocalCredentialBroker, worker_session_id
from omnigent_factory.credentials.capabilities import (
    CapabilityRegistry,
    CapabilityStore,
    read_capability_file,
)
from omnigent_factory.credentials.server import BrokerServer, WorkerGrant, WorkerGrantStore
from omnigent_factory.ports.credentials import TokenGrant, TokenRefusal
from omnigent_factory.service.db import StoreWorker
from omnigent_factory.service.durable import (
    StoreCapabilityStore,
    StoreWorkerGrantStore,
    reenable_issuance_after_boot,
)
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness

from .fakes import FakeGate, FakeMinter
from .test_broker import CTX, REPO, _intent

pytestmark = pytest.mark.asyncio


class Daemon:
    """One daemon boot: its own DB worker, registry, broker and gate."""

    def __init__(self, tmp: Path, gate: FakeGate) -> None:
        self.tmp = tmp
        self.clock = FakeClock()
        self.gate = gate
        self.worker = StoreWorker(tmp / "state.sqlite3", self.clock)
        self.minter = FakeMinter(self.clock)

    async def boot(self) -> Daemon:
        await self.worker.start()
        self.store = StoreCapabilityStore(self.worker)
        self.caps = CapabilityRegistry(
            tmp_caps(self.tmp), self.tmp / "b.sock", REPO, store=self.store
        )
        self.broker = LocalCredentialBroker(
            gate=self.gate,
            minter=self.minter,
            clock=self.clock,
            capabilities=self.caps,
            repository=REPO,
        )
        await self.broker.restore()
        return self

    async def crash(self) -> None:
        await self.worker.close()


def tmp_caps(tmp: Path) -> Path:
    return tmp / "caps"


async def test_store_classes_satisfy_protocols(tmp_path: Path) -> None:
    worker = StoreWorker(tmp_path / "s.sqlite3", FakeClock())
    await worker.start()
    assert isinstance(StoreCapabilityStore(worker), CapabilityStore)
    assert isinstance(StoreWorkerGrantStore(worker), WorkerGrantStore)
    await worker.close()


async def test_capability_survives_restart_but_issuance_stays_denied_until_recheck(
    tmp_path: Path,
) -> None:
    gate = FakeGate()
    first = await Daemon(tmp_path, gate).boot()
    record = await first.broker.provision("S1")
    secret = read_capability_file(record.path).secret
    await first.broker.execute(_intent(EffectKind.ENABLE_ISSUANCE, "S1", "build"), CTX)
    gate.open("S1", CredentialProfile.BUILD)
    assert isinstance(await first.broker.request_token("S1", secret, REPO), TokenGrant)
    await first.crash()

    second = await Daemon(tmp_path, gate).boot()
    restored = second.caps.get("S1")
    assert restored is not None and restored.generation == 1
    assert second.caps.verify("S1", secret) is not None
    # default-deny on boot, even though the gate is open and the capability verifies
    assert await second.broker.request_token("S1", secret, REPO) == TokenRefusal(
        "issuance-disabled"
    )
    gate.close("S1", "fenced")
    assert not await second.broker.reenable_after_recheck("S1", CredentialProfile.BUILD)
    gate.open("S1", CredentialProfile.READ_ONLY)
    assert not await second.broker.reenable_after_recheck("S1", CredentialProfile.BUILD)
    gate.open("S1", CredentialProfile.BUILD)
    assert await second.broker.reenable_after_recheck("S1", CredentialProfile.BUILD)
    grant = await second.broker.request_token("S1", secret, REPO)
    assert isinstance(grant, TokenGrant) and grant.profile == CredentialProfile.BUILD
    assert not await second.broker.reenable_after_recheck("missing", CredentialProfile.BUILD)
    await second.crash()


async def test_generations_continue_after_revocation_and_restart(tmp_path: Path) -> None:
    gate = FakeGate()
    first = await Daemon(tmp_path, gate).boot()
    await first.broker.provision("S1")
    rotated = await first.broker.provision("S1")
    assert rotated.generation == 2
    retired_secret = read_capability_file(rotated.path).secret
    await first.broker.retire("S1")
    await first.crash()

    second = await Daemon(tmp_path, gate).boot()
    assert second.caps.get("S1") is None  # revoked stays revoked
    again = await second.broker.provision("S1")
    assert again.generation == 3
    assert second.caps.verify("S1", retired_secret) is None
    await second.crash()


async def test_worker_binding_and_role_survive_restart(tmp_path: Path) -> None:
    gate = FakeGate()
    first = await Daemon(tmp_path, gate).boot()
    await first.broker.provision("S1")
    worker = await first.broker.provision_worker("S1", "review", CredentialProfile.READ_ONLY)
    worker_secret = read_capability_file(worker.path).secret
    await first.crash()

    second = await Daemon(tmp_path, gate).boot()
    key = worker_session_id("S1", "review")
    assert second.broker.workers_of("S1") == (key,)
    gate.open("S1", CredentialProfile.BUILD)
    # worker keys can never be re-enabled directly; only the stage can
    assert not await second.broker.reenable_after_recheck(key, CredentialProfile.BUILD)
    assert await second.broker.request_token(key, worker_secret, REPO) == TokenRefusal(
        "issuance-disabled"
    )
    assert await second.broker.reenable_after_recheck("S1", CredentialProfile.BUILD)
    grant = await second.broker.request_token(key, worker_secret, REPO)
    assert isinstance(grant, TokenGrant)
    assert grant.profile == CredentialProfile.READ_ONLY  # recorded role, not the stage's
    await second.broker.retire("S1")
    assert second.caps.get(key) is None
    await second.crash()

    third = await Daemon(tmp_path, gate).boot()
    assert third.broker.workers_of("S1") == ()
    assert third.caps.get(key) is None
    await third.crash()


async def test_worker_grant_tuples_persist_and_revoke(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    clock = FakeClock()
    worker = StoreWorker(db, clock)
    await worker.start()
    broker = LocalCredentialBroker(
        gate=FakeGate(),
        minter=FakeMinter(clock),
        clock=clock,
        capabilities=CapabilityRegistry(tmp_path / "caps", tmp_path / "s", REPO, volatile_ok=True),
        repository=REPO,
    )
    server = BrokerServer(broker, tmp_path / "s", grants=StoreWorkerGrantStore(worker))
    grant = WorkerGrant(
        "review", Path("/wt/review"), "factory/issue-1-g1", CredentialProfile.READ_ONLY
    )
    await server.authorize_worker("S1", grant)
    await server.authorize_worker(
        "S2", WorkerGrant("impl", Path("/wt/impl"), "factory/issue-2-g1", CredentialProfile.BUILD)
    )
    with pytest.raises(ValueError):
        await server.authorize_worker(
            "S1", WorkerGrant("rel", Path("relative"), "b", CredentialProfile.BUILD)
        )
    await worker.close()

    worker = StoreWorker(db, clock)
    await worker.start()
    restarted = BrokerServer(broker, tmp_path / "s", grants=StoreWorkerGrantStore(worker))
    await restarted.restore()
    assert restarted._workers == {
        "S1": {"review": grant},
        "S2": {
            "impl": WorkerGrant(
                "impl", Path("/wt/impl"), "factory/issue-2-g1", CredentialProfile.BUILD
            )
        },
    }
    await restarted.revoke_workers("S1")
    await worker.close()

    worker = StoreWorker(db, clock)
    await worker.start()
    again = BrokerServer(broker, tmp_path / "s", grants=StoreWorkerGrantStore(worker))
    await again.restore()
    assert set(again._workers) == {"S2"}
    await worker.close()


async def test_service_reenables_only_current_unfenced_executing_stages(tmp_path: Path) -> None:
    h = Harness()
    h.eligible("A")
    h.send("A", ev.RequestTriage(via=Via.DRAG))
    active = h.create_ok("A")
    h.eligible("B")
    h.send("B", ev.RequestTriage(via=Via.DRAG))
    fenced = h.create_ok("B")
    h.send("B", ev.Stop())
    h.eligible("C")
    h.send("C", ev.RequestTriage(via=Via.DRAG))  # INTENT only: never issued

    gate = FakeGate()
    daemon = await Daemon(tmp_path, gate).boot()
    for sid in (active.session_id, fenced.session_id, h.cur("C").session_id):
        await daemon.broker.provision(sid)
        gate.open(sid, CredentialProfile.READ_ONLY)
    enabled = await reenable_issuance_after_boot(daemon.broker, [h.p(x) for x in "ABC"])
    assert enabled == (active.session_id,)
    assert daemon.broker.issuance_enabled(active.session_id)
    assert not daemon.broker.issuance_enabled(fenced.session_id)
    await daemon.crash()


async def test_registry_requires_store_or_explicit_volatility(tmp_path: Path) -> None:
    from omnigent_factory.credentials.capabilities import CapabilityFileError

    with pytest.raises(CapabilityFileError):
        CapabilityRegistry(tmp_path, tmp_path / "s", REPO)
    registry = CapabilityRegistry(tmp_path / "c", tmp_path / "s", REPO, volatile_ok=True)
    with pytest.raises(ValueError):
        await registry.provision("W", worker_of="S1")
