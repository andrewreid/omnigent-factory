"""One parcel's failed startup observation cannot crash the daemon or reopen its work."""

from types import SimpleNamespace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.predicates import work_allowed
from omnigent_factory.core.types import FenceKind, Hold
from omnigent_factory.omnigent.tree import NodeState, TreeObservation
from omnigent_factory.service.composition import ProductionRuntime
from omnigent_factory.service.doctor import DoctorReport, _check_build_slots
from omnigent_factory.service.observer import OmnigentObserver
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import InvariantViolation, SqliteStore
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.fakes import FakeClock
from tests.test_build_slots import A, B, blocked, building
from tests.test_store import open_store, store_harness


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["tree", "github", "policy"])
async def test_startup_holds_bad_parcel_and_continues(service_config, monkeypatch, caplog, phase):
    config = service_config.model_copy(update={"max_building": 2})
    store = open_store(config.database_path)
    try:
        h = store_harness(store, max_building=2)
        building(h, A)
        building(h, B)
        sessions = {pid: h.cur(pid).session_id for pid in (A, B)}
    finally:
        store.close()
    clock = FakeClock(start_us=max(f.now for f in h.factories.values()) + 1_000_000)
    exits = []
    service = FactoryService(config, clock=clock, fatal_exit=exits.append)
    original = SqliteStore.apply_event
    attempted = []
    kind = {"tree": ev.TreeQuiescent, "github": ev.GitHubSnapshot, "policy": ev.PolicyGuardFailed}[
        phase
    ]

    def fail_one(self, event, *args, **kwargs):
        if isinstance(event.body, kind):
            attempted.append(event.parcel_id)
            if event.parcel_id == A:
                raise InvariantViolation("injected parcel invariant")
        return original(self, event, *args, **kwargs)

    monkeypatch.setattr(SqliteStore, "apply_event", fail_one)

    async def observe_trees(roots):
        return [
            TreeObservation(root, True, {root: NodeState(root, None, "idle")}) for root in roots
        ]

    observer = OmnigentObserver(
        service,
        SimpleNamespace(observe_trees=observe_trees),
        SimpleNamespace(),
        clock,
        interval_seconds=10,
    )

    async def issue_snapshot(ref):
        return snapshot(read_at_us=clock.now_utc_us())

    async def upgrade(session_id):
        return True

    runtime = object.__new__(ProductionRuntime)
    runtime.service = service
    runtime.config = config
    runtime.github = SimpleNamespace(issue_snapshot=issue_snapshot)
    runtime.omnigent_adapter = SimpleNamespace(upgrade_static_policies=upgrade)
    observed = set()

    class Reconcile:
        async def start(self):
            if phase == "tree":
                observed.update(await observer.observe_once())
            elif phase == "github":
                observed.update(await runtime._github_reconcile())
            else:
                parcels = [
                    await service.db.call(lambda s, pid=pid: s.load_parcel(pid)) for pid in (A, B)
                ]
                held = await runtime._upgrade_live_policies(parcels)
                assert sessions[A] in held

        async def close(self):
            pass

        def healthy(self):
            return True

    service.bind_integrations(managed=(Reconcile(),))
    await service.open()
    try:
        await service._finish_start_or_exit()
        assert service.ready and service.accepting_admission and not exits
        assert attempted[:2] == [A, B]
        parcel = await service.db.call(lambda s: s.load_parcel(A))
        assert Hold.SAFETY in parcel.holds
        assert FenceKind.SAFETY in parcel.current_session.fences
        assert not work_allowed(parcel, parcel.current_session)
        assert "observation invariant failed; holding parcel=" + A in caplog.text
        if phase == "tree":
            assert observed == {sessions[B]}
        elif phase == "github":
            assert observed == {B}
    finally:
        await service.stop()
    store = open_store(config.database_path)
    try:
        assert Hold.SAFETY in store.load_parcel(A).holds
    finally:
        store.close()


@pytest.mark.asyncio
async def test_status_and_doctor_report_observed_over_cap(service_config):
    config = service_config.model_copy(update={"max_building": 1})
    store = open_store(config.database_path)
    try:
        h = store_harness(store, max_building=1)
        building(h, A)
        blocked(h, A)
        building(h, B)
        h.send(A, ev.TreeQuiescent(session_id=h.cur(A).session_id, complete=False, busy=False))
    finally:
        store.close()
    service = FactoryService(config, clock=FakeClock())
    await service.open()
    try:
        for command in ("status", "doctor"):
            result = await service.operator_command(command, {})
            assert result["building"] == 2 and result["building_cap"] == 1
            assert result["building_observed_over_cap"] == 1
        report = DoctorReport()
        _check_build_slots(config, report)
        assert report.checks["building"] == "warning: building 2/1, 1 observed over cap"
    finally:
        await service.stop()


def test_doctor_reads_pre_23_occupancy_without_migrating(service_config):
    from omnigent_factory.store.migrations import MIGRATIONS

    store = open_store(service_config.database_path, migrations=MIGRATIONS[:22])
    try:
        store.ensure_repository(service_config.trusted)
        report = DoctorReport()
        _check_build_slots(service_config, report)
        assert (
            report.checks["building"]
            == f"building 0/{service_config.max_building}, 0 observed over cap"
        )
        assert store.schema_version() == 22
    finally:
        store.close()
