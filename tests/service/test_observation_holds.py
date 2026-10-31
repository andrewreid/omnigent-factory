"""Startup containment's safety holds are visible: every parcel held because its
observation failed a store invariant is listed by ``status`` and ``doctor`` (count, last
reason and time, still held), read from its ``ObservationFailed`` events (no migration)."""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import Hold
from omnigent_factory.service.doctor import DoctorReport, _check_observation_holds
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import (
    OBSERVATION_HOLDS_LIMIT,
    InvariantViolation,
    observation_held_count,
    observation_holds,
)
from omnigent_factory.testing.builders import REPO_ID
from omnigent_factory.testing.fakes import FakeClock
from tests.test_store import open_store, store_harness

A = "I_parcel_a"
B = "I_parcel_b"


def _fail(h, pid: str, reason: str = "") -> None:
    r = h.send(pid, ev.ObservationFailed(reason=reason), provenance=Provenance.ADAPTER)
    assert r.audit.accepted


def test_holds_are_counted_per_parcel_with_the_last_reason(tmp_path) -> None:
    store = open_store(tmp_path / "state.sqlite3")
    try:
        h = store_harness(store)
        h.eligible(A)
        h.eligible(B)
        assert observation_holds(store._conn, REPO_ID) == []
        _fail(h, A, "first")
        _fail(h, B)  # recorded before reasons existed: shown as unknown
        _fail(h, A, "observed reservation requires an authorized parked build")
        rows = observation_holds(store._conn, REPO_ID)
        assert [r["parcel"] for r in rows] == [A, B]  # newest failure first
        a = rows[0]
        assert a["count"] == 2 and a["held"] is True
        assert a["last_reason"] == "observed reservation requires an authorized parked build"
        assert a["issue"] == h.p(A).issue_number and a["last_at_us"] > 0
        assert rows[1]["count"] == 1 and rows[1]["last_reason"] == ""
        assert Hold.SAFETY in h.p(A).holds
        assert [r["parcel"] for r in observation_holds(store._conn, REPO_ID, limit=1)] == [A]
    finally:
        store.close()


@pytest.mark.asyncio
async def test_status_and_doctor_list_held_parcels(service_config) -> None:
    store = open_store(service_config.database_path)
    try:
        h = store_harness(store)
        h.eligible(A)
        issue = h.p(A).issue_number
        start = max(f.now for f in h.factories.values()) + 1_000_000
    finally:
        store.close()
    service = FactoryService(service_config, clock=FakeClock(start_us=start))
    await service.open()
    try:
        for _ in range(2):
            try:
                raise InvariantViolation("injected parcel invariant")
            except InvariantViolation:
                # The reason defaults to the exception being handled (startup callers).
                await service.hold_observation_failure(A)
        for command in ("status", "doctor"):
            result = await service.operator_command(command, {})
            [row] = result["observation_holds"]
            assert row["parcel"] == A and row["count"] == 2 and row["held"]
            assert row["last_reason"] == "injected parcel invariant"
        report = DoctorReport()
        _check_observation_holds(service_config, report)
        detail = report.checks["observation_holds"]
        assert detail.startswith("warning: 1 parcel(s) safety-held")
        assert f"#{issue} held x2: injected parcel invariant" in detail
        assert any(w.startswith("observation_holds: ") for w in report.warnings)
    finally:
        await service.stop()


def test_doctor_passes_without_holds_and_lists_released_ones(service_config) -> None:
    report = DoctorReport()
    _check_observation_holds(service_config, report)
    assert report.checks["observation_holds"] in ("none", "no database")
    store = open_store(service_config.database_path)
    try:
        h = store_harness(store)
        h.eligible(A)
        _fail(h, A, "boom")
        # The operator recovered the parcel since: no longer safety-held.
        store.query("UPDATE parcels SET holds_json = '[]' WHERE parcel_id = ?", (A,))
    finally:
        store.close()
    report = DoctorReport()
    _check_observation_holds(service_config, report)
    assert report.checks["observation_holds"].startswith("none held; earlier: ")
    assert "released x1: boom" in report.checks["observation_holds"]
    assert report.warnings == []


def test_a_live_hold_is_not_hidden_by_more_released_history(service_config) -> None:
    """21 parcels with failures: the oldest is still held, the 20 newer were released.
    The 20-row listing puts the held one first, and doctor still warns."""
    pids = [f"I_parcel_{n:02d}" for n in range(OBSERVATION_HOLDS_LIMIT + 1)]
    store = open_store(service_config.database_path)
    try:
        h = store_harness(store)
        for pid in pids:
            h.eligible(pid)
        for pid in pids:  # pids[0] fails first (oldest)
            _fail(h, pid, f"boom {pid}")
        store.query("UPDATE parcels SET holds_json = '[]' WHERE parcel_id != ?", (pids[0],))
        conn = store._conn
        assert observation_held_count(conn, REPO_ID) == 1
        rows = observation_holds(conn, REPO_ID)
        assert len(rows) == OBSERVATION_HOLDS_LIMIT
        assert rows[0]["parcel"] == pids[0] and rows[0]["held"]
        assert not any(r["held"] for r in rows[1:])
        assert [r["parcel"] for r in rows[1:]] == pids[:0:-1][: OBSERVATION_HOLDS_LIMIT - 1]
        held_issue = h.p(pids[0]).issue_number
    finally:
        store.close()
    report = DoctorReport()
    _check_observation_holds(service_config, report)
    detail = report.checks["observation_holds"]
    assert detail.startswith("warning: 1 parcel(s) safety-held")
    assert f"#{held_issue} held x1: boom {pids[0]}" in detail


def test_held_count_is_not_bounded_by_the_listing(service_config) -> None:
    pids = [f"I_parcel_{n:02d}" for n in range(3)]
    store = open_store(service_config.database_path)
    try:
        h = store_harness(store)
        for pid in pids:
            h.eligible(pid)
            _fail(h, pid, "boom")
        conn = store._conn
        assert observation_held_count(conn, REPO_ID) == 3
        assert len(observation_holds(conn, REPO_ID, limit=2)) == 2
    finally:
        store.close()
