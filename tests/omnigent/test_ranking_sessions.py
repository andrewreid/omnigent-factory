"""The ranking run's Omnigent session, and retention of factory-created sessions."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.omnigent import policies as pol
from omnigent_factory.omnigent.adapter import NONCE_LABEL
from omnigent_factory.omnigent.ranking import RANKING_LABEL, RankingSessionError, RankingSessions
from omnigent_factory.service import session_retention as retention
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.service.session_retention import (
    SessionRetention,
    archived_at_us,
    select_candidates,
)
from omnigent_factory.store import ranking as rs
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID
from omnigent_factory.testing.harness import Harness
from tests.credentials.repos import GitEnv
from tests.omnigent.fake_server import T0, FakeSession
from tests.omnigent.support import AGENT, HOST, PROJECT, make_rig

P = "I_parcel_1"
DAY_S = 86_400


# ------------------------------------------------------------------ ranking session


@pytest.mark.asyncio
async def test_ranking_session_is_adopted_after_a_lost_create_and_messaged_once(
    git_env: GitEnv,
):
    rig = make_rig(git_env)
    sessions = RankingSessions(rig.adapter, str(git_env.source))
    rig.server.faults[("POST", "/v1/sessions")].append("timeout-after")
    title = "Factory ranking · 2026-10-08 10:00"
    with pytest.raises(RankingSessionError) as lost:
        await sessions.create(run_id="rk_1", nonce="ranking-n1", title=title)
    assert lost.value.retry
    root = await sessions.create(run_id="rk_1", nonce="ranking-n1", title=title)
    assert rig.server.count("POST", "/v1/sessions") == 1  # adopted by nonce, not re-created
    session = rig.server.sessions[root]
    assert session.title == title and session.git_branch is None
    assert session.labels[NONCE_LABEL] == "ranking-n1" and session.labels[RANKING_LABEL] == "rk_1"
    assert (session.agent_id, session.host_id, session.project_id) == (AGENT, HOST, PROJECT)

    await sessions.prepare(root, "ranking-n1")
    rows = rig.server.policies[root]
    assert sorted(pol.family_of(r["name"]) for r in rows) == sorted(
        [pol.GITHUB_NAME, pol.CEL_NAME, pol.CALLER_NAME, pol.COST_PREFIX]
    )
    [github] = [r for r in rows if pol.family_of(r["name"]) == pol.GITHUB_NAME]
    assert github["factory_params"]["write_repos"] == []  # read-only
    [caller] = [r for r in rows if pol.family_of(r["name"]) == pol.CALLER_NAME]
    assert root in caller["factory_params"]["expression"]  # bound to this session's id
    await sessions.verify(root)

    for _ in range(2):  # a retry after a crash finds the marker and sends nothing
        await sessions.send_once(root, "ranking:rk_1:start", "Order the Triage column.")
    assert len(session.items) == 1
    state = await sessions.state(root)
    assert state is not None and state.idle and not state.archived
    await sessions.archive(root)
    assert session.archived is True
    await sessions.archive("conv_gone")  # a missing root counts as archived
    assert await sessions.state("conv_gone") is None


@pytest.mark.asyncio
async def test_a_tampered_policy_set_fails_verification(git_env: GitEnv):
    rig = make_rig(git_env)
    sessions = RankingSessions(rig.adapter, str(git_env.source))
    root = await sessions.create(run_id="rk_2", nonce="ranking-n2", title="t")
    await sessions.prepare(root, "ranking-n2")
    rig.server.policies[root] = [
        r for r in rig.server.policies[root] if pol.family_of(r["name"]) != pol.CALLER_NAME
    ]
    with pytest.raises(RankingSessionError, match="missing or altered") as failed:
        await sessions.verify(root)
    assert not failed.value.retry


# ------------------------------------------------------------------ retention selection


def _finished() -> tuple[Any, set[str]]:
    h = Harness()
    build = h.to_building(P)
    h.send(P, ev.Closed())
    h.quiesce(P, build.session_id)
    h.send(P, ev.IssueSessionClosed(root_id=build.root_id or ""))
    parcel = h.p(P)
    return parcel, {s.root_id for s in parcel.sessions if s.root_id}


def test_only_recorded_roots_of_finished_issues_and_ranking_runs_are_candidates():
    done, roots = _finished()
    live = Harness()
    live.to_building("I_live")
    draining = Harness()
    b = draining.to_building("I_drain")
    draining.send("I_drain", ev.Closed())  # closed, but its build is still draining
    assert draining.p("I_drain").sessions[-1].session_id == b.session_id
    chosen = select_candidates(
        [done, live.p("I_live"), draining.p("I_drain")],
        [("conv_rank_done", "rk_1", True), ("conv_rank_open", "rk_2", False)],
        deleted={"conv_rank_old"},
    )
    assert {c.root_id for c in chosen} == roots | {"conv_rank_done"}
    again = select_candidates([done], [], deleted=roots)
    assert again == []


def test_archive_time_comes_from_omnigents_label_else_the_last_update():
    assert archived_at_us({"labels": {"omnigent.archived_at": "100"}}) == 100_000_000
    assert archived_at_us({"labels": {}, "updated_at": 200}) == 200_000_000
    assert archived_at_us({"labels": {}}) is None


# ------------------------------------------------------------------ retention sweep


def _config(tmp_path: Path, **over: Any) -> ServiceConfig:
    state, secrets = tmp_path / "svc-state", tmp_path / "svc-secrets"
    for path in (state, secrets):
        path.mkdir(mode=0o700)
        os.chmod(path, 0o700)
    return ServiceConfig(
        state_dir=state,
        secrets_dir=secrets,
        repo_id=REPO_ID,
        owners=frozenset({OWNER_ID}),
        **over,
    )


def _archived(sid: str, *, days: float, label: bool = True, **over: Any) -> FakeSession:
    labels = {"omnigent.archived_at": str(int(T0 - days * DAY_S))}
    if label:
        labels[NONCE_LABEL] = f"nonce-{sid}"
    values: dict[str, Any] = {"id": sid, "agent_id": AGENT, "labels": labels, "archived": True}
    values.update(over)
    return FakeSession(**values)


@pytest.mark.asyncio
async def test_sweep_deletes_only_old_archived_idle_factory_sessions_once(
    git_env: GitEnv, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    rig = make_rig(git_env)
    server = rig.server
    server.add(_archived("conv_old", days=40))
    server.add(FakeSession(id="conv_old_child", agent_id=AGENT, parent_session_id="conv_old"))
    server.add(_archived("conv_recent", days=5))
    server.add(_archived("conv_open", days=40, archived=False))
    server.add(_archived("conv_foreign", days=40, label=False))
    server.add(_archived("conv_busy", days=40))
    server.add(
        FakeSession(
            id="conv_busy_child", agent_id=AGENT, parent_session_id="conv_busy", status="running"
        )
    )
    roots = ["conv_old", "conv_recent", "conv_open", "conv_foreign", "conv_busy", "conv_gone"]

    def recorded(store: Any, *, repo_id: str) -> list[retention.Candidate]:
        ranking = [(root, f"rk_{root}", True) for root in roots]
        return select_candidates([], ranking, rs.deleted_sessions(store, roots))

    monkeypatch.setattr(retention, "_candidates", recorded)
    service = FactoryService(_config(tmp_path), clock=rig.clock)
    await service.start()
    try:
        sweeper = SessionRetention(service, rig.adapter)
        dry = await sweeper.sweep(dry_run=True)
        assert dry["would_delete"] == ["conv_old"] and dry["deleted"] == []
        assert dry["skipped"] == {
            "conv_busy": "a session in its tree is not idle",
            "conv_foreign": "no factory label",
            "conv_open": "not archived",
            "conv_recent": "archived too recently",
        }
        assert server.count("DELETE", "/v1/sessions/conv_old") == 0
        assert "conv_old" in server.sessions
        assert await service.db.call(lambda s: rs.deleted_sessions(s, roots)) == set()

        done = await sweeper.sweep()
        assert done["deleted"] == ["conv_old"]
        assert "conv_old" not in server.sessions and "conv_old_child" not in server.sessions
        assert await service.db.call(lambda s: rs.deleted_sessions(s, roots)) == {
            "conv_old",
            "conv_gone",
        }
        # A restart: recorded deletions are never retried (not even read again).
        reads = server.count("GET", "/v1/sessions/conv_old")
        again = await SessionRetention(service, rig.adapter).sweep()
        assert again["deleted"] == [] and server.count("DELETE", "/v1/sessions/conv_old") == 1
        assert server.count("GET", "/v1/sessions/conv_old") == reads
        assert server.count("GET", "/v1/sessions/conv_gone") == 2  # dry run, first sweep
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_retention_zero_never_deletes(git_env: GitEnv, tmp_path: Path):
    rig = make_rig(git_env)
    rig.server.add(_archived("conv_old", days=400))
    service = FactoryService(_config(tmp_path, session_retention_days=0), clock=rig.clock)
    await service.start()
    try:
        service.session_retention = SessionRetention(service, rig.adapter)
        report = await service.operator_command("sessions-prune", {"dry_run": False})
        assert report["enabled"] is False and report["deleted"] == []
        assert rig.server.requests == []
        assert "conv_old" in rig.server.sessions
    finally:
        await service.stop()
