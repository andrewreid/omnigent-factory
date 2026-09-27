"""Pilot follow-up bundle: ready report, Omnigent login, elicitation mapping, cleanup,
CLI start-up retry and observer restart noise."""

from __future__ import annotations

import json
import logging
import shutil
import time
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from omnigent_factory import cli
from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    Preconditions,
)
from omnigent_factory.core.types import Lifecycle, Readiness, Via
from omnigent_factory.credentials.worktree import StageWiring
from omnigent_factory.omnigent.adapter import ELICITATION_NOT_PENDING
from omnigent_factory.omnigent.auth import CliStoreAuth
from omnigent_factory.service.cleanup import NotSettled, WorkspaceCleaner
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import _ready_text
from omnigent_factory.service.executor import EffectExecutor, ParcelSerializers
from omnigent_factory.service.observer import OmnigentObserver
from omnigent_factory.service.omnigent_auth import expiry_message
from omnigent_factory.store.sqlite import StoredEffect
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness
from tests.credentials.repos import BOT, REPO, GitEnv, git, make_git_env
from tests.service.test_readiness_recovery import BUILD_READY, HEAD

BASE = "https://omni.example"

# ------------------------------------------------------------------ 2. ready report


def test_ready_report_tells_the_owner_what_to_merge_and_why():
    text = _ready_text(
        {"pr_number": 682, "head_sha": HEAD},
        BUILD_READY,
        "17 checks: 13 success, 4 skipped",
        "SA-Ambulance/timesheets",
    )
    assert text.startswith("### Factory: PR #682 is Ready")
    assert "Route guard proven in isolation." in text
    assert "https://github.com/SA-Ambulance/timesheets/pull/682 (head `4950075d373b`)" in text
    assert "**CI:** 17 checks: 13 success, 4 skipped" in text
    assert "**Cross-vendor review:** anthropic → openai: clean" in text
    assert "- `A1` ADVISORY (codex): advisory — disabled-user cases pass" in text
    assert text.endswith("**Next:** review and merge PR #682.")


# ------------------------------------------------------------------ 3. Omnigent login


def _store(tmp: Path, *, access: str, expires_at: float, refresh: str | None = "r1") -> Path:
    entry: dict[str, Any] = {"token": access, "user_id": "u", "expires_at": expires_at}
    if refresh is not None:
        entry["refresh_token"] = refresh
    path = tmp / "auth_tokens.json"
    path.write_text(json.dumps({BASE: entry}))
    return path


async def _call(auth: CliStoreAuth, handler: Any) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(record), auth=auth) as http:
        await http.get(f"{BASE}/v1/hosts")
    return seen


@pytest.mark.asyncio
async def test_cli_login_token_is_used_until_near_expiry_then_refreshed(tmp_path: Path):
    now = [1_000_000.0]
    store = _store(tmp_path, access="login", expires_at=now[0] + 86400)
    auth = CliStoreAuth(store, BASE, now=lambda: now[0])

    def ok(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/token":
            assert request.content == b"grant_type=refresh_token&refresh_token=r1"
            return httpx.Response(200, json={"access_token": "fresh", "expires_in": 3600})
        return httpx.Response(200, json={})

    first = await _call(auth, ok)
    assert [r.headers["Authorization"] for r in first] == ["Bearer login"]
    now[0] += 86400 - 60  # within the refresh margin
    second = await _call(auth, ok)
    assert [r.url.path for r in second] == ["/oauth/token", "/v1/hosts"]
    assert second[-1].headers["Authorization"] == "Bearer fresh"
    third = await _call(auth, ok)  # cached for its hour: no second refresh
    assert [r.url.path for r in third] == ["/v1/hosts"]


@pytest.mark.asyncio
async def test_refused_refresh_is_logged_clearly_and_backs_off(tmp_path: Path, caplog):
    now = [2_000_000.0]
    store = _store(tmp_path, access="old", expires_at=now[0] - 10)
    auth = CliStoreAuth(store, BASE, now=lambda: now[0])

    def refused(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/token":
            return httpx.Response(400, json={"error": "invalid_grant"})
        return httpx.Response(401)

    caplog.set_level(logging.ERROR)
    seen = await _call(auth, refused)
    assert [r.url.path for r in seen] == ["/oauth/token", "/v1/hosts"]
    assert "run `omnigent login`" in caplog.text and "r1" not in caplog.text
    again = await _call(auth, refused)
    assert [r.url.path for r in again] == ["/v1/hosts"]  # backoff: no refresh hammering


def test_expiry_warning_and_error(service_config: ServiceConfig, tmp_path: Path):
    store = _store(tmp_path, access="t", expires_at=time.time() + 3 * 86400)
    config = service_config.model_copy(
        update={"omnigent_cli_store": store, "omnigent_base_url": BASE}
    )
    level, message = expiry_message(config) or ("", "")
    assert level == "warning" and "omnigent login" in message and "3.0 days" in message
    _store(tmp_path, access="t", expires_at=time.time() - 1)
    level, message = expiry_message(config) or ("", "")
    assert level == "error" and "expired" in message
    _store(tmp_path, access="t", expires_at=time.time() + 30 * 86400)
    assert expiry_message(config) is None


# ------------------------------------------------------------------ 4. elicitation gone


@pytest.mark.asyncio
async def test_resolve_on_a_prompt_no_longer_pending_is_elicitation_gone(
    service_config: ServiceConfig,
):
    executor = EffectExecutor(
        None,  # type: ignore[arg-type]
        service_config.trusted,
        FakeClock(),
        (),
        ParcelSerializers(),
        poll_seconds=1,
    )
    recorded: list[Any] = []

    async def record(effect: Any, state: str, event: Any, **_: Any) -> None:
        recorded.append((state, event))

    executor._record = record  # type: ignore[method-assign]
    effect = EffectIntent(
        effect_id="ef_resolve",
        kind=EffectKind.RESOLVE_ELICITATION,
        parcel_id="P1",
        target="S1",
        preconditions=Preconditions(1, 0, session_id="S1"),
        args={"elicitation_id": "el-9"},
    )
    stored = StoredEffect(effect, "claimed", 1, None, 1, None)
    await executor._retry_or_unknown(stored, DefinitiveFailure(ELICITATION_NOT_PENDING))
    [(state, event)] = recorded
    assert state == "failed"
    assert event.body == ev.ElicitationGone(session_id="S1", elicitation_id="el-9")


# ------------------------------------------------------------------ 6. cleanup


class _Directory:
    def __init__(self, parcel: Any, snapshots: dict[str, dict[str, str]]) -> None:
        self.parcel = parcel
        self.snapshots = snapshots
        self.db = SimpleNamespace(call=self._call)

    async def _call(self, operation: Any) -> Any:
        return self.parcel

    def dispatch_snapshot(self, session_id: str) -> dict[str, str] | None:
        return self.snapshots.get(session_id)


@pytest.fixture
def git_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[GitEnv]:
    env = make_git_env(tmp_path, monkeypatch)
    try:
        yield env
    finally:
        shutil.rmtree(env.runtime, ignore_errors=True)


def _finished_parcel(worktree: Path, branch: str, head: str) -> tuple[Any, dict[str, Any]]:
    h = Harness()
    h.to_building()
    p = h.p()
    sessions = tuple(replace(s, lifecycle=Lifecycle.RETIRED) for s in p.sessions)
    parcel = replace(
        p,
        issue_number=9,
        sessions=sessions,
        readiness=Readiness(sessions[-1].session_id, 682, head, verified=True, ready=True),
    )
    snaps = {s.session_id: {"workspace": str(worktree), "branch": branch} for s in sessions}
    return parcel, snaps


def _worktree(git_env: GitEnv) -> tuple[Any, Path, str]:
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    wt = git_env.worktrees / "factory-issue-9"
    git("worktree", "add", "-b", "factory/issue-9", str(wt), "origin/main", cwd=git_env.source)
    ws.configure(
        ws.verify_worktree(wt, "factory/issue-9"),
        StageWiring("S", git_env.runtime / "c", git_env.runtime / "s", REPO),
        BOT,
    )
    (wt / "change.txt").write_text("x\n")
    git("add", "change.txt", cwd=wt)
    git("commit", "-m", "work", cwd=wt)
    return ws, wt, git("rev-parse", "HEAD", cwd=wt)


@pytest.mark.asyncio
async def test_finished_parcel_worktree_and_pushed_branch_are_removed(git_env: GitEnv):
    ws, wt, head = _worktree(git_env)
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    cleaner = WorkspaceCleaner(_Directory(parcel, snaps), ws, git_env.worktrees)  # type: ignore[arg-type]
    detail = await cleaner.cleanup(parcel.parcel_id, merged=False)
    assert detail["removed"] == [str(wt.resolve())] and not wt.exists()
    assert detail["branches_deleted"] == ["factory/issue-9"]
    assert (git_env.source / ".git").is_dir()  # the clone itself is never touched


@pytest.mark.asyncio
async def test_dirty_or_unverified_or_live_work_is_kept(git_env: GitEnv):
    ws, wt, _head = _worktree(git_env)
    (wt / "uncommitted.txt").write_text("wip\n")
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", "0" * 40)
    cleaner = WorkspaceCleaner(_Directory(parcel, snaps), ws, git_env.worktrees)  # type: ignore[arg-type]
    detail = await cleaner.cleanup(parcel.parcel_id, merged=False)
    assert detail["removed"] == [] and wt.exists()
    assert any("uncommitted" in s for s in detail["skipped"])
    assert any("not merged" in s for s in detail["skipped"])  # branch kept: not Ready head
    live = replace(parcel, sessions=(replace(parcel.sessions[-1], lifecycle=Lifecycle.ACTIVE),))
    with pytest.raises(NotSettled):
        await WorkspaceCleaner(_Directory(live, snaps), ws, git_env.worktrees).cleanup(  # type: ignore[arg-type]
            parcel.parcel_id, merged=True
        )
    outside = WorkspaceCleaner(_Directory(parcel, snaps), ws, git_env.worktrees / "elsewhere")  # type: ignore[arg-type]
    detail = await outside.cleanup(parcel.parcel_id, merged=True)
    assert detail["removed"] == [] and any("outside worktree_root" in s for s in detail["skipped"])
    merged = await cleaner.cleanup(parcel.parcel_id, merged=True)  # merged: forced, branch gone
    assert merged["removed"] and merged["branches_deleted"] == ["factory/issue-9"]


# ------------------------------------------------------------------ 7. CLI start-up


def test_cli_waits_for_the_operator_socket_after_a_restart(service_config, monkeypatch, capsys):
    attempts = []

    async def request(*_: Any, **__: Any) -> dict[str, Any]:
        attempts.append(1)
        if len(attempts) < 3:
            raise FileNotFoundError("no socket yet")
        return {"ok": True, "ready": True}

    monkeypatch.setattr(cli, "operator_request", request)
    monkeypatch.setattr(cli.time, "sleep", lambda _: None)
    assert cli._operator(service_config, "status") == 0
    out = capsys.readouterr()
    assert len(attempts) == 3 and '"ready": true' in out.out
    assert out.err.count("waiting for the daemon") == 1


# ------------------------------------------------------------------ 8. observer noise


@pytest.mark.asyncio
async def test_observer_skips_results_already_applied_before_a_restart(
    service_config: ServiceConfig, caplog
) -> None:
    harness = Harness()
    harness.eligible("P")
    harness.send("P", ev.RequestTriage(via=Via.DRAG))
    harness.create_ok("P")
    parcel = harness.p("P")
    events: list[Any] = []
    service = SimpleNamespace(
        config=service_config,
        apply_event=lambda event, **_: events.append(event),
        db=SimpleNamespace(call=_applied),
    )

    class Rest:
        async def paginate(self, *_: object) -> list[object]:
            return [
                {
                    "id": "old-result",
                    "status": "completed",
                    "data": {"role": "assistant", "content": "FACTORY_RESULT_V1\nnot-json"},
                }
            ]

    observer = OmnigentObserver(
        service,  # type: ignore[arg-type]
        SimpleNamespace(rest=Rest()),  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        FakeClock(),
        interval_seconds=1,
    )
    caplog.set_level(logging.WARNING)
    await observer._results(parcel)
    assert events == [] and "result rejected" not in caplog.text


async def _applied(operation: Any) -> Any:
    return True  # every result event already exists in the store


def test_merged_pr_or_closed_issue_schedules_workspace_cleanup():
    h = Harness()
    h.to_building()
    r = h.send(
        "I_parcel_1",
        ev.PRObserved(
            pr_number=682,
            head_sha=HEAD,
            open=False,
            merged=True,
            bot_authored=True,
            parcel_branch=True,
        ),
    )
    [cleanup] = Harness.of(r, EffectKind.CLEANUP_WORKSPACE)
    assert cleanup.args["merged"] is True
    h2 = Harness()
    h2.to_building()
    [closed] = Harness.of(h2.send("I_parcel_1", ev.Closed()), EffectKind.CLEANUP_WORKSPACE)
    assert closed.args["merged"] is False
