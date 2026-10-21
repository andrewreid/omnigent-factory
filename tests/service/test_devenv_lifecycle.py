"""Issue worktree and dev environment lifecycle: kept through Ready, torn down at Done.

The worktree (and any dev environment the agent left running in it) stays while the card
is Ready, so the owner can review it; a rework reuses it. Only a merged PR or a closed
issue removes it, after the repository's ``devenv_teardown`` command ran in it. A failed
teardown keeps the worktree and is retried (bounded, across restarts); a successful one
is never run twice. The Ready report shows the environment's URL only when the agent
reported one.
"""

from __future__ import annotations

import asyncio
import json
import logging
import stat
from dataclasses import replace
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core import protocol
from omnigent_factory.core.effects import (
    Ack,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    Preconditions,
    RetryableReadFailure,
)
from omnigent_factory.core.protocol import Correlation, ResultError, validate_result
from omnigent_factory.core.types import Hold, SessionKind, Stage
from omnigent_factory.service import directory as d
from omnigent_factory.service.cleanup import (
    TEARDOWN_DONE,
    TEARDOWN_FAILURES,
    CleanupAdapter,
    NotFinished,
    WorkspaceCleaner,
)
from omnigent_factory.service.config import ServiceConfig, load_config
from omnigent_factory.service.directory import _ready_text
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.service.setup import OperationsRenderer
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.harness import Harness
from tests.credentials.repos import GitEnv, git
from tests.service.test_followups import _Directory, _finished_parcel, _worktree, git_env
from tests.service.test_readiness_recovery import BUILD_READY, HEAD
from tests.test_merge_conflict import conflict, harness, ready, submit
from tests.test_protocol import build_result

__all__ = ["git_env"]  # the fixture, re-exported for this module

P = "I_parcel_1"
CTX = ExecutionContext("boot", 1, 1, 1)
TEMPLATES = files("omnigent_factory.service") / "templates"


def _cleanup_effects(h: Harness) -> list[EffectIntent]:
    return [e for _, r in h.log for e in r.effects if e.kind == EffectKind.CLEANUP_WORKSPACE]


def _script(tmp_path: Path, *, sleep: int = 0) -> tuple[Path, Path, Path]:
    """A teardown command outside the worktree: logs its cwd and argv, exits with the
    code in ``exit-code`` (0 when absent)."""
    log, code = tmp_path / "teardown.log", tmp_path / "exit-code"
    script = tmp_path / "teardown.sh"
    script.write_text(
        "#!/bin/sh\n"
        f'echo "$(pwd) $*" >> "{log}"\n'
        + (f"sleep {sleep}\n" if sleep else "")
        + f'if [ -f "{code}" ]; then echo "refused: env busy" >&2; exit "$(cat "{code}")"; fi\n'
        "exit 0\n"
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script, log, code


def _runs(log: Path) -> list[str]:
    return log.read_text().splitlines() if log.exists() else []


def _effect(parcel_id: str = P) -> EffectIntent:
    return EffectIntent(
        effect_id="ef_cleanup",
        kind=EffectKind.CLEANUP_WORKSPACE,
        parcel_id=parcel_id,
        target=parcel_id,
        preconditions=Preconditions(1, 0),
        args={"merged": False},
    )


# ------------------------------------------------------------------ 1. Ready keeps it


def test_ready_and_its_rework_never_schedule_cleanup_only_done_does():
    h = harness()
    ready(h)
    assert h.p().stage == Stage.READY and not _cleanup_effects(h)
    # A rework from Ready (a merge conflict here) runs in the same issue session.
    root = h.p().issue_session
    h.send(P, conflict(h))
    run = h.admit(P)
    assert run.kind == SessionKind.BUILD and h.p().issue_session == root
    submit(h)
    assert not _cleanup_effects(h)
    h.send(P, ev.Closed())
    [cleanup] = _cleanup_effects(h)
    assert cleanup.args["merged"] is False


def test_a_close_seen_only_by_a_fresh_read_schedules_cleanup():
    """The close webhook was lost: the issue read that finds it closed is Done too."""
    h = Harness()
    h.to_building()
    f = h.f(P)
    r = h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(open=False, read_at_us=f.now + 1)))
    assert Hold.COMPLETED in h.p().holds
    [cleanup] = Harness.of(r, EffectKind.CLEANUP_WORKSPACE)
    assert cleanup.args["merged"] is False


@pytest.mark.asyncio
async def test_a_ready_parcel_is_never_cleaned_up(git_env: GitEnv, tmp_path: Path):
    ws, wt, head = _worktree(git_env)
    done, snaps = _finished_parcel(wt, "factory/issue-9", head)
    ready_parcel = replace(done, holds=done.holds - {Hold.COMPLETED}, stage=Stage.READY)
    script, log, _ = _script(tmp_path)
    cleaner = WorkspaceCleaner(
        _Directory(ready_parcel, snaps),  # type: ignore[arg-type]
        ws,
        git_env.worktrees,
        teardown=(str(script),),
    )
    with pytest.raises(NotFinished):
        await cleaner.cleanup(ready_parcel.parcel_id, merged=False)
    assert wt.exists() and not _runs(log)
    # The effect of a parcel that is no longer finished acknowledges without removing.
    outcome = await CleanupAdapter(cleaner).execute(_effect(ready_parcel.parcel_id), CTX)
    assert isinstance(outcome, Ack) and wt.exists() and not _runs(log)


@pytest.mark.asyncio
async def test_operator_cleanup_refuses_a_ready_card(service_config: ServiceConfig):
    h = Harness()
    h.to_building()
    parcel = replace(h.p(), stage=Stage.READY)
    service = FactoryService(service_config)
    calls: list[str] = []

    async def call(_operation: Any) -> Any:
        return parcel

    async def cleanup(parcel_id: str, *, merged: bool) -> dict[str, Any]:
        calls.append(parcel_id)
        return {}

    service.db = SimpleNamespace(call=call)  # type: ignore[assignment]
    service.workspace_cleaner = SimpleNamespace(cleanup=cleanup)
    with pytest.raises(ValueError, match="not finished"):
        await service.operator_command("cleanup", {"parcel": parcel.parcel_id})
    assert not calls
    done = replace(parcel, holds=parcel.holds | {Hold.COMPLETED})

    async def call_done(_operation: Any) -> Any:
        return done

    service.db = SimpleNamespace(call=call_done)  # type: ignore[assignment]
    assert (await service.operator_command("cleanup", {"parcel": parcel.parcel_id}))["cleaned"]
    assert calls == [parcel.parcel_id]


# ------------------------------------------------------------------ 2. teardown at Done


@pytest.mark.asyncio
async def test_teardown_runs_in_the_worktree_before_it_is_removed(git_env: GitEnv):
    ws, wt, _ = _worktree(git_env)
    # A repository script (relative path, committed in the issue branch).
    (wt / "scripts").mkdir()
    log = git_env.runtime / "repo-teardown.log"
    (wt / "scripts" / "dev-env.sh").write_text(
        f'#!/bin/sh\n[ -d .git ] || [ -f .git ] || exit 9\necho "$(pwd) $*" >> "{log}"\n'
    )
    (wt / "scripts" / "dev-env.sh").chmod(0o755)
    git("add", "scripts", cwd=wt)
    git("commit", "-m", "script", cwd=wt)
    head = git("rev-parse", "HEAD", cwd=wt)
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    cleaner = WorkspaceCleaner(
        _Directory(parcel, snaps),  # type: ignore[arg-type]
        ws,
        git_env.worktrees,
        teardown=("scripts/dev-env.sh", "--teardown"),
    )
    detail = await cleaner.cleanup(parcel.parcel_id, merged=False)
    assert detail["removed"] == [str(wt.resolve())] and not wt.exists()
    assert _runs(log) == [f"{wt.resolve()} --teardown"]
    assert "teardown_pending" not in detail


@pytest.mark.asyncio
async def test_no_teardown_configured_or_nothing_to_tear_down_just_removes(
    git_env: GitEnv, tmp_path: Path
):
    """Absent key: nothing runs. A script with no environment exits 0: removal as usual."""
    ws, wt, head = _worktree(git_env)
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    script, log, _ = _script(tmp_path)
    cleaner = WorkspaceCleaner(_Directory(parcel, snaps), ws, git_env.worktrees)  # type: ignore[arg-type]
    assert (await cleaner.cleanup(parcel.parcel_id, merged=False))["removed"]
    assert not _runs(log)
    ws2, wt2, head2 = _worktree(git_env)  # the branch went with it (the Ready head)
    parcel2, snaps2 = _finished_parcel(wt2, "factory/issue-9", head2)
    cleaner = WorkspaceCleaner(
        _Directory(parcel2, snaps2),  # type: ignore[arg-type]
        ws2,
        git_env.worktrees,
        teardown=(str(script),),
    )
    assert (await cleaner.cleanup(parcel2.parcel_id, merged=False))["removed"]
    assert len(_runs(log)) == 1


@pytest.mark.asyncio
async def test_failed_teardown_keeps_the_worktree_and_retries_bounded_across_restarts(
    git_env: GitEnv, tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    ws, wt, head = _worktree(git_env)
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    script, log, code = _script(tmp_path)
    code.write_text("1")

    def adapter() -> CleanupAdapter:  # a fresh daemon each time: nothing in memory
        cleaner = WorkspaceCleaner(
            _Directory(parcel, snaps),  # type: ignore[arg-type]
            ws,
            git_env.worktrees,
            teardown=(str(script),),
        )
        return CleanupAdapter(cleaner, max_teardown_attempts=3)

    caplog.set_level(logging.WARNING, logger="omnigent_factory.service.cleanup")
    for attempt in (1, 2):
        outcome = await adapter().execute(_effect(), CTX)
        assert isinstance(outcome, RetryableReadFailure), attempt
        assert outcome.reason == "devenv-teardown-failed" and outcome.retry_after_us
        assert wt.exists() and git("branch", "--list", "factory/issue-9", cwd=git_env.source)
    assert "exit 1: refused: env busy" in caplog.text
    admin = ws.admin_dir(wt)
    assert (admin / TEARDOWN_FAILURES).read_text().strip() == "2"
    # The bound: the third failure stops retrying; the worktree is kept, never forced.
    outcome = await adapter().execute(_effect(), CTX)
    assert isinstance(outcome, Ack) and wt.exists()
    assert outcome.detail["teardown_failures"] == 3
    assert "teardown gave up" in caplog.text
    # By hand once the cause is fixed: torn down and removed.
    code.unlink()
    outcome = await adapter().execute(_effect(), CTX)
    assert isinstance(outcome, Ack) and not wt.exists()
    assert len(_runs(log)) == 4


@pytest.mark.asyncio
async def test_teardown_is_never_run_outside_the_worktree_root(git_env: GitEnv, tmp_path: Path):
    ws, wt, head = _worktree(git_env)
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    script, log, _ = _script(tmp_path)
    cleaner = WorkspaceCleaner(
        _Directory(parcel, snaps),  # type: ignore[arg-type]
        ws,
        git_env.worktrees / "elsewhere",
        teardown=(str(script),),
    )
    detail = await cleaner.cleanup(parcel.parcel_id, merged=True)
    assert any("outside worktree_root" in s for s in detail["skipped"])
    assert wt.exists() and not _runs(log)


@pytest.mark.asyncio
async def test_a_hung_teardown_is_killed_at_its_timeout(git_env: GitEnv, tmp_path: Path):
    ws, wt, head = _worktree(git_env)
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    script, _, _ = _script(tmp_path, sleep=30)
    cleaner = WorkspaceCleaner(
        _Directory(parcel, snaps),  # type: ignore[arg-type]
        ws,
        git_env.worktrees,
        teardown=(str(script),),
        teardown_timeout_seconds=0.5,
    )
    detail = await asyncio.wait_for(cleaner.cleanup(parcel.parcel_id, merged=True), 10)
    assert detail["teardown_pending"] == [str(wt.resolve())] and wt.exists()
    assert any("timed out after 0.5s" in s for s in detail["skipped"])


# ------------------------------------------------------------------ 5. restart safety


@pytest.mark.asyncio
async def test_a_teardown_that_succeeded_before_a_restart_is_not_run_again(
    git_env: GitEnv, tmp_path: Path
):
    ws, wt, head = _worktree(git_env)
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    script, log, _ = _script(tmp_path)

    def cleaner() -> WorkspaceCleaner:
        return WorkspaceCleaner(
            _Directory(parcel, snaps),  # type: ignore[arg-type]
            ws,
            git_env.worktrees,
            teardown=(str(script),),
        )

    # The daemon died after the teardown, before the worktree was removed.
    assert cleaner()._teardown(wt.resolve()) is None
    assert (ws.admin_dir(wt) / TEARDOWN_DONE).exists() and len(_runs(log)) == 1
    detail = await cleaner().cleanup(parcel.parcel_id, merged=False)  # the retried effect
    assert detail["removed"] and not wt.exists() and len(_runs(log)) == 1


@pytest.mark.asyncio
async def test_concurrent_cleanups_tear_down_once(git_env: GitEnv, tmp_path: Path):
    """The effect and the operator's ``cleanup`` at once (or two close events)."""
    ws, wt, head = _worktree(git_env)
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    script, log, _ = _script(tmp_path)
    cleaner = WorkspaceCleaner(
        _Directory(parcel, snaps),  # type: ignore[arg-type]
        ws,
        git_env.worktrees,
        teardown=(str(script),),
    )
    first, second = await asyncio.gather(
        cleaner.cleanup(parcel.parcel_id, merged=True),
        cleaner.cleanup(parcel.parcel_id, merged=True),
    )
    assert len(_runs(log)) == 1 and not wt.exists()
    assert [*first["removed"], *second["removed"]] == [str(wt.resolve())]


@pytest.mark.asyncio
async def test_a_kept_dirty_worktree_is_torn_down_again_by_a_later_cleanup(
    git_env: GitEnv, tmp_path: Path
):
    """Closed unmerged with uncommitted work: kept, so its torn-down mark is dropped (an
    environment may be started in it again before the next cleanup)."""
    ws, wt, head = _worktree(git_env)
    (wt / "wip.txt").write_text("wip\n")
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    script, log, _ = _script(tmp_path)
    cleaner = WorkspaceCleaner(
        _Directory(parcel, snaps),  # type: ignore[arg-type]
        ws,
        git_env.worktrees,
        teardown=(str(script),),
    )
    detail = await cleaner.cleanup(parcel.parcel_id, merged=False)
    assert wt.exists() and any("uncommitted" in s for s in detail["skipped"])
    assert not (ws.admin_dir(wt) / TEARDOWN_DONE).exists()
    await cleaner.cleanup(parcel.parcel_id, merged=True)
    assert not wt.exists() and len(_runs(log)) == 2


# ------------------------------------------------------------------ 4. the URL


def _build(**over: Any) -> dict[str, Any]:
    return {**BUILD_READY, **over}


BUILD = Correlation("P1", "S1", "N1", 3, "build")


def test_build_ready_may_report_a_dev_environment_url():
    url = "https://quiet-river-1234.trycloudflare.com/login"
    parsed = validate_result(build_result(dev_environment_url=url), BUILD)
    assert parsed.result.result.dev_environment_url == url
    assert validate_result(build_result(), BUILD).result.result.dev_environment_url is None
    for bad in (
        "ftp://example.com",
        "javascript:alert(1)",
        "https://user:secret@example.com",
        "https://example.com/a b",
        "https://example.com/<script>",
        "https://" + "a" * 500,
        "",
        7,
    ):
        with pytest.raises(ResultError):
            validate_result(build_result(dev_environment_url=bad), BUILD)
    assert "dev_environment_url" in protocol.RESULT_SHAPES["build_ready"]


def test_schema_file_declares_dev_environment_url():
    schema = json.loads(
        (Path(protocol.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    build = schema["$defs"]["build"]
    field = build["properties"]["dev_environment_url"]
    assert "dev_environment_url" not in build["required"]
    assert field == {
        "type": "string",
        "maxLength": 500,
        "pattern": protocol.DEV_ENVIRONMENT_URL_PATTERN,
    }


def test_ready_report_shows_the_dev_environment_only_when_reported():
    args = {"pr_number": 682, "head_sha": HEAD}
    repo = "SA-Ambulance/timesheets"
    url = "https://quiet-river-1234.trycloudflare.com"
    text = _ready_text(args, _build(dev_environment_url=url), "", repo)
    [line] = [x for x in text.splitlines() if "Dev environment" in x]
    assert line == f"**Dev environment:** {url}"
    # After the PR line: a factory line, kept current with the others.
    assert text.index("**PR:**") < text.index(line)
    assert "Dev environment" not in _ready_text(args, _build(), "", repo)
    assert "Dev environment" not in _ready_text(args, None, "", repo)
    bad = _build(dev_environment_url="https://x.example/)](javascript:alert(1)")
    assert "Dev environment" not in _ready_text(args, bad, "", repo)


def test_build_rework_and_readiness_templates_keep_a_started_dev_environment():
    current = (
        d._FIRST_TEMPLATES[SessionKind.BUILD],
        d._REWORK_TEMPLATE,
        d._ITEMS_REWORK_TEMPLATE,
        d._CONFLICT_TEMPLATE,
        d._READINESS_WAKE_TEMPLATE,
        d._FINDINGS_WAKE_TEMPLATE,
    )
    for name in current:
        text = " ".join((TEMPLATES / name).read_text("utf-8").split())
        assert "if you started one, keep it running" in text, name
        assert "dev_environment_url" in text, name
        assert "the factory tears it down when the issue is done" in text, name
        assert "never required" in text, name
    for name in current[:4]:
        text = " ".join((TEMPLATES / name).read_text("utf-8").split())
        assert "do not tear it down when the PR opens" in text, name
    # In-flight runs stay pinned to the template bytes they were dispatched with.
    for old in (
        "build-v14.txt",
        "build-rework-v11.txt",
        "build-rework-items-v2.txt",
        "build-conflict-v2.txt",
        "readiness-wake-v11.txt",
        "readiness-findings-wake-v3.txt",
    ):
        assert "dev_environment_url" not in (TEMPLATES / old).read_text("utf-8"), old


# ------------------------------------------------------------------ 6. config


def test_devenv_teardown_is_host_config_and_off_by_default(
    service_config: ServiceConfig, tmp_path: Path
):
    assert service_config.devenv_teardown == ()
    path = tmp_path / "config.toml"
    path.write_text(
        "[service]\n"
        'repo_id = "R_1"\n'
        "owners = [1]\n"
        'devenv_teardown = ["scripts/dev-environment.sh", "--teardown"]\n'
        "devenv_teardown_timeout_seconds = 120\n"
    )
    config = load_config(path)
    assert config.devenv_teardown == ("scripts/dev-environment.sh", "--teardown")
    assert config.devenv_teardown_timeout_seconds == 120
    assert config.devenv_teardown_max_attempts == 5
    with pytest.raises(ValueError, match="devenv_teardown"):
        service_config.model_validate({**service_config.model_dump(), "devenv_teardown": [""]})
    sample = OperationsRenderer(tmp_path / "config.toml").render(service_config)[
        "config.example.toml"
    ]
    assert "# devenv_teardown = [" in sample and "devenv_teardown_timeout_seconds" in sample
