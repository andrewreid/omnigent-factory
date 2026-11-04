"""The optional worktree prepare hook (``worktree_prepare``).

One state machine per materialised issue worktree ({absent, running, ok(fingerprint),
failed, timeout, interrupted}, in the worktree's own Git directory); the command runs as
a polled task (never awaited under the parcel serializer), in an isolated Git context,
never for TRIAGE or in a reference snapshot, never fatal, no clone lock, process group
killed on timeout or cancellation.
"""

from __future__ import annotations

import asyncio
import errno
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field, replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from omnigent_factory.core.effects import Ack, EffectKind, RetryableReadFailure
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.types import Lifecycle, SessionKind
from omnigent_factory.credentials import worktree_prepare as wp
from omnigent_factory.credentials.worktree import CLONE_LOCK, REFERENCE_META
from omnigent_factory.credentials.worktree_prepare import (
    DEFAULT_TIMEOUT_SECONDS,
    PENDING,
    PREPARE_RECORD,
    PrepareOutcome,
    WorktreePreparer,
    log_path,
    prepare_env,
    prepare_summary,
    read_record,
    resolve_argv,
)
from omnigent_factory.service.cleanup import WorkspaceCleaner
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.doctor import DoctorReport, _check_worktree_prepare
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID
from tests.credentials.repos import OWNER_TOKEN, GitEnv, git
from tests.omnigent.support import CTX, Directory, Rig, intent, make_rig, spec

BRANCH = "factory/issue-42"


def _config(**update: Any) -> ServiceConfig:
    return ServiceConfig(repo_id=REPO_ID, owners=frozenset({OWNER_ID}), **update)


def _script(path: Path, body: str) -> Path:
    path.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def _counter(tmp_path: Path, name: str = "runs", extra: str = "") -> tuple[Path, Path]:
    """A prepare script that appends its working directory to a file, and that file."""
    runs = tmp_path / name
    script = _script(tmp_path / f"{name}.sh", f'pwd >> "{runs}"\necho installed\n{extra}')
    return script, runs


def _lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


async def _settle(preparer: WorktreePreparer, path: Path, **kw: Any) -> PrepareOutcome | None:
    """Poll as the stage preparation does, until the state settles."""
    for _ in range(1500):
        result = await preparer.poll(path, **kw)
        if result is not PENDING:
            assert result is None or isinstance(result, PrepareOutcome)
            return result
        await asyncio.sleep(0.02)
    raise AssertionError("prepare never settled")


async def _until(check: Any, seconds: float = 10) -> None:
    deadline = time.monotonic() + seconds
    while not check():
        assert time.monotonic() < deadline, "condition never held"
        await asyncio.sleep(0.02)


# ------------------------------------------------------------------ config


def test_config_is_off_by_default_and_validated_like_the_teardown() -> None:
    cfg = _config()
    assert cfg.worktree_prepare == ()
    assert cfg.worktree_prepare_timeout_seconds == DEFAULT_TIMEOUT_SECONDS == 600
    assert cfg.worktree_prepare_inputs == ("pnpm-lock.yaml",)
    good = _config(worktree_prepare=["scripts/factory-prepare.sh", "--quiet"])
    assert good.worktree_prepare == ("scripts/factory-prepare.sh", "--quiet")
    assert _config(worktree_prepare=[]).worktree_prepare == ()
    for bad in ([" "], [""], ["scripts/x.sh", "a\0b"]):
        with pytest.raises(ValidationError, match="worktree_prepare must name a program"):
            _config(worktree_prepare=bad)
    for timeout in (0, -1, 3601):
        with pytest.raises(ValidationError):
            _config(worktree_prepare_timeout_seconds=timeout)
    assert _config(worktree_prepare_timeout_seconds=3600).worktree_prepare_timeout_seconds == 3600
    inputs = ["pnpm-lock.yaml", "apps/api/prisma/schema.prisma", "packages/export-renderer"]
    assert _config(worktree_prepare_inputs=inputs).worktree_prepare_inputs == tuple(inputs)
    for bad in (["/etc/passwd"], ["../x"], ["a/../../b"], [""], ["-x"]):
        with pytest.raises(ValidationError, match="relative paths inside the worktree"):
            _config(worktree_prepare_inputs=bad)


def test_a_relative_program_path_resolves_in_the_worktree(tmp_path: Path) -> None:
    worktree = tmp_path / "factory-issue-7"
    assert resolve_argv(["scripts/p.sh", "x"], worktree) == [str(worktree / "scripts/p.sh"), "x"]
    assert resolve_argv(["/usr/bin/env", "bash", "scripts/p.sh"], worktree) == [
        "/usr/bin/env",
        "bash",
        "scripts/p.sh",
    ]
    assert resolve_argv(["pnpm", "install"], worktree) == ["pnpm", "install"]  # on PATH
    assert log_path(worktree) == tmp_path / "factory-issue-7-prepare.log"


def test_the_environment_is_an_allowlist_with_an_isolated_git_context(tmp_path: Path) -> None:
    env = prepare_env(
        {
            "PATH": "/bin",
            "HOME": "/home/x",
            "LC_ALL": "C.UTF-8",
            "XDG_CACHE_HOME": "/c",
            "GH_TOKEN": "ghs_secret",
            "GITHUB_TOKEN": "x",
            "OMNIGENT_TOKEN": "x",
            "GIT_CONFIG_GLOBAL": "/evil",
            "GIT_CONFIG_PARAMETERS": "'credential.helper'='evil'",
            "NPM_CONFIG__AUTH": "x",
            "SSH_AUTH_SOCK": "/s",
        },
        gh_config_dir=tmp_path,
    )
    assert env == {
        "PATH": "/bin",
        "HOME": "/home/x",
        "LC_ALL": "C.UTF-8",
        "XDG_CACHE_HOME": "/c",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_COUNT": "5",
        "GIT_CONFIG_KEY_0": "credential.helper",
        "GIT_CONFIG_VALUE_0": "",
        "GIT_CONFIG_KEY_1": "credential.https://github.com.helper",
        "GIT_CONFIG_VALUE_1": "",
        "GIT_CONFIG_KEY_2": "credential.interactive",
        "GIT_CONFIG_VALUE_2": "false",
        "GIT_CONFIG_KEY_3": "http.extraHeader",
        "GIT_CONFIG_VALUE_3": "",
        "GIT_CONFIG_KEY_4": "http.https://github.com/.extraHeader",
        "GIT_CONFIG_VALUE_4": "",
        "GH_CONFIG_DIR": str(tmp_path),
    }


# ------------------------------------------------------------------ the runner


def _preparer(argv: list[str], admin: Path, timeout: float = 30, **kw: Any) -> WorktreePreparer:
    admin.mkdir(parents=True, exist_ok=True)
    return WorktreePreparer(argv, timeout, admin_dir=lambda _path: admin, **kw)


def _worktree(tmp_path: Path, n: int) -> Path:
    worktree = tmp_path / f"factory-issue-{n}"
    worktree.mkdir(parents=True, exist_ok=True)
    return worktree


@pytest.mark.asyncio
async def test_runs_once_in_the_worktree_and_logs_its_output(tmp_path: Path) -> None:
    worktree = _worktree(tmp_path, 7)
    (worktree / "scripts").mkdir()
    runs = tmp_path / "runs"
    _script(worktree / "scripts/p.sh", f'pwd >> "{runs}"\necho installed; echo warn >&2')
    preparer = _preparer(["scripts/p.sh"], tmp_path / "admin")
    assert await preparer.poll(worktree) is PENDING  # started: the caller is not held
    first = await _settle(preparer, worktree)
    assert first is not None and first.ok and first.line_shown
    assert _lines(runs) == [str(worktree)]  # cwd = the worktree; relative path resolved
    log = tmp_path / "factory-issue-7-prepare.log"
    assert first.log == str(log)
    assert "installed" in log.read_text() and "warn" in log.read_text()
    assert oct(log.stat().st_mode & 0o777) == "0o600"
    again = await preparer.poll(worktree)  # the same materialisation: never again
    assert isinstance(again, PrepareOutcome) and again.ok and not again.ran
    restarted = _preparer(["scripts/p.sh"], tmp_path / "admin")  # a daemon restart
    assert (await restarted.poll(worktree)).ran is False  # type: ignore[union-attr]
    assert _lines(runs) == [str(worktree)]


@pytest.mark.asyncio
async def test_the_environment_reaches_the_command_without_daemon_secrets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GH_TOKEN", "ghs_should_not_leak")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/nowhere")
    dump = tmp_path / "env"
    script = _script(tmp_path / "p.sh", f'env > "{dump}"; ls -A "$GH_CONFIG_DIR" >> "{dump}"')
    worktree = _worktree(tmp_path, 8)
    outcome = await _settle(_preparer([str(script)], tmp_path / "admin"), worktree)
    assert outcome is not None and outcome.ok
    text = dump.read_text()
    assert "ghs_should_not_leak" not in text and "/nowhere" not in text
    assert f"PATH={os.environ['PATH']}" in text and "GIT_TERMINAL_PROMPT=0" in text
    assert f"GIT_CONFIG_GLOBAL={os.devnull}" in text
    assert f"GH_CONFIG_DIR={tmp_path / 'admin' / 'omnigent-factory-prepare-gh'}" in text
    assert text.endswith(text.splitlines()[-1] + "\n")  # nothing listed in GH_CONFIG_DIR
    assert not os.listdir(tmp_path / "admin" / "omnigent-factory-prepare-gh")


@pytest.mark.asyncio
async def test_a_failure_is_recorded_and_warned_never_raised(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    runs = tmp_path / "runs"
    script = _script(tmp_path / "p.sh", f'pwd >> "{runs}"\necho "lockfile mismatch" >&2\nexit 3')
    worktree = _worktree(tmp_path, 9)
    preparer = _preparer([str(script)], tmp_path / "admin")
    with caplog.at_level(logging.WARNING):
        outcome = await _settle(preparer, worktree)
    assert outcome is not None and outcome.status == "failed" and not outcome.line_shown
    assert outcome.reason == "exit 3: lockfile mismatch"
    assert any("worktree prepare failed" in r.getMessage() for r in caplog.records)
    assert read_record(tmp_path / "admin") == replace(outcome, ran=False)
    again = await preparer.poll(worktree)  # a failure is not retried for this worktree
    assert isinstance(again, PrepareOutcome) and again.status == "failed" and not again.ran
    assert len(_lines(runs)) == 1
    missing = await _settle(_preparer(["/no/such/prepare"], tmp_path / "admin2"), worktree)
    assert missing is not None and missing.status == "failed"
    assert missing.reason.startswith("could not start")


@pytest.mark.asyncio
async def test_a_timeout_kills_the_whole_process_group(tmp_path: Path) -> None:
    child = tmp_path / "child.pid"
    script = _script(tmp_path / "p.sh", f'sleep 60 &\necho $! > "{child}"\nsleep 60')
    worktree = _worktree(tmp_path, 10)
    began = time.monotonic()
    outcome = await _settle(_preparer([str(script)], tmp_path / "admin", timeout=0.5), worktree)
    assert time.monotonic() - began < 10
    assert outcome is not None and outcome.status == "timeout"
    assert outcome.reason == "timed out after 0.5s"
    _assert_dead(int(child.read_text()))


def _assert_dead(pid: int) -> None:
    for _ in range(200):  # reaped by init shortly after the SIGKILL
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.02)
    with open(f"/proc/{pid}/stat") as stat:  # a zombie awaiting its reaper is dead too
        assert stat.read().rsplit(")", 1)[1].split()[0] == "Z"


@pytest.mark.asyncio
async def test_reference_snapshots_are_never_prepared(tmp_path: Path) -> None:
    script, runs = _counter(tmp_path)
    ref = tmp_path / "factory-ref-0123456789ab"
    ref.mkdir()
    preparer = _preparer([str(script)], tmp_path / "admin")
    assert await preparer.poll(ref) is None
    assert await preparer.poll(tmp_path / "factory-ref-anything") is None
    # A snapshot's own metadata marks it too, whatever its directory is called.
    other = _worktree(tmp_path, 11)
    (tmp_path / "admin" / REFERENCE_META).write_text("{}")
    assert await preparer.poll(other) is None
    assert not runs.exists() and not (tmp_path / "admin" / PREPARE_RECORD).exists()
    assert not log_path(ref).exists()


@pytest.mark.asyncio
async def test_concurrent_prepares_overlap_and_hold_no_clone_lock(tmp_path: Path) -> None:
    """Each script waits for the other's start marker: they only finish if both run at
    once. Meanwhile another thread takes the clone lock (no prepare holds it)."""
    gate = tmp_path / "gate"
    worktrees: list[Path] = []
    for n in (21, 22):
        other = 22 if n == 21 else 21
        wt = _worktree(tmp_path, n)
        worktrees.append(wt)
        _script(
            wt / "p.sh",
            f'touch "{tmp_path}/started-{n}"\n'
            f'i=0; while [ ! -e "{tmp_path}/started-{other}" ] || [ ! -e "{gate}" ]; do\n'
            f"  i=$((i+1)); [ $i -gt 500 ] && exit 9; sleep 0.02\ndone",
        )
        (wt / ".admin").mkdir()
    preparers = [
        WorktreePreparer([str(wt / "p.sh")], 30, admin_dir=lambda p: p / ".admin")
        for wt in worktrees
    ]
    for p, wt in zip(preparers, worktrees, strict=True):
        assert await p.poll(wt) is PENDING
    await _until(lambda: all((tmp_path / f"started-{n}").exists() for n in (21, 22)))

    def take_clone_lock() -> bool:
        if not CLONE_LOCK.acquire(timeout=1):
            return False
        CLONE_LOCK.release()
        return True

    assert await asyncio.to_thread(take_clone_lock), "a prepare holds the clone lock"
    gate.touch()
    outcomes = [await _settle(p, wt) for p, wt in zip(preparers, worktrees, strict=True)]
    assert [o.status for o in outcomes if o is not None] == ["ok", "ok"]


# ------------------------------------------------------------------ the log boundary (F4)


@pytest.mark.asyncio
async def test_a_fifo_at_the_log_path_fails_fast_without_running(tmp_path: Path) -> None:
    script, runs = _counter(tmp_path)
    worktree = _worktree(tmp_path, 30)
    os.mkfifo(log_path(worktree))
    began = time.monotonic()
    outcome = await _settle(_preparer([str(script)], tmp_path / "admin"), worktree)
    assert time.monotonic() - began < 5  # never blocked opening it
    assert outcome is not None and outcome.status == "failed"
    assert outcome.reason.startswith("log: ")
    assert not runs.exists()
    # A FIFO with a reader opens without blocking too, and is refused as not regular.
    fifo = log_path(_worktree(tmp_path, 31))
    os.mkfifo(fifo)
    reader = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
    try:
        refused = await _settle(
            _preparer([str(script)], tmp_path / "admin31"), fifo.parent / "factory-issue-31"
        )
    finally:
        os.close(reader)
    assert refused is not None and refused.status == "failed"
    assert refused.reason == "log: not a regular file"
    assert not runs.exists()


@pytest.mark.asyncio
async def test_a_pre_existing_world_readable_log_is_forced_to_0600(tmp_path: Path) -> None:
    script, _ = _counter(tmp_path)
    worktree = _worktree(tmp_path, 32)
    log = log_path(worktree)
    log.write_text("old secret output\n" * 100)
    log.chmod(0o644)
    outcome = await _settle(_preparer([str(script)], tmp_path / "admin"), worktree)
    assert outcome is not None and outcome.ok
    assert oct(log.stat().st_mode & 0o777) == "0o600"
    assert "old secret output" not in log.read_text()  # truncated


@pytest.mark.asyncio
async def test_a_full_disk_writing_the_log_is_a_recorded_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script, runs = _counter(tmp_path)
    worktree = _worktree(tmp_path, 33)

    def full(fd: int, data: bytes) -> None:
        raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

    monkeypatch.setattr(wp, "_write", full)
    outcome = await _settle(_preparer([str(script)], tmp_path / "admin"), worktree)
    assert outcome is not None and outcome.status == "failed"
    assert outcome.reason == f"log: {os.strerror(errno.ENOSPC)}"
    assert read_record(tmp_path / "admin").status == "failed"  # type: ignore[union-attr]
    assert not runs.exists()


# ------------------------------------------------------------------ attempts (F6)


@pytest.mark.asyncio
async def test_a_cancelled_run_is_recorded_interrupted_and_retried_once(tmp_path: Path) -> None:
    child = tmp_path / "child.pid"
    script, runs = _counter(tmp_path, extra=f'sleep 60 &\necho $! > "{child}"\nwait')
    worktree = _worktree(tmp_path, 40)
    preparer = _preparer([str(script)], tmp_path / "admin", wanted_poll_seconds=0.05)
    wanted = True

    async def still_wanted() -> bool:
        return wanted

    assert await preparer.poll(worktree, still_wanted=still_wanted) is PENDING
    await _until(child.exists)
    assert preparer.running(worktree)
    assert read_record(tmp_path / "admin").status == "running"  # type: ignore[union-attr]
    wanted = False  # the owner stopped the run
    await _until(lambda: not preparer.running(worktree))
    _assert_dead(int(child.read_text()))
    record = read_record(tmp_path / "admin")
    assert record is not None and record.status == "interrupted"
    assert record.reason == "cancelled: the run is no longer starting"
    summary = prepare_summary_admin(tmp_path / "admin", worktree)
    assert summary == [("interrupted", "cancelled: the run is no longer starting")]
    # The next start retries it once ...
    child.unlink()
    wanted = True
    assert await preparer.poll(worktree, still_wanted=still_wanted) is PENDING
    await _until(child.exists)
    wanted = False
    await _until(lambda: not preparer.running(worktree))
    assert len(_lines(runs)) == 2
    # ... and an interruption of the retry stays visible, never rerun silently.
    again = await preparer.poll(worktree, still_wanted=still_wanted)
    assert isinstance(again, PrepareOutcome) and again.status == "interrupted"
    assert again.retries == 1 and not again.line_shown
    assert len(_lines(runs)) == 2


def prepare_summary_admin(admin: Path, worktree: Path) -> list[tuple[str, str]]:
    """``prepare_summary`` of one fake clone holding ``admin``."""
    clone = admin.parent / "clone"
    target = clone / ".git" / "worktrees" / worktree.name
    target.mkdir(parents=True, exist_ok=True)
    (target / PREPARE_RECORD).write_text((admin / PREPARE_RECORD).read_text())
    summary = prepare_summary(clone, configured=True)
    return [(str(r["status"]), str(r["reason"])) for r in summary["failed"]]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_a_daemon_that_died_mid_run_is_recovered_as_interrupted(tmp_path: Path) -> None:
    script, runs = _counter(tmp_path)
    worktree = _worktree(tmp_path, 41)
    admin = tmp_path / "admin"
    admin.mkdir()
    orphan = await asyncio.create_subprocess_exec("sleep", "60", start_new_session=True)
    dead = PrepareOutcome(
        "running",
        str(worktree),
        log=str(log_path(worktree)),
        pid=orphan.pid,
        pid_start=wp._proc_start(orphan.pid),
    )
    (admin / PREPARE_RECORD).write_text(json.dumps(dead.to_json()))
    assert prepare_summary_admin(admin, worktree) == []  # its process is alive: running
    restarted = _preparer([str(script)], admin)
    assert await restarted.poll(worktree) is PENDING  # recovered, then retried once
    assert await asyncio.wait_for(orphan.wait(), 5) == -9  # the orphaned group was killed
    outcome = await _settle(restarted, worktree)
    assert outcome is not None and outcome.ok and outcome.retries == 1
    assert len(_lines(runs)) == 1


@pytest.mark.asyncio
async def test_daemon_shutdown_kills_and_records_running_commands(tmp_path: Path) -> None:
    child = tmp_path / "child.pid"
    script = _script(tmp_path / "p.sh", f'sleep 60 &\necho $! > "{child}"\nwait')
    worktree = _worktree(tmp_path, 42)
    preparer = _preparer([str(script)], tmp_path / "admin")
    assert await preparer.poll(worktree) is PENDING
    await _until(child.exists)
    await preparer.close()
    _assert_dead(int(child.read_text()))
    record = read_record(tmp_path / "admin")
    assert record is not None and record.status == "interrupted"
    assert record.reason == "cancelled (daemon stopping)"


@pytest.mark.asyncio
async def test_an_unwritable_outcome_is_never_treated_as_prepared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    script, _ = _counter(tmp_path)
    worktree = _worktree(tmp_path, 43)
    real = wp._write_record

    def final_write_fails(admin: Path, outcome: PrepareOutcome) -> None:
        if outcome.status != "running":
            raise OSError(errno.ENOSPC, "no space")
        real(admin, outcome)

    monkeypatch.setattr(wp, "_write_record", final_write_fails)
    with caplog.at_level(logging.WARNING):
        outcome = await _settle(_preparer([str(script)], tmp_path / "admin"), worktree)
    assert outcome is not None and outcome.status == "ok" and not outcome.line_shown
    assert any("outcome not recorded" in r.getMessage() for r in caplog.records)
    # Nothing marked it prepared on disk: still the attempt marker (recovered later).
    assert read_record(tmp_path / "admin").status == "running"  # type: ignore[union-attr]
    unstarted = _preparer([str(script)], tmp_path / "admin2")
    monkeypatch.setattr(wp, "_write_record", lambda *_: (_ for _ in ()).throw(OSError("ro")))
    refused = await _settle(unstarted, worktree)
    assert refused is not None and refused.status == "failed"
    assert refused.reason.startswith("could not record attempt")


def test_summary_and_doctor_show_failed_prepares(tmp_path: Path) -> None:
    clone = tmp_path / "clone"
    admins = clone / ".git" / "worktrees"
    rows = {
        "factory-issue-1": {"status": "ok"},
        "factory-issue-2": {"status": "failed", "reason": "exit 1: boom"},
        "factory-issue-3": {"status": "timeout", "reason": "timed out after 600s"},
        "factory-issue-4": {"status": "running", "pid": 2**22 + 7, "pid_start": "1"},
    }
    for name, partial in rows.items():
        wt = tmp_path / name
        wt.mkdir()
        (admins / name).mkdir(parents=True)
        data = {**partial, "path": str(wt), "log": str(log_path(wt))}
        (admins / name / PREPARE_RECORD).write_text(json.dumps(data))
    (admins / "gone").mkdir()
    (admins / "gone" / PREPARE_RECORD).write_text(f'{{"status": "failed", "path": "{tmp_path}/x"}}')
    (admins / "never").mkdir()
    summary = prepare_summary(clone, configured=True)
    assert summary["configured"] is True and summary["ok"] == 1 and summary["running"] == []
    assert [(r["status"], Path(str(r["path"])).name) for r in summary["failed"]] == [  # type: ignore[attr-defined]
        ("failed", "factory-issue-2"),
        ("timeout", "factory-issue-3"),
        ("interrupted", "factory-issue-4"),  # no live process: the daemon died mid-run
    ]
    report = DoctorReport()
    _check_worktree_prepare(_config(source_clone=clone, worktree_prepare=["p.sh"]), report)
    detail = report.checks["worktree_prepare"]
    assert detail.startswith(
        "warning: configured (p.sh); 1 prepared, 0 running, 3 failed/timed out/interrupted"
    )
    assert "factory-issue-2: failed exit 1: boom" in detail
    assert f"log {log_path(tmp_path / 'factory-issue-2')}" in detail
    assert report.ok  # advisory only
    quiet = DoctorReport()
    _check_worktree_prepare(_config(source_clone=tmp_path / "none"), quiet)
    assert quiet.checks["worktree_prepare"] == (
        "off; 0 prepared, 0 running, 0 failed/timed out/interrupted"
    )


# ------------------------------------------------------------------ the adapter


@dataclass
class _PrepDirectory(Directory):
    """The rig directory plus the per-run prepare record, the base sync (as the
    dispatch snapshot) and whether the run is still preparing."""

    prepared: dict[str, dict[str, Any]] = field(default_factory=dict)
    syncs: dict[str, dict[str, Any]] = field(default_factory=dict)
    stopped: set[str] = field(default_factory=set)

    async def worktree_prepare(self, session_id: str) -> dict[str, Any] | None:
        return self.prepared.get(session_id)

    async def record_worktree_prepare(self, session_id: str, record: Any) -> None:
        self.prepared[session_id] = dict(record)  # refreshed until the first message

    async def run_preparing(self, session_id: str) -> bool:
        return session_id not in self.stopped

    async def base_sync(self, session_id: str) -> dict[str, Any] | None:
        return self.syncs.get(session_id)

    async def record_base_sync(self, session_id: str, record: Any) -> None:
        self.syncs.setdefault(session_id, dict(record))


def _rig(git_env: GitEnv, argv: list[str], timeout: float = 30) -> Rig:
    rig = make_rig(git_env)
    directory = _PrepDirectory()
    rig.directory = directory
    rig.adapter.directory = directory
    rig.adapter.preparer = WorktreePreparer(
        argv, timeout, admin_dir=git_env.workspaces().admin_dir, wanted_poll_seconds=0.05
    )
    return rig


def _prepared(rig: Rig) -> dict[str, dict[str, Any]]:
    assert isinstance(rig.directory, _PrepDirectory)
    return rig.directory.prepared


async def _create(rig: Rig, sid: str, kind: SessionKind, **kw: Any) -> str:
    rig.directory.specs[sid] = spec(sid, kind=kind, branch=BRANCH, nonce=f"nonce-{sid}", **kw)
    out = await rig.adapter.execute(
        intent(EffectKind.CREATE_SESSION, sid, nonce=f"nonce-{sid}"), CTX
    )
    assert isinstance(out, Ack) and out.remote_id is not None, out
    rig.directory.set_root(sid, out.remote_id)
    return out.remote_id


async def _prep(rig: Rig, sid: str, root: str, *, ok: bool = True, **args: Any) -> Ack:
    """PREPARE_SESSION as the executor retries it: waiting polls return promptly."""
    effect = intent(EffectKind.PREPARE_SESSION, sid, root_id=root, **args)
    for _ in range(1500):
        began = time.monotonic()
        out = await rig.adapter.execute(effect, CTX)
        assert time.monotonic() - began < 5  # never held while the command runs
        if isinstance(out, RetryableReadFailure) and out.reason.startswith("waiting:"):
            await asyncio.sleep(0.02)
            continue
        assert isinstance(out, Ack) and out.detail["ok"] is ok, out
        return out
    raise AssertionError("preparation never finished")


def _reuse(rig: Rig, sid: str, kind: SessionKind, *, root: str, root_nonce: str, gen: int) -> None:
    rig.directory.specs[sid] = replace(
        spec(sid, kind=kind, branch=BRANCH, nonce=f"nonce-{sid}", policy_generation=gen),
        root_id=root,
        root_nonce=root_nonce,
    )


def _worktree_of(git_env: GitEnv) -> Path:
    return git_env.worktrees.resolve() / "factory-issue-42"


def _commit_main(git_env: GitEnv, name: str, content: str) -> str:
    seed = git_env.remote.parent / "seed"
    git("pull", "-q", "origin", "main", cwd=seed)
    (seed / name).write_text(content, encoding="utf-8")
    git("add", name, cwd=seed)
    git("commit", "-q", "-m", f"change {name}", cwd=seed)
    git("push", "-q", "origin", "HEAD:main", cwd=seed)
    return git("rev-parse", "HEAD", cwd=seed)


@pytest.mark.asyncio
async def test_prepared_once_when_created_and_again_after_a_recreate(
    git_env: GitEnv, tmp_path: Path
) -> None:
    script, runs = _counter(tmp_path)
    rig = _rig(git_env, [str(script)])
    # TRIAGE creates the issue worktree: never prepared for a triage run.
    triage_root = await _create(rig, "T1", SessionKind.TRIAGE)
    await _prep(rig, "T1", triage_root)
    assert not runs.exists() and "T1" not in _prepared(rig)
    # The first PLAN run in it prepares it, before the run's first message.
    _reuse(rig, "P1", SessionKind.PLAN, root=triage_root, root_nonce="nonce-T1", gen=2)
    await _prep(rig, "P1", triage_root, reuse=True)
    worktree = _worktree_of(git_env)
    assert _lines(runs) == [str(worktree)]
    record = _prepared(rig)["P1"]
    assert record["status"] == "ok" and record["ran"] is True and record["line_shown"] is True
    # A retried preparation of the same run, and the next BUILD run: never again.
    await _prep(rig, "P1", triage_root, reuse=True)
    _reuse(rig, "B1", SessionKind.BUILD, root=triage_root, root_nonce="nonce-T1", gen=3)
    await _prep(rig, "B1", triage_root, reuse=True)
    assert len(_lines(runs)) == 1
    assert _prepared(rig)["B1"]["status"] == "ok" and _prepared(rig)["B1"]["ran"] is False
    # The idle sweep removes the worktree; the next stage's create recreates it.
    git("clean", "-fdq", cwd=worktree)
    cleaner = WorkspaceCleaner(object(), git_env.workspaces(), git_env.worktrees)  # type: ignore[arg-type]
    assert cleaner.remove_idle(worktree, BRANCH) == "removed"
    root = await _create(rig, "P2", SessionKind.PLAN, bind_worktree=worktree)
    assert worktree.is_dir()
    await _prep(rig, "P2", root)
    assert len(_lines(runs)) == 2  # a new materialisation: prepared again
    # Removed again; this time the reused session's preparation recreates it.
    git("clean", "-fdq", cwd=worktree)
    assert cleaner.remove_idle(worktree, BRANCH) == "removed"
    _reuse(rig, "B2", SessionKind.BUILD, root=root, root_nonce="nonce-P2", gen=2)
    await _prep(rig, "B2", root, reuse=True)
    assert len(_lines(runs)) == 3
    assert git("config", "--get", "factory.stageSession", cwd=worktree) == "B2"  # wired


@pytest.mark.asyncio
async def test_a_worktree_recreated_within_the_same_run_is_prepared_again(
    git_env: GitEnv, tmp_path: Path
) -> None:
    """F5: the run's earlier record never short-circuits the current materialisation."""
    script, runs = _counter(tmp_path)
    rig = _rig(git_env, [str(script)])
    root = await _create(rig, "P1", SessionKind.PLAN)
    await _prep(rig, "P1", root)
    worktree = _worktree_of(git_env)
    assert len(_lines(runs)) == 1
    git("clean", "-fdq", cwd=worktree)
    cleaner = WorkspaceCleaner(object(), git_env.workspaces(), git_env.worktrees)  # type: ignore[arg-type]
    assert cleaner.remove_idle(worktree, BRANCH) == "removed"
    await _prep(rig, "P1", root)  # the same run's retried preparation repairs it
    assert worktree.is_dir() and len(_lines(runs)) == 2
    admin = git_env.workspaces().admin_dir(worktree)
    assert read_record(admin).status == "ok"  # type: ignore[union-attr]
    assert _prepared(rig)["P1"]["ran"] is True  # the decision was refreshed


@pytest.mark.asyncio
async def test_a_fresh_plan_create_is_prepared_in_omnigents_worktree(
    git_env: GitEnv, tmp_path: Path
) -> None:
    script, runs = _counter(tmp_path)
    rig = _rig(git_env, [str(script)])
    root = await _create(rig, "P1", SessionKind.PLAN)
    assert not runs.exists()  # nothing before the session is prepared
    await _prep(rig, "P1", root)
    assert _lines(runs) == [str(_worktree_of(git_env))]
    assert (git_env.worktrees.resolve() / "factory-issue-42-prepare.log").is_file()


@pytest.mark.asyncio
async def test_a_base_fast_forward_that_changes_the_lockfile_reruns_the_hook(
    git_env: GitEnv, tmp_path: Path
) -> None:
    """F2: a success counts only for the dependency inputs it prepared."""
    _commit_main(git_env, "pnpm-lock.yaml", "lockfileVersion: 9\n")
    flag = tmp_path / "fail-next"
    script, runs = _counter(tmp_path, extra=f'[ -e "{flag}" ] && exit 2\nexit 0')
    rig = _rig(git_env, [str(script)])
    triage_root = await _create(rig, "T1", SessionKind.TRIAGE)
    _reuse(rig, "P1", SessionKind.PLAN, root=triage_root, root_nonce="nonce-T1", gen=2)
    await _prep(rig, "P1", triage_root, reuse=True)
    assert len(_lines(runs)) == 1 and _prepared(rig)["P1"]["line_shown"] is True
    # Main moves on without touching the lockfile: still prepared, not rerun.
    _commit_main(git_env, "README.md", "moved\n")
    _reuse(rig, "B1", SessionKind.BUILD, root=triage_root, root_nonce="nonce-T1", gen=3)
    await _prep(rig, "B1", triage_root, reuse=True)
    assert rig.directory.syncs["B1"]["status"] == "fast_forwarded"  # type: ignore[attr-defined]
    assert len(_lines(runs)) == 1 and _prepared(rig)["B1"]["line_shown"] is True
    # Another PR changes the lockfile; the next start fast-forwards: the hook reruns.
    _commit_main(git_env, "pnpm-lock.yaml", "lockfileVersion: 9\npackages: {}\n")
    _reuse(rig, "B2", SessionKind.BUILD, root=triage_root, root_nonce="nonce-T1", gen=4)
    await _prep(rig, "B2", triage_root, reuse=True)
    assert rig.directory.syncs["B2"]["status"] == "fast_forwarded"  # type: ignore[attr-defined]
    assert len(_lines(runs)) == 2
    assert _prepared(rig)["B2"]["ran"] is True and _prepared(rig)["B2"]["line_shown"] is True
    # Changed again and this time the rerun fails: no line.
    _commit_main(git_env, "pnpm-lock.yaml", "lockfileVersion: 9\npackages: {a: 1}\n")
    flag.touch()
    _reuse(rig, "B3", SessionKind.BUILD, root=triage_root, root_nonce="nonce-T1", gen=5)
    await _prep(rig, "B3", triage_root, reuse=True)
    assert len(_lines(runs)) == 3
    assert _prepared(rig)["B3"]["status"] == "failed"
    assert _prepared(rig)["B3"]["line_shown"] is False


class _Loopback:
    """A disposable loopback HTTP server recording every request's headers."""

    def __init__(self) -> None:
        seen: list[str] = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                seen.append(str(self.headers))
                self.send_response(404)
                self.end_headers()

            def log_message(self, *args: object) -> None:
                del args

        self.seen = seen
        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.server.server_port}/"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


HEADER_SENTINEL = "HEADER-SENTINEL-must-never-appear"


@pytest.mark.asyncio
async def test_the_hook_never_reaches_owner_or_previous_run_credentials(
    git_env: GitEnv, tmp_path: Path
) -> None:
    """F1: a fresh worktree (the owner's global helper, a sentinel fixture HOME) and a
    previously wired one (an earlier run's factory helper plus stale worktree helpers)."""
    worktree_sentinel = "WORKTREE-TOKEN-must-never-appear"
    loopback = _Loopback()
    probe = _script(
        tmp_path / "probe.sh",
        "for u in https://github.com/SA-Ambulance/timesheets.git "
        "https://example.com/x.git; do\n"
        "  printf 'url=%s\\n\\n' \"$u\" | git credential fill 2>&1 || echo no-credential\n"
        '  git config --get-urlmatch credential.helper "$u" || true\n'
        "done\n"
        f'git ls-remote "{loopback.base}r.git" >/dev/null 2>&1 || true\n'
        'echo "last-helper=[$(git config --get-all credential.helper | tail -n 1)]"\n'
        f'echo probed >> "{tmp_path}/probed"\n',
    )
    rig = _rig(git_env, [str(probe)])
    try:
        root = await _create(rig, "P1", SessionKind.PLAN)
        await _prep(rig, "P1", root)
        worktree = _worktree_of(git_env)
        log = log_path(worktree)
        fresh = log.read_text()
        assert _lines(tmp_path / "probed") == ["probed"]
        assert git("config", "--get", "factory.stageSession", cwd=worktree) == "P1"
        for key in ("credential.helper", "credential.https://github.com.helper"):
            helper = f"!f() {{ echo password={worktree_sentinel}; }}; f"
            git("config", "--worktree", "--add", key, helper, cwd=worktree)
        admin = git_env.workspaces().admin_dir(worktree)
        (admin / PREPARE_RECORD).unlink()  # as if this worktree were new to the hook
        _reuse(rig, "B1", SessionKind.BUILD, root=root, root_nonce="nonce-P1", gen=2)
        # The hook runs isolated; afterwards the factory's own wiring check refuses the
        # stale helpers (fails closed, as before).
        refused = await _prep(rig, "B1", root, ok=False, reuse=True)
        assert "credential helpers" in str(refused.detail["reason"])
        wired = log.read_text()
    finally:
        loopback.close()
    assert _lines(tmp_path / "probed") == ["probed", "probed"]
    for text in (fresh, wired):
        assert OWNER_TOKEN not in text and worktree_sentinel not in text
        assert "password=" not in text and "git_helper" not in text
        assert text.count("no-credential") == 2
        assert "last-helper=[]" in text  # the effective helper list ends reset
    assert len(loopback.seen) >= 2  # a clean worktree's hook runs and reaches the network
    assert OWNER_TOKEN not in "".join(loopback.seen)


async def _refused_for_headers(
    git_env: GitEnv, tmp_path: Path, configure: Any
) -> tuple[Rig, _Loopback, dict[str, Any]]:
    """A PLAN run whose worktree config gained headers (``configure(worktree, url)``)
    before its preparation; the hook would send a request to the loopback server."""
    loopback = _Loopback()
    url = f"{loopback.base}r.git"
    script, runs = _counter(tmp_path, extra=f'git ls-remote "{url}" >/dev/null 2>&1 || true')
    rig = _rig(git_env, [str(script)])
    root = await _create(rig, "P1", SessionKind.PLAN)
    worktree = _worktree_of(git_env)
    configure(worktree, url)
    try:
        # The hook is refused first; then the factory's own wiring check refuses the URL
        # header for the stage as well (fails closed, as before).
        refused = await _prep(rig, "P1", root, ok=False)
        assert "URL-specific extra header" in str(refused.detail["reason"])
    finally:
        loopback.close()
    assert not runs.exists()  # the hook never ran
    assert loopback.seen == []  # so nothing was ever sent
    record = read_record(git_env.workspaces().admin_dir(worktree))
    assert record is not None and record.status == "failed"
    assert record.reason.startswith("unsafe git config: extraheader present (")
    assert HEADER_SENTINEL not in record.reason  # key names only, never values
    log = log_path(worktree)
    assert not log.exists() or HEADER_SENTINEL not in log.read_text()
    return rig, loopback, _prepared(rig)["P1"]


@pytest.mark.asyncio
async def test_a_repository_scoped_github_header_refuses_the_hook(
    git_env: GitEnv, tmp_path: Path
) -> None:
    key = "http.https://github.com/SA-Ambulance/timesheets.git.extraHeader"

    def configure(worktree: Path, url: str) -> None:
        del url
        git("config", "--worktree", key, f"Authorization: {HEADER_SENTINEL}", cwd=worktree)

    _, _, record = await _refused_for_headers(git_env, tmp_path, configure)
    assert record["line_shown"] is False
    assert "http.https://github.com/SA-Ambulance/timesheets.git.extraheader" in record["reason"]


@pytest.mark.asyncio
async def test_a_loopback_scoped_header_refuses_the_hook_and_nothing_is_sent(
    git_env: GitEnv, tmp_path: Path
) -> None:
    def configure(worktree: Path, url: str) -> None:
        git(
            "config",
            "--worktree",
            f"http.{url}.extraHeader",
            f"X-Leak: {HEADER_SENTINEL}",
            cwd=worktree,
        )

    _, _, record = await _refused_for_headers(git_env, tmp_path, configure)
    assert record["status"] == "failed" and record["line_shown"] is False


@pytest.mark.asyncio
async def test_a_header_from_an_included_config_file_refuses_the_hook(
    git_env: GitEnv, tmp_path: Path
) -> None:
    def configure(worktree: Path, url: str) -> None:
        include = tmp_path / "included.gitconfig"
        include.write_text(f'[http "{url}"]\n\textraHeader = X-Leak: {HEADER_SENTINEL}\n')
        git("config", "--worktree", "include.path", str(include), cwd=worktree)

    _, _, record = await _refused_for_headers(git_env, tmp_path, configure)
    assert record["status"] == "failed"


def test_header_enumeration_names_keys_and_fails_closed(git_env: GitEnv, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    git("init", "-q", str(repo), cwd=tmp_path)
    env = prepare_env()
    assert wp.unsafe_extraheaders(repo, env) == []  # clean: the hook may run
    git("config", "http.extraHeader", "", cwd=repo)  # an empty reset is not a header
    assert wp.unsafe_extraheaders(repo, env) == []
    git("config", "http.http://h.example/x.extraHeader", f"X: {HEADER_SENTINEL}", cwd=repo)
    assert wp.unsafe_extraheaders(repo, env) == ["http.http://h.example/x.extraheader"]
    # Enumeration that cannot run is unsafe (None), never "no headers".
    assert wp.unsafe_extraheaders(repo, {**env, "PATH": str(tmp_path / "nowhere")}) is None
    assert wp.unsafe_extraheaders(tmp_path / "missing", env) is None


@pytest.mark.asyncio
async def test_a_failed_prepare_never_blocks_the_stage(
    git_env: GitEnv, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    script = _script(tmp_path / "p.sh", "echo nope >&2; exit 1")
    rig = _rig(git_env, [str(script)])
    root = await _create(rig, "P1", SessionKind.PLAN)
    with caplog.at_level(logging.WARNING):
        prep = await _prep(rig, "P1", root)  # ok: the stage starts anyway
    assert prep.detail["workspace"]
    record = _prepared(rig)["P1"]
    assert record["status"] == "failed" and record["reason"] == "exit 1: nope"
    assert record["line_shown"] is False
    assert any("worktree prepare failed" in r.getMessage() for r in caplog.records)
    summary = prepare_summary(git_env.source, configured=True)
    assert summary["ok"] == 0
    assert [r["status"] for r in summary["failed"]] == ["failed"]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_a_timed_out_prepare_never_blocks_the_stage(git_env: GitEnv, tmp_path: Path) -> None:
    slow = _rig(git_env, [str(_script(tmp_path / "slow.sh", "sleep 30"))], timeout=0.3)
    root = await _create(slow, "B9", SessionKind.BUILD)
    prep = await _prep(slow, "B9", root)
    assert prep.detail["workspace"]
    record = _prepared(slow)["B9"]
    assert record["status"] == "timeout" and record["reason"] == "timed out after 0.3s"
    summary = prepare_summary(git_env.source, configured=True)
    assert [r["status"] for r in summary["failed"]] == ["timeout"]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_a_stop_during_prepare_kills_it_and_the_stage_never_starts(
    git_env: GitEnv, tmp_path: Path
) -> None:
    """F3: the preparation returns while the command runs (the parcel serializer is free
    for a Stop); the stopped run's command is killed, and its effect is no longer valid,
    so the stage is never started. Removal of the worktree waits for the command."""
    child = tmp_path / "child.pid"
    script = _script(tmp_path / "p.sh", f'sleep 60 &\necho $! > "{child}"\nwait')
    rig = _rig(git_env, [str(script)])
    root = await _create(rig, "P1", SessionKind.PLAN)
    effect = intent(EffectKind.PREPARE_SESSION, "P1", root_id=root)
    began = time.monotonic()
    out = await rig.adapter.execute(effect, CTX)
    assert time.monotonic() - began < 5
    assert isinstance(out, RetryableReadFailure) and out.reason.startswith("waiting:")
    await _until(child.exists)
    preparer = rig.adapter.preparer
    assert preparer is not None
    worktree = _worktree_of(git_env)
    assert preparer.running(worktree)
    git("clean", "-fdq", "-e", "*.log", cwd=worktree)
    guarded = WorkspaceCleaner(
        object(),  # type: ignore[arg-type]
        git_env.workspaces(),
        git_env.worktrees,
        busy=preparer.running,
    )
    assert guarded.remove_idle(worktree, BRANCH) == "kept: worktree prepare running"
    rig.directory.stopped.add("P1")  # type: ignore[attr-defined]  # the owner's Stop applied
    await _until(lambda: not preparer.running(worktree))
    _assert_dead(int(child.read_text()))
    admin = git_env.workspaces().admin_dir(worktree)
    assert read_record(admin).status == "interrupted"  # type: ignore[union-attr]
    # The executor re-checks the run before each attempt: a stopped (draining or fenced)
    # run's preparation is cancelled, never executed again, so no stage starts.
    for lifecycle, fences in ((Lifecycle.DRAINING, ()), (Lifecycle.PREPARING, ("stop",))):
        run = SimpleNamespace(lifecycle=lifecycle, fences=fences)
        parcel = SimpleNamespace(session=lambda _sid, run=run: run)
        assert effect_still_valid(parcel, effect, now_us=0) == "session-not-preparing"  # type: ignore[arg-type]
    assert rig.server.count("POST", "/v1/sessions") == 1  # only the create; nothing sent


# ------------------------------------------------------------------ input contents (F2)


@pytest.mark.asyncio
async def test_an_untracked_lockfile_change_reruns_and_unchanged_contents_do_not(
    tmp_path: Path,
) -> None:
    script, runs = _counter(tmp_path)
    worktree = _worktree(tmp_path, 50)
    lock = worktree / "pnpm-lock.yaml"
    lock.write_text("one\n")
    preparer = _preparer([str(script)], tmp_path / "admin")
    first = await _settle(preparer, worktree)
    assert first is not None and first.ok and first.line_shown and len(_lines(runs)) == 1
    lock.write_text("one\n")  # rewritten with the same contents: unchanged
    again = await preparer.poll(worktree)
    assert isinstance(again, PrepareOutcome) and again.line_shown and len(_lines(runs)) == 1
    lock.write_text("two\n")
    assert await preparer.poll(worktree) is PENDING  # rerun
    rerun = await _settle(preparer, worktree)
    assert rerun is not None and rerun.ok and rerun.line_shown and len(_lines(runs)) == 2
    lock.unlink()  # missing is a valid, stable state (a repository without a lockfile)
    await _settle(preparer, worktree)
    gone = await preparer.poll(worktree)
    assert isinstance(gone, PrepareOutcome) and gone.line_shown and len(_lines(runs)) == 3


@pytest.mark.asyncio
async def test_a_file_added_inside_a_declared_directory_reruns(tmp_path: Path) -> None:
    script, runs = _counter(tmp_path)
    worktree = _worktree(tmp_path, 51)
    pkg = worktree / "packages" / "renderer"
    (pkg / "src").mkdir(parents=True)
    (pkg / "src" / "a.ts").write_text("a\n")
    preparer = _preparer([str(script)], tmp_path / "admin", inputs=("packages/renderer",))
    assert (await _settle(preparer, worktree)).line_shown  # type: ignore[union-attr]
    # Skipped content changes nothing.
    (pkg / "node_modules" / "dep").mkdir(parents=True)
    (pkg / "node_modules" / "dep" / "index.js").write_text("x\n")
    assert isinstance(await preparer.poll(worktree), PrepareOutcome)
    assert len(_lines(runs)) == 1
    (pkg / "src" / "new.ts").write_text("new\n")  # untracked, inside the directory
    assert await preparer.poll(worktree) is PENDING
    assert (await _settle(preparer, worktree)).line_shown  # type: ignore[union-attr]
    assert len(_lines(runs)) == 2


def test_the_fingerprint_hashes_contents_and_is_invalid_when_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worktree = _worktree(tmp_path, 52)
    assert wp.fingerprint(worktree, ("pnpm-lock.yaml",)) == wp.fingerprint(
        worktree, ("pnpm-lock.yaml",)
    )  # missing: valid and stable
    (worktree / "pnpm-lock.yaml").write_text("one\n")
    one = wp.fingerprint(worktree, ("pnpm-lock.yaml",))
    assert one is not None and one.startswith("v2:")
    (worktree / "pnpm-lock.yaml").write_text("two\n")
    assert wp.fingerprint(worktree, ("pnpm-lock.yaml",)) not in (None, one)
    (worktree / "d").mkdir()
    for i in range(3):
        (worktree / "d" / f"{i}.txt").write_text(str(i))
    monkeypatch.setattr(wp, "FINGERPRINT_MAX_FILES", 2)
    assert wp.fingerprint(worktree, ("d",)) is None  # beyond the file bound
    monkeypatch.setattr(wp, "FINGERPRINT_MAX_FILES", 20_000)
    monkeypatch.setattr(wp, "FINGERPRINT_MAX_BYTES", 2)
    assert wp.fingerprint(worktree, ("d",)) is None  # beyond the byte bound
    monkeypatch.setattr(wp, "FINGERPRINT_MAX_BYTES", 512 * 1024 * 1024)

    def broken(fd: int, size: int) -> bytes:
        raise OSError(errno.EIO, os.strerror(errno.EIO))

    monkeypatch.setattr(wp, "_read", broken)
    assert wp.fingerprint(worktree, ("pnpm-lock.yaml",)) is None


@pytest.mark.asyncio
async def test_an_unreadable_input_never_shows_the_line_or_caches_an_ok(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script, runs = _counter(tmp_path)
    worktree = _worktree(tmp_path, 53)
    (worktree / "pnpm-lock.yaml").write_text("one\n")
    admin = tmp_path / "admin"
    preparer = _preparer([str(script)], admin)
    ok = await _settle(preparer, worktree, start="B1")
    assert ok is not None and ok.ok and ok.line_shown

    def broken(fd: int, size: int) -> bytes:
        raise OSError(errno.EIO, os.strerror(errno.EIO))

    monkeypatch.setattr(wp, "_read", broken)
    # The recorded ok cannot be matched: rerun once for this start, then settle, no line.
    assert await preparer.poll(worktree, start="B2") is PENDING
    unverified = await _settle(preparer, worktree, start="B2")
    assert unverified is not None and unverified.status == "unverified"
    assert not unverified.line_shown and len(_lines(runs)) == 2
    record = read_record(admin)
    assert record is not None and record.status == "unverified" and record.fingerprint == ""
    again = await preparer.poll(worktree, start="B2")  # the same start: no second rerun
    assert isinstance(again, PrepareOutcome) and not again.line_shown and len(_lines(runs)) == 2
    assert await preparer.poll(worktree, start="B3") is PENDING  # the next start: once more
    await _settle(preparer, worktree, start="B3")
    assert len(_lines(runs)) == 3
    # Readable again: the unverified record reruns for the next start and becomes ok.
    monkeypatch.undo()
    assert await preparer.poll(worktree, start="B4") is PENDING
    back = await _settle(preparer, worktree, start="B4")
    assert back is not None and back.ok and back.line_shown and len(_lines(runs)) == 4


@pytest.mark.asyncio
async def test_symlinked_or_special_inputs_are_unverifiable(tmp_path: Path) -> None:
    """F2: an input that is, or sits under, a symlink, or is a special file, cannot be
    verified without following or opening it: no line, no ok, never cached."""
    worktree = _worktree(tmp_path, 60)
    target = tmp_path / "real-lock.yaml"
    target.write_text("one\n")
    (worktree / "pnpm-lock.yaml").symlink_to(target)
    (tmp_path / "real-pkg").mkdir()
    (worktree / "linked").symlink_to(tmp_path / "real-pkg")
    (worktree / "d").mkdir()
    (worktree / "d" / "ok.txt").write_text("x")
    os.mkfifo(worktree / "d" / "fifo")
    os.mkfifo(worktree / "pipe")
    for inputs in (("pnpm-lock.yaml",), ("linked/schema.prisma",), ("d",), ("pipe",)):
        assert wp.fingerprint(worktree, inputs) is None, inputs
    script, runs = _counter(tmp_path)
    preparer = _preparer([str(script)], tmp_path / "admin")
    outcome = await _settle(preparer, worktree, start="B1")
    assert outcome is not None and outcome.status == "unverified" and not outcome.line_shown
    assert read_record(tmp_path / "admin").status == "unverified"  # type: ignore[union-attr]
    again = await preparer.poll(worktree, start="B1")
    assert isinstance(again, PrepareOutcome) and not again.line_shown
    assert len(_lines(runs)) == 1


@pytest.mark.asyncio
async def test_a_stop_during_header_enumeration_never_starts_the_hook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F3: still_wanted is re-checked after the preflight, right before spawning."""
    script, runs = _counter(tmp_path)
    worktree = _worktree(tmp_path, 61)
    wanted = True

    async def still_wanted() -> bool:
        return wanted

    def enumerate_then_stop(path: Path, env: Any) -> list[str]:
        nonlocal wanted
        wanted = False  # the owner's Stop lands while the headers are enumerated
        return []

    monkeypatch.setattr(wp, "unsafe_extraheaders", enumerate_then_stop)
    preparer = _preparer([str(script)], tmp_path / "admin")
    outcome = await _settle(preparer, worktree, still_wanted=still_wanted)
    assert outcome is not None and outcome.status == "interrupted" and not outcome.line_shown
    assert outcome.reason == "cancelled: the run is no longer starting"
    assert not runs.exists()  # never spawned


@pytest.mark.asyncio
async def test_the_once_per_start_rerun_limit_survives_restarts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F6: the attempted start is persisted with the attempt, so three daemon instances
    on the same worktree and start rerun an unverifiable prepare once in total."""
    script, runs = _counter(tmp_path)
    worktree = _worktree(tmp_path, 62)
    (worktree / "pnpm-lock.yaml").write_text("one\n")

    def broken(fd: int, size: int) -> bytes:
        raise OSError(errno.EIO, os.strerror(errno.EIO))

    monkeypatch.setattr(wp, "_read", broken)
    admin = tmp_path / "admin"
    for _ in range(3):  # a fresh preparer each time: a daemon restart
        outcome = await _settle(_preparer([str(script)], admin), worktree, start="SAME-RUN")
        assert outcome is not None and outcome.status == "unverified"
        assert not outcome.line_shown
    assert len(_lines(runs)) == 1
    assert read_record(admin).start == "SAME-RUN"  # type: ignore[union-attr]
    # The next stage start still gets its one rerun; a recreated worktree starts fresh.
    await _settle(_preparer([str(script)], admin), worktree, start="NEXT-RUN")
    assert len(_lines(runs)) == 2
    await _settle(
        _preparer([str(script)], tmp_path / "admin-recreated"), worktree, start="NEXT-RUN"
    )
    assert len(_lines(runs)) == 3
