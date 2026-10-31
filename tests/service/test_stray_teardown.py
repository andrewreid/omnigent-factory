"""The worktree sweep skips a dev-environment teardown that cannot apply to a stray entry
(its repository script is not in that directory, or the entry is a plain file) instead of
keeping the entry on ``exit 127``; a directory holding the script still waits for it."""

from __future__ import annotations

import os
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from omnigent_factory.service.cleanup import WorkspaceCleaner
from tests.credentials.repos import GitEnv

TEARDOWN = ("bash", "scripts/dev-env.sh", "--teardown")


def _cleaner(git_env: GitEnv, teardown: tuple[str, ...] = TEARDOWN) -> WorkspaceCleaner:
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    return WorkspaceCleaner(
        SimpleNamespace(),  # type: ignore[arg-type]
        ws,
        git_env.worktrees,
        teardown=teardown,
    )


def _script(directory: Path, log: Path, code: int) -> None:
    scripts = directory / "scripts"
    scripts.mkdir(parents=True)
    script = scripts / "dev-env.sh"
    script.write_text(f'#!/bin/sh\necho "$(pwd -P) $*" >> {log}\nexit {code}\n')
    script.chmod(0o755)


def _bind(path: Path, teardown: tuple[str, ...]) -> str | list[str]:
    """The stray teardown's bound argv (its descriptor closed), or why it is skipped."""
    cleaner = WorkspaceCleaner(
        SimpleNamespace(),  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        path.parent,
        teardown=teardown,
    )
    bound = cleaner._bind_stray_teardown(path)
    if isinstance(bound, str):
        return bound
    argv, fd = bound
    if fd is not None:
        os.close(fd)
    return argv


def _sourced(argv: str | list[str], shell: list[str], rest: list[str]) -> bool:
    """``argv`` is ``<shell> -c '. /dev/fd/N' <rest>``."""
    if isinstance(argv, str):
        return False
    n = len(shell)
    return (
        argv[:n] == shell
        and argv[n] == "-c"
        and re.fullmatch(r"\. /dev/fd/\d+", argv[n + 1]) is not None
        and argv[n + 2 :] == rest
    )


def test_the_script_is_the_first_relative_path_argument(tmp_path: Path) -> None:
    stray = tmp_path / "stray"
    stray.mkdir()
    assert _bind(stray, TEARDOWN) == "no scripts/dev-env.sh in it"
    assert _bind(stray, ("scripts/dev-env.sh", "--teardown")) == "no scripts/dev-env.sh in it"
    # Names no repository script, or a host program: runs as configured.
    assert _bind(stray, ("make", "down")) == ["make", "down"]
    assert _bind(stray, ("/usr/local/bin/teardown",)) == ["/usr/local/bin/teardown"]
    _script(stray, tmp_path / "log", 0)
    assert _sourced(_bind(stray, TEARDOWN), ["bash"], ["scripts/dev-env.sh", "--teardown"])
    # Run directly: its #! shell reads it; $0 stays the path it ran as before.
    direct = _bind(stray, ("scripts/dev-env.sh", "--teardown"))
    assert _sourced(direct, ["/bin/sh"], [str(stray / "scripts/dev-env.sh"), "--teardown"])
    assert _sourced(
        _bind(stray, ("./scripts/dev-env.sh",)), ["/bin/sh"], [str(stray / "scripts/dev-env.sh")]
    )
    assert _sourced(
        _bind(stray, ("bash", "-x", "scripts/dev-env.sh")), ["bash", "-x"], ["scripts/dev-env.sh"]
    )


def test_only_a_shell_can_be_bound(tmp_path: Path) -> None:
    stray = tmp_path / "stray"
    (stray / "scripts").mkdir(parents=True)
    script = stray / "scripts" / "t.sh"
    unbound = "scripts/t.sh is not run by a shell the teardown can bind"
    for shebang, shell in [
        ("#!/usr/bin/env bash", ["/usr/bin/env", "bash"]),
        ("#!/bin/bash -e", ["/bin/bash", "-e"]),
        ("#!/usr/bin/dash", ["/usr/bin/dash"]),
    ]:
        script.write_text(f"{shebang}\nexit 0\n")
        assert _sourced(_bind(stray, ("scripts/t.sh",)), shell, [str(script)])
    for shebang in ("#!/usr/bin/env python3", "#!/bin/bash -e -x", "#!bash", "exit 0"):
        script.write_text(f"{shebang}\nexit 0\n")
        assert _bind(stray, ("scripts/t.sh",)) == unbound
    assert _bind(stray, ("python3", "scripts/t.sh")) == unbound
    assert _bind(stray, ("bash", "-O", "extglob", "scripts/t.sh")) == unbound
    # Not a regular file: a FIFO is never opened for reading (no hang) nor run.
    script.unlink()
    os.mkfifo(script)
    assert _bind(stray, ("bash", "scripts/t.sh")) == "scripts/t.sh is not a regular file beneath it"


def test_stray_directory_without_the_script_is_removed_without_teardown(
    git_env: GitEnv,
) -> None:
    cleaner = _cleaner(git_env)
    stray = git_env.worktrees / "factory-issue-1446-review"
    stray.mkdir()
    (stray / "notes.md").write_text("review notes\n")
    assert cleaner.remove_stray(stray, reason="closed issue") == "removed"
    assert not stray.exists()


def test_stray_plain_file_never_runs_teardown(git_env: GitEnv, tmp_path: Path) -> None:
    log = tmp_path / "ran.log"
    host = tmp_path / "host"
    _script(host, log, 0)
    cleaner = _cleaner(git_env, (str(host / "scripts" / "dev-env.sh"),))
    stray = git_env.worktrees / "factory-issue-1497-notes"
    stray.write_text("notes\n")
    assert cleaner.remove_stray(stray, reason="closed issue") == "removed"
    assert not stray.exists() and not log.exists()


def test_stray_directory_with_the_script_still_waits_for_its_teardown(
    git_env: GitEnv, tmp_path: Path
) -> None:
    log = tmp_path / "ran.log"
    cleaner = _cleaner(git_env)
    failing = git_env.worktrees / "factory-issue-1522-review"
    _script(failing, log, 1)
    kept = cleaner.remove_stray(failing, reason="closed issue")
    assert kept.startswith("kept: dev environment teardown failed: exit 1")
    assert failing.exists()
    passing = git_env.worktrees / "factory-issue-1523-devenv"
    _script(passing, log, 0)
    assert cleaner.remove_stray(passing, reason="closed issue") == "removed"
    assert not passing.exists()
    ran = log.read_text().splitlines()
    assert [line.split()[0] for line in ran] == [str(failing.resolve()), str(passing.resolve())]
    assert all(line.endswith("--teardown") for line in ran)


def test_a_script_outside_the_stray_is_skipped(tmp_path: Path) -> None:
    stray = tmp_path / "stray"
    outside = tmp_path / "outside"
    _script(outside, tmp_path / "log", 0)
    stray.mkdir()
    not_beneath = "scripts/dev-env.sh is not a regular file beneath it"
    # A ".." escape names a script that exists, but not in the stray.
    escape = ("bash", "../outside/scripts/dev-env.sh", "--teardown")
    assert _bind(stray, escape) == "../outside/scripts/dev-env.sh leaves the stray"
    # A symlink from the stray to a script outside it.
    (stray / "scripts").mkdir()
    (stray / "scripts" / "dev-env.sh").symlink_to(outside / "scripts" / "dev-env.sh")
    assert str(_bind(stray, TEARDOWN)).startswith(not_beneath)
    # A symlinked directory leading out of the stray, and a dangling link.
    linked = tmp_path / "linked"
    linked.mkdir()
    (linked / "scripts").symlink_to(outside / "scripts")
    assert str(_bind(linked, TEARDOWN)).startswith(not_beneath)
    dangling = tmp_path / "dangling"
    (dangling / "scripts").mkdir(parents=True)
    (dangling / "scripts" / "dev-env.sh").symlink_to(tmp_path / "nowhere.sh")
    assert str(_bind(dangling, TEARDOWN)).startswith(not_beneath)


def test_any_link_inside_the_stray_is_skipped_too(tmp_path: Path) -> None:
    # Fail closed: no component may be a symlink, even one that stays inside.
    stray = tmp_path / "stray"
    _script(stray, tmp_path / "log", 0)
    (stray / "bin").mkdir()
    (stray / "bin" / "teardown").symlink_to(stray / "scripts" / "dev-env.sh")
    assert str(_bind(stray, ("bin/teardown",))).startswith(
        "bin/teardown is not a regular file beneath it"
    )


def test_a_swap_after_validation_cannot_run_an_outside_script(tmp_path: Path) -> None:
    """The reviewer's race: validate the stray's own script, then replace it with a link
    to an outside script before launch. The validated bytes run; the outside one never."""
    inside_log, outside_log = tmp_path / "inside.log", tmp_path / "outside.log"
    stray = tmp_path / "stray"
    outside = tmp_path / "outside"
    _script(stray, inside_log, 0)
    _script(outside, outside_log, 0)
    cleaner = WorkspaceCleaner(
        SimpleNamespace(),  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        tmp_path,
        teardown=TEARDOWN,
    )
    for teardown in (TEARDOWN, ("scripts/dev-env.sh", "--teardown")):
        cleaner.teardown = teardown
        bound = cleaner._bind_stray_teardown(stray)
        assert not isinstance(bound, str)
        argv, fd = bound
        assert fd is not None
        target = stray / "scripts" / "dev-env.sh"
        kept = tmp_path / "validated.sh"
        target.rename(kept)  # the swap: the path now leads outside
        target.symlink_to(outside / "scripts" / "dev-env.sh")
        try:
            assert cleaner._run_teardown(stray, argv, pass_fds=(fd,)) is None
        finally:
            os.close(fd)
        target.unlink()
        kept.rename(target)
    assert not outside_log.exists()
    ran = inside_log.read_text().splitlines()
    assert ran == [f"{stray.resolve()} --teardown"] * 2


def test_remove_stray_never_runs_a_script_swapped_in_after_validation(
    git_env: GitEnv, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inside_log, outside_log = tmp_path / "inside.log", tmp_path / "outside.log"
    outside = tmp_path / "outside"
    _script(outside, outside_log, 0)
    cleaner = _cleaner(git_env, ("scripts/dev-env.sh", "--teardown"))
    stray = git_env.worktrees / "factory-issue-1531-review"
    _script(stray, inside_log, 0)
    bind = cleaner._bind_stray_teardown

    def bind_then_swap(path: Path) -> tuple[list[str], int | None] | str:
        bound = bind(path)
        target = path / "scripts" / "dev-env.sh"
        target.unlink()
        target.symlink_to(outside / "scripts" / "dev-env.sh")
        return bound

    monkeypatch.setattr(cleaner, "_bind_stray_teardown", bind_then_swap)
    assert cleaner.remove_stray(stray, reason="closed issue") == "removed"
    assert not os.path.lexists(stray)
    assert not outside_log.exists()
    assert inside_log.read_text().splitlines() == [f"{stray.resolve()} --teardown"]


def test_stray_whose_script_links_outside_is_removed_without_running_it(
    git_env: GitEnv, tmp_path: Path
) -> None:
    log = tmp_path / "ran.log"
    outside = tmp_path / "outside"
    _script(outside, log, 0)
    cleaner = _cleaner(git_env)
    stray = git_env.worktrees / "factory-issue-1530-review"
    (stray / "scripts").mkdir(parents=True)
    (stray / "scripts" / "dev-env.sh").symlink_to(outside / "scripts" / "dev-env.sh")
    assert cleaner.remove_stray(stray, reason="closed issue") == "removed"
    assert not os.path.lexists(stray) and not log.exists()
    assert (outside / "scripts" / "dev-env.sh").exists()  # the target is untouched
