"""The optional worktree prepare hook (``worktree_prepare``): dependencies before a stage.

The repository's prepare command (argv from host config; a relative program path
resolves in the worktree, as for ``devenv_teardown``) runs in an issue worktree before
the first PLAN or BUILD run in it starts. It is never run in a reference snapshot
(``factory-ref-*``): those stay free of ``node_modules``.

**State per materialisation.** Each worktree's own Git directory
(``.git/worktrees/<name>/``, removed with the worktree, so a recreate starts afresh)
holds one record, a small state machine::

    absent ──start──> running ──exit 0──> ok(fingerprint)
                         │──exit≠0 / no start / log error──> failed
                         │──timeout──> timeout
                         └──cancelled / daemon died──> interrupted

* ``absent``: run it.
* ``running`` with a live process of this daemon: wait. A ``running`` record nobody
  runs any more (the daemon died mid-run) is recovered as ``interrupted`` (a process
  group it left behind is killed when it is provably the same process).
* ``ok`` whose fingerprint (:func:`fingerprint`: the contents of the dependency inputs,
  by default ``pnpm-lock.yaml``, read from the filesystem, tracked or not) still matches
  the worktree: done, and the first message may say the dependencies are installed. A
  fingerprint that no longer matches (a base fast-forward changed the lockfile) runs it
  again: the repository script's own marker keeps that cheap when nothing it cares
  about changed. An unreadable (invalid) fingerprint never matches: it reruns the
  command at most once per stage start, and a run that exits 0 while its inputs cannot
  be read is recorded ``unverified`` (never ``ok``, no line).
* ``failed`` / ``timeout``: done for this materialisation, no line.
* ``interrupted``: retried once (the next poll or start); interrupted again it stays
  ``interrupted`` (visible in ``status``/``doctor``) until the worktree is recreated.

A ``running`` marker is written before the command starts; if it cannot be written the
command is not started (a ``failed`` outcome). If the final outcome cannot be written the
in-process result is used (logged) but is never ``ok``, so no line is shown.

**Execution.** The command runs as a tracked asyncio task per worktree (an asyncio
subprocess in its own process group, killed whole on timeout or cancellation): callers
*poll* it (:meth:`WorktreePreparer.poll`) and never hold a lock across it, so a stage
preparation returns while it runs and re-checks later. A ``still_wanted`` callback is
polled while it runs; when it says no (the run was stopped or fenced), the command is
cancelled. No clone lock or other lock shared between issues is ever held: preparations
of different worktrees run in parallel.

**Environment.** A small allowlist of the daemon's (toolchain, locale, home and XDG
directories) plus an isolated Git context: no system or global Git config
(``GIT_CONFIG_NOSYSTEM``, ``GIT_CONFIG_GLOBAL=/dev/null``) and command-line resets of
every credential helper and extra header (``credential.helper=``,
``http.extraHeader=``), which come after the worktree's own config, so neither the
owner's helpers nor a previous run's factory helper can be reached. A URL-scoped header
can be more specific than any reset, so before launching, every extra header the
command's Git would see (local, worktree and included config) is enumerated: any
non-empty one, or a failed enumeration, refuses the command (a ``failed`` outcome naming
the keys only, never their values).

The command is owner-trusted repository code run with the owner's HOME, like
``devenv_teardown``. The Git/credential isolation and the extra-header refusal are
defence in depth, not a sandbox: a config change racing between the check and the
launch is not prevented. ``GH_CONFIG_DIR``
points at an empty directory (the owner's ``gh`` login is not reachable either). The
command needs no repository authentication.

**Log.** Output goes to ``<worktree>-prepare.log`` beside the worktree
(``factory-issue-<N>-prepare.log``, the devenv log convention). It is opened without
following a symlink and without blocking, must be a regular file, is forced to 0600 and
truncated; any error opening or writing it is a recorded ``failed`` outcome.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import signal
import stat
import subprocess
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Final

from omnigent_factory.credentials.worktree import REFERENCE_META, REFERENCE_NAME, WorktreeError

LOG = logging.getLogger(__name__)

#: The state file in a worktree's own Git directory (removed with the worktree).
PREPARE_RECORD = "omnigent-factory-worktree-prepare.json"
#: An empty directory beside it: the command's ``GH_CONFIG_DIR``.
_GH_DIR = "omnigent-factory-prepare-gh"
#: The first-message line of a PLAN/BUILD run whose worktree is prepared.
PREPARED_LINE = (
    "Dependencies are installed and prisma/renderer are generated; "
    "reinstall only if you change the lockfile."
)
#: Seconds one prepare may take by default: ~25x the measured cold install (22.8 s),
#: room for code generation and a busy host.
DEFAULT_TIMEOUT_SECONDS = 600.0
#: The dependency inputs fingerprinted by default (paths relative to the worktree root).
DEFAULT_INPUTS: tuple[str, ...] = ("pnpm-lock.yaml",)
#: Bounds of one fingerprint: a lockfile, a schema and a source package are a few
#: hundred files and a few MiB; a declared directory beyond 20k files or 512 MiB is a
#: misdeclaration (or generated output), and reading it on every start would stall the
#: preparation thread. Beyond either bound the fingerprint is invalid.
FINGERPRINT_MAX_FILES = 20_000
FINGERPRINT_MAX_BYTES = 512 * 1024 * 1024
_SKIP_DIRS = frozenset({"node_modules", ".git"})
#: How often a running command's ``still_wanted`` is asked.
WANTED_POLL_SECONDS = 1.0
#: Terminal-or-waiting states of one materialisation.
STATES: Final = frozenset({"running", "ok", "unverified", "failed", "timeout", "interrupted"})
_REASON_CHARS = 200
_TAIL_BYTES = 4096

#: Daemon environment variables the prepare command receives (everything else is dropped).
ENV_ALLOW = frozenset(
    {
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "LANG",
        "LANGUAGE",
        "TZ",
        "TMPDIR",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_CACHE_HOME",
        "XDG_STATE_HOME",
        "XDG_RUNTIME_DIR",
        "PNPM_HOME",
        "COREPACK_HOME",
        "NVM_DIR",
        "VOLTA_HOME",
    }
)
#: Command-line Git config for the command (applied after every config file, so each
#: empty value resets what the system/global/local/worktree files configured).
GIT_RESETS: tuple[tuple[str, str], ...] = (
    ("credential.helper", ""),
    ("credential.https://github.com.helper", ""),
    ("credential.interactive", "false"),
    ("http.extraHeader", ""),
    ("http.https://github.com/.extraHeader", ""),
)


def prepare_env(
    base: Mapping[str, str] | None = None, *, gh_config_dir: Path | None = None
) -> dict[str, str]:
    """The prepare command's environment: the allowlisted daemon variables (and
    ``LC_*``) and an isolated Git context with no credential helper or extra header."""
    source = os.environ if base is None else base
    env = {k: v for k, v in source.items() if k in ENV_ALLOW or k.startswith("LC_")}
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = os.devnull
    env["GIT_CONFIG_COUNT"] = str(len(GIT_RESETS))
    for i, (key, value) in enumerate(GIT_RESETS):
        env[f"GIT_CONFIG_KEY_{i}"] = key
        env[f"GIT_CONFIG_VALUE_{i}"] = value
    if gh_config_dir is not None:
        env["GH_CONFIG_DIR"] = str(gh_config_dir)
    return env


def resolve_argv(argv: Sequence[str], path: Path) -> list[str]:
    """``argv`` with a relative program path resolved in the worktree (as teardown)."""
    resolved = list(argv)
    if "/" in resolved[0] and not Path(resolved[0]).is_absolute():
        resolved[0] = str(path / resolved[0])
    return resolved


def log_path(path: Path) -> Path:
    """Where one worktree's prepare output goes: ``<parent>/<name>-prepare.log``."""
    return path.parent / f"{path.name}-prepare.log"


class _Unreadable(Exception):
    """An input could not be read completely (the fingerprint is invalid)."""


def fingerprint(path: Path, inputs: Sequence[str]) -> str | None:
    """The contents of the dependency inputs of ``path``, read from the filesystem
    (tracked or not): sha256 over the sorted (relative path, type, content hash) of each
    declared file, and of every regular file under each declared directory
    (``node_modules`` and ``.git`` skipped). A missing input is recorded as ``missing``
    (a valid state).

    None: invalid (a read or walk failed; an input that is, or sits under, a symlink,
    or is a FIFO, socket or device, whose content cannot be verified without following
    or opening it; or more than :data:`FINGERPRINT_MAX_FILES` files or
    :data:`FINGERPRINT_MAX_BYTES` bytes). An invalid fingerprint never
    authorises the prompt line and is never recorded with an ``ok``.
    """
    rows: list[tuple[str, str, str]] = []
    budget = [FINGERPRINT_MAX_FILES, FINGERPRINT_MAX_BYTES]
    try:
        for item in inputs:
            _fingerprint_input(path, item, rows, budget)
    except (OSError, _Unreadable):
        return None
    digest = hashlib.sha256(b"worktree-prepare-inputs-v2\0")
    for rel, kind, value in sorted(rows):
        digest.update(f"{rel}\0{kind}\0{value}\0".encode("utf-8", "surrogateescape"))
    return f"v2:{digest.hexdigest()}"


def _fingerprint_input(
    root: Path, item: str, rows: list[tuple[str, str, str]], budget: list[int]
) -> None:
    parts = [part for part in Path(item).parts if part not in ("", ".")]
    current = root
    for depth, part in enumerate(parts):
        current = current / part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            rows.append((item, "missing", ""))
            return
        if stat.S_ISLNK(info.st_mode):
            raise _Unreadable("symlinked input")  # never followed: its content is unknown
        if depth < len(parts) - 1 and not stat.S_ISDIR(info.st_mode):
            rows.append((item, "missing", ""))  # a file where a directory is declared
            return
    if not parts:
        raise _Unreadable("empty input")
    _fingerprint_entry(current, "/".join(parts), rows, budget)


def _fingerprint_entry(
    path: Path, rel: str, rows: list[tuple[str, str, str]], budget: list[int]
) -> None:
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode):
        raise _Unreadable("symlinked input")  # never followed: its content is unknown
    if stat.S_ISREG(info.st_mode):
        rows.append((rel, "file", _hash_file(path, budget)))
    elif stat.S_ISDIR(info.st_mode):
        rows.append((rel, "dir", ""))
        try:
            names = sorted(os.listdir(path))
        except OSError as exc:
            raise _Unreadable(str(exc)) from exc
        for name in names:
            if name in _SKIP_DIRS:
                continue
            _fingerprint_entry(path / name, f"{rel}/{name}", rows, budget)
    else:
        raise _Unreadable("special file input")  # a FIFO, socket or device: never opened


def _hash_file(path: Path, budget: list[int]) -> str:
    budget[0] -= 1
    if budget[0] < 0:
        raise _Unreadable("too many files")
    digest = hashlib.sha256()
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise _Unreadable("not a regular file")
        while chunk := _read(fd, 1 << 20):
            budget[1] -= len(chunk)
            if budget[1] < 0:
                raise _Unreadable("inputs too large")
            digest.update(chunk)
    finally:
        os.close(fd)
    return digest.hexdigest()


def _read(fd: int, size: int) -> bytes:
    """One read of an input (separate, so tests can inject a read failure)."""
    return os.read(fd, size)


def unsafe_extraheaders(path: Path, env: Mapping[str, str]) -> list[str] | None:
    """The config keys of every non-empty ``http[.<url>].extraHeader`` the hook's own Git
    would see in ``path`` (local, worktree and included config, under the hook's
    isolated environment): names only, never values. ``[]``: none. None: the
    enumeration itself failed (treated as unsafe)."""
    try:
        proc = subprocess.run(
            ["git", "config", "-z", "--includes", "--get-regexp", r"^http\..*extraheader$"],  # noqa: S607
            cwd=path,
            env=dict(env),
            capture_output=True,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode == 1 and not proc.stdout:
        return []  # no such key anywhere
    if proc.returncode != 0:
        return None
    keys: list[str] = []
    for entry in proc.stdout.split(b"\0"):
        if not entry:
            continue
        key, newline, value = entry.partition(b"\n")
        if value.strip() or not newline:  # a valueless key is not an empty reset
            name = key.decode("utf-8", "replace")
            if name not in keys:
                keys.append(name)
    return keys


@dataclass(frozen=True, slots=True)
class PrepareOutcome:
    """One worktree's prepare state (see the module docstring). ``ran``: this daemon ran
    it for the caller (False: recorded earlier). ``line_shown``: whether a first message
    may say the dependencies are installed (``ok`` for the current inputs)."""

    status: str
    path: str
    reason: str = ""
    log: str = ""
    started_at: float = 0.0
    duration_s: float = 0.0
    fingerprint: str = ""
    retries: int = 0
    pid: int = 0
    pid_start: str = ""
    #: The stage start (run) that launched this attempt (bounds reruns across restarts).
    start: str = ""
    ran: bool = True
    line_shown: bool = False

    @property
    def ok(self) -> bool:
        return self.status == "ok"

    def to_json(self) -> dict[str, str | float | int | bool]:
        return {
            "status": self.status,
            "path": self.path,
            "reason": self.reason,
            "log": self.log,
            "started_at": self.started_at,
            "duration_s": self.duration_s,
            "fingerprint": self.fingerprint,
            "retries": self.retries,
            "pid": self.pid,
            "pid_start": self.pid_start,
            "start": self.start,
            "ran": self.ran,
            "line_shown": self.line_shown,
        }

    @classmethod
    def from_json(cls, data: object) -> PrepareOutcome | None:
        if not isinstance(data, dict) or data.get("status") not in STATES:
            return None

        def num(key: str) -> float:
            value = data.get(key)
            return float(value) if isinstance(value, int | float) else 0.0

        return cls(
            status=str(data["status"]),
            path=str(data.get("path") or ""),
            reason=str(data.get("reason") or "")[:_REASON_CHARS],
            log=str(data.get("log") or ""),
            started_at=num("started_at"),
            duration_s=num("duration_s"),
            fingerprint=str(data.get("fingerprint") or ""),
            retries=int(num("retries")),
            pid=int(num("pid")),
            pid_start=str(data.get("pid_start") or ""),
            start=str(data.get("start") or ""),
            ran=False,
        )


class _Pending:
    """:meth:`WorktreePreparer.poll`: the command is running; ask again later."""

    def __repr__(self) -> str:
        return "PENDING"


PENDING: Final = _Pending()
Wanted = Callable[[], Awaitable[bool]]


class WorktreePreparer:
    """Runs the configured prepare command per materialised worktree (module docstring).

    ``admin_dir`` maps a worktree to its own Git directory (``Workspaces.admin_dir``).
    """

    def __init__(
        self,
        argv: Sequence[str],
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        *,
        admin_dir: Callable[[Path], Path],
        inputs: Sequence[str] = DEFAULT_INPUTS,
        env: Callable[[Path | None], dict[str, str]] | None = None,
        wall_clock: Callable[[], float] = time.time,
        wanted_poll_seconds: float = WANTED_POLL_SECONDS,
    ) -> None:
        self.argv = tuple(argv)
        self.timeout_seconds = timeout_seconds
        self.inputs = tuple(inputs)
        self._admin_dir = admin_dir
        self._env = env or (lambda gh: prepare_env(gh_config_dir=gh))
        self._wall_clock = wall_clock
        self._wanted_poll = wanted_poll_seconds
        #: The command task of each worktree while it runs (keyed by resolved path).
        self._tasks: dict[Path, asyncio.Task[PrepareOutcome]] = {}
        #: Finished in-process outcomes not yet seen by a poll (an unwritable record).
        self._unrecorded: dict[Path, PrepareOutcome] = {}
        #: The latest poller's ``still_wanted`` of each worktree (handed to a start).
        self._wanted_for: dict[Path, Wanted | None] = {}
        #: The stage start that last launched the command in each worktree.
        self._started_by: dict[Path, str] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.argv)

    def running(self, path: Path) -> bool:
        """A command runs in ``path`` now (worktree removal must wait). Thread-safe."""
        task = self._tasks.get(path)
        return task is not None and not task.done()

    async def poll(
        self, path: Path, *, still_wanted: Wanted | None = None, start: str = ""
    ) -> PrepareOutcome | _Pending | None:
        """The current materialisation's state, starting the command when it is due.

        :data:`PENDING`: it runs (started now or earlier); poll again. None: not
        configured, or a reference snapshot (never prepared). Otherwise the settled
        outcome, ``line_shown`` set for ``ok`` with matching inputs. ``start`` names the
        stage start polling (its run): an unreadable fingerprint reruns the command at
        most once per start. Never raises for a failed command, log or record; returns
        quickly (the command is a task).
        """
        if not self.argv or path.name.startswith("factory-ref-") or REFERENCE_NAME.match(path.name):
            return None
        task = self._tasks.get(path)
        if task is not None and not task.done():
            return PENDING
        just_ran = task is not None
        if task is not None:
            self._tasks.pop(path, None)
            with contextlib.suppress(asyncio.CancelledError, Exception):
                task.result()  # retrieved: never "exception was never retrieved"
        try:
            admin = await asyncio.to_thread(self._admin_dir, path)
        except (OSError, WorktreeError) as exc:
            LOG.warning("worktree prepare skipped path=%s reason=%s", path, exc)
            return None
        if (admin / REFERENCE_META).exists():
            return None  # a reference snapshot under another name: never prepared
        current = await asyncio.to_thread(fingerprint, path, self.inputs)
        self._wanted_for[path] = still_wanted
        unrecorded = self._unrecorded.pop(path, None)
        if unrecorded is not None:
            return replace(unrecorded, line_shown=False)  # never claim an unrecorded ok
        settled = self._settled(admin, path, current, start)
        if settled is not None:
            return replace(settled, ran=just_ran)
        return PENDING

    def _settled(
        self, admin: Path, path: Path, current: str | None, start: str
    ) -> PrepareOutcome | None:
        """The settled state of the current materialisation, or None after starting
        the command (absent, stale or unverifiable ``ok``, or the first retry of an
        interruption). An invalid current fingerprint never matches; it reruns the
        command once per ``start``, then settles without the line."""
        record = read_record(admin)
        if record is not None and record.status == "running":
            record = _recover(admin, record)
        retries = 0
        if record is not None:
            if record.status == "ok" and current is not None and record.fingerprint == current:
                return replace(record, line_shown=True)
            if record.status in ("failed", "timeout"):
                return record
            if record.status == "interrupted":
                if record.retries >= 1:
                    return record
                retries = record.retries + 1
            elif (record.start == start or self._started_by.get(path) == start) and (
                current is None or record.status == "unverified"
            ):
                return replace(record, line_shown=False)  # already rerun for this start
            # ok with other inputs (stale), unverifiable, or a first retry: rerun.
        self._started_by[path] = start
        self._start(path, admin, current, retries, wanted=self._wanted_for.get(path), start=start)
        return None

    def _start(
        self,
        path: Path,
        admin: Path,
        current: str | None,
        retries: int,
        *,
        wanted: Wanted | None,
        start: str = "",
    ) -> None:
        task = asyncio.create_task(
            self._attempt(path, admin, current, retries, wanted=wanted, start=start),
            name=f"prepare:{path.name}",
        )
        self._tasks[path] = task

    async def _attempt(
        self,
        path: Path,
        admin: Path,
        current: str | None,
        retries: int,
        *,
        wanted: Wanted | None,
        start: str = "",
    ) -> PrepareOutcome:
        started = self._wall_clock()
        base = PrepareOutcome(
            "running",
            str(path),
            log=str(log_path(path)),
            started_at=started,
            fingerprint=current or "",
            retries=retries,
            start=start,
        )
        try:
            _write_record(admin, base)
        except OSError as exc:
            outcome = replace(base, status="failed", reason=f"could not record attempt: {exc}")
            self._unrecorded[path] = outcome
            LOG.warning("worktree prepare not started path=%s reason=%s", path, outcome.reason)
            return outcome
        try:
            outcome = await self._run(path, admin, base, wanted)
            if outcome.ok:
                # What it prepared, read after it ran (it may itself touch an input). An
                # unreadable one is never recorded with an ``ok``.
                after = await asyncio.to_thread(fingerprint, path, self.inputs)
                outcome = (
                    replace(outcome, fingerprint=after)
                    if after is not None
                    else replace(
                        outcome,
                        status="unverified",
                        fingerprint="",
                        reason="exit 0, but its dependency inputs could not be read",
                    )
                )
        except asyncio.CancelledError:
            outcome = replace(
                base, status="interrupted", reason="cancelled (daemon stopping)", pid=0
            )
            self._finish(path, admin, outcome)
            raise
        self._finish(path, admin, outcome)
        return outcome

    def _finish(self, path: Path, admin: Path, outcome: PrepareOutcome) -> None:
        try:
            _write_record(admin, outcome)
        except OSError as exc:
            # The run-once state is not durable: never treat this as prepared.
            self._unrecorded[path] = outcome
            LOG.warning(
                "worktree prepare outcome not recorded path=%s status=%s reason=%s",
                path,
                outcome.status,
                exc,
            )
        if outcome.ok:
            LOG.info(
                "worktree prepared path=%s duration_s=%.1f log=%s",
                path,
                outcome.duration_s,
                outcome.log,
            )
        else:
            LOG.warning(
                "worktree prepare %s path=%s reason=%s log=%s%s",
                outcome.status,
                path,
                outcome.reason,
                outcome.log,
                "" if outcome.status == "interrupted" else " (the stage starts anyway)",
            )

    async def _run(
        self, path: Path, admin: Path, base: PrepareOutcome, wanted: Wanted | None
    ) -> PrepareOutcome:
        argv = resolve_argv(self.argv, path)
        log = log_path(path)
        began = time.monotonic()

        def done(status: str, reason: str = "") -> PrepareOutcome:
            return replace(
                base,
                status=status,
                reason=reason[:_REASON_CHARS],
                duration_s=round(time.monotonic() - began, 3),
                pid=0,
                pid_start="",
            )

        try:
            gh = admin / _GH_DIR
            gh.mkdir(mode=0o700, exist_ok=True)
        except OSError as exc:
            return done("failed", f"gh config dir: {exc.strerror or type(exc).__name__}")
        env = self._env(gh)
        headers = await asyncio.to_thread(unsafe_extraheaders, path, env)
        if headers is None:
            return done("failed", "unsafe git config: extraheader present (unreadable)")
        if headers:
            # Never send a request the worktree's config would authenticate.
            return done("failed", f"unsafe git config: extraheader present ({', '.join(headers)})")
        try:
            fd = _open_log(log)
        except OSError as exc:
            return done("failed", f"log: {exc.strerror or type(exc).__name__}")
        try:
            try:
                _write(fd, f"# worktree_prepare {json.dumps(argv)} in {path}\n".encode())
            except OSError as exc:
                return done("failed", f"log: {exc.strerror or type(exc).__name__}")
            if wanted is not None and not await _ask(wanted):
                # Stopped during the preflight: never start the command.
                return done("interrupted", "cancelled: the run is no longer starting")
            try:
                proc = await asyncio.create_subprocess_exec(
                    *argv,
                    cwd=path,
                    env=env,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=fd,
                    stderr=asyncio.subprocess.STDOUT,
                    start_new_session=True,
                )
            except OSError as exc:
                return done("failed", f"could not start: {exc.strerror or type(exc).__name__}")
        finally:
            os.close(fd)
        with contextlib.suppress(OSError):
            _write_record(admin, replace(base, pid=proc.pid, pid_start=_proc_start(proc.pid)))
        waiter = asyncio.ensure_future(proc.wait())
        deadline = time.monotonic() + self.timeout_seconds
        try:
            while True:
                left = deadline - time.monotonic()
                if left <= 0:
                    await _kill_and_reap(proc, waiter)
                    return done("timeout", f"timed out after {self.timeout_seconds:g}s")
                finished, _ = await asyncio.wait({waiter}, timeout=min(left, self._wanted_poll))
                if finished:
                    break
                if wanted is not None and not await _ask(wanted):
                    await _kill_and_reap(proc, waiter)
                    return done("interrupted", "cancelled: the run is no longer starting")
        except asyncio.CancelledError:
            await _kill_and_reap(proc, waiter)
            raise
        if proc.returncode == 0:
            return done("ok")
        last = _last_line(log)
        return done("failed", f"exit {proc.returncode}{f': {last}' if last else ''}")

    async def close(self) -> None:
        """Daemon shutdown: cancel every running command (process groups killed, each
        recorded ``interrupted``)."""
        tasks = [t for t in self._tasks.values() if not t.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def _ask(wanted: Wanted) -> bool:
    try:
        return await wanted()
    except Exception:  # an unreadable answer keeps the command running
        return True


async def _kill_and_reap(proc: asyncio.subprocess.Process, waiter: asyncio.Future[int]) -> None:
    _kill_group(proc.pid)
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.shield(waiter)


def _kill_group(pid: int) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signal.SIGKILL)


def _proc_start(pid: int) -> str:
    """The process's start time in clock ticks since boot ("" when unknown): with the pid
    it identifies the process across a daemon restart (pids are reused)."""
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="ascii", errors="replace")
    except OSError:
        return ""
    fields = text.rsplit(")", 1)[-1].split()
    return fields[19] if len(fields) > 19 else ""


def _alive(record: PrepareOutcome) -> bool:
    return (
        bool(record.pid)
        and bool(record.pid_start)
        and _proc_start(record.pid) == (record.pid_start)
    )


def _recover(admin: Path, record: PrepareOutcome) -> PrepareOutcome:
    """A ``running`` record no task of this daemon runs: the daemon died mid-run. Kill a
    process group it provably left behind, record ``interrupted``."""
    if _alive(record):
        _kill_group(record.pid)
    outcome = replace(record, status="interrupted", reason="interrupted (daemon restarted)")
    try:
        _write_record(admin, outcome)
    except OSError as exc:
        LOG.warning("worktree prepare recovery not recorded path=%s reason=%s", record.path, exc)
    LOG.warning("worktree prepare was interrupted path=%s", record.path)
    return outcome


def _open_log(log: Path) -> int:
    """The log, opened for writing without following a symlink or blocking (a FIFO),
    a regular file only, mode 0600, truncated. Raises ``OSError``."""
    fd = os.open(
        log,
        os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
        0o600,
    )
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(0, "not a regular file")
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        os.set_blocking(fd, True)
    except BaseException:
        os.close(fd)
        raise
    return fd


def _write(fd: int, data: bytes) -> None:
    """One write to the log (separate, so tests can inject a full disk)."""
    os.write(fd, data)


def _last_line(log: Path) -> str:
    try:
        with log.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            stream.seek(max(0, stream.tell() - _TAIL_BYTES))
            tail = stream.read().decode("utf-8", "replace")
    except OSError:
        return ""
    lines = [line.strip() for line in tail.splitlines() if line.strip()]
    return lines[-1] if lines and not lines[-1].startswith("# worktree_prepare ") else ""


def read_record(admin: Path) -> PrepareOutcome | None:
    """The recorded state in a worktree's Git directory (None: absent)."""
    try:
        data = json.loads((admin / PREPARE_RECORD).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return PrepareOutcome.from_json(data)


def _write_record(admin: Path, outcome: PrepareOutcome) -> None:
    target = admin / PREPARE_RECORD
    temporary = target.with_suffix(".tmp")
    data = {**outcome.to_json(), "ran": False, "line_shown": False}  # derived, not state
    temporary.write_text(json.dumps(data), encoding="utf-8")
    os.replace(temporary, target)


def prepare_summary(source_clone: Path, *, configured: bool) -> dict[str, object]:
    """Recorded prepare states of the clone's live worktrees (``status``/``doctor``): ok
    count, ``running`` (a live process), and every other state, an orphaned ``running``
    record showing as ``interrupted``. Reads the worktrees' own Git directories directly
    (no Git subprocess, no lock)."""
    ok = 0
    running: list[str] = []
    problems: list[dict[str, object]] = []
    base = source_clone / ".git" / "worktrees"
    try:
        admins = sorted(p for p in base.iterdir() if p.is_dir())
    except OSError:
        admins = []
    for admin in admins:
        outcome = read_record(admin)
        if outcome is None or not Path(outcome.path).is_dir():
            continue
        status = outcome.status
        if status == "running":
            if _alive(outcome):
                running.append(outcome.path)
                continue
            status = "interrupted"  # no live process: the daemon died mid-run
        if status == "ok":
            ok += 1
            continue
        problems.append(
            {"path": outcome.path, "status": status, "reason": outcome.reason, "log": outcome.log}
        )
    return {"configured": configured, "ok": ok, "running": running, "failed": problems}


__all__ = [
    "DEFAULT_INPUTS",
    "DEFAULT_TIMEOUT_SECONDS",
    "ENV_ALLOW",
    "GIT_RESETS",
    "PENDING",
    "PREPARED_LINE",
    "PREPARE_RECORD",
    "PrepareOutcome",
    "WorktreePreparer",
    "fingerprint",
    "log_path",
    "prepare_env",
    "prepare_summary",
    "read_record",
    "resolve_argv",
    "unsafe_extraheaders",
]
