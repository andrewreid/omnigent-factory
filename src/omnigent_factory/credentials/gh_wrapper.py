"""Factory-owned ``gh`` wrapper (architecture §6.2).

``gh`` takes its token from the environment; there is no executable token-source
setting. The wrapper therefore asks the broker on *every* invocation and executes the
real ``gh`` with a child environment in which inherited token variables are removed and
only the broker token is present, prompting is disabled and configuration is isolated
(``GH_CONFIG_DIR``). It never runs ``gh auth login`` and never touches the owner's
``gh`` configuration.

:func:`install_gh_wrapper` writes an executable named ``gh`` into a factory-owned
directory outside every repository; its absolute path is handed to the stage message.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import NoReturn, TextIO

from omnigent_factory.credentials.capabilities import (
    CapabilityFile,
    CapabilityFileError,
    read_capability_file,
)
from omnigent_factory.credentials.client import BrokerReply, request_token
from omnigent_factory.credentials.worktree import git_env

#: Inherited variables that could carry another identity's token or redirect gh.
SCRUBBED_ENV = (
    "GH_TOKEN",
    "GITHUB_TOKEN",
    "GH_ENTERPRISE_TOKEN",
    "GITHUB_ENTERPRISE_TOKEN",
    "GH_HOST",
    "GH_CONFIG_DIR",
)

ENV_CAPABILITY_FILE = "OMNIGENT_FACTORY_CAPABILITY_FILE"


def child_env(base: Mapping[str, str], token: str, config_dir: Path) -> dict[str, str]:
    env = {k: v for k, v in base.items() if k not in SCRUBBED_ENV}
    env["GH_TOKEN"] = token
    env["GH_PROMPT_DISABLED"] = "1"
    env["GH_NO_UPDATE_NOTIFIER"] = "1"
    env["GH_CONFIG_DIR"] = str(config_dir)
    return env


def _capability_path(env: Mapping[str, str]) -> Path | None:
    explicit = env.get(ENV_CAPABILITY_FILE)
    if explicit:
        return Path(explicit)
    proc = subprocess.run(
        ["git", "config", "--get", "factory.capabilityFile"],  # noqa: S607
        capture_output=True,
        text=True,
        env=git_env(),
        check=False,
        timeout=30,
    )
    value = proc.stdout.strip()
    return Path(value) if value else None


Exec = Callable[[str, Sequence[str], Mapping[str, str]], int]


def _exec(real: str, args: Sequence[str], env: Mapping[str, str]) -> NoReturn:  # pragma: no cover
    os.execve(real, [real, *args], dict(env))  # noqa: S606 - deliberate exec of real gh


def run(
    argv: Sequence[str],
    env: Mapping[str, str],
    stderr: TextIO,
    *,
    execute: Exec = _exec,
    requester: Callable[[CapabilityFile, str], BrokerReply] = request_token,
) -> int:
    """``argv`` is ``--real-gh PATH --config-dir DIR -- <gh args...>``."""
    try:
        sep = list(argv).index("--")
    except ValueError:
        stderr.write("omnigent-factory gh wrapper: malformed invocation\n")
        return 2
    opts, gh_args = list(argv[:sep]), list(argv[sep + 1 :])
    try:
        real = opts[opts.index("--real-gh") + 1]
        config_dir = Path(opts[opts.index("--config-dir") + 1])
    except (ValueError, IndexError):
        stderr.write("omnigent-factory gh wrapper: malformed invocation\n")
        return 2
    cap_path = _capability_path(env)
    if cap_path is None:
        stderr.write("omnigent-factory gh wrapper: no stage capability configured\n")
        return 1
    try:
        cap = read_capability_file(cap_path)
    except CapabilityFileError as exc:
        stderr.write(f"omnigent-factory gh wrapper: {exc}\n")
        return 1
    reply = requester(cap, cap.repository)
    if not reply.ok or reply.token is None:
        stderr.write(f"omnigent-factory gh wrapper: broker refused: {reply.reason}\n")
        return 1
    config_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    return execute(real, gh_args, child_env(env, reply.token, config_dir))


def install_gh_wrapper(bin_dir: Path, real_gh: Path, config_dir: Path) -> Path:
    """Write ``bin_dir/gh`` (0700). ``real_gh`` must be the absolute real binary."""
    if not real_gh.is_absolute():
        raise ValueError("real gh path must be absolute")
    bin_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = bin_dir / "gh"
    if target.resolve() == real_gh.resolve():
        raise ValueError("wrapper would replace the real gh")
    script = (
        "#!/bin/sh\n"
        f"exec {shlex.quote(sys.executable)} -m omnigent_factory.credentials.gh_wrapper "
        f"--real-gh {shlex.quote(str(real_gh))} --config-dir {shlex.quote(str(config_dir))} "
        '-- "$@"\n'
    )
    tmp = target.with_name("gh.tmp")
    tmp.write_text(script, encoding="utf-8")
    os.chmod(tmp, 0o700)
    os.replace(tmp, target)
    return target


def main() -> None:  # pragma: no cover - exercised as a subprocess in tests
    sys.exit(run(sys.argv[1:], os.environ, sys.stderr, execute=_exec))


if __name__ == "__main__":  # pragma: no cover
    main()
