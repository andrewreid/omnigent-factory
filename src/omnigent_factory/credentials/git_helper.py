"""Git credential helper wired into a factory worktree (architecture §6.2).

Invoked by Git as ``<helper> get|store|erase`` with the credential description on stdin.
It honours only ``get`` for ``https://github.com/<configured repository>`` and answers
``username=x-access-token`` plus a broker-issued token. ``store`` is ignored; ``erase``
has no cache to clear (the broker is asked on every call). On any refusal it answers
``quit=1`` so Git stops without falling back to any other (owner) helper or a prompt.

Stage handles come from the worktree's own config (``factory.capabilityFile``,
``factory.repository``); the capability secret itself is read from the private file.
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TextIO

from omnigent_factory.credentials.capabilities import CapabilityFileError, read_capability_file
from omnigent_factory.credentials.client import BrokerReply, request_token
from omnigent_factory.credentials.worktree import git_env


def parse_description(stream: TextIO) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for raw in stream:
        line = raw.rstrip("\n")
        if not line:
            break
        key, sep, value = line.partition("=")
        if sep:
            attrs[key] = value
    return attrs


def normalise_repo_path(path: str) -> str:
    p = path.strip("/")
    if p.endswith(".git"):
        p = p[: -len(".git")]
    return p.lower()


def _git_config(key: str) -> str | None:
    proc = subprocess.run(  # noqa: S603 - fixed argv
        ["git", "config", "--get", key],  # noqa: S607 - git from PATH, as Git itself does
        capture_output=True,
        text=True,
        env=git_env(),
        check=False,
        timeout=30,
    )
    return proc.stdout.strip() or None


Requester = Callable[..., BrokerReply]


def run(
    argv: Sequence[str],
    stdin: TextIO,
    stdout: TextIO,
    stderr: TextIO,
    *,
    config: Callable[[str], str | None] = _git_config,
    requester: Requester = request_token,
) -> int:
    op = argv[0] if argv else ""
    if op != "get":
        # store: never persist; erase: nothing cached client-side.
        parse_description(stdin)
        return 0
    attrs = parse_description(stdin)
    repo = config("factory.repository")
    cap_path = config("factory.capabilityFile")
    if repo is None or cap_path is None:
        return _refuse(stdout, stderr, "worktree is not wired for the factory")
    if attrs.get("protocol") != "https" or attrs.get("host", "").lower() != "github.com":
        return _refuse(stdout, stderr, "only https://github.com is served")
    if normalise_repo_path(attrs.get("path", "")) != repo.lower():
        return _refuse(stdout, stderr, "repository outside the stage scope")
    try:
        cap = read_capability_file(Path(cap_path))
    except CapabilityFileError as exc:
        return _refuse(stdout, stderr, str(exc))
    if cap.repository.lower() != repo.lower():
        return _refuse(stdout, stderr, "capability names a different repository")
    reply = requester(cap, repo)
    if not reply.ok or reply.token is None:
        return _refuse(stdout, stderr, f"broker refused: {reply.reason}")
    stdout.write("username=x-access-token\n")
    stdout.write(f"password={reply.token}\n")
    return 0


def _refuse(stdout: TextIO, stderr: TextIO, reason: str) -> int:
    stderr.write(f"omnigent-factory credential helper: {reason}\n")
    stdout.write("quit=1\n")
    return 0


def main() -> None:  # pragma: no cover - exercised through real git in tests
    sys.exit(run(sys.argv[1:], sys.stdin, sys.stdout, sys.stderr))


if __name__ == "__main__":  # pragma: no cover
    main()
