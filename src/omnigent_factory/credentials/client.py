"""Synchronous broker client used by the Git credential helper and the ``gh`` wrapper.

Every invocation asks the broker; there is no client-side token cache, so a fence or
revocation takes effect on the next helper/wrapper call.
"""

from __future__ import annotations

import json
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from omnigent_factory.credentials.capabilities import CapabilityFile, read_capability_file

DEFAULT_TIMEOUT_S = 30.0


@dataclass(frozen=True, slots=True)
class BrokerReply:
    ok: bool
    reason: str = ""
    token: str | None = None
    expires_at_us: int | None = None


def _call(socket_path: Path, request: dict[str, Any], timeout: float) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(str(socket_path))
        sock.sendall(json.dumps(request).encode("utf-8") + b"\n")
        chunks: list[bytes] = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
            if chunk.endswith(b"\n"):
                break
    reply = json.loads(b"".join(chunks) or b"{}")
    return reply if isinstance(reply, dict) else {}


def request_token(
    cap: CapabilityFile, repository: str, *, timeout: float = DEFAULT_TIMEOUT_S
) -> BrokerReply:
    try:
        reply = _call(
            cap.socket_path,
            {
                "op": "token",
                "session_id": cap.session_id,
                "secret": cap.secret,
                "repository": repository,
            },
            timeout,
        )
    except (OSError, ValueError):
        return BrokerReply(False, "broker-unreachable")
    if reply.get("ok") is True and isinstance(reply.get("token"), str):
        expires = reply.get("expires_at_us")
        return BrokerReply(
            True, token=reply["token"], expires_at_us=expires if isinstance(expires, int) else None
        )
    return BrokerReply(False, str(reply.get("reason") or "refused"))


def register_worktree(
    cap: CapabilityFile,
    worker_id: str,
    path: Path,
    branch: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_S,
) -> BrokerReply:
    try:
        reply = _call(
            cap.socket_path,
            {
                "op": "register_worktree",
                "worker_id": worker_id,
                "session_id": cap.session_id,
                "secret": cap.secret,
                "path": str(path),
                "branch": branch,
            },
            timeout,
        )
    except (OSError, ValueError):
        return BrokerReply(False, "broker-unreachable")
    return BrokerReply(reply.get("ok") is True, str(reply.get("reason") or ""))


def load(path: Path) -> CapabilityFile:
    return read_capability_file(path)
