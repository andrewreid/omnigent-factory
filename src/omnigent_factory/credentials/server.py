"""Unix-domain socket front end for the local broker (architecture §6.1, §6.2).

The socket lives in a private 0700 runtime directory and is itself 0600. Each connection
is authenticated with ``SO_PEERCRED`` (same UID as the daemon) *and* a per-stage capability
secret; one newline-terminated JSON request per connection, one JSON reply.

Operations:

``{"op": "token", "session_id", "secret", "repository"}``
    -> ``{"ok": true, "token", "expires_at_us", "profile"}`` or ``{"ok": false, "reason"}``.

``{"op": "register_worktree", "session_id", "secret", "worker_id", "path", "branch"}``
    Wire a worker's isolated worktree only if ``(worker_id, path, branch)`` exactly equals a
    tuple the daemon recorded for that stage (:meth:`BrokerServer.authorize_worker`), the
    stage's issuance is enabled and its execution gate is open *now*, and the worktree
    verifies (dedicated clone, owned roots, branch). The worker gets its own capability
    bound to its recorded role, so a read-only reviewer never receives build credentials.
    Anything else fails closed.

Errors never include token material; the reply to a refusal carries only a reason code.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from omnigent_factory.core.effects import CredentialProfile
from omnigent_factory.credentials.broker import LocalCredentialBroker
from omnigent_factory.credentials.capabilities import CapabilityRecord, ensure_private_dir
from omnigent_factory.credentials.worktree import (
    BotIdentity,
    StageWiring,
    Workspaces,
    WorktreeError,
)
from omnigent_factory.ports.credentials import TokenRefusal

MAX_REQUEST_BYTES = 64 * 1024


def peer_uid(sock: socket.socket) -> int | None:
    """Return the connecting process's UID via ``SO_PEERCRED`` (Linux)."""
    try:
        raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    except (OSError, AttributeError):
        return None
    _pid, uid, _gid = struct.unpack("3i", raw)
    return int(uid) if uid >= 0 else None


@dataclass(frozen=True, slots=True)
class WorkerGrant:
    """A daemon-recorded worker worktree: exact path, exact branch and fixed role.

    Only these tuples may be wired through a stage capability; there is no branch-prefix
    scope. ``profile`` is the worker's own role (e.g. read-only for a review worker) and is
    capped by the stage's enabled profile at issuance.
    """

    worker_id: str
    path: Path
    branch: str
    profile: CredentialProfile


@runtime_checkable
class WorkerGrantStore(Protocol):
    """Durable daemon-recorded worker tuples (implemented by the service)."""

    async def save(self, stage_session_id: str, grant: WorkerGrant) -> None: ...

    async def delete(self, stage_session_id: str) -> None: ...

    async def load(self) -> Mapping[str, Mapping[str, WorkerGrant]]: ...


class StageProvisioner:
    """``CredentialProvisioner`` for the Omnigent adapter (stage capabilities)."""

    def __init__(self, broker: LocalCredentialBroker, server: BrokerServer) -> None:
        self.broker = broker
        self.server = server

    async def provision(self, session_id: str) -> CapabilityRecord:
        return await self.broker.provision(session_id)


class BrokerServer:
    def __init__(
        self,
        broker: LocalCredentialBroker,
        socket_path: Path,
        *,
        workspaces: Workspaces | None = None,
        identity: BotIdentity | None = None,
        helper_command: str | None = None,
        expected_uid: int | None = None,
        grants: WorkerGrantStore | None = None,
    ) -> None:
        self.broker = broker
        self.grants = grants
        self.socket_path = socket_path
        self.workspaces = workspaces
        self.identity = identity
        self.helper_command = helper_command
        self.expected_uid = os.getuid() if expected_uid is None else expected_uid
        self._workers: dict[str, dict[str, WorkerGrant]] = {}
        self._server: asyncio.base_events.Server | None = None

    async def authorize_worker(self, stage_session_id: str, grant: WorkerGrant) -> None:
        """Record one exact worker tuple the daemon approved for this stage (durably)."""
        if not grant.path.is_absolute():
            raise ValueError("worker worktree path must be absolute")
        if self.grants is not None:
            await self.grants.save(stage_session_id, grant)
        self._workers.setdefault(stage_session_id, {})[grant.worker_id] = grant

    async def revoke_workers(self, stage_session_id: str) -> None:
        # Forget in memory first so a failing store can only leave a stale durable row,
        # which restore() reloads but the stage's closed gate still refuses.
        self._workers.pop(stage_session_id, None)
        if self.grants is not None:
            await self.grants.delete(stage_session_id)

    async def restore(self) -> None:
        """Boot: reload recorded worker tuples. Registration still needs an open gate."""
        if self.grants is None:
            return
        loaded = await self.grants.load()
        self._workers = {stage: dict(grants) for stage, grants in loaded.items()}

    async def start(self) -> None:
        ensure_private_dir(self.socket_path.parent)
        with contextlib.suppress(FileNotFoundError):
            self.socket_path.unlink()
        old = os.umask(0o177)
        try:
            self._server = await asyncio.start_unix_server(self._handle, path=str(self.socket_path))
        finally:
            os.umask(old)
        os.chmod(self.socket_path, 0o600)

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        with contextlib.suppress(FileNotFoundError):
            self.socket_path.unlink()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            sock = writer.get_extra_info("socket")
            uid = peer_uid(sock) if sock is not None else None
            if uid is None or uid != self.expected_uid:
                reply: Mapping[str, Any] = {"ok": False, "reason": "peer-not-authorised"}
            else:
                try:
                    line = await reader.readuntil(b"\n")
                except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
                    line = b""
                reply = await self.dispatch(line[:MAX_REQUEST_BYTES])
            writer.write(json.dumps(reply).encode("utf-8") + b"\n")
            await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def dispatch(self, raw: bytes) -> Mapping[str, Any]:
        try:
            req = json.loads(raw)
        except ValueError:
            return {"ok": False, "reason": "malformed-request"}
        if not isinstance(req, dict):
            return {"ok": False, "reason": "malformed-request"}
        sid, secret = req.get("session_id"), req.get("secret")
        if not isinstance(sid, str) or not isinstance(secret, str):
            return {"ok": False, "reason": "malformed-request"}
        op = req.get("op")
        if op == "token":
            repo = req.get("repository")
            if not isinstance(repo, str):
                return {"ok": False, "reason": "malformed-request"}
            grant = await self.broker.request_token(sid, secret, repo)
            if isinstance(grant, TokenRefusal):
                return {"ok": False, "reason": grant.reason}
            return {
                "ok": True,
                "token": grant.token,
                "expires_at_us": grant.expires_at_us,
                "profile": grant.profile.value,
            }
        if op == "register_worktree":
            path, branch, worker = req.get("path"), req.get("branch"), req.get("worker_id")
            if not all(isinstance(v, str) for v in (path, branch, worker)):
                return {"ok": False, "reason": "malformed-request"}
            return await self._register(sid, secret, str(worker), Path(str(path)), str(branch))
        return {"ok": False, "reason": "unknown-op"}

    async def _register(
        self, sid: str, secret: str, worker_id: str, path: Path, branch: str
    ) -> Mapping[str, Any]:
        if self.broker.capabilities.verify(sid, secret) is None:
            return {"ok": False, "reason": "invalid-capability"}
        if not self.broker.issuance_enabled(sid):
            return {"ok": False, "reason": "issuance-disabled"}
        decision = await self.broker.gate(sid)
        if not decision.allowed:
            return {"ok": False, "reason": decision.reason or "execution-gate-closed"}
        grant = self._workers.get(sid, {}).get(worker_id)
        if grant is None or grant.branch != branch or grant.path != path:
            return {"ok": False, "reason": "worker-not-recorded"}
        if self.workspaces is None or self.identity is None:
            return {"ok": False, "reason": "worktree-registration-unavailable"}
        try:
            verified = await asyncio.to_thread(self.workspaces.verify_worktree, path, branch)
            record = await self.broker.provision_worker(sid, worker_id, grant.profile)
            wiring = StageWiring(
                record.session_id, record.path, self.socket_path, self.workspaces.repository
            )
            await asyncio.to_thread(
                self.workspaces.configure,
                verified,
                wiring,
                self.identity,
                helper_command=self.helper_command,
            )
        except WorktreeError as exc:
            return {"ok": False, "reason": f"worktree-rejected: {exc}"}
        return {
            "ok": True,
            "path": str(verified.path),
            "branch": branch,
            "profile": grant.profile.value,
        }
