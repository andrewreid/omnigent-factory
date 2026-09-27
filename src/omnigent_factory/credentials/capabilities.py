"""Per-stage capability secrets (architecture §6.1).

A capability is not a GitHub token: it only lets a same-UID caller *ask* the broker for
one. The secret lives in a private 0600 file outside any worktree (under a 0700
directory); only its SHA-256 is held by the broker. No capability value appears in
prompts, labels, Git config or logs - the worktree config names only the file path.

Provisioning a session again rotates its capability (new generation, new secret); an old
secret stops matching immediately.

Durability (T3 FOLLOW_UP): with a :class:`CapabilityStore` the hash, generation and worker
binding of every capability are persisted before :meth:`CapabilityRegistry.provision`
returns, and :meth:`CapabilityRegistry.restore` reloads them on boot. Generations never
repeat, even across revocation and restart. Issuance enablement is *not* persisted: after a
restart every capability verifies but issuance stays denied until the service re-enables
it after rechecking the current execution gate.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from omnigent_factory.core.effects import CredentialProfile

_FILE_VERSION = 1


class CapabilityFileError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CapabilityFile:
    """Contents of a stage capability file (read by the helper and ``gh`` wrapper)."""

    capability_id: str
    session_id: str
    secret: str
    socket_path: Path
    repository: str


@dataclass(frozen=True, slots=True)
class CapabilityRecord:
    """A capability's non-secret identity. ``worker_of`` names the owning stage session
    for a registered worker capability, whose fixed role is ``worker_profile``."""

    capability_id: str
    session_id: str
    secret_sha256: str
    generation: int
    path: Path
    worker_of: str | None = None
    worker_profile: CredentialProfile | None = None
    revoked: bool = False


@runtime_checkable
class CapabilityStore(Protocol):
    """Durable capability hashes/generations (never secrets). Implemented by the service."""

    async def save(self, record: CapabilityRecord) -> None:
        """Persist (or rotate) ``record``; generations must strictly increase."""
        ...

    async def revoke(self, session_id: str) -> None:
        """Mark the session's capability revoked, keeping its generation."""
        ...

    async def load(self) -> tuple[CapabilityRecord, ...]:
        """Every persisted record, revoked ones included."""
        ...


def secret_digest(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def ensure_private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    st = path.stat()
    if st.st_uid != os.getuid():
        raise CapabilityFileError(f"{path} is not owned by the daemon user")
    if stat.S_IMODE(st.st_mode) != 0o700:
        os.chmod(path, 0o700)


def write_capability_file(path: Path, cap: CapabilityFile) -> None:
    payload = {
        "version": _FILE_VERSION,
        "capability_id": cap.capability_id,
        "session_id": cap.session_id,
        "secret": cap.secret,
        "socket": str(cap.socket_path),
        "repository": cap.repository,
    }
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, json.dumps(payload).encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)


def read_capability_file(path: Path) -> CapabilityFile:
    """Load a capability file, refusing anything not private to the current user."""
    try:
        st = path.stat()
    except OSError as exc:
        raise CapabilityFileError("capability file unavailable") from exc
    if st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) & 0o077:
        raise CapabilityFileError("capability file is not private to the daemon user")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("version") != _FILE_VERSION:
            raise CapabilityFileError("unsupported capability file version")
        return CapabilityFile(
            capability_id=str(raw["capability_id"]),
            session_id=str(raw["session_id"]),
            secret=str(raw["secret"]),
            socket_path=Path(str(raw["socket"])),
            repository=str(raw["repository"]),
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise CapabilityFileError("malformed capability file") from exc


class CapabilityRegistry:
    """Capability hash registry plus private secret files.

    With ``store`` every provision/revocation is persisted before it takes effect in
    memory, and :meth:`restore` reloads the records after a restart. Without a store the
    registry is volatile and test-only: construction then requires ``volatile_ok=True``.
    Secrets themselves are never persisted by the daemon.
    """

    def __init__(
        self,
        directory: Path,
        socket_path: Path,
        repository: str,
        *,
        store: CapabilityStore | None = None,
        volatile_ok: bool = False,
    ) -> None:
        if store is None and volatile_ok is not True:
            raise CapabilityFileError(
                "volatile CapabilityRegistry is test-only; persist records durably"
            )
        self.directory = directory
        self.socket_path = socket_path
        self.repository = repository
        self.store = store
        self._records: dict[str, CapabilityRecord] = {}
        self._generations: dict[str, int] = {}

    async def restore(self) -> tuple[CapabilityRecord, ...]:
        """Reload persisted records (boot). Returns the live, unrevoked ones."""
        if self.store is None:
            return tuple(self._records.values())
        rows = await self.store.load()
        self._generations = {r.session_id: r.generation for r in rows}
        self._records = {r.session_id: r for r in rows if not r.revoked}
        return tuple(self._records.values())

    async def provision(
        self,
        session_id: str,
        *,
        worker_of: str | None = None,
        worker_profile: CredentialProfile | None = None,
    ) -> CapabilityRecord:
        """Create (or rotate) the capability for ``session_id``."""
        if (worker_of is None) != (worker_profile is None):
            raise ValueError("a worker capability needs both its stage and its profile")
        ensure_private_dir(self.directory)
        generation = self._generations.get(session_id, 0) + 1
        secret = secrets.token_urlsafe(32)
        capability_id = f"cap_{secrets.token_hex(12)}"
        path = self.directory / f"{_safe(session_id)}.cap"
        record = CapabilityRecord(
            capability_id,
            session_id,
            secret_digest(secret),
            generation,
            path,
            worker_of=worker_of,
            worker_profile=worker_profile,
        )
        # Durable hash first: a crash before the file write leaves an unusable (fail
        # closed) capability that the next provision rotates; never a usable secret whose
        # hash is lost.
        if self.store is not None:
            await self.store.save(record)
        self._generations[session_id] = generation
        self._records.pop(session_id, None)
        write_capability_file(
            path,
            CapabilityFile(capability_id, session_id, secret, self.socket_path, self.repository),
        )
        self._records[session_id] = record
        return record

    def verify(self, session_id: str, secret: str) -> CapabilityRecord | None:
        record = self._records.get(session_id)
        if record is None:
            return None
        if not hmac.compare_digest(record.secret_sha256, secret_digest(secret)):
            return None
        return record

    async def revoke(self, session_id: str) -> None:
        record = self._records.pop(session_id, None)
        if self.store is not None and (record is not None or session_id in self._generations):
            await self.store.revoke(session_id)
        if record is not None:
            record.path.unlink(missing_ok=True)

    def get(self, session_id: str) -> CapabilityRecord | None:
        return self._records.get(session_id)

    def usable(self, session_id: str) -> bool:
        """Whether the restored record still has its exact private runtime secret file."""
        record = self._records.get(session_id)
        if record is None:
            return False
        try:
            value = read_capability_file(record.path)
        except CapabilityFileError:
            return False
        return (
            value.capability_id == record.capability_id
            and value.session_id == session_id
            and value.socket_path == self.socket_path
            and value.repository == self.repository
            and hmac.compare_digest(record.secret_sha256, secret_digest(value.secret))
        )

    def records(self) -> tuple[CapabilityRecord, ...]:
        return tuple(self._records.values())


def _safe(session_id: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in session_id)[:128]
