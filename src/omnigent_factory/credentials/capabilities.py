"""Per-stage capability secrets (architecture §6.1).

A capability is not a GitHub token: it only lets a same-UID caller *ask* the broker for
one. The secret lives in a private 0600 file outside any worktree (under a 0700
directory); only its SHA-256 is held by the broker. No capability value appears in
prompts, labels, Git config or logs - the worktree config names only the file path.

Provisioning a session again rotates its capability (new generation, new secret); an old
secret stops matching immediately.
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
    capability_id: str
    session_id: str
    secret_sha256: str
    generation: int
    path: Path


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
    """In-memory hash registry plus private secret files. NOT FOR PRODUCTION AS-IS.

    Hashes/generations live only in memory and are lost on restart. The architecture's
    ``capabilities`` table is not exposed by the Task-1 store API; before Task 4/5
    integration the service must persist :class:`CapabilityRecord` values (via
    :meth:`records` / :meth:`restore` or a durable subclass), keep issuance default-deny on
    boot and re-enable only after re-checking current gates. Construction requires
    ``volatile_ok=True`` so the volatile form cannot be wired silently. Secrets themselves
    are never persisted by the daemon.
    """

    def __init__(
        self, directory: Path, socket_path: Path, repository: str, *, volatile_ok: bool
    ) -> None:
        if volatile_ok is not True:
            raise CapabilityFileError(
                "volatile CapabilityRegistry is test-only; persist records durably"
            )
        self.directory = directory
        self.socket_path = socket_path
        self.repository = repository
        self._records: dict[str, CapabilityRecord] = {}

    def provision(self, session_id: str) -> CapabilityRecord:
        """Create (or rotate) the capability for ``session_id``."""
        ensure_private_dir(self.directory)
        prior = self._records.get(session_id)
        generation = prior.generation + 1 if prior is not None else 1
        secret = secrets.token_urlsafe(32)
        capability_id = f"cap_{secrets.token_hex(12)}"
        path = self.directory / f"{_safe(session_id)}.cap"
        write_capability_file(
            path,
            CapabilityFile(capability_id, session_id, secret, self.socket_path, self.repository),
        )
        record = CapabilityRecord(
            capability_id, session_id, secret_digest(secret), generation, path
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

    def revoke(self, session_id: str) -> None:
        record = self._records.pop(session_id, None)
        if record is not None:
            record.path.unlink(missing_ok=True)

    def get(self, session_id: str) -> CapabilityRecord | None:
        return self._records.get(session_id)

    def records(self) -> tuple[CapabilityRecord, ...]:
        return tuple(self._records.values())

    def restore(self, records: tuple[CapabilityRecord, ...]) -> None:
        self._records = {r.session_id: r for r in records}


def _safe(session_id: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in session_id)[:128]
