"""Local credential broker (architecture §6.1): implements ``ports.credentials.CredentialBroker``.

Issuance requires *all* of:

1. a capability secret matching the session's current capability generation;
2. issuance enabled for the session by an ``ENABLE_ISSUANCE`` effect (denied by default,
   including after every restart, and cleared by ``DISABLE_ISSUANCE``);
3. the target repository equal to the configured one;
4. a fresh :class:`ExecutionGate` decision from persisted state (live, authorised,
   unfenced stage session; bounded checkpoint cleanup allowance is the gate's business).

After a restart, :meth:`LocalCredentialBroker.restore` reloads persisted capabilities
and worker bindings but no issuance: every session stays denied until
:meth:`LocalCredentialBroker.reenable_after_recheck` confirms, from the current persisted
gate, that the stage may hold its fixed profile.

Caller-requested permissions are never accepted: the profile comes from the enable effect
and must agree with the gate. Minted tokens are checked for exactly-requested permissions
and repository, cached per session/profile until shortly before expiry, and every request
re-runs the checks above even on a warm cache. Issuance and fence updates are serialized;
the gate is re-checked after minting. Disabling issuance best-effort revokes cached tokens
(GitHub ``DELETE /installation/token``); that cannot undo in-flight writes or recall
tokens handed out before a daemon crash, which stay valid until expiry.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    CredentialProfile,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    ExecutionContext,
)
from omnigent_factory.credentials.capabilities import CapabilityRecord, CapabilityRegistry
from omnigent_factory.ports.clock import Clock
from omnigent_factory.ports.credentials import CREDENTIAL_EFFECT_KINDS, TokenGrant, TokenRefusal

#: Installation-token permissions per fixed profile (§6.1). Only BUILD may write Workflows
#: (owner decision 2026-10-01: a build may change ``.github/workflows/*`` on its own parcel
#: branch). No Administration or organization Projects for any stage profile.
PROFILE_PERMISSIONS: Mapping[CredentialProfile, Mapping[str, str]] = {
    CredentialProfile.READ_ONLY: {
        "contents": "read",
        "issues": "read",
        "pull_requests": "read",
        "checks": "read",
        "statuses": "read",
        "actions": "read",
        "metadata": "read",
    },
    CredentialProfile.BUILD: {
        "contents": "write",
        "issues": "write",
        "pull_requests": "write",
        "checks": "read",
        "statuses": "read",
        "actions": "read",
        "metadata": "read",
        "workflows": "write",
    },
}

_LEVEL = {"read": 1, "write": 2, "admin": 3}


@dataclass(frozen=True, slots=True)
class GateDecision:
    """Current persisted execution gate for token issuance."""

    allowed: bool
    reason: str = ""
    profile: CredentialProfile | None = None


@runtime_checkable
class ExecutionGate(Protocol):
    """Reads the stage's gate from persisted state (wired by the service)."""

    async def token_gate(self, session_id: str) -> GateDecision: ...


@dataclass(frozen=True, slots=True)
class MintedToken:
    token: str
    expires_at_us: int
    permissions: Mapping[str, str]
    repositories: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MintFailure:
    reason: str


@runtime_checkable
class TokenMinter(Protocol):
    """App installation-token service (Task 2). Never the owner's credentials."""

    async def mint(
        self, repository: str, permissions: Mapping[str, str]
    ) -> MintedToken | MintFailure: ...

    async def revoke(self, token: str) -> bool: ...


def permissions_within(granted: Mapping[str, str], requested: Mapping[str, str]) -> bool:
    """True iff ``granted`` has no key or level beyond ``requested``."""
    for key, level in granted.items():
        want = requested.get(key)
        if want is None or _LEVEL.get(level, 99) > _LEVEL.get(want, 0):
            return False
    return True


def profile_within(worker: CredentialProfile, stage: CredentialProfile) -> bool:
    return worker in (stage, CredentialProfile.READ_ONLY)


def worker_session_id(stage_session_id: str, worker_id: str) -> str:
    return f"{stage_session_id}~worker~{worker_id}"


@dataclass(frozen=True, slots=True)
class WorkerBinding:
    worker_session_id: str
    stage_session_id: str
    profile: CredentialProfile


@dataclass
class _Cached:
    token: MintedToken
    profile: CredentialProfile


@dataclass
class BrokerAudit:
    """Non-secret audit trail (session, outcome, reason)."""

    entries: list[tuple[str, str, str]] = field(default_factory=list)

    def add(self, session_id: str, outcome: str, reason: str = "") -> None:
        self.entries.append((session_id, outcome, reason))


class LocalCredentialBroker:
    def __init__(
        self,
        *,
        gate: ExecutionGate,
        minter: TokenMinter,
        clock: Clock,
        capabilities: CapabilityRegistry,
        repository: str,
        refresh_margin_us: int = 5 * 60 * 1_000_000,
    ) -> None:
        self._gate = gate
        self._minter = minter
        self._clock = clock
        self.capabilities = capabilities
        self.repository = repository
        self._margin = refresh_margin_us
        self._enabled: dict[str, CredentialProfile] = {}
        self._cache: dict[str, _Cached] = {}
        self._workers: dict[str, WorkerBinding] = {}
        self._lock = asyncio.Lock()
        self.audit = BrokerAudit()

    # ------------------------------------------------------------ EffectAdapter

    @property
    def handled_kinds(self) -> frozenset[EffectKind]:
        return CREDENTIAL_EFFECT_KINDS

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        sid = effect.preconditions.session_id
        if sid is None:
            return DefinitiveFailure("credential effect without a session")
        if effect.kind == EffectKind.DISABLE_ISSUANCE:
            await self.disable(sid)
            return Ack(remote_id=sid, detail={"issuance": "disabled"})
        if effect.kind == EffectKind.ENABLE_ISSUANCE:
            raw = effect.args.get("profile")
            try:
                profile = CredentialProfile(str(raw))
            except ValueError:
                return DefinitiveFailure("unknown credential profile")
            if self.capabilities.get(sid) is None:
                return DefinitiveFailure("session has no provisioned capability")
            async with self._lock:
                prior = self._enabled.get(sid)
                if prior is not None and prior != profile:
                    self._drop_cache(sid)
                self._enabled[sid] = profile
            self.audit.add(sid, "enabled", profile.value)
            return Ack(remote_id=sid, detail={"issuance": "enabled", "profile": profile.value})
        return DefinitiveFailure(f"{effect.kind} not handled by the credential broker")

    # ------------------------------------------------------------ capability lifecycle

    async def provision(self, session_id: str) -> CapabilityRecord:
        """Create or rotate a session's capability. Issuance stays as it was (default off)."""
        return await self.capabilities.provision(session_id)

    async def provision_worker(
        self, stage_session_id: str, worker_id: str, profile: CredentialProfile
    ) -> CapabilityRecord:
        """Capability for one daemon-recorded worker of a stage, with its own fixed role.

        A worker token never exceeds the stage's enabled profile, and every request still
        passes the *stage's* issuance flag and execution gate. The binding is persisted
        with the capability so it survives a restart.
        """
        key = worker_session_id(stage_session_id, worker_id)
        record = await self.capabilities.provision(
            key, worker_of=stage_session_id, worker_profile=profile
        )
        self._workers[key] = WorkerBinding(key, stage_session_id, profile)
        return record

    async def restore(self) -> None:
        """Boot: reload capabilities and worker bindings. Issuance stays default-deny."""
        records = await self.capabilities.restore()
        async with self._lock:
            self._enabled.clear()
            self._cache.clear()
            self._workers = {
                r.session_id: WorkerBinding(r.session_id, r.worker_of, r.worker_profile)
                for r in records
                if r.worker_of is not None and r.worker_profile is not None
            }

    async def reenable_after_recheck(self, session_id: str, profile: CredentialProfile) -> bool:
        """Restore issuance for a stage only if its current persisted gate allows ``profile``.

        Called by the service after boot for stages that were executing. A worker key, a
        session without a restored capability, or any closed/mismatched gate stays denied.
        """
        if session_id in self._workers or self.capabilities.get(session_id) is None:
            return False
        decision = await self._gate.token_gate(session_id)
        if not decision.allowed or (decision.profile is not None and decision.profile != profile):
            self.audit.add(session_id, "reenable-refused", decision.reason or "profile")
            return False
        async with self._lock:
            self._enabled[session_id] = profile
        self.audit.add(session_id, "reenabled", profile.value)
        return True

    def workers_of(self, stage_session_id: str) -> tuple[str, ...]:
        return tuple(k for k, w in self._workers.items() if w.stage_session_id == stage_session_id)

    async def gate(self, session_id: str) -> GateDecision:
        return await self._gate.token_gate(session_id)

    async def retire(self, session_id: str) -> None:
        """Disable issuance and delete the session's (and its workers') capability files."""
        await self.disable(session_id)
        for key in self.workers_of(session_id):
            await self.capabilities.revoke(key)
            self._workers.pop(key, None)
        await self.capabilities.revoke(session_id)

    async def disable(self, session_id: str) -> None:
        async with self._lock:
            self._enabled.pop(session_id, None)
            keys = (session_id, *self.workers_of(session_id))
            dropped = [(k, self._cache.pop(k, None)) for k in keys]
        for key, cached in dropped:
            if cached is not None:
                revoked = await self._minter.revoke(cached.token.token)
                self.audit.add(key, "revoked" if revoked else "revoke-failed")
        self.audit.add(session_id, "disabled")

    def _drop_cache(self, session_id: str) -> None:
        self._cache.pop(session_id, None)

    # ------------------------------------------------------------ CredentialBroker

    def issuance_enabled(self, session_id: str) -> bool:
        return session_id in self._enabled

    async def request_token(
        self, session_id: str, capability_secret: str, repository: str
    ) -> TokenGrant | TokenRefusal:
        async with self._lock:
            result = await self._issue(session_id, capability_secret, repository)
        if isinstance(result, TokenRefusal):
            self.audit.add(session_id, "refused", result.reason)
        else:
            self.audit.add(session_id, "issued", result.profile.value)
        return result

    def _resolve(self, session_id: str) -> tuple[str, CredentialProfile] | TokenRefusal:
        """``(gate session, token profile)`` for a stage or a registered worker."""
        worker = self._workers.get(session_id)
        stage = worker.stage_session_id if worker is not None else session_id
        stage_profile = self._enabled.get(stage)
        if stage_profile is None:
            return TokenRefusal("issuance-disabled")
        if worker is None:
            return stage, stage_profile
        if not profile_within(worker.profile, stage_profile):
            return TokenRefusal("worker-profile-exceeds-stage")
        return stage, worker.profile

    async def _issue(
        self, session_id: str, secret: str, repository: str
    ) -> TokenGrant | TokenRefusal:
        if self.capabilities.verify(session_id, secret) is None:
            return TokenRefusal("invalid-capability")
        resolved = self._resolve(session_id)
        if isinstance(resolved, TokenRefusal):
            return resolved
        stage, profile = resolved
        if repository.lower() != self.repository.lower():
            return TokenRefusal("wrong-repository")
        refusal = await self._check_gate(stage, self._enabled[stage])
        if refusal is not None:
            return refusal
        now = self._clock.now_utc_us()
        cached = self._cache.get(session_id)
        if (
            cached is not None
            and cached.profile == profile
            and cached.token.expires_at_us - self._margin > now
        ):
            return TokenGrant(
                cached.token.token, profile, self.repository, cached.token.expires_at_us
            )
        if cached is not None:
            self._cache.pop(session_id, None)
        requested = PROFILE_PERMISSIONS[profile]
        minted = await self._minter.mint(self.repository, requested)
        if isinstance(minted, MintFailure):
            return TokenRefusal(f"mint-failed: {minted.reason}")
        if not permissions_within(minted.permissions, requested) or tuple(
            r.lower() for r in minted.repositories
        ) != (self.repository.lower(),):
            await self._minter.revoke(minted.token)
            return TokenRefusal("minted-token-broader-than-profile")
        if minted.expires_at_us - self._margin <= self._clock.now_utc_us():
            await self._minter.revoke(minted.token)
            return TokenRefusal("minted-token-expires-too-soon")
        # Recheck after minting: a fence may have landed while the mint was in flight.
        stage_profile = self._enabled.get(stage)
        refusal = (
            await self._check_gate(stage, stage_profile) if stage_profile is not None else None
        )
        if refusal is not None or self._resolve(session_id) != (stage, profile):
            await self._minter.revoke(minted.token)
            return refusal or TokenRefusal("issuance-disabled")
        self._cache[session_id] = _Cached(minted, profile)
        return TokenGrant(minted.token, profile, self.repository, minted.expires_at_us)

    async def _check_gate(self, session_id: str, profile: CredentialProfile) -> TokenRefusal | None:
        decision = await self._gate.token_gate(session_id)
        if not decision.allowed:
            return TokenRefusal(decision.reason or "execution-gate-closed")
        if decision.profile is not None and decision.profile != profile:
            return TokenRefusal("profile-mismatch")
        return None
