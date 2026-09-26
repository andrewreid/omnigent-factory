"""Local credential broker port (implemented by Task 3).

Effect kinds: ``DISABLE_ISSUANCE`` and ``ENABLE_ISSUANCE``
(see :data:`CREDENTIAL_EFFECT_KINDS`). The broker loads the stage's fixed permission
profile and current execution gate from the store; caller-requested permissions are
ignored. Issuance is denied by default at startup and for fenced/retired/unknown sessions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from omnigent_factory.core.effects import CredentialProfile, EffectKind
from omnigent_factory.ports.adapter import EffectAdapter

CREDENTIAL_EFFECT_KINDS = frozenset({EffectKind.DISABLE_ISSUANCE, EffectKind.ENABLE_ISSUANCE})


@dataclass(frozen=True, slots=True)
class TokenGrant:
    """An opaque, repository-scoped installation token (never persisted or logged)."""

    token: str
    profile: CredentialProfile
    repository: str
    expires_at_us: int


@dataclass(frozen=True, slots=True)
class TokenRefusal:
    reason: str


@runtime_checkable
class CredentialBroker(EffectAdapter, Protocol):
    def issuance_enabled(self, session_id: str) -> bool: ...

    async def request_token(
        self, session_id: str, capability_secret: str, repository: str
    ) -> TokenGrant | TokenRefusal: ...
