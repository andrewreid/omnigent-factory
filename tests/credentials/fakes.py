"""Fake App token minter and execution gate (no real GitHub, no owner credentials)."""

from __future__ import annotations

import itertools
from collections.abc import Mapping
from dataclasses import dataclass, field

from omnigent_factory.core.effects import CredentialProfile
from omnigent_factory.credentials.broker import GateDecision, MintedToken, MintFailure
from omnigent_factory.ports.clock import Clock

HOUR_US = 3_600_000_000


@dataclass
class FakeMinter:
    clock: Clock
    repository: str = "SA-Ambulance/timesheets"
    minted: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    revoked: list[str] = field(default_factory=list)
    broaden: bool = False
    fail: str | None = None
    lifetime_us: int = HOUR_US
    on_mint: object = None  # optional callback run while "in flight"
    _ids: itertools.count[int] = field(default_factory=lambda: itertools.count(1))

    async def mint(
        self, repository: str, permissions: Mapping[str, str]
    ) -> MintedToken | MintFailure:
        if callable(self.on_mint):
            self.on_mint()
        if self.fail is not None:
            return MintFailure(self.fail)
        token = f"ghs_bot_{next(self._ids)}"
        self.minted.append((token, dict(permissions)))
        granted = dict(permissions)
        if self.broaden:
            granted["administration"] = "write"
        return MintedToken(
            token, self.clock.now_utc_us() + self.lifetime_us, granted, (repository,)
        )

    async def revoke(self, token: str) -> bool:
        self.revoked.append(token)
        return True


@dataclass
class FakeGate:
    decisions: dict[str, GateDecision] = field(default_factory=dict)

    def open(self, session_id: str, profile: CredentialProfile) -> None:
        self.decisions[session_id] = GateDecision(True, "", profile)

    def close(self, session_id: str, reason: str) -> None:
        self.decisions[session_id] = GateDecision(False, reason)

    async def token_gate(self, session_id: str) -> GateDecision:
        return self.decisions.get(session_id, GateDecision(False, "unknown-session"))
