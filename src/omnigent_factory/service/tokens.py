"""Refreshable daemon installation credential."""

from __future__ import annotations

import asyncio

from omnigent_factory.github.auth import DaemonToken, InstallationTokenService
from omnigent_factory.ports.clock import Clock
from omnigent_factory.ports.credentials import TokenRefusal


class DaemonTokenProvider:
    def __init__(
        self,
        service: InstallationTokenService,
        clock: Clock,
        *,
        refresh_margin_us: int = 5 * 60 * 1_000_000,
        initial: DaemonToken | None = None,
    ) -> None:
        self.service = service
        self.clock = clock
        self.margin = refresh_margin_us
        self._token = initial
        self._lock = asyncio.Lock()

    async def token(self) -> str:
        async with self._lock:
            if (
                self._token is None
                or self._token.expires_at_us - self.margin <= self.clock.now_utc_us()
            ):
                minted = await self.service.mint_daemon()
                if isinstance(minted, TokenRefusal):
                    raise RuntimeError("daemon installation credential unavailable")
                self._token = minted
            return self._token.token

    def cached(self) -> DaemonToken | None:
        """Return the current non-persisted token for same-process transport handoff."""
        return self._token
