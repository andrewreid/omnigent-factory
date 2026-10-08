"""A short-lived cache of the project board's open issues (one paginated GitHub read).

Shared by idle-time auto-triage, ``factory_list_issues`` and related-issue marking, so a
burst of calls costs one board read. A failed read is never cached.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.ports.clock import Clock
from omnigent_factory.ports.github import BoardIssue

BoardReader = Callable[[], Awaitable[list[BoardIssue] | RetryableReadFailure]]

#: How long one board read is reused.
BOARD_TTL_US = 60 * 1_000_000


class BoardUnavailable(RuntimeError):
    """The board could not be read just now (retry later)."""


class BoardIndex:
    def __init__(self, reader: BoardReader, clock: Clock, *, ttl_us: int = BOARD_TTL_US) -> None:
        self.reader = reader
        self.clock = clock
        self.ttl_us = ttl_us
        self._cached: tuple[int, tuple[BoardIssue, ...]] | None = None
        self._lock = asyncio.Lock()

    async def issues(self, *, fresh: bool = False) -> tuple[BoardIssue, ...]:
        """The board's open repository issues; raises :class:`BoardUnavailable`."""
        async with self._lock:
            now = self.clock.now_utc_us()
            if not fresh and self._cached is not None and now - self._cached[0] < self.ttl_us:
                return self._cached[1]
            result = await self.reader()
            if isinstance(result, RetryableReadFailure):
                raise BoardUnavailable(result.reason)
            issues = tuple(result)
            self._cached = (now, issues)
            return issues
