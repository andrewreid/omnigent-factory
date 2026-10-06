"""Shared archive-inclusive session inventory for tree scans (architecture §5.3).

A tree scan closes descendants over ``parent_session_id`` across *every* session,
archived included: the child-list route hides archived children (it never passes
``include_archived`` and the store excludes archived rows by default;
``routes_items.py:177-187``, ``sqlalchemy_store.py:2645,2873-2876`` @1c0153aa) and
``GET /v1/sessions`` has no parent/root filter (``routes_core.py:1313-1327``). Reading
the whole instance on every scan costs O(all sessions) round trips, so the parent map is
kept here and refreshed incrementally:

* a session's parent never changes, so a row once read stays valid;
* new sessions are read newest-first (``sort_by=created_at``, ``order=desc``) and paging
  stops after the first page reaching ``overlap_s`` below the newest ``created_at`` of
  the last complete read (absorbs replica clock skew and same-second ties);
* a full read replaces the map whenever none exists yet and every ``resync_s`` (drops
  deleted sessions, bounds any drift).

Refreshes are single-flight: concurrent callers share one read that *started after they
asked*, so no caller is answered from a read older than its request. A failed read
raises :class:`OmnigentReadError` and leaves the map unchanged; the scan then reports
itself incomplete, never idle.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

from omnigent_factory.omnigent.rest import OmnigentRest
from omnigent_factory.ports.clock import Clock, SystemClock

_INVENTORY: Mapping[str, str] = {
    "kind": "any",
    "include_archived": "true",
    "visibility": "all",
    "sort_by": "created_at",
}
DEFAULT_OVERLAP_S = 300
DEFAULT_RESYNC_S = 900.0


def _created(row: Mapping[str, Any]) -> int | None:
    value = row.get("created_at")
    return value if isinstance(value, int) and not isinstance(value, bool) else None


class SessionIndex:
    def __init__(
        self,
        rest: OmnigentRest,
        *,
        clock: Clock | None = None,
        overlap_s: int = DEFAULT_OVERLAP_S,
        resync_s: float = DEFAULT_RESYNC_S,
    ) -> None:
        self._rest = rest
        self._clock = clock or SystemClock()
        self._overlap_s = overlap_s
        self._resync_us = int(resync_s * 1_000_000)
        self._children: dict[str, set[str]] = defaultdict(set)
        #: Newest ``created_at`` of every complete read so far; ``None`` forces a full read.
        self._watermark: int | None = None
        self._full_read_at_us: int | None = None
        self._lock = asyncio.Lock()
        self._started = 0
        self._finished = 0

    def children(self, session_id: str) -> Iterable[str]:
        return tuple(sorted(self._children.get(session_id, ())))

    async def refresh(self) -> None:
        """Bring the map up to date with a read that starts after this call."""
        wanted = self._started + 1
        async with self._lock:
            if self._finished >= wanted:
                return  # a read that started after we asked has completed
            self._started += 1
            ticket = self._started
            await self._read()
            self._finished = ticket

    async def _read(self) -> None:
        now = self._clock.monotonic_us()
        watermark, last_full = self._watermark, self._full_read_at_us
        if watermark is None or last_full is None or now - last_full >= self._resync_us:
            rows = await self._rest.paginate("/v1/sessions", {**_INVENTORY, "order": "asc"})
            self._children = defaultdict(set)
            self._watermark = None
            self._full_read_at_us = now
        else:
            floor = watermark - self._overlap_s
            rows = await self._rest.paginate(
                "/v1/sessions",
                {**_INVENTORY, "order": "desc"},
                until=lambda row: (created := _created(row)) is not None and created < floor,
            )
        for row in rows:
            sid, parent = row.get("id"), row.get("parent_session_id")
            if isinstance(sid, str) and isinstance(parent, str):
                self._children[parent].add(sid)
            created = _created(row)
            if created is not None and (self._watermark is None or created > self._watermark):
                self._watermark = created
