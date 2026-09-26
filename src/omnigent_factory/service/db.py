"""Single-threaded async facade for the thread-affine SQLite store."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TypeVar

from omnigent_factory.ports.clock import Clock
from omnigent_factory.store.sqlite import SqliteStore

T = TypeVar("T")


class StoreWorker:
    def __init__(self, path: Path, clock: Clock) -> None:
        self._path = path
        self._clock = clock
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="factory-db")
        self._store: SqliteStore | None = None

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._store = await loop.run_in_executor(
            self._executor, lambda: SqliteStore.open(self._path, self._clock)
        )

    async def call(self, operation: Callable[[SqliteStore], T]) -> T:
        store = self._store
        if store is None:
            raise RuntimeError("store worker is not started")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, operation, store)

    async def close(self) -> None:
        if self._store is not None:
            await self.call(lambda store: store.close())
            self._store = None
        self._executor.shutdown(wait=True, cancel_futures=True)
