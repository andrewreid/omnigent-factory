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
        #: Bumped after every call that changed the database (and on :meth:`poke`). The
        #: background loops sleep until it moves instead of polling the store.
        self.generation = 0
        #: Calls made (for measurements: an idle factory makes almost none).
        self.calls = 0
        self._wakes: list[asyncio.Event] = []
        self._dirty = False

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._store = await loop.run_in_executor(
            self._executor, lambda: SqliteStore.open(self._path, self._clock)
        )

    def subscribe(self) -> asyncio.Event:
        """An event set whenever the database changes (or :meth:`poke` is called).

        The subscriber clears it *before* reading what it acts on, so a change committed
        while it works sets it again and its next wait returns at once.
        """
        wake = asyncio.Event()
        self._wakes.append(wake)
        return wake

    def poke(self) -> None:
        """Wake every subscriber: state they read outside the store changed (a config
        reload, an operator release, shutdown)."""
        self.generation += 1
        for wake in self._wakes:
            wake.set()

    async def call(self, operation: Callable[[SqliteStore], T]) -> T:
        store = self._store
        if store is None:
            raise RuntimeError("store worker is not started")
        loop = asyncio.get_running_loop()
        self.calls += 1
        try:
            return await loop.run_in_executor(self._executor, self._run, operation, store)
        finally:
            if self._dirty:
                self._dirty = False
                self.poke()

    def _run(self, operation: Callable[[SqliteStore], T], store: SqliteStore) -> T:
        # Worker thread. ``total_changes`` counts rows written through this (the daemon's
        # only) connection; a rolled-back write still counts, which only costs a wake-up.
        before = store.total_changes
        try:
            return operation(store)
        finally:
            if store.total_changes != before:
                self._dirty = True

    async def close(self) -> None:
        if self._store is not None:
            await self.call(lambda store: store.close())
            self._store = None
        self._executor.shutdown(wait=True, cancel_futures=True)
