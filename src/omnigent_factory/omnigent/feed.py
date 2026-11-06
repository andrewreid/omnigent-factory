"""Change feed for the tree observer: which nodes changed, never what they now are.

The observer used to re-read every node of every tree on a timer. This feed tells it
which nodes to re-read instead. Its events only mark nodes *dirty*; the observer then
reads those nodes (lite snapshots), so an event is never evidence of state.

Two channels, chosen per tree at runtime with no configuration:

* **Root SSE stream** (``GET /v1/sessions/{root}/stream``, preferred) for every watched
  tree, active, settled or parked (an idle stream costs the server nothing). Every
  server carries the root's own ``session.status`` and its
  direct children's ``session.child_session.updated`` / ``session.created``
  (``helpers.py:_publish_child_status_to_parent`` before 6b46bfd2). Servers with
  ``perf/repl-push-updates`` (6b46bfd2) also publish every deeper descendant's events
  on each ancestor's stream, still naming the child's own parent as
  ``conversation_id`` (``helpers.py:_publish_to_further_ancestors``), and republish a
  child's row on a title/labels/archive PATCH. There is no capability flag, so support
  is *detected*: the first child event on a root stream whose ``conversation_id`` is
  not that root proves subtree push for the process. Until then the stream covers only
  trees no deeper than the root's children; deeper trees also go on the WebSocket.
* **WebSocket watch-set** (``WS /v1/sessions/updates``, ``routes_core.py:2440-2560``):
  ``{"type": "watch", "session_ids": [...]}`` replaces the watch-set and is answered by
  one ``snapshot`` frame; then ``changed`` (rows), ``removed`` (ids),
  ``access_changed`` (ids) and ``heartbeat`` (30 s idle) frames. Rows are
  ``SessionListItem`` (status, ``updated_at``, labels, archived,
  ``pending_elicitations_count``, ``background_work_running``, ``blocked_on``,
  ``parent_session_id``). At most 500 ids are watched (``_SESSION_UPDATES_MAX_WATCHED``);
  the server re-reads watched rows every 4 s per user. It reports state edges of ids
  already known only: it never announces a spawned child (``session_added`` is only
  published by create/fork/share routes, ``user_session_stream.py:3-15``), and the
  parent's spawn marker is a ``resource_event`` that does not even move the parent's
  ``updated_at`` (``sqlalchemy_store.py:3041-3048``). So the WebSocket never covers
  topology: the observer polls a WebSocket tree's topology (root, child list, inventory)
  on that tree's old cadence.

A tree covered by neither channel (both down, or past the watch cap) is on ``timer``
coverage: the observer reads it at the old cadence, never blind. Neither channel
replays: each (re)connect bumps the channel's epoch, and the observer reconciles a tree
with a full scan whenever its ``(coverage, epoch)`` changes. Reconnects back off
exponentially (bounded); a connection that stayed up resets the backoff.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections import Counter
from collections.abc import AsyncIterator, Callable, Collection, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from websockets.asyncio.client import connect as ws_connect

from omnigent_factory.omnigent.rest import OmnigentReadError, OmnigentRest, as_map
from omnigent_factory.ports.clock import Clock

LOG = logging.getLogger(__name__)

SSE = "sse"
WS = "ws"
TIMER = "timer"

#: Server cap on a watch-set (``_SESSION_UPDATES_MAX_WATCHED``, common.py:737 @68bf3df7).
MAX_WATCHED = 500
#: The server sends a heartbeat after 30 s without a frame; twice that plus slack of
#: silence means the socket is dead.
WS_IDLE_TIMEOUT_S = 75.0
#: A connection that stayed up this long resets the reconnect backoff.
STABLE_CONNECTION_S = 60.0
#: A stream that was live and dropped this recently is resuming (``ChangeFeed.resuming``).
RESUME_GRACE_S = 10.0
#: Consecutive failed opens of one root's stream (auth, gateway, network: anything but
#: an explicit "unsupported") before that stream is parked for ``STREAM_PARK_S``; its tree
#: is watched on the WebSocket (with topology polls) meanwhile. Never permanent.
MAX_OPEN_FAILURES = 5
STREAM_PARK_S = 600.0
#: Stream drops are reported at WARNING at most this often (a summary line).
DROP_WARN_INTERVAL_S = 600.0
#: The server's connect snapshot lists at most this many children (``limit=100``).
SNAPSHOT_CHILD_LIMIT = 100
#: HTTP statuses on a stream open that mean "this server has no such stream".
_UNSUPPORTED = frozenset({400, 405, 406, 415, 501})
#: ``SessionListItem`` fields whose change is an edge (anything else, e.g. ``updated_at``
#: alone, is ordinary progress on a busy node).
_WS_EDGE_FIELDS = (
    "status",
    "archived",
    "labels",
    "pending_elicitations_count",
    "background_work_running",
    "blocked_on",
    "parent_session_id",
)
#: ``ChildSessionSummary`` fields whose change is an edge (a preview-only delta is not).
_CHILD_EDGE_FIELDS = (
    "busy",
    "status",
    "current_task_id",
    "current_task_status",
    "archived",
    "labels",
    "last_task_error",
)


@dataclass(slots=True)
class DirtyMark:
    """Why a node should be re-read."""

    #: A state edge (status, prompt, archive, task...): read it promptly. False: ordinary
    #: progress (e.g. only ``updated_at`` moved), read at most once per observer interval.
    edge: bool = False
    #: Its children may have changed (a spawn): list them too.
    spawn: bool = False
    #: When it was first marked (monotonic us); a full scan that started later clears it.
    marked_at_us: int = 0


class WsConnection(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...


SseConnect = Callable[[str, Callable[[], None]], AsyncIterator[dict[str, Any]]]
WsConnect = Callable[[], AbstractAsyncContextManager[WsConnection]]


def _ws_url(base_url: str) -> str:
    if base_url.startswith("https://"):
        return "wss://" + base_url[len("https://") :] + "/v1/sessions/updates"
    if base_url.startswith("http://"):
        return "ws://" + base_url[len("http://") :] + "/v1/sessions/updates"
    return base_url + "/v1/sessions/updates"


def default_ws_connect(rest: OmnigentRest) -> WsConnect:
    """``websockets`` client for the updates socket, authenticated like REST."""

    @asynccontextmanager
    async def connect() -> AsyncIterator[WsConnection]:
        headers = await rest.auth_headers()
        rest.requests["ws_connect"] += 1
        async with ws_connect(
            _ws_url(rest.base_url),
            additional_headers=headers,
            open_timeout=15,
            max_size=16 * 1024 * 1024,
        ) as ws:
            yield ws

    return connect


def _depth_ok(root_id: str, parents: Mapping[str, str | None] | None) -> bool:
    """No node below the root's children (what every server's root stream carries)."""
    if parents is None:
        return True
    return all(parent in (None, root_id) for node, parent in parents.items() if node != root_id)


class ChangeFeed:
    """Marks nodes of watched trees dirty from the root SSE streams and the updates WS."""

    def __init__(
        self,
        rest: OmnigentRest,
        clock: Clock,
        *,
        sse_connect: SseConnect | None = None,
        ws_connect: WsConnect | None = None,
        backoff_initial_s: float = 1.0,
        backoff_max_s: float = 60.0,
        ws_idle_timeout_s: float = WS_IDLE_TIMEOUT_S,
        max_watched: int = MAX_WATCHED,
        sleep: Callable[[float], Any] = asyncio.sleep,
        ws_enabled: bool = True,
    ) -> None:
        self._rest = rest
        self._clock = clock
        self._sse_connect: SseConnect = sse_connect or (
            lambda root, on_open: rest.stream(root, on_open=on_open)
        )
        self._ws_connect: WsConnect = ws_connect or default_ws_connect(rest)
        self._backoff_initial = backoff_initial_s
        self._backoff_max = backoff_max_s
        self._ws_idle_timeout = ws_idle_timeout_s
        self._max_watched = max_watched
        self._sleep = sleep
        self._ws_enabled = ws_enabled
        #: Set whenever a node is marked or a channel (re)connects: wakes the observer.
        self.wake = asyncio.Event()
        self.counters: Counter[str] = Counter()
        self._closed = False
        # Targets (from the observer, each pass).
        self._trees: dict[str, Mapping[str, str | None] | None] = {}
        self._sse_roots: set[str] = set()
        self._owner: dict[str, str] = {}
        # Dirty marks: root -> node -> mark.
        self._dirty: dict[str, dict[str, DirtyMark]] = {}
        # SSE state.
        #: None: unknown; False: this server has no root stream; True: proven present.
        self.sse_supported: bool | None = None
        #: Proven when a root stream carried a deeper descendant's event (6b46bfd2).
        self.subtree_push = False
        self._sse_tasks: dict[str, asyncio.Task[None]] = {}
        self._sse_epoch: dict[str, int] = {}
        self._sse_up: set[str] = set()
        self._children_seen: dict[str, tuple[object, ...]] = {}
        self._children_updated: dict[str, object] = {}
        #: Roots whose stream is open but whose connect snapshot is still arriving:
        #: (child summaries, heartbeats) seen so far.
        self._pending_snapshot: dict[str, tuple[int, int]] = {}
        self._snapshot_complete: dict[str, bool] = {}
        self._dropped_at: dict[str, int] = {}
        #: Consecutive failed opens per root (any successful open clears them all).
        self._open_failures: dict[str, int] = {}
        self._parked_streams: set[str] = set()
        #: Set (and replaced) when any stream opens: wakes every parked stream early.
        self._unpark = asyncio.Event()
        self._drops_since_warn = 0
        self._last_drop_warn: int | None = None
        # WS state.
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_wanted = asyncio.Event()
        self._ws_rewatch = asyncio.Event()
        self._ws_watch: tuple[str, ...] = ()
        self._ws_sent: frozenset[str] = frozenset()
        self._ws_acked: frozenset[str] = frozenset()
        self._ws_epoch = 0
        self._ws_up = False
        self._ws_rows: dict[str, tuple[object, ...]] = {}
        self._ws_roots: dict[str, int] = {}
        self._ws_root_ids: dict[str, frozenset[str]] = {}
        self._ws_overflow: set[str] = set()
        #: Every (re)connect or (re)acknowledgement takes a fresh epoch, so a tree that left
        #: a channel and came back is never mistaken for one watched throughout.
        self._epochs = 0

    # ------------------------------------------------------------ observer API

    def set_targets(
        self, sse_roots: Collection[str], trees: Mapping[str, Mapping[str, str | None] | None]
    ) -> None:
        """The trees to watch (root -> node -> parent; None: not scanned yet) and which
        of them should use their root stream (the active ones)."""
        if self._closed:
            return
        self._trees = dict(trees)
        self._owner = {}
        for root, parents in self._trees.items():
            self._owner[root] = root
            for node in parents or ():
                self._owner.setdefault(node, root)
        for root in set(self._dirty) - set(self._trees):
            self._dirty.pop(root, None)
        wanted = (
            {r for r in sse_roots if r in self._trees} if self.sse_supported is not False else set()
        )
        for root in set(self._sse_tasks) - wanted:
            self._sse_tasks.pop(root).cancel()
            self._sse_up.discard(root)
        for root in sorted(wanted - set(self._sse_tasks)):
            self._sse_tasks[root] = asyncio.create_task(
                self._sse_loop(root), name=f"omnigent-feed-sse-{root}"
            )
        self._sse_roots = wanted
        self._set_watch()

    def coverage(self, root_id: str) -> tuple[str, int]:
        """How ``root_id``'s tree is watched now and the channel's connection epoch."""
        if self._sse_covers(root_id):
            return SSE, self._sse_epoch.get(root_id, 0)
        if self._ws_up and root_id in self._ws_roots:
            return WS, self._ws_roots[root_id]
        return TIMER, 0

    def take_dirty(
        self, root_id: str, keep: Callable[[str, DirtyMark], bool] | None = None
    ) -> dict[str, DirtyMark]:
        """Remove and return ``root_id``'s marks, except those ``keep`` holds back."""
        marks = self._dirty.get(root_id)
        if not marks:
            return {}
        taken = {n: m for n, m in marks.items() if keep is None or not keep(n, m)}
        for node in taken:
            marks.pop(node, None)
        if not marks:
            self._dirty.pop(root_id, None)
        return taken

    def has_dirty(self, root_id: str) -> bool:
        return bool(self._dirty.get(root_id))

    def clear_before(self, root_id: str, started_us: int) -> None:
        """A full scan that started at ``started_us`` (monotonic) supersedes older marks."""
        marks = self._dirty.get(root_id)
        if not marks:
            return
        for node in [n for n, m in marks.items() if m.marked_at_us < started_us]:
            marks.pop(node, None)
        if not marks:
            self._dirty.pop(root_id, None)

    def status(self) -> dict[str, object]:
        return {
            "sse_supported": self.sse_supported,
            "subtree_push": self.subtree_push,
            "sse_streams": len(self._sse_tasks),
            "sse_connected": len(self._sse_up),
            "sse_parked": len(self._parked_streams),
            "ws_connected": self._ws_up,
            "ws_watched": len(self._ws_acked),
            "ws_over_cap_trees": len(self._ws_overflow),
            "dirty_nodes": sum(len(m) for m in self._dirty.values()),
            "events": dict(sorted(self.counters.items())),
        }

    async def close(self) -> None:
        self._closed = True
        tasks = [*self._sse_tasks.values(), *([self._ws_task] if self._ws_task else [])]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._sse_tasks.clear()
        self._ws_task = None
        self._sse_up.clear()
        self._ws_up = False

    # ------------------------------------------------------------ marking

    def _mark(self, root_id: str | None, node_id: str, *, edge: bool, spawn: bool = False) -> None:
        if root_id is None or root_id not in self._trees:
            return
        marks = self._dirty.setdefault(root_id, {})
        mark = marks.get(node_id)
        if mark is None:
            mark = marks[node_id] = DirtyMark(marked_at_us=self._clock.monotonic_us())
        mark.edge = mark.edge or edge
        mark.spawn = mark.spawn or spawn
        self.counters["marks_edge" if edge else "marks_progress"] += 1
        if edge or spawn:
            # Progress alone is read at most once per observer interval anyway: it waits
            # for the next timed pass instead of waking one.
            self.wake.set()

    # ------------------------------------------------------------ SSE

    def _sse_covers(self, root_id: str) -> bool:
        if root_id not in self._sse_up:
            return False
        return self.subtree_push or _depth_ok(root_id, self._trees.get(root_id))

    def snapshot_complete(self, root_id: str) -> bool:
        """Whether the current connection of ``root_id``'s stream delivered its connect
        snapshot (every direct child's full summary) before going live."""
        return self._snapshot_complete.get(root_id, False)

    def resuming(self, root_id: str) -> bool:
        """``root_id``'s stream was up and dropped moments ago, and is reconnecting: an
        observer pass in that gap waits for the resume instead of re-scanning the tree."""
        dropped = self._dropped_at.get(root_id)
        task = self._sse_tasks.get(root_id)
        return (
            dropped is not None
            and task is not None
            and not task.done()
            and self._clock.monotonic_us() - dropped < RESUME_GRACE_S * 1e6
        )

    def _went_live(self, root_id: str, *, complete: bool) -> None:
        """The stream is live (its connect snapshot, if any, has been delivered)."""
        self._pending_snapshot.pop(root_id, None)
        self._snapshot_complete[root_id] = complete
        self._dropped_at.pop(root_id, None)
        self._epochs += 1
        self._sse_epoch[root_id] = self._epochs
        self._sse_up.add(root_id)
        self._set_watch()  # a covered tree leaves the watch-set
        self.wake.set()

    async def _sse_loop(self, root_id: str) -> None:
        backoff = self._backoff_initial
        while not self._closed:
            opened_at: int | None = None

            def on_open(root: str = root_id) -> None:
                nonlocal opened_at
                opened_at = self._clock.monotonic_us()
                self._open_failures.clear()  # the server serves streams: retry all
                self._parked_streams.discard(root)
                if self._parked_streams:
                    self._unpark.set()
                    self._unpark = asyncio.Event()
                # Live only once the connect snapshot is through (or shown absent).
                self._pending_snapshot[root] = (0, 0)
                self.counters["sse_connects"] += 1
                if self.sse_supported is not True:
                    self.sse_supported = True
                    LOG.info("change feed: root session streams available")

            try:
                async for event in self._sse_connect(root_id, on_open):
                    self._on_sse(root_id, event)
                reason = "closed by the server"
            except asyncio.CancelledError:
                self._sse_up.discard(root_id)
                self._pending_snapshot.pop(root_id, None)
                raise
            except OmnigentReadError as exc:
                reason = exc.reason
                if exc.status in _UNSUPPORTED and opened_at is None:
                    self._sse_up.discard(root_id)
                    self._sse_unsupported(f"HTTP {exc.status}")
                    return
            except Exception as exc:  # any transport failure: reconnect
                reason = f"{type(exc).__name__}: {exc}"[:200]
            was_live = root_id in self._sse_up
            self._sse_up.discard(root_id)
            self._pending_snapshot.pop(root_id, None)
            self.counters["sse_drops"] += 1
            self._warn_drop(root_id, reason, opened=opened_at is not None)
            if was_live:
                self._dropped_at[root_id] = self._clock.monotonic_us()
                self._set_watch()
                self.wake.set()
            if opened_at is None:
                failures = self._open_failures.get(root_id, 0) + 1
                self._open_failures[root_id] = failures
                if failures >= MAX_OPEN_FAILURES:
                    # Transient (auth, outage): park this root's stream, never give up.
                    if root_id not in self._parked_streams:
                        LOG.warning(
                            "change feed: stream of %s failed to open %d times (%s); "
                            "retrying in %ds, watched on the WebSocket meanwhile",
                            root_id,
                            failures,
                            reason,
                            int(STREAM_PARK_S),
                        )
                    self._parked_streams.add(root_id)
                    self._open_failures[root_id] = 0
                    await self._park(self._unpark)
                    self._parked_streams.discard(root_id)
                    backoff = self._backoff_initial
                    continue
            if (
                opened_at is not None
                and self._clock.monotonic_us() - opened_at >= STABLE_CONNECTION_S * 1e6
            ):
                backoff = self._backoff_initial
            await self._sleep(backoff)
            backoff = min(backoff * 2, self._backoff_max)

    async def _park(self, unpark: asyncio.Event) -> None:
        """Until ``STREAM_PARK_S`` passes or another root's stream opens."""
        waits = {
            asyncio.ensure_future(self._sleep(STREAM_PARK_S)),
            asyncio.ensure_future(unpark.wait()),
        }
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for wait in waits:
                wait.cancel()

    def _warn_drop(self, root_id: str, reason: str, *, opened: bool) -> None:
        """Stream drops at WARNING, at most one line per ``DROP_WARN_INTERVAL_S``
        summarising the drops since the last (the first drop is reported at once)."""
        self._drops_since_warn += 1
        now = self._clock.monotonic_us()
        if self._last_drop_warn is not None and now - self._last_drop_warn < (
            DROP_WARN_INTERVAL_S * 1e6
        ):
            return
        LOG.warning(
            "change feed: %d root stream drop(s) since the last report; latest %s %s: %s",
            self._drops_since_warn,
            root_id,
            "after connecting" if opened else "before connecting",
            reason,
        )
        self._last_drop_warn = now
        self._drops_since_warn = 0

    def _sse_unsupported(self, why: str) -> None:
        if self.sse_supported is not False:
            LOG.warning("change feed: root session streams unusable (%s); using ws", why)
        self.sse_supported = False
        for root, task in list(self._sse_tasks.items()):
            if task is not asyncio.current_task():
                task.cancel()
            self._sse_tasks.pop(root, None)
        self._sse_up.clear()
        self._pending_snapshot.clear()
        self._set_watch()
        self.wake.set()

    def _on_sse(self, root_id: str, event: Mapping[str, Any]) -> None:
        kind = event.get("type")
        if not isinstance(kind, str):
            return
        self.counters["sse_events"] += 1
        pending = self._pending_snapshot.get(root_id)
        if pending is not None:
            # The connect snapshot (routes_events.py:3956-4040): every direct child's full
            # summary (at most 100), terminals, then a changed-files invalidation and the
            # presence list. Live events follow it.
            children, beats = pending
            if kind == "session.changed_files.invalidated":
                self._went_live(root_id, complete=children < SNAPSHOT_CHILD_LIMIT)
            elif kind == "session.child_session.updated":
                self._pending_snapshot[root_id] = (children + 1, beats)
            elif kind == "session.heartbeat" and beats == 0:
                self._pending_snapshot[root_id] = (children, 1)  # one opens the stream
            elif kind != "session.resource.created":
                # No snapshot from this server (a live event, or a second heartbeat):
                # live from here, its children not re-listed.
                self._went_live(root_id, complete=False)
        if kind == "session.status":
            cid = event.get("conversation_id")
            self._mark(root_id, cid if isinstance(cid, str) else root_id, edge=True)
        elif kind == "session.child_session.updated":
            child, parent = event.get("child_session_id"), event.get("conversation_id")
            if not isinstance(child, str):
                return
            if isinstance(parent, str) and parent != root_id:
                self._prove_subtree(root_id)
            summary = as_map(event.get("child"))
            seen = self._children_seen.get(child)
            # A partial delta carries only what changed: keep the other fields.
            merged = tuple(
                summary[f] if f in summary else (seen[i] if seen is not None else None)
                for i, f in enumerate(_CHILD_EDGE_FIELDS)
            )
            self._children_seen[child] = merged
            edge = seen is None or merged != seen
            moved = summary.get("updated_at")
            if "updated_at" in summary:
                progress = moved != self._children_updated.get(child)
                self._children_updated[child] = moved
            else:
                progress = True  # a partial delta (e.g. a new message preview)
            # A full summary equal to what was seen (a reconnect's snapshot) marks nothing.
            if edge or progress or child not in self._owner:
                self._mark(root_id, child, edge=edge, spawn=child not in self._owner)
            if isinstance(parent, str) and parent not in self._owner:
                self._mark(root_id, parent, edge=True, spawn=True)
        elif kind == "session.created":
            child = event.get("child_session_id")
            parent = event.get("parent_session_id") or event.get("conversation_id")
            if isinstance(parent, str):
                if parent != root_id:
                    self._prove_subtree(root_id)
                self._mark(root_id, parent, edge=True, spawn=True)
            if isinstance(child, str):
                self._mark(root_id, child, edge=True, spawn=True)
        elif kind.startswith("response.elicitation") or kind.startswith("session.input"):
            # The root's snapshot mirrors every descendant's prompt; re-read it.
            self._mark(root_id, root_id, edge=True)

    def _prove_subtree(self, root_id: str) -> None:
        if not self.subtree_push:
            self.subtree_push = True
            LOG.info("change feed: root streams carry whole subtrees (seen on %s)", root_id)
            self._set_watch()
            self.wake.set()

    # ------------------------------------------------------------ WS

    def _set_watch(self) -> None:
        """Watch every tree no root stream fully covers, whole trees, up to the cap."""
        if self._closed:
            return
        order = sorted(self._trees, key=lambda r: (r not in self._sse_roots, r))
        ids: list[str] = []
        roots: dict[str, frozenset[str]] = {}
        overflow: set[str] = set()
        for root in order:
            if self._sse_covers(root):
                continue
            tree = frozenset({root, *(self._trees.get(root) or ())})
            if len(ids) + len(tree) > self._max_watched:
                overflow.add(root)
                continue
            ids.extend(sorted(tree))
            roots[root] = tree
        if overflow != self._ws_overflow and overflow:
            LOG.info("change feed: %d trees over the watch cap stay on timer", len(overflow))
        self._ws_overflow = overflow
        self._ws_root_ids = roots
        for root in set(self._ws_roots) - set(roots):
            self._ws_roots.pop(root, None)
        watch = tuple(ids)
        if watch != self._ws_watch:
            self._ws_watch = watch
            self._ws_rewatch.set()
        if watch and self._ws_enabled:
            self._ws_wanted.set()
            if self._ws_task is None or self._ws_task.done():
                self._ws_task = asyncio.create_task(self._ws_loop(), name="omnigent-feed-ws")
        else:
            self._ws_wanted.clear()

    async def _ws_loop(self) -> None:
        backoff = self._backoff_initial
        while not self._closed:
            await self._ws_wanted.wait()
            opened_at: int | None = None
            try:
                async with self._ws_connect() as ws:
                    opened_at = self._clock.monotonic_us()
                    self._ws_epoch += 1
                    self._ws_rows = {}
                    self._ws_roots = {}
                    self._ws_acked = frozenset()
                    self.counters["ws_connects"] += 1
                    await self._ws_session(ws)
                reason = "closed"
            except asyncio.CancelledError:
                self._ws_down()
                raise
            except Exception as exc:  # transport, handshake, timeout: reconnect
                reason = type(exc).__name__
            was_up = self._ws_up
            self._ws_down()
            if opened_at is not None:
                self.counters["ws_drops"] += 1
                if was_up:
                    LOG.debug("change feed: updates socket dropped (%s)", reason)
                self.wake.set()
            if not self._ws_wanted.is_set():
                backoff = self._backoff_initial
                continue  # closed on purpose: nothing to watch
            if (
                opened_at is not None
                and self._clock.monotonic_us() - opened_at >= STABLE_CONNECTION_S * 1e6
            ):
                backoff = self._backoff_initial
            await self._sleep(backoff)
            backoff = min(backoff * 2, self._backoff_max)

    def _ws_down(self) -> None:
        self._ws_up = False
        self._ws_roots = {}
        self._ws_acked = frozenset()

    async def _ws_session(self, ws: WsConnection) -> None:
        await self._send_watch(ws)
        recv: asyncio.Future[str | bytes] = asyncio.ensure_future(ws.recv())
        try:
            while not self._closed:
                if not self._ws_watch:
                    return  # nothing left to watch: close the socket
                rewatch: asyncio.Future[Any] = asyncio.ensure_future(self._ws_rewatch.wait())
                waiting: set[asyncio.Future[Any]] = {recv, rewatch}
                done, _ = await asyncio.wait(
                    waiting,
                    timeout=self._ws_idle_timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if rewatch not in done:
                    rewatch.cancel()
                if not done:
                    raise TimeoutError("updates socket silent")
                if rewatch in done and self._ws_watch:
                    await self._send_watch(ws)
                if recv in done:
                    self._on_ws(recv.result())
                    recv = asyncio.ensure_future(ws.recv())
        finally:
            if not recv.done():
                recv.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await recv

    async def _send_watch(self, ws: WsConnection) -> None:
        self._ws_rewatch.clear()
        self._ws_sent = frozenset(self._ws_watch)
        await ws.send(json.dumps({"type": "watch", "session_ids": list(self._ws_watch)}))

    def _on_ws(self, raw: str | bytes) -> None:
        try:
            frame = json.loads(raw)
        except ValueError:
            return
        if not isinstance(frame, dict):
            return
        kind = frame.get("type")
        self.counters["ws_frames"] += 1
        if kind == "snapshot":
            first = not self._ws_up
            self._ws_up = True
            self._ws_acked = self._ws_sent
            for root, ids in self._ws_root_ids.items():
                if root not in self._ws_roots and ids <= self._ws_acked:
                    self._epochs += 1
                    self._ws_roots[root] = self._epochs
            for row in frame.get("items") or ():
                self._on_row(row, baseline=first)
            if first:
                LOG.debug("change feed: updates socket watching %d ids", len(self._ws_acked))
            self.wake.set()
        elif kind == "changed":
            for row in frame.get("items") or ():
                self._on_row(row, baseline=False)
        elif kind in ("removed", "access_changed"):
            for sid in frame.get("ids") or ():
                if isinstance(sid, str):
                    self._mark(self._owner.get(sid), sid, edge=True)

    def _on_row(self, row: object, *, baseline: bool) -> None:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str):
            return
        sid = row["id"]
        fields = tuple(row.get(f) for f in _WS_EDGE_FIELDS)
        progress = row.get("updated_at")
        previous = self._ws_rows.get(sid)
        self._ws_rows[sid] = (*fields, progress)
        root = self._owner.get(sid)
        if root is None:
            # Not watched: a new session pushed by discovery. A child of a watched node
            # is a spawn the watch-set could not have announced.
            parent = row.get("parent_session_id")
            if isinstance(parent, str) and parent in self._owner:
                owner = self._owner[parent]
                self._mark(owner, parent, edge=True, spawn=True)
                self._mark(owner, sid, edge=True, spawn=True)
            return
        if baseline:
            return  # first snapshot of a connection: the reconnect scan reads everything
        if previous is None:
            self._mark(root, sid, edge=False)  # newly watched: ordinary refresh
        elif previous[:-1] != fields:
            self._mark(root, sid, edge=True)
        elif previous[-1] != progress:
            self._mark(root, sid, edge=False)
