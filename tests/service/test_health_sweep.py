"""The health sweep end to end over the real reducer (#891).

A Harness-backed service stands in for the store; Omnigent is an httpx mock answering
``/health``, ``/health?session_ids=...`` and ``/v1/hosts/<id>``; tree readings are what
the observer would have kept. Times are UTC microseconds on one fake clock.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json, parcel_to_json
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.events import Event
from omnigent_factory.core.types import (
    MICROS_PER_MINUTE,
    BotState,
    Hold,
    StallEscalation,
)
from omnigent_factory.omnigent.rest import OmnigentRest
from omnigent_factory.omnigent.tree import NodeState, TreeObservation, node_from_snapshot
from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig
from omnigent_factory.service.health_sweep import HealthSweep, progress_signature
from omnigent_factory.service.observer import TreeReading
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID
from omnigent_factory.testing.builders import config as trusted_config
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness

MIN = MICROS_PER_MINUTE
HOST = "host-1"


@dataclass
class Omnigent:
    healthy: bool = True
    host: str = "online"
    runner_online: dict[str, bool] = field(default_factory=dict)
    host_online: bool = True
    calls: list[str] = field(default_factory=list)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(str(request.url))
        if request.url.path == "/health":
            if not self.healthy:
                return httpx.Response(503)
            body: dict[str, Any] = {"status": "ok"}
            ids = request.url.params.get("session_ids")
            if ids is not None:
                body["sessions"] = {
                    sid: {
                        "runner_online": self.runner_online.get(sid, True),
                        "host_online": self.host_online,
                        "host_version": "0.19",
                    }
                    for sid in ids.split(",")
                }
            return httpx.Response(200, json=body)
        if request.url.path == f"/v1/hosts/{HOST}":
            return httpx.Response(200, json={"host_id": HOST, "status": self.host})
        return httpx.Response(404)


class _Store:
    def __init__(self, world: World) -> None:
        self.world = world

    def load_parcel(self, parcel_id: str):
        return self.world.h.parcels.get(parcel_id)

    def load_admission(self, _repo_id: str):
        return self.world.h.admission

    def get_effect(self, effect_id: str):
        state = self.world.effect_states.get(effect_id, "done")
        return SimpleNamespace(state=state)


class _Db:
    def __init__(self, store: _Store) -> None:
        self.store = store

    async def call(self, fn):
        return fn(self.store)


class World:
    """The sweep's service: the harness parcels, a fake clock and config."""

    def __init__(self, **config: object) -> None:
        self.h = Harness(cfg=trusted_config(max_building=10, max_open_bot_prs=10))
        self.clock = FakeClock(start_us=self.h.f("I_parcel_1").now)
        self.config = ServiceConfig(repo_id=REPO_ID, owners=frozenset({OWNER_ID}), **config)
        self.db = _Db(_Store(self))
        self.effect_states: dict[str, str] = {}
        self.applied: list[Any] = []
        self.readings: dict[str, TreeReading] = {}
        self.omnigent = Omnigent()
        #: Workers last moved before the tests start (a fixed progress signature).
        self.worker_at = self.clock.now_utc_us() - MIN

    async def open_run_parcel_ids(self) -> list[str]:
        return sorted(
            pid
            for pid, p in self.h.parcels.items()
            if p.current_session is not None
            and p.current_session.root_id is not None
            and not p.current_session.execution_closed
        )

    async def apply_event(self, event: Event):
        result = self.h.apply(event)
        self.applied.append(result)
        return SimpleNamespace(
            accepted=result.audit.accepted, reason=result.audit.reason, effects=result.effects
        )

    def sweep(self) -> HealthSweep:
        rest = OmnigentRest("http://o.test", transport=httpx.MockTransport(self.omnigent.handle))
        return HealthSweep(
            self,  # type: ignore[arg-type]
            rest,
            self.readings.get,
            host_id=HOST,
            branch_head=self.head,
        )

    async def head(self, _session_id: str) -> str:
        return self.branch

    branch: str = "c0"

    # ------------------------------------------------------------- run shapes

    def quiet_build(self, pid: str = "I_parcel_1"):
        b = self.h.to_building(pid)
        self.h.quiesce(pid, b.session_id)
        self.sync()
        return self.h.cur(pid)

    def sync(self) -> None:
        """Harness time follows the fake clock (its events are stamped after it)."""
        for f in self.h.factories.values():
            f._time = max(f.now, self.clock.now_utc_us())
        self.clock._now = max(f.now for f in self.h.factories.values())

    def tree(
        self,
        pid: str = "I_parcel_1",
        *,
        idle_since: int,
        busy: bool = False,
        worker_at: int | None = None,
        root_status: str = "idle",
        root_error: str | None = None,
        background: bool = False,
    ) -> None:
        s = self.h.cur(pid)
        assert s.root_id is not None
        nodes = {
            s.root_id: NodeState(
                s.root_id,
                None,
                "running" if busy else root_status,
                updated_at_us=idle_since,
                last_error=root_error,
                background_active=background,
            )
        }
        worker = f"{s.root_id}-w"
        nodes[worker] = NodeState(
            worker, s.root_id, "idle", updated_at_us=worker_at or self.worker_at
        )
        obs = TreeObservation(s.root_id, True, nodes)
        now = self.clock.now_utc_us()
        self.readings[s.session_id] = TreeReading(obs, now, now)

    def advance(self, us: int) -> None:
        self.clock.advance(us)
        self.sync()

    def messages(self, purpose: MessagePurpose = MessagePurpose.STALL_NUDGE) -> list[Any]:
        return [
            e
            for r in self.applied
            for e in r.effects
            if e.kind == EffectKind.SEND_MESSAGE and e.args.get("purpose") == purpose.value
        ]

    def comments(self) -> list[Any]:
        return [e for r in self.applied for e in r.effects if e.kind == EffectKind.POST_COMMENT]


async def quiet_for(world: World, minutes: float, pid: str = "I_parcel_1") -> int:
    """The build went quiet ``minutes`` ago (its tree's last activity)."""
    since = world.clock.now_utc_us()
    world.advance(int(minutes * MIN))
    world.tree(pid, idle_since=since)
    return since


# ---------------------------------------------------------------------- #891


@pytest.mark.asyncio
async def test_891_replay_quiet_build_one_nudge_then_needs_you_restart_safe():
    w = World()
    run = w.quiet_build()
    since = await quiet_for(w, 29)
    sweep = w.sweep()
    await sweep.sweep_once()
    assert w.messages() == []  # 29 minutes: still quiet
    assert sweep.status()["runs"]["1"]["state"] == "quiet"
    w.advance(1 * MIN)
    w.tree(idle_since=since)
    await sweep.sweep_once()
    [nudge] = w.messages()
    assert nudge.preconditions.session_id == run.session_id
    assert sweep.status()["nudges_last_sweep"] == 1
    nudged_at = w.h.cur().stall.nudged_at_us
    # The root answers with a bare reply (root items only) and goes quiet again.
    w.h.send("I_parcel_1", ev.TreeQuiescent(session_id=run.session_id, complete=True, busy=True))
    w.advance(2 * MIN)
    w.h.quiesce("I_parcel_1", run.session_id)
    w.tree(idle_since=w.clock.now_utc_us())
    for _ in range(3):  # sweeps inside the escalation window: nothing
        w.advance(5 * MIN)
        w.tree(idle_since=nudged_at + 2 * MIN)
        await sweep.sweep_once()
    assert len(w.messages()) == 1 and w.comments() == []
    # Restart: a fresh sweep, the parcel reloaded from its stored JSON.
    w.h.parcels["I_parcel_1"] = parcel_from_json(parcel_to_json(w.h.p()))
    sweep = w.sweep()
    await sweep.sweep_once()
    assert len(w.messages()) == 1 and w.comments() == []
    w.advance(nudged_at + 30 * MIN - w.clock.now_utc_us())
    w.tree(idle_since=nudged_at + 2 * MIN)
    await sweep.sweep_once()
    [comment] = w.comments()
    assert comment.args["template"] == "stalled-quiet"
    p = w.h.p()
    assert Hold.STALLED in p.holds and p.bot == BotState.NEEDS_YOU
    # Later sweeps (and another restart) add nothing.
    sweep = w.sweep()
    for _ in range(3):
        w.advance(5 * MIN)
        w.tree(idle_since=nudged_at + 2 * MIN)
        await sweep.sweep_once()
    assert len(w.messages()) == 1 and len(w.comments()) == 1
    assert w.h.cur().stall.escalated == StallEscalation.QUIET


@pytest.mark.asyncio
async def test_worker_progress_after_a_nudge_allows_a_second_nudge():
    w = World()
    run = w.quiet_build()
    await quiet_for(w, 30)
    sweep = w.sweep()
    await sweep.sweep_once()
    assert len(w.messages()) == 1
    nudged_at = w.h.cur().stall.nudged_at_us
    w.h.send("I_parcel_1", ev.TreeQuiescent(session_id=run.session_id, complete=True, busy=True))
    w.h.quiesce("I_parcel_1", run.session_id)
    # A worker reported after the nudge; then everything went quiet for 30 minutes.
    reported = nudged_at + 5 * MIN
    w.advance(reported + 30 * MIN - w.clock.now_utc_us())
    w.tree(idle_since=reported, worker_at=reported)
    await sweep.sweep_once()
    assert len(w.messages()) == 2 and w.comments() == []
    assert w.h.cur().stall.nudges_sent == 2


@pytest.mark.asyncio
async def test_a_bare_reply_is_not_progress_for_the_sweep():
    w = World()
    run = w.quiet_build()
    since = await quiet_for(w, 30)
    sweep = w.sweep()
    await sweep.sweep_once()
    w.h.send("I_parcel_1", ev.TreeQuiescent(session_id=run.session_id, complete=True, busy=True))
    w.h.quiesce("I_parcel_1", run.session_id)
    w.advance(31 * MIN)
    # The root replied (its updated_at moved) but no worker or commit changed.
    assert since
    w.tree(idle_since=w.clock.now_utc_us() - 31 * MIN)
    await sweep.sweep_once()
    assert len(w.messages()) == 1
    assert [c.args["template"] for c in w.comments()] == ["stalled-quiet"]


@pytest.mark.asyncio
async def test_loop_guard_at_three_nudges_without_commit():
    w = World()
    run = w.quiet_build()
    sweep = w.sweep()
    for n in range(4):
        since = w.clock.now_utc_us()
        w.advance(31 * MIN)
        # A worker moved since the last nudge (progress), but no commit.
        w.tree(idle_since=since, worker_at=since - n - 1)
        await sweep.sweep_once()
        w.h.send(
            "I_parcel_1", ev.TreeQuiescent(session_id=run.session_id, complete=True, busy=True)
        )
        w.h.quiesce("I_parcel_1", run.session_id)
        w.sync()
    assert len(w.messages()) == 3
    assert [c.args["template"] for c in w.comments()] == ["stalled-loop"]
    assert w.h.p().bot == BotState.NEEDS_YOU


# ------------------------------------------------------------------- case B


@pytest.mark.asyncio
async def test_runner_dead_idle_gets_a_recovery_nudge_outside_the_budget():
    w = World()
    run = w.quiet_build()
    assert run.root_id is not None
    w.omnigent.runner_online[run.root_id] = False
    await quiet_for(w, 30)
    await w.sweep().sweep_once()
    [nudge] = w.messages()
    assert nudge.args["recovery"] is True
    assert w.h.cur().stall.budget == 0


@pytest.mark.asyncio
async def test_running_root_with_a_dead_runner_is_left_to_omnigent():
    w = World()
    b = w.h.to_building()
    w.h.send("I_parcel_1", ev.TreeQuiescent(session_id=b.session_id, complete=True, busy=True))
    w.sync()
    assert b.root_id is not None
    w.omnigent.runner_online[b.root_id] = False
    since = w.clock.now_utc_us()
    w.advance(20 * MIN)
    w.tree(idle_since=since, busy=True)
    sweep = w.sweep()
    await sweep.sweep_once()
    assert w.messages() == [] and w.comments() == []
    assert "waiting for Omnigent to reconcile" in sweep.status()["runs"]["1"]["state"]


@pytest.mark.asyncio
async def test_host_offline_for_the_session_means_no_nudge():
    w = World()
    w.quiet_build()
    w.omnigent.host_online = False
    await quiet_for(w, 30)
    sweep = w.sweep()
    await sweep.sweep_once()
    assert w.messages() == []
    assert sweep.status()["runs"]["1"]["state"].endswith("host not online")


# ------------------------------------------------------------- global gate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("healthy", "host", "reason"),
    [(True, "offline", "host_offline"), (False, "online", "omnigent_unhealthy")],
)
async def test_omnigent_down_or_host_offline_sends_nothing_and_degrades(healthy, host, reason):
    w = World()
    w.quiet_build()
    w.omnigent.healthy, w.omnigent.host = healthy, host
    await quiet_for(w, 120)
    sweep = w.sweep()
    await sweep.sweep_once()
    assert w.messages() == [] and w.comments() == []
    degraded = sweep.degraded()
    assert degraded is not None and degraded["reason"] == reason
    assert sweep.status()["omnigent"] == reason
    # Back up: the degradation clears and the run is nudged.
    w.omnigent.healthy, w.omnigent.host = True, "online"
    await sweep.sweep_once()
    assert sweep.degraded() is None and len(w.messages()) == 1


@pytest.mark.asyncio
async def test_breaker_opens_after_three_failed_checks_and_closes_after_30_minutes():
    w = World()
    w.quiet_build()
    await quiet_for(w, 60)
    w.omnigent.healthy = False
    sweep = w.sweep()
    for _ in range(3):
        await sweep.sweep_once()
    assert sweep.status()["breaker"]["open"] is True
    w.omnigent.healthy = True
    w.advance(5 * MIN)
    await sweep.sweep_once()  # healthy again, but the breaker only observes
    assert w.messages() == []
    assert "observe only" in sweep.status()["runs"]["1"]["state"]
    assert sweep.degraded() == {"reason": "omnigent_breaker_open", "detail": sweep.breaker.reason}
    w.advance(26 * MIN)  # 31 minutes after it opened
    w.tree(idle_since=w.clock.now_utc_us() - 90 * MIN)
    await sweep.sweep_once()
    assert sweep.status()["breaker"]["open"] is False and len(w.messages()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unknown", "failed", "runner_failed_to_start"])
async def test_breaker_opens_on_a_failed_nudge(failure):
    w = World()
    w.quiet_build("I_parcel_1")
    w.quiet_build("I_parcel_2")
    since = await quiet_for(w, 30, "I_parcel_1")
    w.tree("I_parcel_2", idle_since=since)
    sweep = w.sweep()
    w.config = w.config.model_copy(update={"max_nudges_per_sweep": 1})  # hot reload
    await sweep.sweep_once()
    [nudge] = w.messages()
    if failure == "runner_failed_to_start":
        first = nudge.preconditions.session_id == w.h.cur("I_parcel_1").session_id
        pid = "I_parcel_1" if first else "I_parcel_2"
        w.advance(MIN)
        w.tree(pid, idle_since=w.clock.now_utc_us(), root_error="runner_failed_to_start")
    else:
        w.effect_states[nudge.effect_id] = failure
    w.advance(5 * MIN)
    await sweep.sweep_once()
    assert sweep.status()["breaker"]["open"] is True
    assert len(w.messages()) == 1  # the other run waits for the breaker


@pytest.mark.asyncio
async def test_at_most_three_nudges_per_sweep():
    w = World()
    pids = [f"I_parcel_{n}" for n in range(1, 6)]
    for pid in pids:
        w.quiet_build(pid)
    since = w.clock.now_utc_us()
    w.advance(30 * MIN)
    for pid in pids:
        w.tree(pid, idle_since=since)
    sweep = w.sweep()
    await sweep.sweep_once()
    assert len(w.messages()) == 3 and sweep.status()["nudges_last_sweep"] == 3
    await sweep.sweep_once()
    assert len(w.messages()) == 5


# ------------------------------------------------------------------- case C


@pytest.mark.asyncio
async def test_frozen_busy_run_note_and_needs_you_no_message():
    w = World()
    b = w.h.to_building()
    w.h.send("I_parcel_1", ev.TreeQuiescent(session_id=b.session_id, complete=True, busy=True))
    w.sync()
    since = w.clock.now_utc_us()
    w.advance(119 * MIN)
    w.tree(idle_since=since, busy=True)
    sweep = w.sweep()
    await sweep.sweep_once()
    assert w.comments() == []
    w.advance(MIN)
    w.tree(idle_since=since, busy=True)
    await sweep.sweep_once()
    assert w.messages() == [] and not any(
        e.kind == EffectKind.INTERRUPT_TREE for r in w.applied for e in r.effects
    )
    [comment] = w.comments()
    assert comment.args["template"] == "stalled-frozen"
    assert w.h.p().note.startswith("Stalled: no activity since ")
    assert w.h.p().bot == BotState.NEEDS_YOU
    # New activity anywhere clears it.
    w.advance(MIN)
    w.tree(idle_since=w.clock.now_utc_us(), busy=True)
    await sweep.sweep_once()
    assert Hold.STALLED not in w.h.p().holds


@pytest.mark.asyncio
async def test_a_busy_run_with_a_live_background_task_is_not_frozen():
    w = World()
    b = w.h.to_building()
    w.h.send("I_parcel_1", ev.TreeQuiescent(session_id=b.session_id, complete=True, busy=True))
    w.sync()
    since = w.clock.now_utc_us()
    w.advance(200 * MIN)
    w.tree(idle_since=since, busy=True, background=True)
    await w.sweep().sweep_once()
    assert w.comments() == []


# ----------------------------------------------------------------- gates


@pytest.mark.asyncio
async def test_stopped_and_awaiting_owner_runs_are_never_nudged():
    w = World()
    w.quiet_build("I_parcel_1")
    w.h.send("I_parcel_1", ev.Stop())
    w.h.triage("I_parcel_2")  # triage posted: the owner's move
    w.sync()
    since = w.clock.now_utc_us()
    w.advance(120 * MIN)
    for pid in ("I_parcel_1", "I_parcel_2"):
        if w.h.p(pid).current_session.root_id is not None:  # type: ignore[union-attr]
            w.tree(pid, idle_since=since)
    await w.sweep().sweep_once()
    assert w.messages() == [] and w.comments() == []
    assert not any(r.audit.accepted and r.audit.kind == "StallObserved" for r in w.applied)


@pytest.mark.asyncio
async def test_sweep_off_does_nothing():
    w = World(health_sweep=False)
    w.quiet_build()
    await quiet_for(w, 120)
    sweep = w.sweep()
    await sweep.start()
    await sweep.close()
    assert w.omnigent.calls == [] and sweep.degraded() is None


def test_settings_are_hot_reloadable_with_owner_agreed_defaults():
    keys = {
        "health_sweep",
        "health_sweep_interval_minutes",
        "stall_nudge_minutes",
        "stall_escalate_minutes",
        "stall_frozen_minutes",
        "max_stall_nudges_per_run",
        "max_nudges_per_sweep",
        "omnigent_breaker_minutes",
    }
    assert keys <= HOT_RELOAD_KEYS
    c = ServiceConfig(repo_id=REPO_ID, owners=frozenset({OWNER_ID}))
    assert (c.health_sweep, c.health_sweep_interval_minutes) == (True, 5.0)
    assert (c.stall_nudge_minutes, c.stall_escalate_minutes, c.stall_frozen_minutes) == (
        30.0,
        30.0,
        120.0,
    )
    assert (c.max_stall_nudges_per_run, c.max_nudges_per_sweep) == (3, 3)
    assert c.omnigent_breaker_minutes == 30.0


# ---------------------------------------------------------------- signals


def test_progress_signature_ignores_the_root_and_tracks_workers_and_commits():
    root = NodeState("r", None, "idle", updated_at_us=1)
    worker = NodeState("w", "r", "idle", updated_at_us=5)
    base = progress_signature(TreeObservation("r", True, {"r": root, "w": worker}), "c0")
    moved_root = NodeState("r", None, "idle", updated_at_us=99)
    assert (
        progress_signature(TreeObservation("r", True, {"r": moved_root, "w": worker}), "c0") == base
    )
    moved_worker = NodeState("w", "r", "idle", updated_at_us=6)
    assert (
        progress_signature(TreeObservation("r", True, {"r": root, "w": moved_worker}), "c0") != base
    )
    new_worker = NodeState("w2", "r", "running")
    assert (
        progress_signature(
            TreeObservation("r", True, {"r": root, "w": worker, "w2": new_worker}), "c0"
        )
        != base
    )
    assert progress_signature(TreeObservation("r", True, {"r": root, "w": worker}), "c1") != base


def test_snapshot_updated_at_and_last_task_error_are_read():
    node = node_from_snapshot(
        "r",
        None,
        {
            "status": "idle",
            "updated_at": 1_792_020_851,
            "last_task_error": {"code": "runner_failed_to_start", "message": "x"},
        },
    )
    assert node.updated_at_us == 1_792_020_851_000_000
    assert node.last_error == "runner_failed_to_start"
    iso = node_from_snapshot("r", None, {"updated_at": "2026-10-14T23:34:11Z"})
    assert iso.updated_at_us == 1_792_020_851_000_000
    assert node_from_snapshot("r", None, {}).updated_at_us == 0
    summary = node_from_snapshot("c", "r", {}, {"updated_at": 1_792_020_851.5})
    assert summary.updated_at_us == 1_792_020_851_500_000


def test_nudge_and_escalation_texts():
    from omnigent_factory.service.directory import _status_text, _template

    text = _template("stall-nudge-v1.txt").format(run_id="ss_1")
    assert "your turn ended without submitting a result" in text
    assert "a worker may have finished without waking you" in text and "blocked" in text
    args = {"since": "Oct 14 23:34 UTC", "nudged": "Oct 15 00:04 UTC", "nudges": 3}
    for kind in ("quiet", "loop", "frozen"):
        rendered = _status_text(f"stalled-{kind}", args)
        assert rendered.endswith("A reply here reaches me.") and "{" not in rendered
    assert "Oct 15 00:04 UTC" in _status_text("stalled-quiet", args)
    assert "3 nudges" in _status_text("stalled-loop", args)
