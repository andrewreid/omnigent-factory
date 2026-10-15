"""Health sweep: Omnigent liveness and stalled-run recovery (#891).

Every ``health_sweep_interval_minutes`` (default 5), while ``health_sweep`` is on:

1. **Global gate.** ``GET /health`` and the configured host (``GET /v1/hosts/<id>``).
   Either failing means no per-run action at all this sweep; ``/healthz`` reports
   ``degraded`` with the reason (``omnigent_unhealthy`` or ``host_offline``). No board
   note is written: an outage touches every card at once, and N card writes during it
   (then N more to clear them) would only add GitHub load; the card keeps its real
   reason and the operator signal is ``/healthz``, ``status`` and the log.
2. **Circuit breaker.** It opens for ``omnigent_breaker_minutes`` after 3 failed global
   checks in a row, a nudge whose write failed or is unconfirmed (a 5xx, a timeout), or
   a nudge Omnigent answered with ``runner_failed_to_start``. While open the sweep only
   observes (``status`` shows what it would do). In memory: a restart closes it, and the
   next failed checks reopen it.
3. **Per run.** The current run of every parcel with an open run, read from the
   observer's latest tree scan (no extra tree reads), the parcel's persisted state, one
   batched ``GET /health?session_ids=...`` for runner/host liveness and the run's branch
   head (a local ``git rev-parse``). It proposes a ``StallObserved`` event; the reducer
   re-checks every gate (``core.stall.stall_gate``) and owns the episode state.

Cases (all run kinds: triage, plan and build; only ACTIVE current runs: a WAITING run
waits on the owner, checks or approval, and a DRAINING one has its drain timeout):

* **A. Quiet without a result.** The run's turn ended (tree quiescent), no result for
  the stage, nothing new anywhere in the tree for ``stall_nudge_minutes``: one nudge
  per stall episode. A new episode needs real progress since the nudge; none within
  ``stall_escalate_minutes``: Needs you. ``max_stall_nudges_per_run`` nudges with no new
  commit or result between them: Needs you.
* **B. Runner gone** (``runner_online`` false, host online). Quiet: as A, but the nudge
  (which relaunches the runner) is not counted against the run's loop guard. Still
  ``running`` with a stale ``updated_at``: wait for Omnigent to reconcile it to idle.
* **C. Busy but frozen.** Busy with nothing new anywhere in the tree for
  ``stall_frozen_minutes`` and no live background task: Needs you, no message.
* **D. Root failed.** The observer's ``SessionCrashed`` (unchanged).

**Real progress** (what lets a new episode start, or clears an escalation): a worker
(any node but the root) appeared, or its ``updated_at``, status or task status changed
(it started, worked or reported); the run's branch head moved (a commit, local or
pushed); a result was submitted; a question was asked. The root's own items (a bare
reply to the nudge) are not progress. A frozen run's escalation also clears on any new
activity anywhere in its tree.

Nudges go out only with the host online and at most ``max_nudges_per_sweep`` per
sweep. Escalations are Needs you with one comment and a card note.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Event, Provenance, StallAction
from omnigent_factory.core.stall import iso_utc as _iso
from omnigent_factory.core.stall import stall_gate, stall_view
from omnigent_factory.core.types import (
    MICROS_PER_MINUTE,
    AdmissionSnapshot,
    Parcel,
    StallEscalation,
)
from omnigent_factory.omnigent.rest import OmnigentReadError, OmnigentRest
from omnigent_factory.omnigent.tree import TreeObservation
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.observer import TreeReading
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore, StoredEffect

LOG = logging.getLogger(__name__)

#: Failed global checks in a row that open the breaker.
BREAKER_FAILURES = 3
#: A root still ``running`` with its runner gone and no activity this long is left for
#: Omnigent's own reconcile (case B): it is never poked.
RUNNING_ORPHAN_US = 15 * MICROS_PER_MINUTE
#: A sent nudge is checked for a failed write or ``runner_failed_to_start`` this long.
NUDGE_WATCH_US = 15 * MICROS_PER_MINUTE
#: The runner could not be started for a message (the host could not launch it).
RUNNER_FAILED_TO_START = "runner_failed_to_start"


@dataclass(frozen=True, slots=True)
class Liveness:
    """``GET /health?session_ids=...`` for one root."""

    runner_online: bool | None
    host_online: bool | None


@dataclass(frozen=True, slots=True)
class Proposal:
    """What the sweep makes of one run: an event to apply (or None) and why."""

    body: ev.StallObserved | None
    state: str


@dataclass
class Breaker:
    failures: int = 0
    open_until_us: int = 0
    reason: str = ""

    def is_open(self, now_us: int) -> bool:
        return now_us < self.open_until_us

    def trip(self, reason: str, now_us: int, minutes: float) -> None:
        if not self.is_open(now_us):
            LOG.warning("health sweep breaker open minutes=%s reason=%s", minutes, reason)
        self.open_until_us = now_us + int(minutes * MICROS_PER_MINUTE)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class SentNudge:
    session_id: str
    sent_at_us: int


@dataclass
class SweepState:
    last_sweep_us: int = 0
    #: The global check's failure ("" healthy), e.g. ``host_offline``.
    problem: str = ""
    detail: str = ""
    nudges_last_sweep: int = 0
    runs: dict[str, dict[str, Any]] = field(default_factory=dict)


def progress_signature(observation: TreeObservation, branch_head: str) -> str:
    """Real progress (see the module docstring): every worker's identity, status, task
    status and ``updated_at``, plus the branch head; never the root's own activity."""
    workers = "|".join(
        f"{n.node_id}:{n.status}:{n.task_status}:{n.updated_at_us}" for n in observation.workers
    )
    digest = hashlib.sha256(workers.encode()).hexdigest()[:16]
    return f"w{len(observation.workers)}:{digest}:h{branch_head[:12]}"


def propose(
    p: Parcel,
    admission: AdmissionSnapshot,
    *,
    reading: TreeReading | None,
    live: Liveness | None,
    branch_head: str,
    now_us: int,
    config: ServiceConfig,
) -> Proposal:
    """The sweep's proposal for ``p``'s current run (pure; the reducer re-checks)."""
    s = p.current_session
    if s is None or s.root_id is None:
        return Proposal(None, "no current run")
    if reading is None or reading.observation.root_id != s.root_id:
        return Proposal(None, "no tree read yet")
    stale_us = int(max(600.0, 2 * config.parked_observation_interval_seconds) * 1_000_000)
    if now_us - reading.read_at_us > stale_us:
        return Proposal(None, "tree read stale")
    obs = reading.observation
    if not obs.complete:
        return Proposal(None, "tree read incomplete")
    w = s.stall
    idle_since = reading.last_activity_us
    progress = progress_signature(obs, branch_head)

    def body(
        action: StallAction,
        *,
        recovery: bool = False,
        escalate_after_us: int = 0,
        max_nudges: int = 3,
    ) -> ev.StallObserved:
        return ev.StallObserved(
            session_id=s.session_id,
            action=action,
            idle_since_us=idle_since,
            progress=progress,
            commit=branch_head,
            recovery=recovery,
            escalate_after_us=escalate_after_us,
            max_nudges=max_nudges,
        )

    if w.escalated is not None:
        gate = stall_gate(p, admission, StallAction.PROGRESS)
        moved = progress != w.progress or (
            w.escalated == StallEscalation.FROZEN and idle_since > w.idle_since_us
        )
        if gate is None and moved:
            return Proposal(body(StallAction.PROGRESS), "progress since escalation")
        return Proposal(None, f"escalated ({w.escalated.value})")
    if s.quiescent and obs.quiescent:
        if w.nudged_at_us and progress == w.progress:
            due = w.nudged_at_us + int(config.stall_escalate_minutes * MICROS_PER_MINUTE)
            if now_us < due:
                return Proposal(None, "nudged, waiting for progress")
            gate = stall_gate(p, admission, StallAction.ESCALATE)
            if gate is not None:
                return Proposal(None, f"nudged, not escalated: {gate}")
            return Proposal(
                body(
                    StallAction.ESCALATE,
                    escalate_after_us=int(config.stall_escalate_minutes * MICROS_PER_MINUTE),
                ),
                "no progress since the nudge",
            )
        if now_us - idle_since < config.stall_nudge_minutes * MICROS_PER_MINUTE:
            return Proposal(None, "quiet")
        gate = stall_gate(p, admission, StallAction.NUDGE)
        if gate is not None:
            return Proposal(None, f"quiet, not nudged: {gate}")
        if live is None or live.host_online is not True:
            return Proposal(None, "quiet, not nudged: host not online")
        recovery = live.runner_online is False
        return Proposal(
            body(
                StallAction.NUDGE,
                recovery=recovery,
                max_nudges=config.max_stall_nudges_per_run,
            ),
            "runner gone, quiet without a result" if recovery else "quiet without a result",
        )
    if not s.quiescent and obs.busy:
        if any(n.background_live for n in obs.nodes.values()):
            return Proposal(None, "busy (background task)")
        if now_us - idle_since >= config.stall_frozen_minutes * MICROS_PER_MINUTE:
            gate = stall_gate(p, admission, StallAction.FROZEN)
            if gate is None:
                return Proposal(body(StallAction.FROZEN), "busy with nothing moving")
            return Proposal(None, f"frozen, not escalated: {gate}")
        root = obs.root
        if (
            root is not None
            and root.status == "running"
            and live is not None
            and live.runner_online is False
            and now_us - idle_since >= RUNNING_ORPHAN_US
        ):
            return Proposal(None, "runner gone mid-turn: waiting for Omnigent to reconcile")
        return Proposal(None, "busy")
    return Proposal(None, "observing")


class HealthSweep:
    """The periodic sweep (see the module docstring). A managed runtime."""

    def __init__(
        self,
        service: FactoryService,
        rest: OmnigentRest,
        readings: Callable[[str], TreeReading | None],
        *,
        host_id: str,
        branch_head: Callable[[str], Awaitable[str]] | None = None,
    ) -> None:
        self.service = service
        self.rest = rest
        self.readings = readings
        self.host_id = host_id
        self.branch_head = branch_head
        self.breaker = Breaker()
        self.state = SweepState()
        self._sent: dict[str, SentNudge] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def config(self) -> ServiceConfig:
        return self.service.config

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="health-sweep")

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def healthy(self) -> bool:
        return self._task is None or not self._task.done()

    async def _run(self) -> None:
        while not self._stop.is_set():
            if self.config.health_sweep:
                try:
                    await self.sweep_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOG.warning("health sweep failed", exc_info=True)  # retried next sweep
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop.wait(), self.config.health_sweep_interval_minutes * 60
                )

    # ------------------------------------------------------------------ reporting

    def degraded(self) -> dict[str, str] | None:
        """Why ``/healthz`` is degraded (None: it is not, or the sweep is off)."""
        if not self.config.health_sweep:
            return None
        if self.state.problem:
            return {"reason": self.state.problem, "detail": self.state.detail}
        now = self.service.clock.now_utc_us()
        if self.breaker.is_open(now):
            return {"reason": "omnigent_breaker_open", "detail": self.breaker.reason}
        return None

    def status(self) -> dict[str, object]:
        now = self.service.clock.now_utc_us()
        cfg = self.config
        return {
            "enabled": cfg.health_sweep,
            "interval_minutes": cfg.health_sweep_interval_minutes,
            "last_sweep": _iso(self.state.last_sweep_us),
            "omnigent": self.state.problem or "ok",
            "detail": self.state.detail,
            "breaker": {
                "open": self.breaker.is_open(now),
                "until": _iso(self.breaker.open_until_us) if self.breaker.is_open(now) else None,
                "reason": self.breaker.reason if self.breaker.is_open(now) else "",
                "consecutive_failures": self.breaker.failures,
            },
            "nudges_last_sweep": self.state.nudges_last_sweep,
            "runs": dict(self.state.runs),
        }

    def run_status(self, session_id: str) -> dict[str, Any] | None:
        return next((r for r in self.state.runs.values() if r.get("session") == session_id), None)

    # ---------------------------------------------------------------------- sweep

    async def sweep_once(self) -> SweepState:
        cfg = self.config
        clock = self.service.clock
        now = clock.now_utc_us()
        if self.breaker.open_until_us and not self.breaker.is_open(now):
            LOG.info("health sweep breaker closed reason=%s", self.breaker.reason)
            self.breaker = Breaker()
        problem, detail = await self._omnigent_problem()
        parcels = await self._parcels()
        roots = sorted(
            {
                p.current_session.root_id
                for p in parcels
                if p.current_session and p.current_session.root_id
            }
        )
        liveness: dict[str, Liveness] = {}
        if problem is None and roots:
            try:
                liveness = await self._liveness(roots)
            except OmnigentReadError as exc:
                problem, detail = "omnigent_unhealthy", exc.reason
        self.state.last_sweep_us = now
        self.state.nudges_last_sweep = 0
        if problem is not None:
            self.breaker.failures += 1
            self.state.problem, self.state.detail = problem, detail
            LOG.warning("health sweep: %s (%s); no run actions", problem, detail)
            if self.breaker.failures >= BREAKER_FAILURES:
                self.breaker.trip(f"{problem}: {detail}", now, cfg.omnigent_breaker_minutes)
            self.state.runs = {}
            return self.state
        self.breaker.failures = 0
        self.state.problem = self.state.detail = ""
        await self._check_sent(now)
        admission = await self.service.db.call(lambda store: store.load_admission(cfg.repo_id))
        observe_only = self.breaker.is_open(now)
        runs: dict[str, dict[str, Any]] = {}
        sent = 0
        for parcel in parcels:
            s = parcel.current_session
            if s is None or s.root_id is None:
                continue
            head = await self._branch_head(s.session_id)
            proposal = propose(
                parcel,
                admission,
                reading=self.readings(s.session_id),
                live=liveness.get(s.root_id),
                branch_head=head,
                now_us=now,
                config=cfg,
            )
            state = proposal.state
            body = proposal.body
            if body is not None and observe_only:
                state, body = f"{state} (breaker open: observe only)", None
            if (
                body is not None
                and body.action == StallAction.NUDGE
                and sent >= cfg.max_nudges_per_sweep
            ):
                state, body = f"{state} (sweep nudge cap reached)", None
            if body is not None:
                result = await self.service.apply_event(
                    Event(
                        event_id=f"stall:{s.session_id}:{body.action.value}:{now}",
                        repo_id=cfg.repo_id,
                        parcel_id=parcel.parcel_id,
                        source_time_us=now,
                        provenance=Provenance.ADAPTER,
                        body=body,
                    )
                )
                if result.accepted:
                    for effect in result.effects:
                        if effect.kind == EffectKind.SEND_MESSAGE:
                            sent += 1
                            self._sent[effect.effect_id] = SentNudge(s.session_id, now)
                    LOG.info(
                        "health sweep %s issue=%s session=%s: %s",
                        body.action.value,
                        parcel.issue_number,
                        s.session_id,
                        proposal.state,
                    )
                    state = f"{body.action.value}: {proposal.state}"
                else:
                    state = f"{proposal.state} ({body.action.value} refused: {result.reason})"
            fresh = await self.service.db.call(partial(_load_parcel, parcel_id=parcel.parcel_id))
            cur = fresh.current_session if fresh is not None else s
            runs[str(parcel.issue_number)] = {
                "session": s.session_id,
                "kind": s.kind.value,
                "state": state,
                "stall": stall_view(cur.stall) if cur is not None else {},
            }
        self.state.runs = runs
        self.state.nudges_last_sweep = sent
        return self.state

    async def _omnigent_problem(self) -> tuple[str | None, str]:
        try:
            body = await self.rest.get_json("/health")
        except OmnigentReadError as exc:
            return "omnigent_unhealthy", exc.reason
        if body.get("status") != "ok":
            return "omnigent_unhealthy", f"/health status {body.get('status')!r}"
        try:
            host = await self.rest.get_json(f"/v1/hosts/{self.host_id}")
        except OmnigentReadError as exc:
            if exc.status == 404:
                return "host_offline", "configured host not found"
            return "omnigent_unhealthy", exc.reason
        if host.get("status") != "online":
            return "host_offline", f"host status {host.get('status')!r}"
        return None, ""

    async def _liveness(self, roots: list[str]) -> dict[str, Liveness]:
        body = await self.rest.get_json("/health", {"session_ids": ",".join(roots)})
        sessions = body.get("sessions")
        out: dict[str, Liveness] = {}
        if not isinstance(sessions, Mapping):
            return out
        for root, row in sessions.items():
            if isinstance(row, Mapping):
                runner, host = row.get("runner_online"), row.get("host_online")
                out[str(root)] = Liveness(
                    runner if isinstance(runner, bool) else None,
                    host if isinstance(host, bool) else None,
                )
        return out

    async def _check_sent(self, now: int) -> None:
        """Trip the breaker on a nudge whose write failed or is unconfirmed, or that
        Omnigent answered with ``runner_failed_to_start``."""
        for effect_id, nudge in list(self._sent.items()):
            stored = await self.service.db.call(partial(_effect, effect_id=effect_id))
            state = stored.state if stored is not None else "missing"
            if state in ("unknown", "failed"):
                self._sent.pop(effect_id)
                self.breaker.trip(f"nudge write {state}", now, self.config.omnigent_breaker_minutes)
                continue
            reading = self.readings(nudge.session_id)
            root = reading.observation.root if reading is not None else None
            if (
                state == "done"
                and root is not None
                and root.last_error == RUNNER_FAILED_TO_START
                and reading is not None
                and reading.read_at_us >= nudge.sent_at_us
                # An older failure's error, still on the root, is not this nudge's.
                and (root.updated_at_us == 0 or root.updated_at_us >= nudge.sent_at_us)
            ):
                self._sent.pop(effect_id)
                self.breaker.trip(RUNNER_FAILED_TO_START, now, self.config.omnigent_breaker_minutes)
                continue
            if state == "cancelled" or now - nudge.sent_at_us > NUDGE_WATCH_US:
                self._sent.pop(effect_id)

    async def _parcels(self) -> list[Parcel]:
        ids = await self.service.open_run_parcel_ids()
        out: list[Parcel] = []
        for parcel_id in ids:
            parcel = await self.service.db.call(partial(_load_parcel, parcel_id=parcel_id))
            if parcel is not None:
                out.append(parcel)
        return out

    async def _branch_head(self, session_id: str) -> str:
        if self.branch_head is None:
            return ""
        try:
            return await self.branch_head(session_id)
        except Exception:
            LOG.debug("branch head unreadable session=%s", session_id, exc_info=True)
            return ""


def _load_parcel(store: SqliteStore, *, parcel_id: str) -> Parcel | None:
    return store.load_parcel(parcel_id)


def _effect(store: SqliteStore, *, effect_id: str) -> StoredEffect | None:
    return store.get_effect(effect_id)


__all__ = [
    "BREAKER_FAILURES",
    "Breaker",
    "HealthSweep",
    "Liveness",
    "Proposal",
    "progress_signature",
    "propose",
]
