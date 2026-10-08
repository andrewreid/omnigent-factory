"""The Omnigent side of a triage ranking run: one read-only root session per run.

Each step is safe to repeat after a crash or a lost response:

* create adopts the root carrying the run's ``factory.dispatch`` nonce label (archived
  included) before it POSTs, and an unclear POST is never retried blindly (the next pass
  adopts by nonce);
* policies are reconciled by exact name and parameters (add and verify before removing)
  and verified as the exact set: the read-only GitHub policy, the fixed CEL rule in its
  read-only form (no Git command that moves HEAD in the source clone), the
  per-session ``factory-caller`` identity guard and one cost generation;
* the start message carries a unique marker and is sent only when the session's history
  does not already hold it;
* archive and delete are idempotent (a missing root counts as done).

The session gets no credential capability: it reads through the factory tools only.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from omnigent_factory.core.types import MICROS_PER_HOUR, SessionKind
from omnigent_factory.omnigent import policies as pol
from omnigent_factory.omnigent.adapter import (
    NONCE_LABEL,
    OmnigentExecutionAdapter,
    _is_user_message,
    _message_texts,
    effect_marker,
)
from omnigent_factory.omnigent.rest import OmnigentReadError, WriteClass, as_map, classify_write

#: Label naming the ranking run a root belongs to.
RANKING_LABEL = "factory.ranking"
#: The run's cost backstop: one hour at the factory's hourly rate (an ask, never a stop).
RANKING_COST_GRANT_US = MICROS_PER_HOUR


class RankingSessionError(RuntimeError):
    """A step failed. ``retry``: transient or unclear (the next pass repeats it)."""

    def __init__(self, reason: str, *, retry: bool) -> None:
        super().__init__(reason)
        self.reason = reason
        self.retry = retry


@dataclass(frozen=True, slots=True)
class RootState:
    """What a ranking run needs from its root snapshot."""

    archived: bool
    #: Idle with nothing running or pending (the turn ended).
    idle: bool


class RankingSessions:
    def __init__(self, adapter: OmnigentExecutionAdapter, workspace: str) -> None:
        self.adapter = adapter
        self.rest = adapter.rest
        self.config = adapter.config
        self.workspace = workspace

    def policies(self, root_id: str) -> tuple[pol.PolicySpec, ...]:
        return (
            pol.github_policy(SessionKind.TRIAGE, self.config.repository, ""),
            *self.adapter._cel_read_only,
            pol.caller_policy(root_id),
            pol.cost_policy(1, pol.cost_threshold_usd(None, RANKING_COST_GRANT_US)),
        )

    async def find(self, nonce: str) -> str | None:
        """The root carrying ``nonce`` (archived included), None when none exists."""
        rows = await self.adapter._nonce_rows(nonce)
        if not isinstance(rows, list):
            raise RankingSessionError(rows.reason, retry=True)
        if len(rows) > 1:
            raise RankingSessionError("several sessions carry this run's nonce", retry=False)
        return str(rows[0]["id"]) if rows else None

    async def create(self, *, run_id: str, nonce: str, title: str) -> str:
        """The run's root: adopted by nonce, else created (raises on failure)."""
        rows = await self.adapter._nonce_rows(nonce)
        if not isinstance(rows, list):
            raise RankingSessionError(rows.reason, retry=True)
        if len(rows) > 1:
            raise RankingSessionError("several sessions carry this run's nonce", retry=False)
        if rows:
            problem = self._identity_problem(rows[0])
            if problem is not None:
                raise RankingSessionError(f"adopted session: {problem}", retry=False)
            return str(rows[0]["id"])
        body: dict[str, Any] = {
            "agent_id": self.config.agent_id,
            "host_type": "external",
            "host_id": self.config.host_id,
            "workspace": self.workspace,
            "title": title,
            "labels": {NONCE_LABEL: nonce, RANKING_LABEL: run_id, "factory.stage": "ranking"},
            "initial_items": [],
        }
        if self.config.project_id is not None:
            body["project_id"] = self.config.project_id
        resp = await self.rest.post_json("/v1/sessions", body)
        cls = classify_write(resp)
        if cls == WriteClass.DEFINITIVE:
            raise RankingSessionError(f"create rejected: HTTP {resp.status}", retry=False)
        root = (resp.body or {}).get("id") or (resp.body or {}).get("session_id")
        if cls != WriteClass.OK or not isinstance(root, str) or not root:
            # Unclear: the next pass adopts the root by its nonce label (or creates it).
            raise RankingSessionError(f"create outcome unknown: {resp.status}", retry=True)
        return root

    def _identity_problem(self, row: Mapping[str, Any]) -> str | None:
        if row.get("agent_id") != self.config.agent_id:
            return "agent mismatch"
        if row.get("host_id") != self.config.host_id:
            return "host mismatch"
        if row.get("project_id") != self.config.project_id:
            return "project mismatch"
        if row.get("parent_session_id") is not None:
            return "not a root session"
        return None

    async def prepare(self, root_id: str, nonce: str) -> None:
        """Verify the root and attach the run's policies (verified before any message)."""
        snap = await self._snapshot(root_id)
        if as_map(snap.get("labels")).get(NONCE_LABEL) != nonce:
            raise RankingSessionError("nonce label mismatch", retry=False)
        problem = self._identity_problem(snap)
        if problem is not None:
            raise RankingSessionError(problem, retry=False)
        turn = self.adapter._unexpected_turn(snap)
        if turn is not None:
            raise RankingSessionError(f"unexpected activity: {turn}", retry=False)
        try:
            await pol.reconcile_policies(self.rest, root_id, self.policies(root_id))
        except pol.PolicyError as exc:
            raise RankingSessionError(f"policies: {exc.reason}", retry=not exc.conflict) from exc

    async def verify(self, root_id: str) -> None:
        """The exact policy set is in force (after the propagation barrier)."""
        try:
            await pol.verify_policies(self.rest, root_id, self.policies(root_id))
        except pol.PolicyError as exc:
            retry = "missing or altered" not in exc.reason and "superseded" not in exc.reason
            raise RankingSessionError(f"policies: {exc.reason}", retry=retry) from exc

    async def send_once(self, root_id: str, key: str, text: str) -> None:
        """Send the start message unless the history already holds its marker."""
        tag = effect_marker(key)
        try:
            items = await self.rest.paginate(f"/v1/sessions/{root_id}/items", {"order": "asc"})
            snap = await self._snapshot(root_id)
        except OmnigentReadError as exc:
            raise RankingSessionError(f"history read: {exc.reason}", retry=True) from exc
        pending = [p for p in snap.get("pending_inputs") or () if isinstance(p, dict)]
        if any(
            tag in t
            for item in [*(i for i in items if _is_user_message(i)), *pending]
            for t in _message_texts(item)
        ):
            return
        resp = await self.rest.post_json(
            f"/v1/sessions/{root_id}/events",
            {
                "type": "message",
                "data": {
                    "role": "user",
                    "content": [{"type": "input_text", "text": f"{text}\n\n{tag}"}],
                },
            },
        )
        cls = classify_write(resp)
        if cls == WriteClass.DEFINITIVE or (resp.body or {}).get("denied") is True:
            raise RankingSessionError(f"message rejected: HTTP {resp.status}", retry=False)
        if cls != WriteClass.OK:
            raise RankingSessionError(f"message outcome unknown: {resp.status}", retry=True)

    async def state(self, root_id: str) -> RootState | None:
        """The root's archive/idle state (None: the root is gone)."""
        try:
            snap = await self.rest.get_json(f"/v1/sessions/{root_id}")
        except OmnigentReadError as exc:
            if exc.status == 404:
                return None
            raise RankingSessionError(f"snapshot: {exc.reason}", retry=True) from exc
        idle = self.adapter._unexpected_turn(snap, reuse=True) is None
        return RootState(archived=snap.get("archived") is True, idle=idle)

    async def interrupt(self, root_id: str) -> None:
        """Best effort: stop a turn still running (the archive follows)."""
        await self.rest.post_json(
            f"/v1/sessions/{root_id}/events", {"type": "interrupt", "data": {}}
        )

    async def archive(self, root_id: str) -> None:
        resp = await self.rest.patch_json(f"/v1/sessions/{root_id}", {"archived": True})
        if classify_write(resp) == WriteClass.OK or resp.status == 404:
            return
        raise RankingSessionError(f"archive failed: {resp.status or resp.error}", retry=True)

    async def _snapshot(self, root_id: str) -> dict[str, Any]:
        try:
            return await self.rest.get_json(f"/v1/sessions/{root_id}")
        except OmnigentReadError as exc:
            raise RankingSessionError(f"snapshot: {exc.reason}", retry=exc.status != 404) from exc
