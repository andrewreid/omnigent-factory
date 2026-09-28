"""Source-shaped in-memory Omnigent server for ``httpx.MockTransport``.

Shapes follow the pinned upstream source (1c0153aa): ``SessionResponse`` / ``SessionListItem``
/ ``ChildSessionSummary`` / ``SessionPolicyObject`` field names, ``PaginatedList`` cursor
envelopes (``data``/``first_id``/``last_id``/``has_more``), ``stale_cursor`` 400, 201 on
JSON create, 202-style ``{"queued", "item_id"}`` event acks, ``{"deleted": true}`` policy
delete, and the child-list route hiding archived children (it inherits the store's
``include_archived=false``). Worktree creation happens *before* the session row, as in
``orchestration.py``, so a failure between the two leaves an orphan worktree.

Fault injection: ``faults[(METHOD, path)]`` is a FIFO of actions consumed per matching
request: an int status (returned *before* processing), ``"timeout-before"`` (raise,
nothing happens), ``"timeout-after"`` (process, then raise: lost ack),
``"500-after"`` (process, then answer 500), ``"worktree-then-500"`` (create only the
worktree, then 500).
"""

from __future__ import annotations

import itertools
import json
import subprocess
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

T0 = 1_800_000_000


@dataclass
class FakeSession:
    id: str
    agent_id: str
    labels: dict[str, str] = field(default_factory=dict)
    workspace: str | None = None
    git_branch: str | None = None
    status: str = "idle"
    parent_session_id: str | None = None
    archived: bool = False
    items: list[dict[str, Any]] = field(default_factory=list)
    pending_elicitations: list[dict[str, Any]] = field(default_factory=list)
    pending_inputs: list[dict[str, Any]] = field(default_factory=list)
    background_tasks: list[dict[str, Any]] | None = None
    total_cost_usd: float | None = None
    project_id: str | None = None
    host_id: str | None = None
    current_task_status: str | None = None
    active_response_id: str | None = None
    native: bool = False
    created_at: int = T0
    title: str | None = None
    context_window: int | None = None
    last_total_tokens: int | None = None

    @property
    def kind(self) -> str:
        return "sub_agent" if self.parent_session_id else "default"

    def response(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "agent_id": self.agent_id,
            "agent_name": "molly",
            "status": self.status,
            "background_task_count": len(self.background_tasks or []),
            "background_tasks": self.background_tasks,
            "created_at": self.created_at,
            "labels": dict(self.labels),
            "items": [dict(i) for i in self.items],
            "kind": self.kind,
            "parent_session_id": self.parent_session_id,
            "total_cost_usd": self.total_cost_usd,
            "pending_elicitations": [dict(e) for e in self.pending_elicitations],
            "pending_inputs": [dict(p) for p in self.pending_inputs],
            "workspace": self.workspace,
            "git_branch": self.git_branch,
            "archived": self.archived,
            "active_response_id": self.active_response_id,
            "project_id": self.project_id,
            "host_id": self.host_id,
            "title": self.title,
            "context_window": self.context_window,
            "last_total_tokens": self.last_total_tokens,
        }

    def list_item(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "agent_id": self.agent_id,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.created_at,
            "labels": dict(self.labels),
            "pending_elicitations_count": len(self.pending_elicitations),
            "workspace": self.workspace,
            "git_branch": self.git_branch,
            "archived": self.archived,
            "parent_session_id": self.parent_session_id,
            "project_id": self.project_id,
            "host_id": self.host_id,
        }

    def child_summary(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "object": "child_session",
            "parent_session_id": self.parent_session_id,
            "kind": "sub_agent",
            "created_at": self.created_at,
            "updated_at": self.created_at,
            "agent_id": self.agent_id,
            "current_task_status": self.current_task_status,
            "busy": self.status in ("running", "waiting"),
            "labels": dict(self.labels),
            "pending_elicitations_count": len(self.pending_elicitations),
        }


def elicitation(
    eid: str, *, policy_name: str | None = None, fields: tuple[str, ...] = ("answer",)
) -> dict[str, Any]:
    return {
        "type": "response.elicitation_request",
        "elicitation_id": eid,
        "method": "elicitation/create",
        "params": {
            "mode": "form",
            "message": "Question?",
            "policy_name": policy_name,
            "requestedSchema": {
                "type": "object",
                "properties": {f: {"type": "string"} for f in fields},
                "required": list(fields),
            },
        },
    }


class FakeOmnigentServer:
    def __init__(
        self,
        *,
        source_clone: Path | None = None,
        worktree_root: Path | None = None,
    ) -> None:
        self.sessions: dict[str, FakeSession] = {}
        self.policies: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.faults: dict[tuple[str, str], deque[Any]] = defaultdict(deque)
        self.requests: list[tuple[str, str, Any]] = []
        self.interrupts: list[str] = []
        self.resolved: list[tuple[str, str, dict[str, Any]]] = []
        self.misrouted: list[tuple[str, str]] = []
        self.source_clone = source_clone
        self.worktree_root = worktree_root
        self._ids = itertools.count(1)
        self.fail_paths: set[str] = set()  # GET paths that answer 503

    # ------------------------------------------------------------ helpers

    def add(self, session: FakeSession) -> FakeSession:
        self.sessions[session.id] = session
        return session

    def new_id(self, prefix: str = "conv") -> str:
        return f"{prefix}_{next(self._ids):04d}"

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def count(self, method: str, path: str) -> int:
        return sum(1 for m, p, _ in self.requests if m == method and p == path)

    @staticmethod
    def _page(rows: list[dict[str, Any]], params: httpx.QueryParams) -> httpx.Response:
        limit = int(params.get("limit", 20))
        after = params.get("after")
        start = 0
        if after is not None:
            ids = [r["id"] for r in rows]
            if after not in ids:
                return httpx.Response(
                    400, json={"error": {"code": "stale_cursor", "message": "stale cursor"}}
                )
            start = ids.index(after) + 1
        page = rows[start : start + limit]
        return httpx.Response(
            200,
            json={
                "object": "list",
                "data": page,
                "first_id": page[0]["id"] if page else None,
                "last_id": page[-1]["id"] if page else None,
                "has_more": start + limit < len(rows),
            },
        )

    def _make_worktree(self, branch: str, base: str | None) -> str:
        assert self.source_clone is not None and self.worktree_root is not None
        path = self.worktree_root / branch.replace("/", "-")
        subprocess.run(  # noqa: S603 - simulates the host creating the worktree
            ["git", "worktree", "add", "-b", branch, str(path), base or "HEAD"],  # noqa: S607
            cwd=self.source_clone,
            check=True,
            capture_output=True,
        )
        return str(path)

    # ------------------------------------------------------------ dispatch

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else None
        self.requests.append((request.method, path, body))
        action = None
        queue = self.faults.get((request.method, path))
        if queue:
            action = queue.popleft()
        if isinstance(action, int):
            return httpx.Response(action, json={"error": {"code": "injected", "message": "x"}})
        if action == "timeout-before":
            raise httpx.ReadTimeout("injected", request=request)
        if request.method == "GET" and path in self.fail_paths:
            return httpx.Response(503, json={"error": {"code": "unavailable", "message": "x"}})
        response = self._route(request, path, body, action)
        if action == "timeout-after":
            raise httpx.ReadTimeout("injected after processing", request=request)
        if action == "500-after":
            return httpx.Response(500, json={"error": {"code": "internal", "message": "x"}})
        return response

    def _route(self, request: httpx.Request, path: str, body: Any, action: Any) -> httpx.Response:
        parts = path.strip("/").split("/")
        method = request.method
        if parts[:2] != ["v1", "sessions"]:
            return httpx.Response(404, json={"error": {"code": "not_found", "message": "x"}})
        if len(parts) == 2:
            if method == "POST":
                return self._create(body, action)
            return self._list(request.url.params)
        sid = parts[2]
        session = self.sessions.get(sid)
        if session is None:
            return httpx.Response(
                404, json={"error": {"code": "not_found", "message": "Session not found"}}
            )
        if len(parts) == 3 and method == "PATCH":
            for key in ("archived", "title"):
                if key in (body or {}):
                    setattr(session, key, body[key])
            return httpx.Response(200, json=session.response())
        if len(parts) == 3 and method == "GET":
            body = session.response()
            body["pending_elicitations"] = self._snapshot_elicitations(session)
            return httpx.Response(200, json=body)
        sub = parts[3]
        if sub == "child_sessions":
            rows = [
                s.child_summary()
                for s in self.sessions.values()
                if s.parent_session_id == sid and not s.archived
            ]
            return self._page(rows, request.url.params)
        if sub == "items":
            return self._page([dict(i) for i in session.items], request.url.params)
        if sub == "events":
            return self._event(session, body)
        if sub == "elicitations" and len(parts) == 6 and parts[5] == "resolve":
            return self._resolve(session, parts[4], body)
        if sub == "policies":
            return self._policies(method, session, parts, body)
        return httpx.Response(404, json={"error": {"code": "not_found", "message": "x"}})

    def _snapshot_elicitations(self, session: FakeSession) -> list[dict[str, Any]]:
        """Own prompts first, then every descendant's prompt with
        ``params.target_session_id`` set, duplicates skipped (helpers.py:1617-1660)."""
        events = [dict(e) for e in session.pending_elicitations]
        seen = {e.get("elicitation_id") for e in events}
        queue = [session.id]
        visited = {session.id}
        while queue:
            parent = queue.pop(0)
            for child in self.sessions.values():
                if child.parent_session_id != parent or child.id in visited:
                    continue
                visited.add(child.id)
                queue.append(child.id)
                for event in child.pending_elicitations:
                    if event.get("elicitation_id") in seen:
                        continue
                    seen.add(event.get("elicitation_id"))
                    mirrored = dict(event)
                    mirrored["params"] = {**event.get("params", {}), "target_session_id": child.id}
                    events.append(mirrored)
        return events

    def _list(self, params: httpx.QueryParams) -> httpx.Response:
        kind = params.get("kind", "default")
        include_archived = params.get("include_archived") == "true"
        rows = [
            s.list_item()
            for s in self.sessions.values()
            if (include_archived or not s.archived) and kind in ("any", s.kind)
        ]
        return self._page(rows, params)

    def _create(self, body: dict[str, Any], action: Any) -> httpx.Response:
        if not isinstance(body.get("agent_id"), str):
            return httpx.Response(422, json={"detail": [{"msg": "agent_id required"}]})
        git = body.get("git") or {}
        workspace = body.get("workspace")
        branch = None
        if git:
            branch = git["branch_name"]
            if not git.get("existing_worktree"):
                workspace = self._make_worktree(branch, git.get("base_branch"))
        if action == "worktree-then-500":
            return httpx.Response(500, json={"error": {"code": "internal", "message": "x"}})
        session = self.add(
            FakeSession(
                id=self.new_id(),
                agent_id=body["agent_id"],
                labels=dict(body.get("labels") or {}),
                workspace=workspace,
                git_branch=branch,
                project_id=body.get("project_id"),
                host_id=body.get("host_id"),
                title=body.get("title"),
            )
        )
        return httpx.Response(201, json=session.response())

    def _event(self, session: FakeSession, body: dict[str, Any]) -> httpx.Response:
        kind = body.get("type")
        if kind == "interrupt":
            self.interrupts.append(session.id)
            if session.status in ("running", "launching", "waiting"):
                session.status = "idle"
            return httpx.Response(202, json={"queued": False})
        if kind == "message":
            data = body.get("data") or {}
            if session.native:
                pid = self.new_id("pending")
                session.pending_inputs.append({"pending_id": pid, "content": data.get("content")})
                return httpx.Response(202, json={"queued": True})
            item_id = self.new_id("msg")
            session.items.append(
                {"id": item_id, "type": "message", "status": "completed", "created_at": T0, **data}
            )
            return httpx.Response(202, json={"queued": True, "item_id": item_id})
        return httpx.Response(202, json={"queued": False})

    def _resolve(self, session: FakeSession, eid: str, body: dict[str, Any]) -> httpx.Response:
        if eid in {
            e.get("elicitation_id") for e in self._snapshot_elicitations(session)
        } and not any(e["elicitation_id"] == eid for e in session.pending_elicitations):
            # Pinned resolver is session-scoped: an ancestor URL answers 202 but the
            # child's pending Future is not resolved.
            self.misrouted.append((session.id, eid))
            return httpx.Response(202, json={"queued": False})
        for e in list(session.pending_elicitations):
            if e["elicitation_id"] == eid:
                session.pending_elicitations.remove(e)
                self.resolved.append((session.id, eid, body))
                return httpx.Response(202, json={"queued": False})
        return httpx.Response(404, json={"error": {"code": "not_found", "message": "x"}})

    def _policies(
        self, method: str, session: FakeSession, parts: list[str], body: Any
    ) -> httpx.Response:
        rows = self.policies[session.id]
        if method == "GET" and len(parts) == 4:
            return httpx.Response(200, json={"object": "list", "data": [dict(r) for r in rows]})
        if method == "POST" and len(parts) == 4:
            if any(r["name"] == body["name"] for r in rows):
                return httpx.Response(
                    409, json={"error": {"code": "conflict", "message": "exists"}}
                )
            row = {
                "id": self.new_id("pol"),
                "object": "session.policy",
                "name": body["name"],
                "type": body["type"],
                "handler": body["handler"],
                "enabled": True,
                "source": "session",
                "created_at": T0,
                "updated_at": None,
            }
            if body.get("factory_params") is not None:
                row["factory_params"] = body["factory_params"]
            rows.append(row)
            return httpx.Response(200, json=row)
        if method == "DELETE" and len(parts) == 5:
            self.policies[session.id] = [r for r in rows if r["id"] != parts[4]]
            return httpx.Response(200, json={"deleted": True})
        return httpx.Response(405, json={"error": {"code": "method", "message": "x"}})
