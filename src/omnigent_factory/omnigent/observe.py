"""Stream and snapshot normalization into core observation bodies (architecture §5.3-§5.4).

The SSE stream has no replay: after a disconnect the caller reconnects *and* takes a
snapshot; nothing is inferred from ``Last-Event-ID``. The normalizer is stateful only in
the sense of dedupe - an elicitation already mirrored (persisted open decision or seen
earlier) is never emitted twice, so a reconnect cannot replay a question.

ASK distinction: a native prompt whose ``params.policy_name`` is one of our cost-grant
policies is a cost ASK (``cost_ask=True``: checkpoint protocol, never auto-accepted).
Every other prompt - agent questions and github/CEL/permission ASKs alike - becomes a
material owner decision with conservative ``DecisionImpact.UNKNOWN``; the model cannot
label its own question within-contract. ``waiting`` status alone is async work, not a
question.

Missing prompts: a persisted open prompt absent from a *complete* scan is
``ElicitationGone`` (never "approved"); an incomplete scan proves nothing.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.types import DecisionImpact
from omnigent_factory.omnigent.policies import is_cost_ask
from omnigent_factory.omnigent.rest import OmnigentReadError, OmnigentRest, as_map
from omnigent_factory.omnigent.tree import TreeObservation

_MARKER_PREFIX = "[factory-effect "


def _texts(payload: object) -> list[str]:
    if isinstance(payload, str):
        return [payload]
    if isinstance(payload, list):
        return [t for p in payload for t in _texts(p)]
    if isinstance(payload, Mapping):
        if isinstance(payload.get("text"), str):
            return [payload["text"]]
        return [t for key in ("content", "data") for t in _texts(payload.get(key))]
    return []


#: Bounds and redaction for the prompt description relayed into GitHub.
_SUMMARY_CHARS = 300
_SECRETISH = re.compile(
    r"(gh[pousr]_[A-Za-z0-9_]{8,}|github_pat_[A-Za-z0-9_]{8,}|sk-[A-Za-z0-9_-]{8,}"
    r"|(?i:bearer)\s+[A-Za-z0-9._~+/=-]{8,}"
    r"|(?i:token|secret|password|passwd|api[_-]?key)\s*[=:]\s*\S+)"
)


def elicitation_summary(params: Mapping[str, Any]) -> str:
    """Short, redacted description of a prompt: its message, else tool + argument preview.

    Pinned shapes: policy asks carry ``policy_name`` and ``message``; harness permission
    cards carry ``message`` ("<harness> wants to use **Tool**") and ``content_preview``.
    """
    parts: list[str] = []
    policy = params.get("policy_name")
    message = params.get("message")
    preview = params.get("content_preview")
    if isinstance(policy, str) and policy:
        parts.append(f"{policy}:")
    if isinstance(message, str) and message.strip():
        parts.append(message)
    if isinstance(preview, str) and preview.strip() and preview not in (message or ""):
        parts.append(f"({preview})")
    text = " ".join(" ".join(parts).split())
    text = _SECRETISH.sub("[redacted]", text)
    if len(text) > _SUMMARY_CHARS:
        text = text[: _SUMMARY_CHARS - 1].rstrip() + "…"
    return text


@dataclass
class StreamNormalizer:
    """Normalizes one stage tree's stream/snapshot observations.

    ``seen_elicitations`` should be seeded from persisted decisions (open or not) so a
    daemon restart does not re-open them. ``own_item_ids`` / ``own_effect_ids`` /
    ``own_resolved`` come from the own-send ledger.
    """

    session_id: str
    root_id: str
    seen_elicitations: set[str] = field(default_factory=set)
    own_item_ids: set[str] = field(default_factory=set)
    own_effect_ids: set[str] = field(default_factory=set)
    own_resolved: set[str] = field(default_factory=set)
    new_nodes: set[str] = field(default_factory=set)
    statuses: dict[str, str] = field(default_factory=dict)

    def _opened(
        self, elicitation_id: str, params: Mapping[str, Any], node_id: str | None = None
    ) -> list[ev.EventBody]:
        if elicitation_id in self.seen_elicitations:
            return []
        self.seen_elicitations.add(elicitation_id)
        return [
            ev.ElicitationOpened(
                session_id=self.session_id,
                elicitation_id=elicitation_id,
                impact=DecisionImpact.UNKNOWN,
                cost_ask=is_cost_ask(params.get("policy_name")),
                summary=elicitation_summary(params),
                node_id=node_id,
            )
        ]

    def _ours(self, item_id: str | None, payload: object) -> bool:
        if item_id is not None and item_id in self.own_item_ids:
            return True
        for text in _texts(payload):
            start = text.rfind(_MARKER_PREFIX)
            if start >= 0:
                effect_id = text[start + len(_MARKER_PREFIX) :].split("]", 1)[0]
                if effect_id in self.own_effect_ids:
                    return True
        return False

    def on_event(self, node_id: str, event: Mapping[str, Any]) -> list[ev.EventBody]:
        kind = event.get("type")
        if kind == "response.elicitation_request":
            eid = event.get("elicitation_id")
            params = as_map(event.get("params"))
            return self._opened(eid, params, node_id) if isinstance(eid, str) else []
        if kind == "response.elicitation_resolved":
            eid = event.get("elicitation_id")
            if not isinstance(eid, str):
                return []
            return [
                ev.ElicitationResolved(
                    session_id=self.session_id,
                    elicitation_id=eid,
                    correlated=eid in self.own_resolved,
                )
            ]
        if kind == "session.status":
            status = event.get("status")
            if not isinstance(status, str):
                return []
            self.statuses[node_id] = status
            return [
                ev.RuntimeActivity(
                    session_id=self.session_id, busy=status in ("running", "launching")
                )
            ]
        if kind == "session.input.consumed":
            data = as_map(event.get("data"))
            item_id = data.get("item_id") if isinstance(data.get("item_id"), str) else None
            if item_id is None or self._ours(item_id, data.get("data")):
                return []
            return [ev.OwnerDirectOmnigentMessage(session_id=self.session_id, item_id=item_id)]
        if kind == "session.child_session.updated":
            child = event.get("child_session_id")
            if isinstance(child, str):
                self.new_nodes.add(child)
        return []

    def on_snapshot(
        self, obs: TreeObservation, open_elicitations: Iterable[str] = ()
    ) -> list[ev.EventBody]:
        """Reconcile after (re)connect: new prompts opened; persisted prompts gone."""
        out: list[ev.EventBody] = []
        owned = obs.owned_elicitations()  # deduplicated by the true owning session
        present = set(owned)
        for eid, (owner, event) in owned.items():
            out.extend(self._opened(eid, as_map(event.get("params")), owner))
        if obs.complete:
            out.extend(
                ev.ElicitationGone(session_id=self.session_id, elicitation_id=eid)
                for eid in sorted(set(open_elicitations) - present)
            )
        return out


@dataclass(frozen=True, slots=True)
class StreamResult:
    events: tuple[ev.EventBody, ...]
    gap: bool  # ended by error: reconnect and snapshot before trusting state
    reason: str = ""


async def drain_stream(
    rest: OmnigentRest, node_id: str, normalizer: StreamNormalizer, *, limit: int | None = None
) -> StreamResult:
    """Consume one stream connection. An error or EOF is a gap: snapshot next."""
    out: list[ev.EventBody] = []
    count = 0
    try:
        async for event in rest.stream(node_id):
            out.extend(normalizer.on_event(node_id, event))
            count += 1
            if limit is not None and count >= limit:
                return StreamResult(tuple(out), gap=False)
    except (OmnigentReadError, OSError) as exc:
        return StreamResult(tuple(out), gap=True, reason=str(exc))
    except Exception as exc:
        return StreamResult(tuple(out), gap=True, reason=type(exc).__name__)
    return StreamResult(tuple(out), gap=True, reason="stream closed")
