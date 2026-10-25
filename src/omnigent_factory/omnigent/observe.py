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

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

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


# ------------------------------------------------------------ harness permission prompts

#: Harness permission cards by policy name: ``<vendor>_native_permission`` (Claude,
#: Cursor, Antigravity, Kiro... hooks), ``native_permission`` (generic hook),
#: ``<harness>_sdk_permission`` (SDK executors), Codex's approval requests.
_PERMISSION_POLICY = re.compile(
    r"(?:^|_)(?:native|sdk)_permissions?(?:_approval)?$"
    r"|^codex_native_(?:command|file_change|apply_patch)_approval$"
)
#: Hook phase of a harness's own tool gate (policy-engine ASKs use the agent phases).
_PERMISSION_PHASE = "pre_tool_use"
#: Gated tools that are really a question for the owner, not a permission.
_QUESTION_TOOLS = frozenset({"AskUserQuestion"})
_SHELL_TOOLS = frozenset({"bash", "shell", "run_shell_command", "exec_command", "execute"})
_TOOL_NAME = re.compile(r"[A-Za-z0-9_.:-]{1,64}")
_BOLD_TOOL = re.compile(r"\*\*([A-Za-z0-9_.:-]{1,64})\*\*")
#: A string field of a (possibly truncated) JSON tool input.
_JSON_FIELD = r'"{}"\s*:\s*"((?:[^"\\]|\\.)*)'
#: A relative or absolute file path with an extension, e.g. ``src/scripts/seed.ts``.
_PATH = re.compile(
    r"(?<![\w/.~-])((?:\.{1,2}/|/)?(?:[\w.-]+/)+[\w.-]*\w\.[A-Za-z0-9]{1,8})(?![\w/])"
)
_TARGET_CHARS = 120
_DETAIL_CHARS = 300


@dataclass(frozen=True, slots=True)
class PermissionAsk:
    """A harness permission prompt, readable: one line, plus what it touches."""

    tool: str
    #: "A worker is asking permission to ...". (no trailing call to action)
    summary: str
    #: A short, safe detail line ("" = none).
    detail: str = ""


def _safe(text: str, limit: int) -> str:
    """One line, credentials redacted, bounded; safe inside a code span."""
    text = _SECRETISH.sub("[redacted]", " ".join(text.split())).replace("`", "'")
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _tool_input(tool: str, preview: object) -> dict[str, Any]:
    """The tool input from a ``Tool({...json...})`` preview; truncated JSON (the preview
    is capped) still yields its leading string fields."""
    if not isinstance(preview, str):
        return {}
    body = preview.removeprefix(f"{tool}(") if tool else preview
    body = body.removesuffix(")")
    try:
        value = json.loads(body)
    except ValueError:
        value = None
    if isinstance(value, dict):
        return value
    out: dict[str, Any] = {}
    for key in ("command", "file_path", "notebook_path", "path", "url", "description"):
        match = re.search(_JSON_FIELD.format(key), body)
        if match is not None:
            try:
                out[key] = json.loads(f'"{match.group(1)}"')
            except ValueError:
                out[key] = match.group(1)
    return out


def _tool_name(params: Mapping[str, Any]) -> str:
    tool = params.get("tool_name")
    if isinstance(tool, str) and _TOOL_NAME.fullmatch(tool):
        return tool
    message = params.get("message")
    match = _BOLD_TOOL.search(message) if isinstance(message, str) else None
    if match is not None:
        return match.group(1)
    preview = params.get("content_preview")
    head = preview.split("(", 1)[0] if isinstance(preview, str) and "(" in preview else ""
    return head if _TOOL_NAME.fullmatch(head) else ""


def is_permission_prompt(params: Mapping[str, Any]) -> bool:
    """A harness asks to run a tool (Claude/Codex/Cursor/... native permission card)."""
    policy = params.get("policy_name")
    harness = (isinstance(policy, str) and _PERMISSION_POLICY.search(policy) is not None) or (
        params.get("phase") == _PERMISSION_PHASE
    )
    return harness and not is_cost_ask(policy) and _tool_name(params) not in _QUESTION_TOOLS


def permission_ask(params: Mapping[str, Any]) -> PermissionAsk | None:
    """The readable form of a harness permission prompt (None: not one)."""
    if not is_permission_prompt(params):
        return None
    policy = str(params.get("policy_name") or "")
    tool = _tool_name(params)
    data = _tool_input(tool, params.get("content_preview"))
    why = params.get("reason") if isinstance(params.get("reason"), str) else None
    why = why or (data.get("description") if isinstance(data.get("description"), str) else None)
    command = params.get("command") if policy.startswith("codex_") else data.get("command")
    files = [str(f) for f in params.get("files") or () if isinstance(f, str)]
    grant_root = params.get("grant_root")
    target = next(
        (
            data[key]
            for key in ("file_path", "notebook_path", "path")
            if isinstance(data.get(key), str) and data[key]
        ),
        None,
    )
    detail = ""
    if isinstance(command, str) and command.strip():
        lines = command.strip().splitlines()
        shell = "a Bash command" if tool.lower() == "bash" else "a command"
        touched = _PATH.search(command)
        what = f"run {shell}"
        if touched is not None:
            what += f" that touches `{_safe(touched.group(1), _TARGET_CHARS)}`"
        more = " (first line of several)" if len(lines) > 1 else ""
        detail = f"Command: `{_safe(lines[0], _TARGET_CHARS)}`{more}"
    elif policy == "codex_native_apply_patch_approval" or files:
        named = ", ".join(f"`{_safe(f, 80)}`" for f in files[:3])
        what = "apply a patch" + (f" to {named}" if named else "")
        if len(files) > 3:
            what += f" and {len(files) - 3} more files"
    elif policy == "codex_native_file_change_approval":
        what = "change files" + (
            f" under `{_safe(grant_root, _TARGET_CHARS)}`" if isinstance(grant_root, str) else ""
        )
    elif policy == "codex_native_permissions_approval":
        what = "take additional permissions"
    elif target is not None:
        what = f"use {_safe(tool, 64) or 'a tool'} on `{_safe(target, _TARGET_CHARS)}`"
    elif isinstance(data.get("url"), str) and data["url"]:
        host = urlsplit(data["url"]).hostname or data["url"]
        what = f"use {_safe(tool, 64) or 'a tool'} on `{_safe(host, _TARGET_CHARS)}`"
    else:
        what = f"use {_safe(tool, 64)}" if tool else "use a tool"
    if why:
        reason = f"Why: {_safe(why, _DETAIL_CHARS)}"
        detail = f"{detail}\n{reason}" if detail else reason
    summary = f"A worker is asking permission to {what}."
    return PermissionAsk(tool=tool, summary=summary, detail=detail)


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
        ask = permission_ask(params)
        return [
            ev.ElicitationOpened(
                session_id=self.session_id,
                elicitation_id=elicitation_id,
                impact=DecisionImpact.UNKNOWN,
                cost_ask=is_cost_ask(params.get("policy_name")),
                summary=elicitation_summary(params) if ask is None else ask.summary,
                node_id=node_id,
                permission=ask is not None,
                detail="" if ask is None else ask.detail,
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
