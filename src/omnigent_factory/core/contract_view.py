"""Deterministic human view of a canonical contract (plan comment rendering v1).

The approval hash stays the SHA-256 of the canonical contract bytes (§7.1). The plan
comment shows the contract as markdown produced by :func:`render_contract_section`, a
pure function of those bytes, so for a given renderer version (the ``v1`` in the
``factory-parcel`` marker) each hash corresponds to exactly one visible contract text.
No JSON is published or parsed back from GitHub: publication is verified by comparing
the section between the begin/end markers with this rendering of the stored contract.

Escaping is part of the rendering (never applied afterwards): mentions are broken with
a zero-width space, ``<`` becomes ``&lt;`` (no HTML, hidden comments or marker spoofing),
code fences are broken and embedded newlines collapse to spaces so list structure holds.
"""

from __future__ import annotations

import json
import re
from typing import Any

RENDERER_VERSION = "v1"
SECTION_BEGIN = "<!-- factory-contract-begin -->"
SECTION_END = "<!-- factory-contract-end -->"
_MARKER = re.compile(r"<!-- factory-parcel v1 issue=(?P<issue>\d+) hash=(?P<hash>[0-9a-f]+) -->")


class ContractViewError(ValueError):
    """The canonical contract cannot be rendered (malformed stored bytes)."""


def parcel_marker(issue_number: int, hash_prefix: str) -> str:
    return f"<!-- factory-parcel {RENDERER_VERSION} issue={issue_number} hash={hash_prefix} -->"


def marker_hash(body: str) -> str | None:
    """The hash prefix of the single ``factory-parcel`` marker in ``body``, else ``None``."""
    found = _MARKER.findall(body)
    return found[0][1] if len(found) == 1 else None


def escape_inline(value: object) -> str:
    text = " ".join(str(value).split())
    text = text.replace("&", "&amp;").replace("<", "&lt;")
    text = text.replace("```", "`\u200b``").replace("~~~", "~\u200b~~")
    return re.sub(r"(?<![\w@])@(?=[A-Za-z0-9])", "@\u200b", text)


def render_contract_section(canonical: str) -> str:
    """Markdown for the approved contract, including its begin/end markers."""
    try:
        body: Any = json.loads(canonical)
    except ValueError as exc:
        raise ContractViewError("stored contract is not JSON") from exc
    if not isinstance(body, dict):
        raise ContractViewError("stored contract is not an object")
    lines = [
        SECTION_BEGIN,
        f"**Goal:** {escape_inline(body.get('goal', ''))}",
        "",
        f"**Size:** {escape_inline(body.get('size', ''))}",
        "",
        "**Acceptance criteria**",
        "",
    ]
    for number, item in enumerate(_objects(body.get("acceptance_criteria")), 1):
        lines.append(
            f"{number}. **{escape_inline(item.get('id', ''))}** "
            f"{escape_inline(item.get('criterion', ''))}"
        )
        lines.append(f"   Verify: {escape_inline(item.get('verification', ''))}")
    non_goals = body.get("non_goals")
    if isinstance(non_goals, list) and non_goals:
        lines += ["", "**Non-goals**", ""]
        lines += [f"- {escape_inline(item)}" for item in non_goals]
    resolved = _objects(body.get("resolved_decisions"))
    if resolved:
        lines += ["", "**Resolved decisions**", ""]
        lines += [
            f"- `{escape_inline(item.get('decision_id', ''))}`: "
            f"{escape_inline(item.get('answer', ''))}"
            for item in resolved
        ]
    lines.append(SECTION_END)
    return "\n".join(lines)


def extract_contract_section(body: str) -> str | None:
    """The single begin..end section of a posted comment (markers included), else None."""
    text = body.replace("\r\n", "\n")
    if text.count(SECTION_BEGIN) != 1 or text.count(SECTION_END) != 1:
        return None
    start = text.index(SECTION_BEGIN)
    end = text.index(SECTION_END) + len(SECTION_END)
    return text[start:end] if start < end else None


def _objects(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
