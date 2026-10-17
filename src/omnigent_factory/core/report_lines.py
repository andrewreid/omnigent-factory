"""Checkbox lines the factory writes into GitHub text: the Ready report's Deferred section
(``- [ ] ... <!-- factory-action:<id> -->``) and the consolidated follow-up issue's
checklist (``<!-- factory-item:<key> -->``).

Pure text helpers. A line is identified by its hidden marker, never by its text, so the
factory can rewrite a line's status while keeping the owner's ticks: every write merges
the checkbox states of the body it replaces (:func:`keep_ticks`).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

ACTION = "action"
ITEM = "item"

_LINE = re.compile(
    r"^(?P<lead>[ \t]*- \[)(?P<box>[ xX])(?P<mid>\] )(?P<text>.*?)[ \t]*"
    r"<!-- factory-(?P<kind>action|item):(?P<id>[A-Za-z0-9-]{1,64}) -->[ \t]*$",
    re.MULTILINE,
)
#: The status the factory appended to a line (" → fixing (rework started)").
_STATUS = re.compile(r" → [^→]*$")


def marker(kind: str, ident: str) -> str:
    return f"<!-- factory-{kind}:{ident} -->"


def line(kind: str, ident: str, text: str, *, ticked: bool = False, status: str = "") -> str:
    """One checkbox line; ``status`` is appended as " → <status>"."""
    box = "x" if ticked else " "
    suffix = f" → {status}" if status else ""
    return f"- [{box}] {text}{suffix} {marker(kind, ident)}"


def checked(body: str, kind: str = ACTION) -> dict[str, bool]:
    """Marker id -> ticked, for every ``kind`` line of ``body``."""
    found: dict[str, bool] = {}
    for m in _LINE.finditer(body or ""):
        if m.group("kind") == kind:
            found[m.group("id")] = m.group("box") in "xX"
    return found


def newly_ticked(before: str, after: str, kind: str = ACTION) -> list[str]:
    """Ids ticked in ``after`` but not in ``before`` (an id absent before counts as not
    ticked), in the order they appear."""
    old = checked(before, kind)
    return [
        ident for ident, ticked in checked(after, kind).items() if ticked and not old.get(ident)
    ]


def keep_ticks(current: str, text: str, kind: str = ACTION) -> str:
    """``text`` with every ``kind`` line ticked that ``current`` shows ticked: a rewrite
    never clears a tick it has not processed."""
    ticked = {ident for ident, on in checked(current, kind).items() if on}
    if not ticked:
        return text

    def tick(m: re.Match[str]) -> str:
        if m.group("kind") != kind or m.group("id") not in ticked or m.group("box") in "xX":
            return m.group(0)
        return m.group(0).replace(m.group("lead") + m.group("box"), m.group("lead") + "x", 1)

    return _LINE.sub(tick, text)


def with_status(body: str, states: Mapping[str, str], kind: str = ACTION) -> str:
    """``body`` with each ``kind`` line named in ``states`` ticked and its status set to
    ``states[id]`` ("" clears it); every other line, owner ticks included, is unchanged."""

    def update(m: re.Match[str]) -> str:
        ident = m.group("id")
        if m.group("kind") != kind or ident not in states:
            return m.group(0)
        base = _STATUS.sub("", m.group("text"))
        indent = m.group("lead")[: -len("- [")]
        return indent + line(kind, ident, base, ticked=True, status=states[ident])

    return _LINE.sub(update, body)


def ids(body: str, kind: str = ACTION) -> set[str]:
    return set(checked(body, kind))


def append_lines(body: str, lines: Iterable[str], before: str) -> str:
    """``body`` with ``lines`` added after its last checklist line (or before the marker
    text ``before`` when it has none)."""
    new = list(lines)
    if not new:
        return body
    last = None
    for m in _LINE.finditer(body):
        last = m
    if last is not None:
        return body[: last.end()] + "\n" + "\n".join(new) + body[last.end() :]
    cut = body.find(before)
    if cut < 0:
        return body.rstrip() + "\n\n" + "\n".join(new) + "\n"
    return body[:cut].rstrip() + "\n\n" + "\n".join(new) + "\n\n" + body[cut:]


__all__ = [
    "ACTION",
    "ITEM",
    "append_lines",
    "checked",
    "ids",
    "keep_ticks",
    "line",
    "marker",
    "newly_ticked",
    "with_status",
]
