"""Checkbox lines the factory writes into GitHub text: the Ready report's Deferred section
(``- [ ] ... <!-- factory-action:<id> -->``), the consolidated follow-up issue's checklist
(``<!-- factory-item:<key> -->``, in its factory-managed region) and a bundle proposal's
confirmation box (``<!-- factory-bundle:<id> -->``).

Pure text helpers. A line is identified by its hidden marker, never by its text, so the
factory can rewrite a line's status while keeping the owner's ticks: every write merges
the checkbox states of the body it replaces (:func:`keep_ticks`).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

ACTION = "action"
ITEM = "item"
#: A bundle proposal's "Build these together" checkbox (``service.bundles``).
BUNDLE = "bundle"

_LINE = re.compile(
    r"^(?P<lead>[ \t]*- \[)(?P<box>[ xX])(?P<mid>\] )(?P<text>.*?)[ \t]*"
    r"<!-- factory-(?P<kind>action|item|bundle):(?P<id>[A-Za-z0-9-]{1,64}) -->[ \t]*$",
    re.MULTILINE,
)
#: The status the factory appended to a line (" → fixing (rework started)").
_STATUS = re.compile(r" → [^→]*$")
#: A Ready report's Deferred section at most (``service.deferred``): the rest of a Ready
#: report or status comment fits beside it within GitHub's 65,536-character limit.
SECTION_BUDGET = 40_000
#: What :func:`cut_text` appends where it cut.
TRUNCATED = "\n\n[truncated]"
_FOLLOW_UP_REF = re.compile(r"\(in #(\d+)\)")
#: The longest "N more items" line :func:`fit_section` adds.
_MORE_MAX = 120


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


def texts(body: str, kind: str = ACTION) -> dict[str, str]:
    """Marker id -> the line's text without the factory's status, for every ``kind`` line."""
    return {
        m.group("id"): _STATUS.sub("", m.group("text"))
        for m in _LINE.finditer(body or "")
        if m.group("kind") == kind
    }


def statuses(body: str, kind: str = ACTION) -> dict[str, str]:
    """Marker id -> the status the factory appended to the line ("" when none), for every
    ``kind`` line."""
    out: dict[str, str] = {}
    for m in _LINE.finditer(body or ""):
        if m.group("kind") == kind:
            text = m.group("text")
            found = _STATUS.search(text)
            out[m.group("id")] = found.group(0)[len(" → ") :] if found else ""
    return out


def span(body: str, kind: str = ACTION) -> tuple[int, int] | None:
    """(start of the first, end of the last) ``kind`` line of ``body``; None: none."""
    found = [m for m in _LINE.finditer(body or "") if m.group("kind") == kind]
    return (found[0].start(), found[-1].end()) if found else None


def with_text(body: str, new: Mapping[str, str], kind: str = ACTION) -> str:
    """``body`` with the text of each ``kind`` line named in ``new`` replaced; its
    checkbox (ticked or not) and any status the factory appended are kept."""

    def update(m: re.Match[str]) -> str:
        ident = m.group("id")
        if m.group("kind") != kind or ident not in new:
            return m.group(0)
        text = m.group("text")
        status = text[len(_STATUS.sub("", text)) :]
        return (
            m.group("lead")
            + m.group("box")
            + m.group("mid")
            + new[ident]
            + status
            + " "
            + marker(kind, ident)
        )

    return _LINE.sub(update, body)


def cut_text(text: str, limit: int) -> str:
    """``text`` within ``limit`` characters, cut after a whole line (with
    :data:`TRUNCATED`); "" when not even that fits."""
    if len(text) <= limit:
        return text
    room = limit - len(TRUNCATED)
    if room <= 0:
        return ""
    head = text[:room]
    if "\n" in head and not text[room:].startswith("\n"):
        head = head[: head.rindex("\n")]
    return head.rstrip() + TRUNCATED


def fit_section(section: str, room: int) -> tuple[str, int]:
    """The Deferred section within ``room`` characters, and how many checkbox lines it
    left out. A section that fits is unchanged; otherwise its table goes and as many
    whole checkbox lines as fit stay, in number order, followed by a line saying how
    many more there are and where they are listed (never a line or marker cut)."""
    if len(section) <= room:
        return section, 0
    lines = section.split("\n")
    boxes = [n for n, text in enumerate(lines) if _LINE.fullmatch(text)]
    first = boxes[0] if boxes else len(lines)
    # The heading before the table or boxes (not the table itself).
    head = "\n".join(
        text for text in lines[:first] if not text.startswith(("| ", "_The ratings table"))
    ).rstrip("\n")
    if len(head) + _MORE_MAX > room:
        return "", len(boxes)
    out = head
    kept = 0
    for n in boxes:
        if len(out) + 1 + len(lines[n]) + 1 + _MORE_MAX > room:
            break
        out += "\n" + lines[n]
        kept += 1
    left = len(boxes) - kept
    if left:
        refs = sorted({int(m) for n in boxes[kept:] for m in _FOLLOW_UP_REF.findall(lines[n])})
        where = (
            "the follow-up issue " + ", ".join(f"#{n}" for n in refs[:3])
            if refs
            else "the follow-up issue"
        )
        out += "\n" + f"{left} more items - see {where}"[:_MORE_MAX]
    return out, left


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
    "BUNDLE",
    "ITEM",
    "SECTION_BUDGET",
    "TRUNCATED",
    "append_lines",
    "checked",
    "cut_text",
    "fit_section",
    "ids",
    "keep_ticks",
    "line",
    "marker",
    "newly_ticked",
    "span",
    "statuses",
    "texts",
    "with_status",
    "with_text",
]
