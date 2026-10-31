"""Recognise an owner's plain issue comment that clearly approves the posted plan.

A plain owner comment is plan feedback (it revises the plan). A short comment that
clearly says "approved" is treated like ``/approve`` instead. The grammar is positive and
fails closed: anything it does not explicitly allow stays feedback (a false negative only
costs the owner a ``/approve``; a false approval would start a build). The rules (also in
the README section "Approving in plain words"):

1. The raw comment, trimmed, is exactly one non-empty line made only of ASCII letters,
   digits, spaces, ``. , ! : - '`` and the emoji in :data:`APPROVAL_EMOJI`. Anything
   else rejects it *before* any normalisation: code (backticks), blockquotes (``>``),
   HTML, links, Markdown marks, question marks, tabs, smart quotes or apostrophes, other
   Unicode punctuation or letters (lookalikes), zero-width characters.
2. It must start with a letter or digit. Lower-cased, trailing ``. , ! : - '``, spaces and
   allowed emoji are dropped; the rest is split into words at spaces and ``. , ! : -``.
3. The first words must be an approval phrase (:data:`APPROVAL_PHRASES`); every further
   word (at most :data:`MAX_TRAILING_WORDS`) must be in :data:`TRAILING_WORDS`. Any other
   word (``pending``, ``merge``, ``ci``, ``don't``, ...) or an emoji not at the end makes
   it feedback. "Plan approved per conversation in-session." approves.
4. Hash form: exactly one word of 12-64 hex digits anywhere, removed before rule 3
   (which then also allows the word ``hash``). Whether it names one of the parcel's
   published plans is decided by the reducer (``effective_event``); a hash that names
   none (a commit SHA, say) leaves the comment as feedback.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: Word sequences that, at the very start of a comment, mean "the plan is approved".
APPROVAL_PHRASES: tuple[str, ...] = (
    "plan approved",
    "plan is approved",
    "plan looks good",
    "approved plan",
    "approve plan",
    "approve the plan",
    "approved",
    "approve",
    "approving",
    "i approve",
    "lgtm",
    "looks good to me",
    "looks good",
    "ship it",
    "go ahead",
)
#: The only words that may follow the approval phrase.
TRAILING_WORDS: frozenset[str] = frozenset(
    {
        "per",
        "as",
        "discussed",
        "agreed",
        "in",
        "session",
        "conversation",
        "our",
        "the",
        "this",
        "plan",
        "it",
        "thanks",
        "thank",
        "you",
        "cheers",
        "now",
        "go",
        "ahead",
        "with",
        "on",
        "omnigent",
        "chat",
        "above",
    }
)
#: At most this many words after the approval phrase.
MAX_TRAILING_WORDS = 6
#: Emoji allowed, and only as trailing decoration: thumbs up (with skin tones), rocket,
#: check marks, party popper, OK hand, hundred, folded hands. Escaped to keep the source
#: ASCII; U+FE0F is the emoji presentation selector some keyboards append.
APPROVAL_EMOJI = (
    "\U0001f44d\U0001f3fb\U0001f3fc\U0001f3fd\U0001f3fe\U0001f3ff"
    "\U0001f680✅✔️\U0001f389\U0001f44c\U0001f4af\U0001f64f"
)
_SAFE_LINE = re.compile(r"[A-Za-z0-9 .,!:'\-" + APPROVAL_EMOJI + r"]+")
_TRAILING = re.compile(r"[ .,!:'\-" + APPROVAL_EMOJI + r"]+$")
_WORD_SPLIT = re.compile(r"[ .,!:\-]+")
_HEX = re.compile(r"[0-9a-f]{12,64}")
_PHRASES: tuple[tuple[str, ...], ...] = tuple(
    sorted((tuple(p.split()) for p in APPROVAL_PHRASES), key=len, reverse=True)
)


@dataclass(frozen=True, slots=True)
class ApprovalText:
    """A comment that reads as plan approval; ``hash_text`` is the named hash, if any."""

    hash_text: str | None = None


def _words(text: str) -> list[str] | None:
    """Rules 1-2: the comment's words, or None when it is not a single safe line."""
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) != 1:
        return None
    line = lines[0].strip(" ")
    if _SAFE_LINE.fullmatch(line) is None or not line[0].isalnum():
        return None
    line = _TRAILING.sub("", line.lower())
    return [w for w in _WORD_SPLIT.split(line) if w]


def _approves(words: list[str], *, extra: frozenset[str] = frozenset()) -> bool:
    """Rule 3: an approval phrase followed only by allowlisted words."""
    for phrase in _PHRASES:
        if tuple(words[: len(phrase)]) == phrase:
            rest = words[len(phrase) :]
            allowed = TRAILING_WORDS | extra
            return len(rest) <= MAX_TRAILING_WORDS and all(w in allowed for w in rest)
    return False


def classify_approval(text: str) -> ApprovalText | None:
    """The approval this owner comment clearly expresses, else None (plan feedback)."""
    words = _words(text)
    if not words:
        return None
    hashes = [w for w in words if _HEX.fullmatch(w)]
    if not hashes:
        return ApprovalText() if _approves(words) else None
    if len(hashes) > 1:
        return None
    [token] = hashes
    rest = [w for w in words if w != token]
    return ApprovalText(token) if _approves(rest, extra=frozenset({"hash"})) else None


__all__ = [
    "APPROVAL_EMOJI",
    "APPROVAL_PHRASES",
    "MAX_TRAILING_WORDS",
    "TRAILING_WORDS",
    "ApprovalText",
    "classify_approval",
]
