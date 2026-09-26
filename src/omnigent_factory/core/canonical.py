"""Canonicalization and hashing of plan contracts and waiver issue snapshots (§2.3).

Contract normalization: schema-validate the contract object, then recursively normalize
every string (CRLF/CR → LF; trailing horizontal whitespace trimmed per line; leading
whitespace, Unicode code points and list order retained). Serialize as UTF-8 JSON with
sorted object keys and no insignificant whitespace. Non-finite numbers and duplicate keys
are rejected. The exact canonical bytes are posted inside the ``parcel-contract`` fence and
hashed with SHA-256; the full digest is persisted, 12-character prefixes are display
handles only and must resolve uniquely.

Waiver normalization applies the same encoding to exactly ``{"title": ..., "body": ...}``
with a null body mapped to ``""``.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

PREFIX_LENGTH = 12
_TRAILING_HWS = re.compile(r"[ \t]+$", re.MULTILINE)
_HEX = re.compile(r"^[0-9a-f]+$")

CONTRACT_SIZES = frozenset({"S", "M", "L"})
_TEXT_MAX = 16000


class CanonicalizationError(ValueError):
    """Input cannot be canonicalized (invalid shape, duplicate key, non-finite...)."""


def normalize_text(value: str) -> str:
    """CRLF/CR → LF, then trim trailing spaces/tabs on every line."""
    unified = value.replace("\r\n", "\n").replace("\r", "\n")
    return _TRAILING_HWS.sub("", unified)


def _normalize(value: object) -> object:
    if isinstance(value, str):
        return normalize_text(value)
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalizationError("non-finite number")
        return value
    if isinstance(value, Mapping):
        out: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CanonicalizationError("object keys must be strings")
            out[key] = _normalize(item)
        return out
    if isinstance(value, list | tuple):
        return [_normalize(item) for item in value]
    raise CanonicalizationError(f"unsupported JSON value type {type(value).__name__}")


def canonical_bytes(value: object) -> bytes:
    """Deterministic UTF-8 JSON encoding of a normalized value."""
    normalized = _normalize(value)
    try:
        text = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except ValueError as exc:  # pragma: no cover - _normalize rejects first
        raise CanonicalizationError(str(exc)) from exc
    return text.encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise CanonicalizationError(f"duplicate key {key!r}")
        out[key] = value
    return out


def _reject_constant(name: str) -> object:
    raise CanonicalizationError(f"non-finite number {name}")


def parse_json_strict(text: str) -> object:
    """Parse JSON rejecting duplicate keys and NaN/Infinity."""
    try:
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicates,
            parse_constant=_reject_constant,
        )
    except json.JSONDecodeError as exc:
        raise CanonicalizationError(f"invalid JSON: {exc.msg}") from exc


# ------------------------------------------------------------------- contracts


def _require_text(value: object, where: str) -> str:
    if not isinstance(value, str) or not value or len(value) > _TEXT_MAX:
        raise CanonicalizationError(f"{where}: expected non-empty string ≤{_TEXT_MAX}")
    return value


def _require_texts(value: object, where: str, *, max_items: int = 100) -> list[str]:
    if not isinstance(value, list) or len(value) > max_items:
        raise CanonicalizationError(f"{where}: expected array ≤{max_items}")
    return [_require_text(v, f"{where}[{i}]") for i, v in enumerate(value)]


def _require_object(value: object, where: str, keys: Iterable[str]) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise CanonicalizationError(f"{where}: expected object")
    expected = set(keys)
    actual = set(value.keys())
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise CanonicalizationError(f"{where}: missing {missing} unknown {extra}")
    return value


def validate_contract(contract: object) -> Mapping[str, object]:
    """Validate a contract object against the §7.2 ``contract`` definition.

    Also enforces the cross-record rule that criterion and decision IDs are unique.
    """
    obj = _require_object(
        contract,
        "contract",
        ("goal", "acceptance_criteria", "non_goals", "size", "resolved_decisions"),
    )
    _require_text(obj["goal"], "goal")
    criteria = obj["acceptance_criteria"]
    if not isinstance(criteria, list) or not 1 <= len(criteria) <= 100:
        raise CanonicalizationError("acceptance_criteria: expected 1..100 items")
    seen: set[str] = set()
    for i, item in enumerate(criteria):
        c = _require_object(item, f"acceptance_criteria[{i}]", ("id", "criterion", "verification"))
        cid = _require_text(c["id"], f"acceptance_criteria[{i}].id")
        _require_text(c["criterion"], f"acceptance_criteria[{i}].criterion")
        _require_text(c["verification"], f"acceptance_criteria[{i}].verification")
        if cid in seen:
            raise CanonicalizationError(f"duplicate acceptance criterion id {cid!r}")
        seen.add(cid)
    _require_texts(obj["non_goals"], "non_goals")
    if obj["size"] not in CONTRACT_SIZES:
        raise CanonicalizationError("size: expected S, M or L")
    decisions = obj["resolved_decisions"]
    if not isinstance(decisions, list) or len(decisions) > 100:
        raise CanonicalizationError("resolved_decisions: expected array ≤100")
    seen_decisions: set[str] = set()
    for i, item in enumerate(decisions):
        d = _require_object(
            item, f"resolved_decisions[{i}]", ("decision_id", "answer", "source_event_id")
        )
        did = _require_text(d["decision_id"], f"resolved_decisions[{i}].decision_id")
        _require_text(d["answer"], f"resolved_decisions[{i}].answer")
        _require_text(d["source_event_id"], f"resolved_decisions[{i}].source_event_id")
        if did in seen_decisions:
            raise CanonicalizationError(f"duplicate resolved decision id {did!r}")
        seen_decisions.add(did)
    return obj


def canonical_contract(contract: object) -> bytes:
    """Validate then canonicalize a contract object."""
    return canonical_bytes(validate_contract(contract))


def contract_digest(contract: object) -> str:
    """Full SHA-256 hex digest of the canonical contract bytes."""
    return sha256_hex(canonical_contract(contract))


# ------------------------------------------------------------- waiver snapshots


def canonical_issue_snapshot(title: str, body: str | None) -> bytes:
    return canonical_bytes({"title": title, "body": "" if body is None else body})


def issue_snapshot_digest(title: str, body: str | None) -> str:
    return sha256_hex(canonical_issue_snapshot(title, body))


# -------------------------------------------------------------- prefix handles


def display_prefix(full_hash: str) -> str:
    return full_hash[:PREFIX_LENGTH]


def resolve_hash(handle: str, digests: Sequence[str]) -> str | None:
    """Resolve an owner-typed hash handle to exactly one full digest, else ``None``.

    The handle must be lowercase hex of at least 12 characters (the displayed prefix
    length) and must be a prefix of exactly one distinct known digest.
    """
    candidate = handle.strip().lower()
    if len(candidate) < PREFIX_LENGTH or not _HEX.match(candidate):
        return None
    matches = {d for d in digests if d.startswith(candidate)}
    if len(matches) != 1:
        return None
    return next(iter(matches))


def prefix_collisions(digests: Sequence[str]) -> set[str]:
    """Display prefixes shared by more than one distinct digest."""
    by_prefix: dict[str, set[str]] = {}
    for d in digests:
        by_prefix.setdefault(display_prefix(d), set()).add(d)
    return {p for p, ds in by_prefix.items() if len(ds) > 1}
