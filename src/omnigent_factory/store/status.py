"""Durable state of the status comments (migration 20).

Plain functions over :class:`~omnigent_factory.store.sqlite.SqliteStore`, run on the
service's store worker (as ``store.discipline``). Every write is one short ``BEGIN
IMMEDIATE`` transaction.

* One row per parcel: its status comment, what was last written there and when, and the
  stage history the comment shows (``service.status_comment``).
* One row per repository: when status comments started and whether pinning works.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, replace

from omnigent_factory.store.sqlite import SqliteStore

#: Stage events a status comment keeps in its History (oldest dropped first).
HISTORY_MAX = 30


@dataclass(frozen=True, slots=True)
class StatusRow:
    """A parcel's status comment. ``state`` ``creating``: a create was attempted and may
    have landed (``comment_id`` None until it is seen); ``posted``: the comment exists."""

    parcel_id: str
    issue_number: int
    state: str
    comment_id: int | None = None
    node_id: str = ""
    url: str = ""
    #: Digest of the body last written (owner ticks aside) and when it was written.
    digest: str = ""
    written_at_us: int = 0
    #: A changed body is waiting for the per-issue edit rate limit.
    pending: bool = False
    #: Pin attempt: "" not tried, "pinned", "failed" (GitHub refused) or "skipped".
    pin: str = ""
    #: The parcel version last rendered.
    seen_version: int = 0
    headline: str = ""
    #: (time, headline) of each stage event shown, oldest first.
    history: tuple[tuple[int, str], ...] = ()


@dataclass(frozen=True, slots=True)
class StatusSettings:
    #: Parcels unchanged since this time get no status comment until they change.
    baseline_us: int
    #: "unknown" (never tried), "supported" or "unsupported" (recorded once, never retried).
    pin_state: str = "unknown"
    pin_message: str = ""


_COLUMNS = (
    "parcel_id, issue_number, state, comment_id, node_id, url, digest, written_at_us, "
    "pending, pin, seen_version, headline, history_json"
)


def _row(row: Sequence[object]) -> StatusRow:
    history = json.loads(str(row[12]))
    return StatusRow(
        parcel_id=str(row[0]),
        issue_number=int(str(row[1])),
        state=str(row[2]),
        comment_id=None if row[3] is None else int(str(row[3])),
        node_id=str(row[4]),
        url=str(row[5]),
        digest=str(row[6]),
        written_at_us=int(str(row[7])),
        pending=bool(row[8]),
        pin=str(row[9]),
        seen_version=int(str(row[10])),
        headline=str(row[11]),
        history=tuple((int(at), str(text)) for at, text in history),
    )


def status_row(store: SqliteStore, *, repo_id: str, parcel_id: str) -> StatusRow | None:
    row = store._conn.execute(
        f"SELECT {_COLUMNS} FROM status_comments WHERE repo_id = ? AND parcel_id = ?",
        (repo_id, parcel_id),
    ).fetchone()
    return None if row is None else _row(row)


def status_rows(store: SqliteStore, *, repo_id: str) -> list[StatusRow]:
    rows = store._conn.execute(
        f"SELECT {_COLUMNS} FROM status_comments WHERE repo_id = ? ORDER BY issue_number",
        (repo_id,),
    ).fetchall()
    return [_row(r) for r in rows]


def put_status(store: SqliteStore, *, repo_id: str, row: StatusRow) -> None:
    """Insert or replace the parcel's row (its history cut to :data:`HISTORY_MAX`)."""
    now = store._clock.now_utc_us()
    history = row.history[-HISTORY_MAX:]
    with store._txn() as conn:
        conn.execute(
            "INSERT INTO status_comments (repo_id, parcel_id, issue_number, state, comment_id, "
            "node_id, url, digest, written_at_us, pending, pin, seen_version, headline, "
            "history_json, created_at_us, updated_at_us) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (repo_id, parcel_id) DO UPDATE SET issue_number = excluded.issue_number, "
            "state = excluded.state, comment_id = excluded.comment_id, "
            "node_id = excluded.node_id, url = excluded.url, digest = excluded.digest, "
            "written_at_us = excluded.written_at_us, pending = excluded.pending, "
            "pin = excluded.pin, seen_version = excluded.seen_version, "
            "headline = excluded.headline, history_json = excluded.history_json, "
            "updated_at_us = excluded.updated_at_us",
            (
                repo_id,
                row.parcel_id,
                row.issue_number,
                row.state,
                row.comment_id,
                row.node_id,
                row.url,
                row.digest,
                row.written_at_us,
                int(row.pending),
                row.pin,
                row.seen_version,
                row.headline,
                json.dumps([[at, text] for at, text in history]),
                now,
                now,
            ),
        )


def status_settings(store: SqliteStore, *, repo_id: str) -> StatusSettings:
    """The repository's settings, created on first use (baseline: now)."""
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO status_settings (repo_id, baseline_us, updated_at_us) "
            "VALUES (?, ?, ?)",
            (repo_id, now, now),
        )
        row = conn.execute(
            "SELECT baseline_us, pin_state, pin_message FROM status_settings WHERE repo_id = ?",
            (repo_id,),
        ).fetchone()
    return StatusSettings(int(row[0]), str(row[1]), str(row[2]))


def set_pin_state(store: SqliteStore, *, repo_id: str, state: str, message: str = "") -> bool:
    """Record whether pinning works. ``unsupported`` is final: returns False (nothing
    changed) when it was already recorded, so the caller logs it only once."""
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        cur = conn.execute(
            "UPDATE status_settings SET pin_state = ?, pin_message = ?, updated_at_us = ? "
            "WHERE repo_id = ? AND pin_state != 'unsupported' AND pin_state != ?",
            (state, message[:500], now, repo_id, state),
        )
        return cur.rowcount == 1


def with_event(row: StatusRow, at_us: int, headline: str) -> StatusRow:
    """``row`` with ``headline`` as its newest stage event (unchanged when it is already)."""
    if headline == row.headline:
        return row
    history = (*row.history, (at_us, headline))[-HISTORY_MAX:]
    return replace(row, headline=headline, history=history)


__all__ = [
    "HISTORY_MAX",
    "StatusRow",
    "StatusSettings",
    "put_status",
    "set_pin_state",
    "status_row",
    "status_rows",
    "status_settings",
    "with_event",
]
