"""Durable state of the triage ranking and of session retention (migration 11).

Plain functions over :class:`~omnigent_factory.store.sqlite.SqliteStore`, run on the
service's store worker. Every write is one short ``BEGIN IMMEDIATE`` transaction; a
submission and its outbox rows (``ranking_writes``) commit together.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from omnigent_factory.store.sqlite import SqliteStore

#: Run states with work still to do (at most one such run per repository).
OPEN_STATES = ("creating", "preparing", "sending", "running", "applying")
#: Finished runs kept for ``ranking status``; older ones are pruned once archived.
KEEP_FINISHED_RUNS = 10


@dataclass(frozen=True, slots=True)
class RankingState:
    enabled_override: bool | None = None
    override_config: bool | None = None
    now_requested_at_us: int | None = None
    last_completed_at_us: int | None = None
    last_triage_digest: str | None = None
    failures: int = 0
    retry_after_us: int | None = None

    def enabled(self, config_value: bool) -> bool:
        """The override while the config still says what it said then; else the config."""
        if self.enabled_override is None or self.override_config != config_value:
            return config_value
        return self.enabled_override

    def source(self, config_value: bool) -> str:
        overridden = self.enabled_override is not None and self.override_config == config_value
        return "operator" if overridden else "config"


@dataclass(frozen=True, slots=True)
class RankingRun:
    run_id: str
    nonce: str
    title: str
    state: str
    triage_digest: str
    created_at_us: int
    forced: bool = False
    root_id: str | None = None
    policy_ready_at_us: int | None = None
    submission_json: str | None = None
    outcome: str = ""
    started_at_us: int | None = None
    finished_at_us: int | None = None
    archived_at_us: int | None = None

    @property
    def open(self) -> bool:
        return self.state in OPEN_STATES


@dataclass(frozen=True, slots=True)
class RankingWrite:
    write_id: str
    run_id: str
    seq: int
    kind: str
    payload: Mapping[str, Any]
    issue_node_id: str | None = None
    issue_number: int | None = None
    state: str = "pending"
    attempts: int = 0
    detail: str = ""


@dataclass(frozen=True, slots=True)
class BoardField:
    """What the factory last wrote to one issue's field, and the owner's choice."""

    issue_node_id: str
    field: str
    factory_value: str | None = None
    owner_value: str | None = None
    #: Rank: pinned at ``owner_value``. Priority: the owner chose it (sticky).
    owner_set: bool = False
    #: An owner webhook changed the field: the next board read records its value.
    owner_refresh: bool = False


# ---------------------------------------------------------------------- state


def ranking_state(store: SqliteStore, repo_id: str) -> RankingState:
    row = store.query("SELECT * FROM ranking WHERE repo_id = ?", (repo_id,))
    if not row:
        return RankingState()
    r = row[0]
    return RankingState(
        enabled_override=None if r["enabled_override"] is None else bool(r["enabled_override"]),
        override_config=None if r["override_config"] is None else bool(r["override_config"]),
        now_requested_at_us=r["now_requested_at_us"],
        last_completed_at_us=r["last_completed_at_us"],
        last_triage_digest=r["last_triage_digest"],
        failures=int(r["failures"]),
        retry_after_us=r["retry_after_us"],
    )


def _upsert_state(
    conn: sqlite3.Connection, repo_id: str, now_us: int, assignments: Mapping[str, object]
) -> None:
    conn.execute(
        "INSERT INTO ranking (repo_id, updated_at_us) VALUES (?, ?) "
        "ON CONFLICT(repo_id) DO NOTHING",
        (repo_id, now_us),
    )
    columns = ", ".join(f"{name} = ?" for name in assignments)
    conn.execute(
        f"UPDATE ranking SET {columns}, updated_at_us = ? WHERE repo_id = ?",
        (*assignments.values(), now_us, repo_id),
    )


def set_ranking_override(
    store: SqliteStore, repo_id: str, enabled: bool, config_value: bool, now_us: int
) -> None:
    with store._txn() as conn:
        _upsert_state(
            conn,
            repo_id,
            now_us,
            {"enabled_override": int(enabled), "override_config": int(config_value)},
        )


def request_ranking_now(store: SqliteStore, repo_id: str, now_us: int) -> None:
    with store._txn() as conn:
        _upsert_state(conn, repo_id, now_us, {"now_requested_at_us": now_us})


# ---------------------------------------------------------------------- runs


def _run(r: sqlite3.Row) -> RankingRun:
    return RankingRun(
        run_id=r["run_id"],
        nonce=r["nonce"],
        title=r["title"],
        state=r["state"],
        triage_digest=r["triage_digest"],
        created_at_us=int(r["created_at_us"]),
        forced=bool(r["forced"]),
        root_id=r["root_id"],
        policy_ready_at_us=r["policy_ready_at_us"],
        submission_json=r["submission_json"],
        outcome=r["outcome"],
        started_at_us=r["started_at_us"],
        finished_at_us=r["finished_at_us"],
        archived_at_us=r["archived_at_us"],
    )


def open_run(store: SqliteStore, repo_id: str) -> RankingRun | None:
    rows = store.query(
        "SELECT * FROM ranking_runs WHERE repo_id = ? AND state IN "
        "(SELECT value FROM json_each(?)) ORDER BY created_at_us LIMIT 1",
        (repo_id, json.dumps(OPEN_STATES)),
    )
    return _run(rows[0]) if rows else None


def run_by_root(store: SqliteStore, root_id: str) -> RankingRun | None:
    rows = store.query("SELECT * FROM ranking_runs WHERE root_id = ?", (root_id,))
    return _run(rows[0]) if rows else None


def get_run(store: SqliteStore, run_id: str) -> RankingRun | None:
    rows = store.query("SELECT * FROM ranking_runs WHERE run_id = ?", (run_id,))
    return _run(rows[0]) if rows else None


def recent_runs(store: SqliteStore, repo_id: str, limit: int = 5) -> list[RankingRun]:
    rows = store.query(
        "SELECT * FROM ranking_runs WHERE repo_id = ? ORDER BY created_at_us DESC LIMIT ?",
        (repo_id, limit),
    )
    return [_run(r) for r in rows]


def unarchived_runs(store: SqliteStore, repo_id: str) -> list[RankingRun]:
    """Finished runs whose session still needs archiving."""
    rows = store.query(
        "SELECT * FROM ranking_runs WHERE repo_id = ? AND state IN ('done', 'failed') "
        "AND root_id IS NOT NULL AND archived_at_us IS NULL ORDER BY created_at_us",
        (repo_id,),
    )
    return [_run(r) for r in rows]


def unresolved_runs(store: SqliteStore, repo_id: str) -> list[RankingRun]:
    """Failed runs whose create outcome was never learned (a session may exist)."""
    rows = store.query(
        "SELECT * FROM ranking_runs WHERE repo_id = ? AND state = 'failed' "
        "AND root_id IS NULL AND archived_at_us IS NULL ORDER BY created_at_us",
        (repo_id,),
    )
    return [_run(r) for r in rows]


def insert_run(store: SqliteStore, repo_id: str, run: RankingRun) -> bool:
    """Record a new run (its intent) unless one is open; a ``now`` request is consumed."""
    with store._txn() as conn:
        busy = conn.execute(
            "SELECT 1 FROM ranking_runs WHERE repo_id = ? AND state IN "
            "(SELECT value FROM json_each(?)) LIMIT 1",
            (repo_id, json.dumps(OPEN_STATES)),
        ).fetchone()
        if busy is not None:
            return False
        conn.execute(
            "INSERT INTO ranking_runs (run_id, repo_id, nonce, title, state, forced, "
            "triage_digest, created_at_us, updated_at_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run.run_id,
                repo_id,
                run.nonce,
                run.title,
                run.state,
                int(run.forced),
                run.triage_digest,
                run.created_at_us,
                run.created_at_us,
            ),
        )
        _upsert_state(conn, repo_id, run.created_at_us, {"now_requested_at_us": None})
    return True


_RUN_COLUMNS = frozenset(
    {"state", "root_id", "policy_ready_at_us", "outcome", "started_at_us", "archived_at_us"}
)


def update_run(store: SqliteStore, run_id: str, now_us: int, **fields: object) -> None:
    unknown = set(fields) - _RUN_COLUMNS
    if unknown:
        raise ValueError(f"unknown ranking run columns: {sorted(unknown)}")
    columns = ", ".join(f"{name} = ?" for name in fields)
    with store._txn() as conn:
        conn.execute(
            f"UPDATE ranking_runs SET {columns}, updated_at_us = ? WHERE run_id = ?",
            (*fields.values(), now_us, run_id),
        )


def accept_submission(
    store: SqliteStore,
    run_id: str,
    submission: Mapping[str, Any],
    writes: Sequence[RankingWrite],
    now_us: int,
) -> bool:
    """Record the run's one submission and its outbox in one transaction.

    Refused (False) unless the run is still waiting for its result.
    """
    with store._txn() as conn:
        row = conn.execute(
            "SELECT state, submission_json FROM ranking_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None or row["state"] != "running" or row["submission_json"] is not None:
            return False
        conn.execute(
            "UPDATE ranking_runs SET state = 'applying', submission_json = ?, "
            "updated_at_us = ? WHERE run_id = ?",
            (json.dumps(submission, sort_keys=True), now_us, run_id),
        )
        conn.executemany(
            "INSERT INTO ranking_writes (write_id, run_id, seq, kind, issue_node_id, "
            "issue_number, payload_json, state, updated_at_us) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)",
            [
                (
                    w.write_id,
                    run_id,
                    w.seq,
                    w.kind,
                    w.issue_node_id,
                    w.issue_number,
                    json.dumps(w.payload, sort_keys=True),
                    now_us,
                )
                for w in writes
            ],
        )
    return True


def run_writes(store: SqliteStore, run_id: str) -> list[RankingWrite]:
    rows = store.query("SELECT * FROM ranking_writes WHERE run_id = ? ORDER BY seq", (run_id,))
    return [
        RankingWrite(
            write_id=r["write_id"],
            run_id=r["run_id"],
            seq=int(r["seq"]),
            kind=r["kind"],
            payload=json.loads(r["payload_json"]),
            issue_node_id=r["issue_node_id"],
            issue_number=r["issue_number"],
            state=r["state"],
            attempts=int(r["attempts"]),
            detail=r["detail"],
        )
        for r in rows
    ]


def update_write(
    store: SqliteStore, write_id: str, state: str, detail: str, now_us: int, *, attempt: bool
) -> None:
    with store._txn() as conn:
        conn.execute(
            "UPDATE ranking_writes SET state = ?, detail = ?, attempts = attempts + ?, "
            "updated_at_us = ? WHERE write_id = ?",
            (state, detail[:300], int(attempt), now_us, write_id),
        )


def finish_run(
    store: SqliteStore,
    repo_id: str,
    run_id: str,
    *,
    ok: bool,
    outcome: str,
    now_us: int,
    backoff_us: int = 0,
) -> None:
    """End a run: a completed one resets the trigger; a failed one backs off."""
    with store._txn() as conn:
        row = conn.execute(
            "SELECT state, triage_digest FROM ranking_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None or row["state"] in ("done", "failed"):
            return
        conn.execute(
            "UPDATE ranking_runs SET state = ?, outcome = ?, finished_at_us = ?, "
            "updated_at_us = ? WHERE run_id = ?",
            ("done" if ok else "failed", outcome[:500], now_us, now_us, run_id),
        )
        if ok:
            _upsert_state(
                conn,
                repo_id,
                now_us,
                {
                    "last_completed_at_us": now_us,
                    "last_triage_digest": row["triage_digest"],
                    "failures": 0,
                    "retry_after_us": None,
                },
            )
        else:
            conn.execute(
                "INSERT INTO ranking (repo_id, updated_at_us) VALUES (?, ?) "
                "ON CONFLICT(repo_id) DO NOTHING",
                (repo_id, now_us),
            )
            conn.execute(
                "UPDATE ranking SET failures = failures + 1, retry_after_us = ?, "
                "updated_at_us = ? WHERE repo_id = ?",
                (now_us + backoff_us, now_us, repo_id),
            )


def prune_runs(
    store: SqliteStore,
    repo_id: str,
    *,
    keep: int = KEEP_FINISHED_RUNS,
    require_deleted: bool = True,
) -> int:
    """Delete finished runs (and their outbox rows) beyond the newest ``keep``.

    A run's row is its session's only record: it goes once session retention deleted the
    session (``require_deleted``), else once the session is archived.
    """
    settled = (
        "root_id IN (SELECT root_id FROM session_deletions)"
        if require_deleted
        else "archived_at_us IS NOT NULL"
    )
    with store._txn() as conn:
        rows = conn.execute(
            "SELECT run_id FROM ranking_runs WHERE repo_id = ? AND state IN ('done', 'failed') "
            f"AND ((root_id IS NULL AND archived_at_us IS NOT NULL) OR {settled}) "
            "ORDER BY created_at_us DESC LIMIT -1 OFFSET ?",
            (repo_id, keep),
        ).fetchall()
        ids = [str(r[0]) for r in rows]
        for run_id in ids:
            conn.execute("DELETE FROM ranking_writes WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM ranking_runs WHERE run_id = ?", (run_id,))
    return len(ids)


def ranking_roots(store: SqliteStore, repo_id: str) -> list[tuple[str, str, bool]]:
    """(root, run id, finished) of every ranking run with a session."""
    rows = store.query(
        "SELECT root_id, run_id, state FROM ranking_runs WHERE repo_id = ? AND root_id IS NOT NULL",
        (repo_id,),
    )
    return [(str(r[0]), str(r[1]), str(r[2]) in ("done", "failed")) for r in rows]


def count_triage_results(store: SqliteStore, since_us: int) -> int:
    """Accepted triage results (one per triage submission) since ``since_us``."""
    row = store.query(
        "SELECT COUNT(*) FROM mcp_receipts WHERE tool = 'submit' AND created_at_us > ? "
        "AND json_extract(receipt_json, '$.kind') = 'triage'",
        (since_us,),
    )
    return int(row[0][0])


# ---------------------------------------------------------------------- fields


def board_fields(store: SqliteStore, node_ids: Iterable[str]) -> dict[tuple[str, str], BoardField]:
    rows = store.query(
        "SELECT * FROM board_fields WHERE issue_node_id IN (SELECT value FROM json_each(?))",
        (json.dumps(sorted(set(node_ids))),),
    )
    return {
        (r["issue_node_id"], r["field"]): BoardField(
            issue_node_id=r["issue_node_id"],
            field=r["field"],
            factory_value=r["factory_value"],
            owner_value=r["owner_value"],
            owner_set=bool(r["owner_set"]),
            owner_refresh=bool(r["owner_refresh"]),
        )
        for r in rows
    }


def save_board_field(store: SqliteStore, field: BoardField, now_us: int) -> None:
    with store._txn() as conn:
        conn.execute(
            "INSERT INTO board_fields (issue_node_id, field, factory_value, owner_value, "
            "owner_set, owner_refresh, updated_at_us) VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(issue_node_id, field) DO UPDATE SET "
            "factory_value = excluded.factory_value, owner_value = excluded.owner_value, "
            "owner_set = excluded.owner_set, owner_refresh = excluded.owner_refresh, "
            "updated_at_us = excluded.updated_at_us",
            (
                field.issue_node_id,
                field.field,
                field.factory_value,
                field.owner_value,
                int(field.owner_set),
                int(field.owner_refresh),
                now_us,
            ),
        )


def mark_owner_change(
    store: SqliteStore, node_id: str, field: str, value: str | None, now_us: int
) -> None:
    """An owner webhook changed ``field``: Rank pins (or unpins when cleared); Priority
    becomes the owner's for good. ``value`` None: unknown, re-read on the next board read.
    """
    with store._txn() as conn:
        conn.execute(
            "INSERT INTO board_fields (issue_node_id, field, updated_at_us) VALUES (?, ?, ?) "
            "ON CONFLICT(issue_node_id, field) DO NOTHING",
            (node_id, field, now_us),
        )
        conn.execute(
            "UPDATE board_fields SET owner_set = 1, owner_value = ?, owner_refresh = ?, "
            "updated_at_us = ? WHERE issue_node_id = ? AND field = ?",
            (value, int(value is None), now_us, node_id, field),
        )


# ---------------------------------------------------------------------- retention


def deleted_sessions(store: SqliteStore, root_ids: Iterable[str]) -> set[str]:
    rows = store.query(
        "SELECT root_id FROM session_deletions WHERE root_id IN (SELECT value FROM json_each(?))",
        (json.dumps(sorted(set(root_ids))),),
    )
    return {str(r[0]) for r in rows}


def record_session_deletion(
    store: SqliteStore, root_id: str, source: str, outcome: str, now_us: int
) -> None:
    with store._txn() as conn:
        conn.execute(
            "INSERT INTO session_deletions (root_id, source, outcome, deleted_at_us) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(root_id) DO NOTHING",
            (root_id, source, outcome, now_us),
        )
