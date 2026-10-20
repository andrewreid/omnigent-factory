"""Durable state of the follow-up discipline (migration 17).

Plain functions over :class:`~omnigent_factory.store.sqlite.SqliteStore`, run on the
service's store worker (as ``store.ranking``). Every write is one short ``BEGIN
IMMEDIATE`` transaction and idempotent: a repeated call (a retry, a restart, a webhook
delivered twice) changes nothing it already recorded.

* Deferred findings of a source issue and the one consolidated follow-up issue per PR
  (``service.deferred``).
* Ready report checkboxes the owner ticked and the rework batches they started.
* The owner's "Review rounds" field, the review-bot rounds seen per issue and "one more
  round" grants (``service.review_rounds``).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass

from omnigent_factory.store.sqlite import SqliteStore

# ------------------------------------------------------------------ deferred findings


@dataclass(frozen=True, slots=True)
class DeferredItem:
    """One deferred finding of ``source_issue`` (``outcome`` ``filed``: a legacy
    ``follow_ups`` issue the run opened itself, ``issue_number``)."""

    source_issue: int
    item_key: str
    seq: int
    pr_number: int
    session_id: str
    severity: str
    path: str
    title: str
    thread_url: str
    reason: str
    outcome: str
    blocks: tuple[int, ...]
    issue_number: int | None
    #: In the PR's consolidated follow-up issue body.
    listed: bool
    #: That issue's line was updated after the item was fixed in a rework.
    closed_out: bool

    @property
    def action_id(self) -> str:
        """The Ready report checkbox of this item (never its text)."""
        return f"d-{self.item_key[:16]}"


_ITEM_COLUMNS = (
    "source_issue, item_key, seq, pr_number, session_id, severity, path, title, thread_url, "
    "reason, outcome, blocks_json, issue_number, listed, closed_out"
)


def _item(row: Sequence[object]) -> DeferredItem:
    return DeferredItem(
        source_issue=int(str(row[0])),
        item_key=str(row[1]),
        seq=int(str(row[2])),
        pr_number=int(str(row[3])),
        session_id=str(row[4]),
        severity=str(row[5]),
        path=str(row[6]),
        title=str(row[7]),
        thread_url=str(row[8]),
        reason=str(row[9]),
        outcome=str(row[10]),
        blocks=tuple(int(n) for n in json.loads(str(row[11]))),
        issue_number=None if row[12] is None else int(str(row[12])),
        listed=bool(row[13]),
        closed_out=bool(row[14]),
    )


@dataclass(frozen=True, slots=True)
class NewDeferred:
    item_key: str
    severity: str
    path: str
    title: str
    thread_url: str
    reason: str
    outcome: str
    blocks: tuple[int, ...] = ()
    issue_number: int | None = None


def record_deferred(
    store: SqliteStore,
    *,
    repo_id: str,
    source_issue: int,
    pr_number: int,
    session_id: str,
    items: Sequence[NewDeferred],
) -> list[DeferredItem]:
    """Record the result's deferred items (an item already recorded keeps its row; only
    its ``blocks`` grow). Returns the rows of ``items``."""
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM deferred_items WHERE repo_id = ? "
            "AND source_issue = ?",
            (repo_id, source_issue),
        ).fetchone()
        seq = int(row[0])
        for item in items:
            old = conn.execute(
                "SELECT blocks_json FROM deferred_items WHERE repo_id = ? AND source_issue = ? "
                "AND item_key = ?",
                (repo_id, source_issue, item.item_key),
            ).fetchone()
            if old is not None:
                blocks = sorted({*json.loads(str(old[0])), *item.blocks})
                conn.execute(
                    "UPDATE deferred_items SET blocks_json = ?, updated_at_us = ? "
                    "WHERE repo_id = ? AND source_issue = ? AND item_key = ?",
                    (json.dumps(blocks), now, repo_id, source_issue, item.item_key),
                )
                continue
            seq += 1
            conn.execute(
                "INSERT INTO deferred_items (repo_id, source_issue, item_key, seq, pr_number, "
                "session_id, severity, path, title, thread_url, reason, outcome, blocks_json, "
                "issue_number, created_at_us, updated_at_us) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    repo_id,
                    source_issue,
                    item.item_key,
                    seq,
                    pr_number,
                    session_id,
                    item.severity,
                    item.path,
                    item.title,
                    item.thread_url,
                    item.reason,
                    item.outcome,
                    json.dumps(sorted(set(item.blocks))),
                    item.issue_number,
                    now,
                    now,
                ),
            )
    keys = {item.item_key for item in items}
    return [
        i
        for i in deferred_items(store, repo_id=repo_id, source_issue=source_issue)
        if i.item_key in keys
    ]


def deferred_items(store: SqliteStore, *, repo_id: str, source_issue: int) -> list[DeferredItem]:
    rows = store._conn.execute(
        f"SELECT {_ITEM_COLUMNS} FROM deferred_items WHERE repo_id = ? AND source_issue = ? "
        "ORDER BY seq",
        (repo_id, source_issue),
    ).fetchall()
    return [_item(r) for r in rows]


def mark_items(
    store: SqliteStore,
    *,
    repo_id: str,
    source_issue: int,
    keys: Sequence[str],
    column: str,
) -> None:
    """Set ``listed`` or ``closed_out`` on the items ``keys``."""
    if column not in ("listed", "closed_out"):
        raise ValueError(f"unknown deferred item flag {column}")
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        for key in keys:
            conn.execute(
                f"UPDATE deferred_items SET {column} = 1, updated_at_us = ? "
                "WHERE repo_id = ? AND source_issue = ? AND item_key = ?",
                (now, repo_id, source_issue, key),
            )


@dataclass(frozen=True, slots=True)
class Consolidated:
    """The consolidated follow-up issue of one PR (``issue_number`` None until known)."""

    source_issue: int
    pr_number: int
    issue_number: int | None
    state: str
    #: When the state last changed (a ``creating`` row: when the create was attempted).
    updated_at_us: int = 0


def consolidated(
    store: SqliteStore, *, repo_id: str, source_issue: int, pr_number: int | None = None
) -> list[Consolidated]:
    sql = (
        "SELECT source_issue, pr_number, issue_number, state, updated_at_us "
        "FROM consolidated_follow_ups "
        "WHERE repo_id = ? AND source_issue = ?"
    )
    params: tuple[object, ...] = (repo_id, source_issue)
    if pr_number is not None:
        sql += " AND pr_number = ?"
        params = (*params, pr_number)
    rows = store._conn.execute(sql + " ORDER BY pr_number", params).fetchall()
    return [
        Consolidated(
            int(r[0]), int(r[1]), None if r[2] is None else int(r[2]), str(r[3]), int(r[4])
        )
        for r in rows
    ]


def set_consolidated(
    store: SqliteStore,
    *,
    repo_id: str,
    source_issue: int,
    pr_number: int,
    state: str,
    issue_number: int | None = None,
) -> None:
    """Record the consolidated issue's state; a known issue number is never replaced."""
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        conn.execute(
            "INSERT INTO consolidated_follow_ups (repo_id, source_issue, pr_number, "
            "issue_number, state, created_at_us, updated_at_us) VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(repo_id, source_issue, pr_number) DO UPDATE SET "
            "issue_number = COALESCE(consolidated_follow_ups.issue_number, "
            "excluded.issue_number), state = CASE WHEN consolidated_follow_ups.state = "
            "'created' THEN 'created' ELSE excluded.state END, updated_at_us = "
            "excluded.updated_at_us",
            (repo_id, source_issue, pr_number, issue_number, state, now, now),
        )


def unsettled_sources(store: SqliteStore, *, repo_id: str) -> list[int]:
    """Source issues with follow-up items not yet in their consolidated issue, or items
    fixed in a rework whose consolidated line is not updated yet."""
    rows = store._conn.execute(
        "SELECT DISTINCT source_issue FROM deferred_items WHERE repo_id = ? AND outcome = "
        "'follow_up' AND listed = 0 ORDER BY source_issue",
        (repo_id,),
    ).fetchall()
    return [int(r[0]) for r in rows]


# ------------------------------------------------------------------ Ready report checkboxes


@dataclass(frozen=True, slots=True)
class ReportAction:
    """A Ready report checkbox the owner ticked (``ticked``), and what came of it."""

    parcel_id: str
    action_id: str
    #: The Ready report comment the box is in.
    comment_id: int
    state: str
    ticked_at_us: int
    actor_id: int
    via: str
    batch_id: str | None
    detail: str


def _action(row: Sequence[object]) -> ReportAction:
    return ReportAction(
        parcel_id=str(row[0]),
        action_id=str(row[1]),
        comment_id=int(str(row[2])),
        state=str(row[3]),
        ticked_at_us=int(str(row[4])),
        actor_id=int(str(row[5])),
        via=str(row[6]),
        batch_id=None if row[7] is None else str(row[7]),
        detail=str(row[8]),
    )


_ACTION_COLUMNS = (
    "parcel_id, action_id, comment_id, state, ticked_at_us, actor_id, via, batch_id, detail"
)


def record_tick(
    store: SqliteStore,
    *,
    repo_id: str,
    parcel_id: str,
    action_id: str,
    comment_id: int,
    ticked_at_us: int,
    actor_id: int,
    via: str,
) -> bool:
    """Record the owner's tick of ``action_id`` once (a tick already recorded, whatever
    became of it, is never recorded again). Returns whether it was new."""
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO report_actions (repo_id, parcel_id, action_id, comment_id, "
            "state, ticked_at_us, actor_id, via, created_at_us, updated_at_us) "
            "VALUES (?, ?, ?, ?, 'ticked', ?, ?, ?, ?, ?)",
            (repo_id, parcel_id, action_id, comment_id, ticked_at_us, actor_id, via, now, now),
        )
        return cur.rowcount == 1


def report_actions(store: SqliteStore, *, repo_id: str, parcel_id: str) -> dict[str, ReportAction]:
    rows = store._conn.execute(
        f"SELECT {_ACTION_COLUMNS} FROM report_actions WHERE repo_id = ? AND parcel_id = ?",
        (repo_id, parcel_id),
    ).fetchall()
    return {str(r[1]): _action(r) for r in rows}


def ticked_actions(store: SqliteStore, *, repo_id: str) -> list[ReportAction]:
    """Ticks not yet acted on, oldest first."""
    rows = store._conn.execute(
        f"SELECT {_ACTION_COLUMNS} FROM report_actions WHERE repo_id = ? AND state = 'ticked' "
        "ORDER BY ticked_at_us, action_id",
        (repo_id,),
    ).fetchall()
    return [_action(r) for r in rows]


@dataclass(frozen=True, slots=True)
class ActionBatch:
    batch_id: str
    parcel_id: str
    event_id: str
    actions: tuple[str, ...]
    brief: str
    state: str
    reason: str


_BATCH_COLUMNS = "batch_id, parcel_id, event_id, actions_json, brief, state, reason"


def _batch(row: Sequence[object]) -> ActionBatch:
    return ActionBatch(
        batch_id=str(row[0]),
        parcel_id=str(row[1]),
        event_id=str(row[2]),
        actions=tuple(str(a) for a in json.loads(str(row[3]))),
        brief=str(row[4]),
        state=str(row[5]),
        reason=str(row[6]),
    )


def start_batch(
    store: SqliteStore,
    *,
    repo_id: str,
    parcel_id: str,
    batch_id: str,
    event_id: str,
    actions: Sequence[str],
    brief: str,
) -> ActionBatch:
    """Bind the ticked ``actions`` to one batch (``applying``) before its rework event is
    applied: a restart re-applies the same event (deduplicated by its id), never a second
    one. Returns the stored batch (an existing one is returned unchanged)."""
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        found = conn.execute(
            f"SELECT {_BATCH_COLUMNS} FROM report_action_batches WHERE repo_id = ? "
            "AND batch_id = ?",
            (repo_id, batch_id),
        ).fetchone()
        if found is not None:
            return _batch(found)
        conn.execute(
            "INSERT INTO report_action_batches (repo_id, batch_id, parcel_id, event_id, "
            "actions_json, brief, state, created_at_us, updated_at_us) "
            "VALUES (?, ?, ?, ?, ?, ?, 'applying', ?, ?)",
            (repo_id, batch_id, parcel_id, event_id, json.dumps(list(actions)), brief, now, now),
        )
        for action_id in actions:
            conn.execute(
                "UPDATE report_actions SET state = 'started', batch_id = ?, updated_at_us = ? "
                "WHERE repo_id = ? AND parcel_id = ? AND action_id = ? AND state = 'ticked'",
                (batch_id, now, repo_id, parcel_id, action_id),
            )
    return ActionBatch(batch_id, parcel_id, event_id, tuple(actions), brief, "applying", "")


def finish_batch(
    store: SqliteStore, *, repo_id: str, batch_id: str, state: str, reason: str = ""
) -> None:
    """``started`` (the rework was accepted), ``refused`` (its actions with it) or
    ``done`` (the rework's build_ready was accepted; ``reason`` is the head)."""
    if state not in ("started", "refused", "done"):
        raise ValueError(f"unknown batch state {state}")
    now = store._clock.now_utc_us()
    action_state = {"started": "started", "refused": "refused", "done": "done"}[state]
    with store._txn() as conn:
        conn.execute(
            "UPDATE report_action_batches SET state = ?, reason = ?, updated_at_us = ? "
            "WHERE repo_id = ? AND batch_id = ?",
            (state, reason, now, repo_id, batch_id),
        )
        conn.execute(
            "UPDATE report_actions SET state = ?, detail = ?, updated_at_us = ? "
            "WHERE repo_id = ? AND batch_id = ?",
            (action_state, reason, now, repo_id, batch_id),
        )


def batches(
    store: SqliteStore, *, repo_id: str, parcel_id: str | None = None, state: str | None = None
) -> list[ActionBatch]:
    sql = f"SELECT {_BATCH_COLUMNS} FROM report_action_batches WHERE repo_id = ?"
    params: tuple[object, ...] = (repo_id,)
    if parcel_id is not None:
        sql += " AND parcel_id = ?"
        params = (*params, parcel_id)
    if state is not None:
        sql += " AND state = ?"
        params = (*params, state)
    rows = store._conn.execute(sql + " ORDER BY created_at_us, batch_id", params).fetchall()
    return [_batch(r) for r in rows]


def batch_by_event(store: SqliteStore, *, repo_id: str, event_id: str) -> ActionBatch | None:
    row = store._conn.execute(
        f"SELECT {_BATCH_COLUMNS} FROM report_action_batches WHERE repo_id = ? AND event_id = ?",
        (repo_id, event_id),
    ).fetchone()
    return None if row is None else _batch(row)


# ------------------------------------------------------------------ review rounds


def set_round_budget(
    store: SqliteStore,
    *,
    repo_id: str,
    issue_node_id: str,
    value: int | None,
    actor_id: int,
    at_us: int,
) -> bool:
    """The owner set (or, ``value`` None, cleared) the issue's "Review rounds" field at
    ``at_us`` (an older edit than the one recorded changes nothing). Returns whether the
    stored value changed."""
    with store._txn() as conn:
        row = conn.execute(
            "SELECT value, changed_at_us FROM review_round_budgets WHERE repo_id = ? "
            "AND issue_node_id = ?",
            (repo_id, issue_node_id),
        ).fetchone()
        if row is not None and at_us < int(row[1]):
            return False
        conn.execute(
            "INSERT INTO review_round_budgets (repo_id, issue_node_id, value, actor_id, "
            "changed_at_us) VALUES (?, ?, ?, ?, ?) ON CONFLICT(repo_id, issue_node_id) DO "
            "UPDATE SET value = excluded.value, actor_id = excluded.actor_id, "
            "changed_at_us = excluded.changed_at_us",
            (repo_id, issue_node_id, value, actor_id, at_us),
        )
        return row is None or (None if row[0] is None else int(row[0])) != value


def round_budget(store: SqliteStore, *, repo_id: str, issue_node_id: str) -> int | None:
    """The owner's "Review rounds" value for the issue (None: never set by the owner, or
    cleared)."""
    row = store._conn.execute(
        "SELECT value FROM review_round_budgets WHERE repo_id = ? AND issue_node_id = ?",
        (repo_id, issue_node_id),
    ).fetchone()
    return None if row is None or row[0] is None else int(row[0])


def record_rounds(
    store: SqliteStore,
    *,
    repo_id: str,
    parcel_id: str,
    pr_number: int,
    rounds: Sequence[tuple[str, int]],
) -> int:
    """Record review-bot reviews (commit key, review time) of the issue's PR; a commit is
    one round however often it is seen. Returns how many were new."""
    now = store._clock.now_utc_us()
    added = 0
    with store._txn() as conn:
        for key, at_us in rounds:
            cur = conn.execute(
                "INSERT OR IGNORE INTO review_rounds (repo_id, parcel_id, commit_key, "
                "pr_number, reviewed_at_us, recorded_at_us) VALUES (?, ?, ?, ?, ?, ?)",
                (repo_id, parcel_id, key, pr_number, at_us, now),
            )
            added += cur.rowcount
    return added


def review_rounds(store: SqliteStore, *, repo_id: str, parcel_id: str) -> list[tuple[str, int]]:
    """(commit key, review time) of every round recorded for the issue, oldest first."""
    rows = store._conn.execute(
        "SELECT commit_key, reviewed_at_us FROM review_rounds WHERE repo_id = ? "
        "AND parcel_id = ? ORDER BY reviewed_at_us, commit_key",
        (repo_id, parcel_id),
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def grant_round(
    store: SqliteStore, *, repo_id: str, parcel_id: str, grant_key: str, at_us: int
) -> bool:
    """One more review round for the issue, once per ``grant_key``. Returns whether new."""
    with store._txn() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO review_round_grants (repo_id, parcel_id, grant_key, "
            "granted_at_us) VALUES (?, ?, ?, ?)",
            (repo_id, parcel_id, grant_key, at_us),
        )
        return cur.rowcount == 1


def round_grants(store: SqliteStore, *, repo_id: str, parcel_id: str) -> int:
    row = store._conn.execute(
        "SELECT COUNT(*) FROM review_round_grants WHERE repo_id = ? AND parcel_id = ?",
        (repo_id, parcel_id),
    ).fetchone()
    return int(row[0])


# ------------------------------------------------------------------ owner-directed splits


@dataclass(frozen=True, slots=True)
class SplitIssue:
    """One ``split_out`` item of ``source_issue`` and the issue the factory filed for it."""

    source_issue: int
    item_key: str
    seq: int
    session_id: str
    title: str
    body: str
    reason: str
    relation: str
    blocks: tuple[int, ...]
    issue_number: int | None
    state: str
    done: frozenset[str]
    attempt_at_us: int

    @property
    def source_relation(self) -> str:
        """``relation`` with a legacy value (``blocks``/``blocked_by``) read as
        ``after_source``."""
        return split_relation(self.relation)

    def pending_links(self) -> tuple[str, ...]:
        """Link actions still to do: ``parent`` (the source's epic), ``blocks:<M>``
        ("M blocked by this issue") and ``blocked_by:<S>`` (this issue blocked by S).
        The source link is done once either direction to the source is done, so a legacy
        row linked under the old reading is never linked the other way too."""
        wanted = ["parent", *(f"blocks:{n}" for n in self.blocks)]
        source = (f"blocks:{self.source_issue}", f"blocked_by:{self.source_issue}")
        if not self.done.intersection(source):
            if self.source_relation == "before_source":
                wanted.append(source[0])
            elif self.source_relation == "after_source":
                wanted.append(source[1])
        return tuple(a for a in dict.fromkeys(wanted) if a not in self.done)


#: Split relations before ``after_source``/``before_source``; their subject was unstated,
#: so both read as the common case, ``after_source``.
LEGACY_SPLIT_RELATIONS = frozenset({"blocks", "blocked_by"})


def split_relation(relation: str) -> str:
    """A split's relation to its source issue: ``none``, ``after_source`` (the new issue
    is blocked by the source) or ``before_source`` (the new issue blocks the source)."""
    return "after_source" if relation in LEGACY_SPLIT_RELATIONS else relation


@dataclass(frozen=True, slots=True)
class NewSplit:
    item_key: str
    title: str
    body: str
    reason: str
    relation: str = "none"
    blocks: tuple[int, ...] = ()


_SPLIT_COLUMNS = (
    "source_issue, item_key, seq, session_id, title, body, reason, relation, blocks_json, "
    "issue_number, state, done_json, attempt_at_us"
)


def _split(row: Sequence[object]) -> SplitIssue:
    return SplitIssue(
        source_issue=int(str(row[0])),
        item_key=str(row[1]),
        seq=int(str(row[2])),
        session_id=str(row[3]),
        title=str(row[4]),
        body=str(row[5]),
        reason=str(row[6]),
        relation=str(row[7]),
        blocks=tuple(int(n) for n in json.loads(str(row[8]))),
        issue_number=None if row[9] is None else int(str(row[9])),
        state=str(row[10]),
        done=frozenset(str(a) for a in json.loads(str(row[11]))),
        attempt_at_us=int(str(row[12])),
    )


def record_splits(
    store: SqliteStore,
    *,
    repo_id: str,
    source_issue: int,
    session_id: str,
    items: Sequence[NewSplit],
) -> int:
    """Record a result's ``split_out`` items (an item already recorded is kept as it is).
    Returns how many were new."""
    now = store._clock.now_utc_us()
    added = 0
    with store._txn() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM split_issues WHERE repo_id = ? AND source_issue = ?",
            (repo_id, source_issue),
        ).fetchone()
        seq = int(row[0])
        for item in items:
            cur = conn.execute(
                "INSERT OR IGNORE INTO split_issues (repo_id, source_issue, item_key, seq, "
                "session_id, title, body, reason, relation, blocks_json, created_at_us, "
                "updated_at_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    repo_id,
                    source_issue,
                    item.item_key,
                    seq + 1,
                    session_id,
                    item.title,
                    item.body,
                    item.reason,
                    item.relation,
                    json.dumps(sorted(set(item.blocks))),
                    now,
                    now,
                ),
            )
            if cur.rowcount == 1:
                seq += 1
                added += 1
    return added


def splits(
    store: SqliteStore, *, repo_id: str, source_issue: int | None = None
) -> list[SplitIssue]:
    sql = f"SELECT {_SPLIT_COLUMNS} FROM split_issues WHERE repo_id = ?"
    params: tuple[object, ...] = (repo_id,)
    if source_issue is not None:
        sql += " AND source_issue = ?"
        params = (*params, source_issue)
    rows = store._conn.execute(sql + " ORDER BY source_issue, seq", params).fetchall()
    return [_split(r) for r in rows]


def set_split_state(
    store: SqliteStore,
    *,
    repo_id: str,
    source_issue: int,
    item_key: str,
    state: str,
    issue_number: int | None = None,
) -> None:
    """``creating`` (stamps the attempt) or ``created`` (with its number, never replaced)."""
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        conn.execute(
            "UPDATE split_issues SET state = CASE WHEN state = 'created' THEN 'created' "
            "ELSE ? END, issue_number = COALESCE(issue_number, ?), attempt_at_us = CASE "
            "WHEN ? = 'creating' THEN ? ELSE attempt_at_us END, updated_at_us = ? "
            "WHERE repo_id = ? AND source_issue = ? AND item_key = ?",
            (state, issue_number, state, now, now, repo_id, source_issue, item_key),
        )


def mark_split_done(
    store: SqliteStore, *, repo_id: str, source_issue: int, item_key: str, action: str
) -> None:
    with store._txn() as conn:
        row = conn.execute(
            "SELECT done_json FROM split_issues WHERE repo_id = ? AND source_issue = ? "
            "AND item_key = ?",
            (repo_id, source_issue, item_key),
        ).fetchone()
        if row is None:
            return
        done = sorted({*json.loads(str(row[0])), action})
        conn.execute(
            "UPDATE split_issues SET done_json = ?, updated_at_us = ? WHERE repo_id = ? "
            "AND source_issue = ? AND item_key = ?",
            (json.dumps(done), store._clock.now_utc_us(), repo_id, source_issue, item_key),
        )
