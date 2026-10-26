"""Durable state of issue bundling (migration 21).

Plain functions over :class:`~omnigent_factory.store.sqlite.SqliteStore`, run on the
service's store worker (as ``store.discipline``). Every write is one short ``BEGIN
IMMEDIATE`` transaction and idempotent: a repeated call (a retry, a restart, a webhook
delivered twice) changes nothing it already recorded.

* Proposals: one row per proposed member set (``bundle_proposals``), keyed by its sorted
  members, so the same set proposed again is the same proposal (no second comment).
* Bundles: one row per confirmed bundle (``bundles``) and its members' states
  (``bundle_members``). A member is in at most one bundle at a time (``held``).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass

from omnigent_factory.store.sqlite import SqliteStore

#: Proposal states in which the owner may still tick its checkbox.
OPEN_PROPOSAL_STATES = frozenset({"pending", "posting", "posted"})
#: Bundle states whose members are held.
ACTIVE_BUNDLE_STATES = frozenset({"creating", "open"})


def member_key(members: Sequence[int]) -> str:
    """The stable key of a member set (order and repeats aside)."""
    text = ",".join(str(n) for n in sorted(set(members)))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True, slots=True)
class Proposal:
    proposal_key: str
    members: tuple[int, ...]
    theme: str
    reason: str
    shared_area: str
    run_id: str
    anchor_issue: int
    comment_id: int | None
    comment_node_id: str
    state: str
    detail: str
    confirmed_by: int | None
    confirmed_via: str | None
    confirmed_at_us: int | None
    attempt_at_us: int
    created_at_us: int

    @property
    def action_id(self) -> str:
        """The proposal comment's checkbox (never its text)."""
        return f"bundle-{self.proposal_key[:16]}"

    @property
    def open(self) -> bool:
        return self.state in OPEN_PROPOSAL_STATES


_PROPOSAL_COLUMNS = (
    "proposal_key, members_json, theme, reason, shared_area, run_id, anchor_issue, comment_id, "
    "comment_node_id, state, detail, confirmed_by, confirmed_via, confirmed_at_us, "
    "attempt_at_us, created_at_us"
)


def _proposal(row: Sequence[object]) -> Proposal:
    return Proposal(
        proposal_key=str(row[0]),
        members=tuple(int(n) for n in json.loads(str(row[1]))),
        theme=str(row[2]),
        reason=str(row[3]),
        shared_area=str(row[4]),
        run_id=str(row[5]),
        anchor_issue=int(str(row[6])),
        comment_id=None if row[7] is None else int(str(row[7])),
        comment_node_id=str(row[8]),
        state=str(row[9]),
        detail=str(row[10]),
        confirmed_by=None if row[11] is None else int(str(row[11])),
        confirmed_via=None if row[12] is None else str(row[12]),
        confirmed_at_us=None if row[13] is None else int(str(row[13])),
        attempt_at_us=int(str(row[14])),
        created_at_us=int(str(row[15])),
    )


@dataclass(frozen=True, slots=True)
class NewProposal:
    members: tuple[int, ...]
    theme: str
    reason: str
    shared_area: str = ""


def record_proposals(
    store: SqliteStore, *, repo_id: str, run_id: str, proposals: Sequence[NewProposal]
) -> list[Proposal]:
    """Record proposals (a member set already proposed keeps its row and comment: never a
    duplicate). Returns the rows of ``proposals``."""
    now = store._clock.now_utc_us()
    keys: list[str] = []
    with store._txn() as conn:
        for item in proposals:
            members = sorted(set(item.members))
            key = member_key(members)
            keys.append(key)
            conn.execute(
                "INSERT INTO bundle_proposals (repo_id, proposal_key, members_json, theme, "
                "reason, shared_area, run_id, anchor_issue, state, created_at_us, "
                "updated_at_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?) "
                "ON CONFLICT(repo_id, proposal_key) DO NOTHING",
                (
                    repo_id,
                    key,
                    json.dumps(members),
                    item.theme,
                    item.reason,
                    item.shared_area,
                    run_id,
                    members[0],
                    now,
                    now,
                ),
            )
    found = {p.proposal_key: p for p in proposals_of(store, repo_id=repo_id)}
    return [found[k] for k in keys if k in found]


def proposals_of(
    store: SqliteStore, *, repo_id: str, states: Sequence[str] | None = None
) -> list[Proposal]:
    sql = f"SELECT {_PROPOSAL_COLUMNS} FROM bundle_proposals WHERE repo_id = ?"
    params: list[object] = [repo_id]
    if states is not None:
        sql += " AND state IN (SELECT value FROM json_each(?))"
        params.append(json.dumps(list(states)))
    rows = store._conn.execute(sql + " ORDER BY created_at_us, proposal_key", params).fetchall()
    return [_proposal(r) for r in rows]


def proposal(store: SqliteStore, *, repo_id: str, proposal_key: str) -> Proposal | None:
    row = store._conn.execute(
        f"SELECT {_PROPOSAL_COLUMNS} FROM bundle_proposals WHERE repo_id = ? AND proposal_key = ?",
        (repo_id, proposal_key),
    ).fetchone()
    return None if row is None else _proposal(row)


def proposal_by_comment(store: SqliteStore, *, repo_id: str, comment_id: int) -> Proposal | None:
    row = store._conn.execute(
        f"SELECT {_PROPOSAL_COLUMNS} FROM bundle_proposals WHERE repo_id = ? AND comment_id = ?",
        (repo_id, comment_id),
    ).fetchone()
    return None if row is None else _proposal(row)


def set_proposal(
    store: SqliteStore,
    *,
    repo_id: str,
    proposal_key: str,
    state: str,
    detail: str | None = None,
    comment_id: int | None = None,
    comment_node_id: str | None = None,
    attempt: bool = False,
) -> None:
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        conn.execute(
            "UPDATE bundle_proposals SET state = ?, detail = COALESCE(?, detail), "
            "comment_id = COALESCE(?, comment_id), "
            "comment_node_id = COALESCE(?, comment_node_id), "
            "attempt_at_us = CASE WHEN ? THEN ? ELSE attempt_at_us END, updated_at_us = ? "
            "WHERE repo_id = ? AND proposal_key = ?",
            (
                state,
                detail,
                comment_id,
                comment_node_id,
                int(attempt),
                now,
                now,
                repo_id,
                proposal_key,
            ),
        )


def confirm_proposal(
    store: SqliteStore,
    *,
    repo_id: str,
    proposal_key: str,
    actor_id: int,
    via: str,
    at_us: int,
) -> bool:
    """Record the owner's tick once (only while the proposal is open). True: new."""
    with store._txn() as conn:
        cur = conn.execute(
            "UPDATE bundle_proposals SET state = 'confirmed', confirmed_by = ?, "
            "confirmed_via = ?, confirmed_at_us = ?, updated_at_us = ? WHERE repo_id = ? "
            "AND proposal_key = ? AND state IN ('pending', 'posting', 'posted')",
            (actor_id, via, at_us, store._clock.now_utc_us(), repo_id, proposal_key),
        )
        return cur.rowcount > 0


# ------------------------------------------------------------------ bundles


@dataclass(frozen=True, slots=True)
class Member:
    issue_number: int
    seq: int
    state: str
    reason: str
    #: The member's hold (or its release) was applied to its parcel.
    applied: bool


@dataclass(frozen=True, slots=True)
class Bundle:
    bundle_key: str
    theme: str
    reason: str
    source: str
    owner_id: int
    via: str
    confirmed_at_us: int
    issue_number: int | None
    issue_node_id: str
    state: str
    detail: str
    done: frozenset[str]
    attempt_at_us: int
    members: tuple[Member, ...]

    @property
    def active(self) -> bool:
        return self.state in ACTIVE_BUNDLE_STATES

    @property
    def held(self) -> tuple[int, ...]:
        """Members still in the bundle (ejected and released ones aside)."""
        return tuple(m.issue_number for m in self.members if m.state in ("held", "done"))

    def member(self, number: int) -> Member | None:
        for m in self.members:
            if m.issue_number == number:
                return m
        return None


_BUNDLE_COLUMNS = (
    "bundle_key, theme, reason, source, owner_id, via, confirmed_at_us, issue_number, "
    "issue_node_id, state, detail, done_json, attempt_at_us"
)


def _bundles(store: SqliteStore, where: str, params: Sequence[object]) -> list[Bundle]:
    rows = store._conn.execute(
        f"SELECT {_BUNDLE_COLUMNS} FROM bundles WHERE {where} ORDER BY created_at_us, bundle_key",
        params,
    ).fetchall()
    out: list[Bundle] = []
    for r in rows:
        members = store._conn.execute(
            "SELECT issue_number, seq, state, reason, applied FROM bundle_members WHERE "
            "repo_id = ? AND bundle_key = ? ORDER BY seq",
            (params[0], r[0]),
        ).fetchall()
        out.append(
            Bundle(
                bundle_key=str(r[0]),
                theme=str(r[1]),
                reason=str(r[2]),
                source=str(r[3]),
                owner_id=int(r[4]),
                via=str(r[5]),
                confirmed_at_us=int(r[6]),
                issue_number=None if r[7] is None else int(r[7]),
                issue_node_id=str(r[8]),
                state=str(r[9]),
                detail=str(r[10]),
                done=frozenset(str(a) for a in json.loads(str(r[11]))),
                attempt_at_us=int(r[12]),
                members=tuple(
                    Member(int(m[0]), int(m[1]), str(m[2]), str(m[3]), bool(m[4])) for m in members
                ),
            )
        )
    return out


def bundles_of(
    store: SqliteStore, *, repo_id: str, states: Sequence[str] | None = None
) -> list[Bundle]:
    if states is None:
        return _bundles(store, "repo_id = ?", (repo_id,))
    return _bundles(
        store,
        "repo_id = ? AND state IN (SELECT value FROM json_each(?))",
        (repo_id, json.dumps(list(states))),
    )


def bundle(store: SqliteStore, *, repo_id: str, bundle_key: str) -> Bundle | None:
    found = _bundles(store, "repo_id = ? AND bundle_key = ?", (repo_id, bundle_key))
    return found[0] if found else None


def bundle_by_issue(store: SqliteStore, *, repo_id: str, issue_number: int) -> Bundle | None:
    found = _bundles(store, "repo_id = ? AND issue_number = ?", (repo_id, issue_number))
    return found[0] if found else None


def bundle_of_member(store: SqliteStore, *, repo_id: str, issue_number: int) -> Bundle | None:
    """The active bundle ``issue_number`` is held in (None: none)."""
    row = store._conn.execute(
        "SELECT m.bundle_key FROM bundle_members m JOIN bundles b ON b.repo_id = m.repo_id "
        "AND b.bundle_key = m.bundle_key WHERE m.repo_id = ? AND m.issue_number = ? AND "
        "m.state = 'held' AND b.state IN ('creating', 'open') LIMIT 1",
        (repo_id, issue_number),
    ).fetchone()
    return None if row is None else bundle(store, repo_id=repo_id, bundle_key=str(row[0]))


def held_members(store: SqliteStore, *, repo_id: str) -> dict[int, Bundle]:
    """Every member held in an active bundle, with that bundle."""
    out: dict[int, Bundle] = {}
    for b in bundles_of(store, repo_id=repo_id, states=sorted(ACTIVE_BUNDLE_STATES)):
        for m in b.members:
            if m.state == "held":
                out[m.issue_number] = b
    return out


def create_bundle(
    store: SqliteStore,
    *,
    repo_id: str,
    bundle_key: str,
    members: Sequence[int],
    theme: str,
    reason: str,
    source: str,
    owner_id: int,
    via: str,
    confirmed_at_us: int,
) -> bool:
    """Record a confirmed bundle once (a repeat changes nothing). False: it existed."""
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        cur = conn.execute(
            "INSERT INTO bundles (repo_id, bundle_key, theme, reason, source, owner_id, via, "
            "confirmed_at_us, state, created_at_us, updated_at_us) VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, 'creating', ?, ?) "
            "ON CONFLICT(repo_id, bundle_key) DO NOTHING",
            (repo_id, bundle_key, theme, reason, source, owner_id, via, confirmed_at_us, now, now),
        )
        if cur.rowcount == 0:
            return False
        for seq, number in enumerate(members):
            conn.execute(
                "INSERT INTO bundle_members (repo_id, bundle_key, issue_number, seq, state, "
                "updated_at_us) VALUES (?, ?, ?, ?, 'held', ?)",
                (repo_id, bundle_key, number, seq, now),
            )
        return True


def set_bundle(
    store: SqliteStore,
    *,
    repo_id: str,
    bundle_key: str,
    state: str | None = None,
    detail: str | None = None,
    issue_number: int | None = None,
    issue_node_id: str | None = None,
    attempt: bool = False,
) -> None:
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        conn.execute(
            "UPDATE bundles SET state = COALESCE(?, state), detail = COALESCE(?, detail), "
            "issue_number = COALESCE(?, issue_number), "
            "issue_node_id = COALESCE(?, issue_node_id), "
            "attempt_at_us = CASE WHEN ? THEN ? ELSE attempt_at_us END, updated_at_us = ? "
            "WHERE repo_id = ? AND bundle_key = ?",
            (
                state,
                detail,
                issue_number,
                issue_node_id,
                int(attempt),
                now,
                now,
                repo_id,
                bundle_key,
            ),
        )


def mark_bundle_done(store: SqliteStore, *, repo_id: str, bundle_key: str, action: str) -> None:
    with store._txn() as conn:
        row = conn.execute(
            "SELECT done_json FROM bundles WHERE repo_id = ? AND bundle_key = ?",
            (repo_id, bundle_key),
        ).fetchone()
        if row is None:
            return
        done = sorted({*json.loads(str(row[0])), action})
        conn.execute(
            "UPDATE bundles SET done_json = ?, updated_at_us = ? WHERE repo_id = ? "
            "AND bundle_key = ?",
            (json.dumps(done), store._clock.now_utc_us(), repo_id, bundle_key),
        )


def set_member(
    store: SqliteStore,
    *,
    repo_id: str,
    bundle_key: str,
    issue_number: int,
    state: str | None = None,
    reason: str | None = None,
    applied: bool | None = None,
) -> bool:
    """Change a held member's state (ejected, released, done: marked unapplied) or its
    applied flag. A member that left ``held`` never goes back. True: something changed."""
    now = store._clock.now_utc_us()
    with store._txn() as conn:
        row = conn.execute(
            "SELECT state, applied FROM bundle_members WHERE repo_id = ? AND bundle_key = ? "
            "AND issue_number = ?",
            (repo_id, bundle_key, issue_number),
        ).fetchone()
        if row is None:
            return False
        old_state, old_applied = str(row[0]), bool(row[1])
        if state is not None and state != old_state:
            if old_state != "held":
                return False  # only a held member moves on (ejected, released or done)
            conn.execute(
                "UPDATE bundle_members SET state = ?, reason = COALESCE(?, reason), "
                "applied = 0, updated_at_us = ? WHERE repo_id = ? AND bundle_key = ? "
                "AND issue_number = ?",
                (state, reason, now, repo_id, bundle_key, issue_number),
            )
            return True
        if applied is not None and applied != old_applied:
            conn.execute(
                "UPDATE bundle_members SET applied = ?, updated_at_us = ? WHERE repo_id = ? "
                "AND bundle_key = ? AND issue_number = ?",
                (int(applied), now, repo_id, bundle_key, issue_number),
            )
            return True
        return False


__all__ = [
    "ACTIVE_BUNDLE_STATES",
    "OPEN_PROPOSAL_STATES",
    "Bundle",
    "Member",
    "NewProposal",
    "Proposal",
    "bundle",
    "bundle_by_issue",
    "bundle_of_member",
    "bundles_of",
    "confirm_proposal",
    "create_bundle",
    "held_members",
    "mark_bundle_done",
    "member_key",
    "proposal",
    "proposal_by_comment",
    "proposals_of",
    "record_proposals",
    "set_bundle",
    "set_member",
]
