"""Session retention: delete factory-created Omnigent sessions long archived.

Runs with the periodic history retention sweep, a few sessions per sweep. A session is
deleted only when every rule holds:

* the factory created it and recorded it in its own store: an issue session or a stage
  run's root of a parcel, or a triage ranking run's root;
* its issue is finished (merged or closed, or in Done) with every stage run settled; a
  ranking run is finished;
* Omnigent shows it as a root session, archived, carrying the factory's
  ``factory.dispatch`` label, archived at least ``session_retention_days`` ago (the
  server's ``omnigent.archived_at`` label, else its last update);
* a complete scan of its tree finds nothing running or waiting.

``DELETE /v1/sessions/{id}`` (pinned Omnigent server) removes the root and every
descendant (child and worker sessions, items, labels, policies) and stops its tasks; the
git worktree and branch stay (no ``delete_branch``). Each deletion, or a root already
gone, is recorded in ``session_deletions`` and never retried. ``session_retention_days =
0`` deletes nothing.
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.projection import finished
from omnigent_factory.core.types import Hold, IssueSessionStatus, Parcel, Stage
from omnigent_factory.omnigent.adapter import NONCE_LABEL, OmnigentExecutionAdapter
from omnigent_factory.omnigent.rest import OmnigentReadError, WriteClass, as_map, classify_write
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import ranking as rs
from omnigent_factory.store.sqlite import SqliteStore

LOG = logging.getLogger(__name__)

#: Omnigent's archive-time label (epoch seconds), written when a session is archived.
ARCHIVED_AT_LABEL = "omnigent.archived_at"
#: Sessions examined per sweep (each costs a snapshot read and a tree scan).
BATCH = 10
MICROS_PER_DAY = 86_400_000_000


@dataclass(frozen=True, slots=True)
class Candidate:
    root_id: str
    #: The parcel or ranking run that recorded it.
    source: str


def issue_finished(parcel: Parcel) -> bool:
    """Merged/closed or in Done, every stage run settled, and the issue session not live."""
    done = Hold.COMPLETED in parcel.holds or parcel.stage == Stage.DONE
    issue = parcel.issue_session
    live = issue is not None and issue.status == IssueSessionStatus.LIVE
    return done and finished(parcel) and not live


def select_candidates(
    parcels: Iterable[Parcel],
    ranking_roots: Iterable[tuple[str, str, bool]],
    deleted: Collection[str],
) -> list[Candidate]:
    """Factory-recorded roots of finished issues and finished ranking runs, not deleted.

    Every root of a finished parcel (its issue session and the roots of earlier,
    replaced issue sessions its runs recorded); ranking roots as (root, run, finished).
    """
    found: dict[str, str] = {}
    for parcel in parcels:
        if not issue_finished(parcel):
            continue
        roots = {s.root_id for s in parcel.sessions if s.root_id}
        if parcel.issue_session is not None:
            roots.add(parcel.issue_session.root_id)
        for root in roots:
            found.setdefault(str(root), parcel.parcel_id)
    for root, run_id, run_finished in ranking_roots:
        if run_finished:
            found.setdefault(root, run_id)
    return [
        Candidate(root, source) for root, source in sorted(found.items()) if root not in deleted
    ]


def _candidates(store: SqliteStore, *, repo_id: str) -> list[Candidate]:
    parcels = [
        parcel_from_json(str(row[0]))
        for row in store.query("SELECT aggregate_json FROM parcels WHERE repo_id = ?", (repo_id,))
    ]
    ranking = rs.ranking_roots(store, repo_id)
    roots = {s.root_id for p in parcels for s in p.sessions if s.root_id}
    roots |= {p.issue_session.root_id for p in parcels if p.issue_session is not None}
    roots |= {root for root, _run, _finished in ranking}
    return select_candidates(parcels, ranking, rs.deleted_sessions(store, roots))


def archived_at_us(snapshot: Mapping[str, Any]) -> int | None:
    """When the session was archived: Omnigent's label, else its last update."""
    label = as_map(snapshot.get("labels")).get(ARCHIVED_AT_LABEL)
    try:
        if label is not None:
            return int(float(str(label)) * 1_000_000)
    except ValueError:
        pass
    updated = snapshot.get("updated_at")
    if isinstance(updated, int | float) and not isinstance(updated, bool):
        # Seconds (Omnigent's unit) unless it is clearly already microseconds.
        return int(updated) if updated > 10**14 else int(updated * 1_000_000)
    if isinstance(updated, str):
        try:
            return int(datetime.fromisoformat(updated.replace("Z", "+00:00")).timestamp() * 1e6)
        except ValueError:
            return None
    return None


class SessionRetention:
    def __init__(self, service: FactoryService, adapter: OmnigentExecutionAdapter) -> None:
        self.service = service
        self.adapter = adapter

    async def sweep(self, *, dry_run: bool = False, limit: int = BATCH) -> dict[str, object]:
        """Delete (or with ``dry_run`` list) at most ``limit`` sessions retention selects."""
        days = self.service.config.session_retention_days
        if days <= 0:
            return {"enabled": False, "deleted": [], "would_delete": [], "skipped": {}}
        repo_id = self.service.config.repo_id
        candidates = await self.service.db.call(lambda store: _candidates(store, repo_id=repo_id))
        cutoff = self.service.clock.now_utc_us() - int(days * MICROS_PER_DAY)
        deleted: list[str] = []
        selected: list[str] = []
        skipped: dict[str, str] = {}
        for candidate in candidates:
            if len(selected) >= limit:
                break
            verdict = await self._eligible(candidate, cutoff)
            if verdict == "gone":
                if not dry_run:
                    await self._record(candidate, "gone")
                continue
            if verdict is not None:
                skipped[candidate.root_id] = verdict
                continue
            selected.append(candidate.root_id)
            if dry_run:
                continue
            outcome = await self._delete(candidate)
            if outcome is not None:
                deleted.append(candidate.root_id)
        if deleted:
            LOG.info("session retention deleted count=%s roots=%s", len(deleted), ",".join(deleted))
        return {
            "enabled": True,
            "retention_days": days,
            "dry_run": dry_run,
            "deleted": deleted,
            "would_delete": selected if dry_run else [],
            "skipped": skipped,
        }

    async def _eligible(self, candidate: Candidate, cutoff_us: int) -> str | None:
        """None: delete it; "gone": already deleted; otherwise why it stays."""
        try:
            snap = await self.adapter.rest.get_json(f"/v1/sessions/{candidate.root_id}")
        except OmnigentReadError as exc:
            return "gone" if exc.status == 404 else f"unreadable: {exc.reason}"
        if snap.get("archived") is not True:
            return "not archived"
        if snap.get("parent_session_id") is not None:
            return "not a root session"
        if not as_map(snap.get("labels")).get(NONCE_LABEL):
            return "no factory label"
        archived = archived_at_us(snap)
        if archived is None or archived > cutoff_us:
            return "archived too recently"
        try:
            tree = await self.adapter.observe_tree(candidate.root_id)
        except OmnigentReadError as exc:
            return f"tree unreadable: {exc.reason}"
        if not tree.complete:
            return "tree scan incomplete"
        if not tree.quiescent or tree.pending_waiter:
            return "a session in its tree is not idle"
        return None

    async def _delete(self, candidate: Candidate) -> str | None:
        resp = await self.adapter.rest.delete(f"/v1/sessions/{candidate.root_id}")
        if classify_write(resp) == WriteClass.OK:
            outcome = "deleted"
        elif resp.status == 404:
            outcome = "gone"
        else:
            LOG.warning(
                "session retention delete failed root=%s status=%s",
                candidate.root_id,
                resp.status or resp.error,
            )
            return None
        await self._record(candidate, outcome)
        return outcome

    async def _record(self, candidate: Candidate, outcome: str) -> None:
        now = self.service.clock.now_utc_us()
        await self.service.db.call(
            lambda store: rs.record_session_deletion(
                store, candidate.root_id, candidate.source, outcome, now
            )
        )


__all__ = ["SessionRetention", "archived_at_us", "issue_finished", "select_candidates"]
