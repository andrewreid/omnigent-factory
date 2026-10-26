"""Migration 17 (follow-up discipline) upgrades a database with existing follow-ups, and
the setup, config and doctor pieces of the "Review rounds" field; the Ready report keeps
the owner's ticks when the factory edits it in place, and its Deferred markers survive
the publication's safety pass."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig
from omnigent_factory.service.doctor import DoctorReport
from omnigent_factory.store.migrations import MIGRATIONS
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import REPO_ID, config
from omnigent_factory.testing.fakes import FakeClock

LATEST = max(m.version for m in MIGRATIONS)


def test_migration_17_upgrades_a_database_with_follow_ups_and_keeps_them(tmp_path: Path):
    from omnigent_factory.store import discipline as ds

    path = tmp_path / "state.sqlite3"
    before = tuple(m for m in MIGRATIONS if m.version < 17)
    old = SqliteStore.open(path, FakeClock(), migrations=before)
    old.ensure_repository(config())
    old.record_follow_up(REPO_ID, 904, source_issue=747, origin="reported", blocks=(882,))
    old.set_epic_follow_up_builds(REPO_ID, 878, True, 5)
    assert old.schema_version() == 16
    old.close()

    store = SqliteStore.open(path, FakeClock())
    try:
        assert store.schema_version() == LATEST == 21
        row = store.follow_up(REPO_ID, 904)
        assert row is not None and row.source_issue == 747 and row.blocks == (882,)
        assert store.epic_follow_up_builds(REPO_ID, 878) == (True, 5)
        assert store.follow_up_lineage(REPO_ID, 904) == (1, 747)
        assert store.follow_up_lineage(REPO_ID, 747) == (0, None)
        # The new tables are there and usable.
        item = ds.NewDeferred("k1", "P2", "a.ts", "t", "", "r", "follow_up")
        recorded = ds.record_deferred(
            store, repo_id=REPO_ID, source_issue=747, pr_number=7, session_id="s", items=[item]
        )
        assert [i.item_key for i in recorded] == ["k1"]
        assert ds.set_round_budget(
            store, repo_id=REPO_ID, issue_node_id="I_1", value=3, actor_id=1, at_us=10
        )
        assert not ds.set_round_budget(  # an older edit changes nothing
            store, repo_id=REPO_ID, issue_node_id="I_1", value=9, actor_id=1, at_us=5
        )
        assert ds.round_budget(store, repo_id=REPO_ID, issue_node_id="I_1") == 3
        assert (
            ds.record_rounds(
                store, repo_id=REPO_ID, parcel_id="I_1", pr_number=7, rounds=[("a", 1), ("a", 1)]
            )
            == 1
        )
        assert ds.grant_round(store, repo_id=REPO_ID, parcel_id="I_1", grant_key="g", at_us=1)
        assert not ds.grant_round(store, repo_id=REPO_ID, parcel_id="I_1", grant_key="g", at_us=2)
    finally:
        store.close()
    # Re-opening applies nothing again (checksums match).
    SqliteStore.open(path, FakeClock()).close()


def test_migration_18_adds_splits_to_a_v17_database_and_keeps_its_rows(tmp_path: Path):
    from omnigent_factory.store import discipline as ds

    path = tmp_path / "state.sqlite3"
    v17 = SqliteStore.open(
        path, FakeClock(), migrations=tuple(m for m in MIGRATIONS if m.version < 18)
    )
    v17.ensure_repository(config())
    item = ds.NewDeferred("k1", "P2", "a.ts", "t", "", "r", "follow_up")
    ds.record_deferred(
        v17, repo_id=REPO_ID, source_issue=7, pr_number=3, session_id="s", items=[item]
    )
    assert v17.schema_version() == 17
    v17.close()
    store = SqliteStore.open(path, FakeClock())
    try:
        assert store.schema_version() == LATEST
        assert [i.item_key for i in ds.deferred_items(store, repo_id=REPO_ID, source_issue=7)] == [
            "k1"
        ]
        split = ds.NewSplit("s1", "t", "b", "r", "blocks", (5,))
        assert (
            ds.record_splits(store, repo_id=REPO_ID, source_issue=7, session_id="x", items=[split])
            == 1
        )
        assert (
            ds.record_splits(store, repo_id=REPO_ID, source_issue=7, session_id="y", items=[split])
            == 0
        )
        [row] = ds.splits(store, repo_id=REPO_ID, source_issue=7)
        # A legacy "blocks" relation reads as after_source: the new issue waits on #7.
        assert row.pending_links() == ("parent", "blocks:5", "blocked_by:7")
    finally:
        store.close()


def test_migration_19_widens_split_relations_and_keeps_legacy_rows_and_done_steps(
    tmp_path: Path,
):
    from omnigent_factory.store import discipline as ds

    path = tmp_path / "state.sqlite3"
    v18 = SqliteStore.open(
        path, FakeClock(), migrations=tuple(m for m in MIGRATIONS if m.version < 19)
    )
    v18.ensure_repository(config())
    legacy = [
        ds.NewSplit("a", "t", "b", "r", "blocks"),
        ds.NewSplit("b", "u", "b", "r", "blocked_by"),
    ]
    ds.record_splits(v18, repo_id=REPO_ID, source_issue=7, session_id="x", items=legacy)
    ds.mark_split_done(v18, repo_id=REPO_ID, source_issue=7, item_key="a", action="blocks:7")
    assert v18.schema_version() == 18
    v18.close()
    store = SqliteStore.open(path, FakeClock())
    try:
        assert store.schema_version() == LATEST
        rows = {row.item_key: row for row in ds.splits(store, repo_id=REPO_ID, source_issue=7)}
        assert (rows["a"].relation, rows["a"].done) == ("blocks", frozenset({"blocks:7"}))
        assert rows["a"].pending_links() == ("parent",)  # linked under the old reading
        assert rows["b"].pending_links() == ("parent", "blocked_by:7")
        new = [
            ds.NewSplit("c", "v", "b", "r", "after_source"),
            ds.NewSplit("d", "w", "b", "r", "before_source"),
        ]
        assert (
            ds.record_splits(store, repo_id=REPO_ID, source_issue=7, session_id="y", items=new) == 2
        )
        rows = {row.item_key: row for row in ds.splits(store, repo_id=REPO_ID, source_issue=7)}
        assert rows["c"].pending_links() == ("parent", "blocked_by:7")
        assert rows["d"].pending_links() == ("parent", "blocks:7")
    finally:
        store.close()


def test_follow_up_lineage_counts_hops_and_stops_on_cycles(tmp_path: Path):
    store = SqliteStore.open(tmp_path / "s.sqlite3", FakeClock())
    try:
        store.ensure_repository(config())
        store.record_follow_up(REPO_ID, 2, source_issue=1, origin="reported")
        store.record_follow_up(REPO_ID, 3, source_issue=2, origin="detected")
        assert store.follow_up_lineage(REPO_ID, 3) == (2, 2)
        store.set_follow_up_hold(REPO_ID, 9, "held", 1)  # a label alone is not lineage
        assert store.follow_up_lineage(REPO_ID, 9) == (0, None)
        store.record_follow_up(REPO_ID, 1, source_issue=3, origin="reported")  # a cycle
        assert store.follow_up_lineage(REPO_ID, 3)[0] == 2
    finally:
        store.close()


# ------------------------------------------------------------------ setup, config, doctor


def test_setup_render_lists_the_review_rounds_field():
    from omnigent_factory.github.setup import render_project_migration

    rendered = render_project_migration()
    fields = [f["input"]["name"] for f in rendered["create_fields"]]
    assert "Review rounds" in fields
    assert any(
        f["input"] == {**f["input"], "name": "Review rounds", "dataType": "NUMBER"}
        for f in rendered["create_fields"]
        if f["input"]["name"] == "Review rounds"
    )
    assert "review_rounds_field_node_id" in " ".join(rendered["post_apply_verification"])


def test_the_default_and_depth_are_reloadable_the_field_id_is_not():
    assert {"review_rounds_default", "follow_up_max_depth"} <= HOT_RELOAD_KEYS
    assert "review_rounds_field_node_id" not in HOT_RELOAD_KEYS
    base = ServiceConfig(repo_id="R", owners=frozenset({1}))
    assert (base.review_rounds_default, base.follow_up_max_depth) == (2, 1)
    with pytest.raises(ValueError):
        ServiceConfig(repo_id="R", owners=frozenset({1}), review_rounds_default=0)


@pytest.mark.parametrize(
    ("field_id", "rows", "check"),
    [
        ("", {}, "unset"),
        ("PVTF_r", {"PVTF_r": {"id": "PVTF_r", "name": "Review rounds", "dataType": "NUMBER"}}, ""),
        ("PVTF_r", {"PVTF_r": {"id": "PVTF_r", "name": "Rank", "dataType": "NUMBER"}}, "failed"),
        ("PVTF_x", {}, "failed"),
    ],
)
def test_doctor_checks_the_review_rounds_field(field_id: str, rows: Any, check: str):
    from omnigent_factory.service.doctor import _check_review_rounds_field

    cfg = ServiceConfig(repo_id="R", owners=frozenset({1}), review_rounds_field_node_id=field_id)
    report = DoctorReport()
    _check_review_rounds_field(cfg, rows, report)
    result = report.checks["github_review_rounds_field"]
    if check == "":
        assert result == "Review rounds number field ID matches" and report.ok
    elif check == "unset":
        assert result.startswith("not configured: every issue gets review_rounds_default")
        assert report.ok and not report.warnings
    else:
        assert result == "failed" and not report.ok


# ------------------------------------------------------------------ the Ready report


@pytest.mark.asyncio
async def test_an_in_place_report_edit_keeps_the_owners_ticks():
    from omnigent_factory.core import report_lines as rl
    from tests.github.test_ready_report_edit import MARKER, OLD, Repo, run

    section = "\n".join(
        [
            "",
            "**Deferred** (tick an item to have it done in this PR):",
            rl.line(rl.ACTION, "d-a", "Fix now: P2 A (in #100)"),
            rl.line(rl.ACTION, "d-b", "Fix now: P3 B (advisory: style)"),
        ]
    )
    owner = (OLD + section).replace("- [ ] Fix now: P3 B", "- [x] Fix now: P3 B")
    repo = Repo(f"{owner}\n\n{MARKER}")
    newer = OLD + "\n**Review bot:** Codex: 👍 on `abc1234`" + section  # rendered unticked
    await run(repo, newer)
    assert repo.patches == 1
    assert rl.checked(repo.body) == {"d-a": False, "d-b": True}
    assert "**Review bot:** Codex: 👍" in repo.body


@pytest.mark.asyncio
async def test_the_deferred_markers_survive_the_publication_safety_pass(tmp_path: Path):
    from omnigent_factory.core import report_lines as rl
    from omnigent_factory.core.effects import EffectKind
    from omnigent_factory.service.directory import PublicationRenderer

    renderer = PublicationRenderer.__new__(PublicationRenderer)
    renderer.config = ServiceConfig(repo_id="R", owners=frozenset({1}), state_dir=tmp_path)
    marker = rl.line(rl.ACTION, "d-a", "Fix now: P2 A")

    class Parcel:
        readiness = None

    class Db:
        async def call(self, _fn: Any) -> Any:
            return Parcel()

    class Directory:
        db = Db()

        def latest_result(self, _sid: str) -> None:
            return None

    async def section(_parcel: Any) -> str:
        return "\n\n**Deferred**:\n" + marker

    renderer.directory = Directory()  # type: ignore[assignment]
    renderer.deferred_section = section
    renderer.split_section = None

    effect = SimpleNamespace(
        parcel_id="I_1",
        kind=EffectKind.PUBLISH_REPORT,
        args={"report": "ready", "pr_number": 7, "head_sha": "a" * 40},
    )
    text = await renderer(effect)  # type: ignore[arg-type]
    assert text is not None and text.endswith(marker)
    assert "<!-- factory-action:d-a -->" in text
