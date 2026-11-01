"""Numbered, rated follow-ups in the Ready report (owner request, 2026-11-01).

Each deferred item gets a stable number (F1, F2, ... in recording order: an item never
changes number, a new item gets the next one) and the agent's optional rating (bug,
likely, edge, tidy) and one-line why. The report shows a table in number order, then
the same checkbox list as before with each line prefixed by its number. A tick still
means "fix this now in this PR": boxes are named by their hidden marker only, so tick
reading is unchanged.
"""

from __future__ import annotations

import json
import re
from importlib.resources import files
from pathlib import Path
from typing import Any

import pytest

from omnigent_factory.core import report_lines as rl
from omnigent_factory.core.protocol import (
    MAX_WHY_CHARS,
    RATINGS,
    RESULT_SHAPES,
    ResultError,
    validate_result,
)
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.store import discipline as ds
from omnigent_factory.store.migrations import MIGRATIONS
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import REPO_ID, config
from omnigent_factory.testing.fakes import FakeClock
from tests.service.test_deferred import (
    ADV,
    FU_A,
    FU_B,
    S,
    action,
    edited,
    report,
    result,
    rig,
    ticked,
)
from tests.test_follow_up_discipline import FOLLOW_UP, build, corr

TEMPLATES = files("omnigent_factory.service") / "templates"


def rated(item: dict[str, Any], rating: str, why: str = "") -> dict[str, Any]:
    return {**item, "rating": rating, **({"why": why} if why else {})}


def table(section: str) -> list[list[str]]:
    """The Deferred table's body rows, as cells."""
    rows = [line for line in section.splitlines() if line.startswith("| ")]
    assert rows[:2] == ["| # | Item | Rating | Why |", "| --- | --- | --- | --- |"]
    return [[c.strip() for c in re.split(r"(?<!\\)\|", row)[1:-1]] for row in rows[2:]]


def boxes(section: str) -> list[str]:
    return [line for line in section.splitlines() if line.startswith("- [")]


NEW = {
    "severity": "P3",
    "title": "Log line repeats the row id",
    "reason": "cosmetic",
    "outcome": "advisory",
}
EXTRA = {"severity": "P2", "title": "Export skips archived rows", "reason": "outside the plan"}


# ------------------------------------------------------------------ rendering


@pytest.mark.asyncio
async def test_unrated_items_render_a_numbered_table_with_dashes_and_the_same_boxes(
    service_config: ServiceConfig,
):
    """Back-compat: an old result (no rating) renders '-' and the same markers."""
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, ADV))
        section = await r.deferred.report_section(await r.parcel())
        assert table(section) == [
            ["F1", "P2 Retry leaks the delivery row", "-", "-"],
            ["F2", "P3 Clock going backwards", "-", "-"],
        ]
        assert boxes(section) == [
            "- [ ] F1 Fix now: P2 `api/src/export.ts` Retry leaks the delivery row (in #100) "
            f"{rl.marker(rl.ACTION, action(FU_A))}",
            "- [ ] F2 Fix now: P3 Clock going backwards (advisory: no realistic trigger here) "
            f"{rl.marker(rl.ACTION, action(ADV))}",
        ]
        # The table comes first; the checkboxes stay list items (GitHub task lists do not
        # work in table cells), after a blank line.
        head, _, rest = section.partition("| # | Item | Rating | Why |")
        assert head == "\n\n**Deferred** (tick an item to have it done in this PR):\n\n"
        assert "\n\n- [ ] F1 " in rest


@pytest.mark.asyncio
async def test_the_table_lists_items_in_number_order_with_their_ratings(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        items = (
            rated(FU_A, "edge", "needs two retries racing"),
            rated(FU_B, "bug", "hits every outage"),
            rated(ADV, "tidy"),
            EXTRA,  # unrated: last
            rated(NEW, "likely", "every export logs it"),
        )
        await r.deferred.record_result(await r.parcel(), "s1", result(*items))
        section = await r.deferred.report_section(await r.parcel())
        assert table(section) == [
            ["F1", "P2 Retry leaks the delivery row", "Edge", "needs two retries racing"],
            ["F2", "P1 Unbounded retry loop", "Bug", "hits every outage"],
            ["F3", "P3 Clock going backwards", "Tidy", "-"],
            ["F4", "P2 Export skips archived rows", "-", "-"],
            ["F5", "P3 Log line repeats the row id", "Likely", "every export logs it"],
        ]
        # The boxes stay in number order, one per item, each with its own marker.
        lines = boxes(section)
        assert [line.split(" ", 4)[3] for line in lines] == ["F1", "F2", "F3", "F4", "F5"]
        assert [rl.ids(line) for line in lines] == [{action(i)} for i in items]


@pytest.mark.asyncio
async def test_numbers_are_stable_across_re_renders_and_new_items_get_the_next_one(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, ADV))
        first = await r.deferred.report_section(await r.parcel())
        assert first == await r.deferred.report_section(await r.parcel())  # re-render
        # A later result repeats the items in another order, rates one and adds one.
        await r.deferred.record_result(
            await r.parcel(), "s2", result(EXTRA, rated(ADV, "bug", "now seen live"), FU_A)
        )
        second = await r.deferred.report_section(await r.parcel())
        numbers = {next(iter(rl.ids(line))): line.split(" ", 4)[3] for line in boxes(second)}
        assert numbers == {action(FU_A): "F1", action(ADV): "F2", action(EXTRA): "F3"}
        assert table(second)[1] == ["F2", "P3 Clock going backwards", "Bug", "now seen live"]
        # Earlier boxes read exactly as before (only the newly rated table row changed).
        assert boxes(second)[:2] == boxes(first)


@pytest.mark.asyncio
async def test_agent_text_cannot_break_the_table(service_config: ServiceConfig):
    async with rig(service_config) as r:
        piped = {**EXTRA, "title": "a | b\nc"}
        await r.deferred.record_result(
            await r.parcel(), "s1", result(rated(piped, "edge", "x | y → z"))
        )
        section = await r.deferred.report_section(await r.parcel())
        [row] = [line for line in section.splitlines() if line.startswith("| F1 ")]
        assert row == "| F1 | P2 a \\| b c | Edge | x \\| y -> z |"


@pytest.mark.asyncio
async def test_the_follow_up_issue_line_carries_the_rating_and_keeps_its_marker(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.deferred.record_result(
            await r.parcel(), "s1", result(rated(FU_A, "likely", "seen in staging"), FU_B)
        )
        body = r.repo.issues[100]["body"]
        assert "- [ ] P2 (Likely) `api/src/export.ts` Retry leaks" in body
        assert "outside the plan Why: seen in staging <!-- factory-item:" in body
        assert "- [ ] P1 `api/src/retry.ts` Unbounded retry loop" in body  # unrated
        assert len(rl.ids(body, rl.ITEM)) == 2


# ------------------------------------------------------------------ ticks unchanged


@pytest.mark.asyncio
async def test_ticks_read_the_same_from_the_numbered_report_as_from_the_old_layout(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.deferred.record_result(
            await r.parcel(), "s1", result(rated(FU_A, "bug"), FU_B, rated(ADV, "tidy"))
        )
        body = await report(r)
        after = ticked(body, action(FU_B))
        # The layout the factory wrote before numbering: the same markers, no numbers or
        # table. Tick reading sees the same boxes in both.
        legacy = "\n".join(
            re.sub(r"^- \[ \] F\d+ ", "- [ ] ", line)
            for line in body.splitlines()
            if not line.startswith("| ")
        )
        assert rl.checked(body) == rl.checked(legacy)
        assert rl.checked(after) == rl.checked(ticked(legacy, action(FU_B)))
        assert rl.newly_ticked(body, after) == [action(FU_B)]
        # The flow is unchanged: one owner tick, one rework whose brief is that item.
        assert await r.deferred.on_comment_delivery(edited(r, body, after)) == 1
        r.clock.advance(121 * S)
        assert await r.deferred.tick() == 1
        parcel = await r.parcel()
        [rework] = [
            a for a in parcel.authorizations if a.source_event_id.startswith("report-items:")
        ]
        brief = await r.deferred.rework_brief(parcel, rework.source_event_id)
        assert brief is not None and brief.splitlines() == [
            "- Fix the P1 finding in `api/src/retry.ts`: Unbounded retry loop (listed in "
            "follow-up issue #100). Thread: https://github.com/o/r/pull/7#discussion_r2. "
            "It was deferred because: too large for this PR"
        ]
        # The status rewrite keeps the number and the table.
        text = r.repo.comments[555]["body"]
        assert "- [x] F2 Fix now: P1 `api/src/retry.ts` Unbounded retry loop" in text
        assert "→ fixing (rework started)" in text and "| F1 |" in text


# ------------------------------------------------------------------ protocol


def test_rating_and_why_are_optional_and_validated():
    validate_result(build(deferred=[FOLLOW_UP]), corr())  # old results stay valid
    for rating in RATINGS:
        validate_result(build(deferred=[rated(FOLLOW_UP, rating, "one line")]), corr())
    bad = [
        rated(FOLLOW_UP, "critical"),
        rated(FOLLOW_UP, "Bug"),
        {**FOLLOW_UP, "rating": None, "why": ""},
        rated(FOLLOW_UP, "bug", "x" * (MAX_WHY_CHARS + 1)),
    ]
    for item in bad:
        with pytest.raises(ResultError):
            validate_result(build(deferred=[item]), corr())
    validate_result(build(deferred=[rated(FOLLOW_UP, "edge", "x" * MAX_WHY_CHARS)]), corr())


def test_schema_file_and_shapes_mirror_the_rating():
    from omnigent_factory import core as core_pkg

    schema = json.loads(
        (Path(core_pkg.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    props = schema["$defs"]["deferred"]["properties"]
    assert props["rating"]["enum"] == list(RATINGS) == ["bug", "likely", "edge", "tidy"]
    assert props["why"]["maxLength"] == MAX_WHY_CHARS and props["why"]["minLength"] == 1
    assert "rating" not in schema["$defs"]["deferred"]["required"]
    for kind in ("build_ready", "blocked"):
        assert '"rating"?:"bug(' in RESULT_SHAPES[kind] and '"why"?:str(' in RESULT_SHAPES[kind]


# ------------------------------------------------------------------ prompts


def test_reporting_prompts_carry_the_rating_rubric_and_old_versions_are_kept():
    from omnigent_factory.service import directory as d

    pairs = {
        d._FIRST_TEMPLATES[d.SessionKind.BUILD]: "build-v19.txt",
        d._READINESS_WAKE_TEMPLATE: "readiness-wake-v14.txt",
        d._FINDINGS_WAKE_TEMPLATE: "readiness-findings-wake-v6.txt",
    }
    assert set(pairs) == {
        "build-v20.txt",
        "readiness-wake-v15.txt",
        "readiness-findings-wake-v7.txt",
    }
    for name, old_name in pairs.items():
        text = (TEMPLATES / name).read_text("utf-8")
        old = (TEMPLATES / old_name).read_text("utf-8")
        assert "Deferred ratings" not in old  # the released template is untouched
        assert text.startswith(old.rstrip() + "\n"), name
        flat = " ".join(text.split())
        for word in ('"bug"', '"likely"', '"edge"', '"tidy"', "why is one short line"):
            assert word in flat, (name, word)
        assert "Never tick a checkbox" in flat, name


# ------------------------------------------------------------------ migration


def test_migration_24_orders_existing_items_as_recorded_and_leaves_them_unrated(
    tmp_path: Path,
):
    path = tmp_path / "state.sqlite3"
    old = SqliteStore.open(path, FakeClock(), migrations=MIGRATIONS[:23])
    old.ensure_repository(config())
    rows = [  # (source, key, seq, created) recorded out of key order
        (2, "k-b", 1, 300),
        (1, "k-a", 1, 100),
        (1, "k-c", 2, 300),
    ]
    with old._txn() as conn:
        for source, key, seq, at in rows:
            conn.execute(
                "INSERT INTO deferred_items (repo_id, source_issue, item_key, seq, title, "
                "outcome, created_at_us, updated_at_us) VALUES (?, ?, ?, ?, 't', "
                "'follow_up', ?, ?)",
                (REPO_ID, source, key, seq, at, at),
            )
    old.close()
    store = SqliteStore.open(path, FakeClock())
    try:
        found = {
            i.item_key: i
            for n in (1, 2)
            for i in ds.deferred_items(store, repo_id=REPO_ID, source_issue=n)
        }
        assert {k: i.report_order for k, i in found.items()} == {"k-a": 1, "k-c": 2, "k-b": 3}
        assert all(i.rating == "" and i.why == "" for i in found.values())
        # The next item recorded comes after all of them.
        [new] = ds.record_deferred(
            store,
            repo_id=REPO_ID,
            source_issue=1,
            pr_number=7,
            session_id="s",
            items=[ds.NewDeferred("k-d", "P2", "", "t", "", "r", "follow_up", rating="bug")],
        )
        assert (new.report_order, new.seq, new.rating) == (4, 3, "bug")
    finally:
        store.close()
    with pytest.raises(Exception, match="CHECK"):
        bad = SqliteStore.open(path, FakeClock())
        try:
            with bad._txn() as conn:
                conn.execute("UPDATE deferred_items SET rating = 'critical'")
        finally:
            bad.close()
