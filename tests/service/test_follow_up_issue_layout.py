"""The consolidated follow-up issue laid out like the Ready report's Deferred section
(owner request, 2026-11-02): a numbered ``| # | Item | Rating | Why |`` table in the
report's own numbers, a details block per item, then the checklist (``factory-item``
markers unchanged), all in a factory-managed region; text outside it is the owner's.
Old-layout issues are migrated on their next sync with ticks, statuses and owner text
kept, and the body stays within GitHub's limit with every box."""

from __future__ import annotations

import re
from functools import partial
from typing import Any

import pytest

from omnigent_factory.core import report_lines as rl
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.deferred import (
    ISSUE_LIMIT,
    REGION_BEGIN,
    REGION_END,
    issue_marker,
    item_key,
)
from omnigent_factory.store import discipline as ds
from tests.service.test_comment_style import FORBIDDEN
from tests.service.test_deferred import ADV, FU_A, FU_B, action, report, result, rig
from tests.service.test_follow_up_ratings_fit import biggest


def key(item: dict[str, Any]) -> str:
    return item_key(
        item["severity"], item.get("path", ""), item["title"], item.get("thread_url", "")
    )


def rated(item: dict[str, Any], rating: str, why: str = "") -> dict[str, Any]:
    return {**item, "rating": rating, **({"why": why} if why else {})}


def numbers_in_report(section: str) -> dict[str, str]:
    """Action id -> F-number, from the Ready report's checkbox lines."""
    out = {}
    for line in section.splitlines():
        m = re.match(r"- \[[ x]\] (F\d+) .*<!-- factory-action:([\w-]+) -->$", line)
        if m:
            out[m.group(2)] = m.group(1)
    return out


def item_lines(body: str) -> dict[str, str]:
    return {next(iter(rl.ids(ln, rl.ITEM))): ln for ln in body.splitlines() if "item:" in ln}


@pytest.mark.asyncio
async def test_a_new_issue_has_the_table_then_details_then_the_checklist(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.deferred.record_result(
            await r.parcel(),
            "s1",
            result(rated(FU_A, "bug", "seen live"), ADV, rated(FU_B, "edge", "x | y")),
        )
        body = r.repo.issues[100]["body"]
        begin, end = body.index(REGION_BEGIN), body.index(REGION_END)
        assert body.index(issue_marker(1, 7)) > end
        region = body[begin:end]
        # The Ready report numbers FU_A F1, the advisory F2, FU_B F3: the same here.
        table = region.index("| # | Item | Rating | Why |")
        f1 = region.index("| F1 | P2 Retry leaks the delivery row | Bug | seen live |")
        f3 = region.index("| F3 | P1 Unbounded retry loop | Edge | x \\| y |")
        assert table < f1 < f3 and "| F2 |" not in region  # advisories are not filed here
        d1 = region.index("### F1 · Retry leaks the delivery row")
        d3 = region.index("### F3 · Unbounded retry loop")
        assert f3 < d1 < d3
        detail = region[d1:d3]
        assert "**P2** · Bug: seen live · `api/src/export.ts`" in detail
        assert "Deferred: outside the plan" in detail
        assert "[Review thread](https://github.com/o/r/pull/7#discussion_r1)" in detail
        checklist = region.index("**Checklist**")
        assert d3 < checklist
        lines = item_lines(body)
        assert list(lines) == [key(FU_A), key(FU_B)]
        assert lines[key(FU_A)] == rl.line(
            rl.ITEM, key(FU_A), "F1 P2 (Bug) Retry leaks the delivery row"
        )
        assert lines[key(FU_B)] == rl.line(rl.ITEM, key(FU_B), "F3 P1 (Edge) Unbounded retry loop")
        assert all(region.index(ln) > checklist for ln in lines.values())
        assert "Clock going backwards" not in body
        assert not [bad for bad in FORBIDDEN if bad in body]  # plain GitHub Markdown

        # The numbers are the Ready report's numbers for the same items.
        report_numbers = numbers_in_report(await report(r))
        assert report_numbers[action(FU_A)] == "F1" and report_numbers[action(FU_B)] == "F3"
        assert report_numbers[action(ADV)] == "F2"


@pytest.mark.asyncio
async def test_an_item_without_rating_reason_or_thread_gets_only_what_is_known(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        bare = {"severity": "P3", "title": "Name the constant"}
        await r.deferred.record_result(await r.parcel(), "s1", result(bare))
        body = r.repo.issues[100]["body"]
        block = body[body.index("### F1 · Name the constant") : body.index("**Checklist**")]
        assert block.strip().splitlines() == ["### F1 · Name the constant", "", "**P3**"]
        assert "| F1 | P3 Name the constant | - | - |" in body


@pytest.mark.asyncio
async def test_an_old_layout_issue_is_migrated_keeping_ticks_statuses_and_owner_text(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, FU_B))
        fixed = "fixed in #7 (`abc1234`)"
        # The layout the factory wrote before the region (lines only), edited by the
        # owner: intro reworded, a note between the lines, a tick, a note at the end.
        old = (
            "Owner's own intro.\n\n"
            + rl.line(
                rl.ITEM,
                key(FU_A),
                "P2 `api/src/export.ts` Retry leaks the delivery row: outside the plan",
            )
            + "\nOwner's note between the lines."
            + "\n"
            + rl.line(
                rl.ITEM, key(FU_B), "P1 Unbounded retry loop: too large", ticked=True, status=fixed
            )
            + f"\n\n{issue_marker(1, 7)}\n\nOwner note at the end."
        )
        r.repo.issues[100]["body"] = old
        more = {**FU_B, "title": "Second retry path", "thread_url": ""}
        await r.deferred.record_result(
            await r.parcel(), "s2", result(FU_A, rated(FU_B, "bug"), more)
        )
        body = r.repo.issues[100]["body"]
        assert body.startswith("Owner's own intro.\n\n" + REGION_BEGIN + "\n| # | Item |")
        assert body.endswith(
            REGION_END
            + "\n\nOwner's note between the lines.\n\n"
            + issue_marker(1, 7)
            + "\n\nOwner note at the end."
        )
        assert rl.checked(body, rl.ITEM) == {key(FU_A): False, key(FU_B): True, key(more): False}
        lines = item_lines(body)
        assert lines[key(FU_B)] == rl.line(
            rl.ITEM, key(FU_B), "F2 P1 (Bug) Unbounded retry loop", ticked=True, status=fixed
        )
        assert lines[key(more)].startswith("- [ ] F3 P1 Second retry path")
        assert "### F3 · Second retry path" in body
        # Migrated once: a re-sync writes nothing.
        writes = len(r.repo.writes)
        await r.deferred.record_result(await r.parcel(), "s3", result(FU_A, more))
        await r.restarted().retry_pending()
        assert len(r.repo.writes) == writes and r.repo.issues[100]["body"] == body


@pytest.mark.asyncio
async def test_a_rating_change_rewrites_its_line_table_row_and_details_only(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, FU_B))
        body = r.repo.issues[100]["body"]
        # Owner text inside the intro and after the marker; FU_B fixed and ticked.
        fixed = "fixed in #7 (`abc1234`)"
        body = rl.with_status(body, {key(FU_B): fixed}, rl.ITEM) + "\n\nOwner note."
        r.repo.issues[100]["body"] = body
        await r.deferred.record_result(
            await r.parcel(), "s2", result(rated(FU_A, "likely", "seen once"), FU_B)
        )
        new = r.repo.issues[100]["body"]
        assert item_lines(new) == {
            key(FU_A): rl.line(rl.ITEM, key(FU_A), "F1 P2 (Likely) Retry leaks the delivery row"),
            key(FU_B): item_lines(body)[key(FU_B)],
        }
        assert "| F1 | P2 Retry leaks the delivery row | Likely | seen once |" in new
        assert "**P2** · Likely: seen once" in new and new.endswith("Owner note.")


@pytest.mark.asyncio
async def test_fifty_biggest_items_keep_every_box_within_the_issue_limit(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        items = [{**biggest(n), "outcome": "follow_up", "blocks": []} for n in range(50)]
        await r.deferred.record_result(await r.parcel(), "s1", result(*items))
        body = r.repo.issues[100]["body"]
        keys = {key(i) for i in items}
        assert len(body) <= ISSUE_LIMIT and rl.ids(body, rl.ITEM) == keys
        assert "| F1 |" in body and "### F1 · " in body  # shrunk, still there
        numbers = re.findall(r"- \[ \] (F\d+) ", body)
        assert numbers == [f"F{n}" for n in range(1, 51)]
        # Fixed later (the longest status, written without re-fitting): still within.
        statuses = dict.fromkeys(keys, "fixed in #99999 (`abcdef1`)")
        assert len(rl.with_status(body, statuses, rl.ITEM)) <= ISSUE_LIMIT
        # A long owner note: the details go first, then the table; never a box.
        owner = "\n\n" + "o" * 30_000
        r.repo.issues[100]["body"] = body + owner
        rows = await r.service.db.call(
            partial(ds.deferred_items, repo_id=r.service.config.repo_id, source_issue=1)
        )
        layout = r.deferred._issue_layout
        numbers_map = {i.item_key: f"F{n}" for n, i in enumerate(rows, start=1)}
        tight = layout(body + owner, 1, 7, rows, numbers_map)
        assert tight is not None and len(tight) <= ISSUE_LIMIT and tight.endswith(owner)
        assert rl.ids(tight, rl.ITEM) == keys and "| F1 |" in tight
        assert "Item details are left out" in tight
        longer = "\n\n" + "o" * 55_000
        tighter = layout(body + longer, 1, 7, rows, numbers_map)
        assert tighter is not None and len(tighter) <= ISSUE_LIMIT and tighter.endswith(longer)
        assert rl.ids(tighter, rl.ITEM) == keys and "Item details are left out" in tighter
        assert "| # | Item |" not in tighter  # the table shrank too (numbers, ratings)
        # Not even the shortest boxes fit beside the owner's text: nothing is written
        # (GitHub would refuse it), the owner's text is never cut.
        assert layout(body + longer * 2, 1, 7, rows, numbers_map) is None


def test_statuses_and_span_read_the_factorys_lines_only():
    body = "\n".join(
        [
            "intro",
            rl.line(rl.ITEM, "a", "a", ticked=True, status="fixed in #7 (`abc1234`)"),
            "owner",
            rl.line(rl.ITEM, "b", "b"),
            rl.line(rl.ACTION, "c", "c", status="fixing"),
        ]
    )
    assert rl.statuses(body, rl.ITEM) == {"a": "fixed in #7 (`abc1234`)", "b": ""}
    found = rl.span(body, rl.ITEM)
    assert found is not None
    assert body[found[0] : found[1]].splitlines()[0].startswith("- [x] a")
    assert body[found[0] : found[1]].splitlines()[-1].endswith("factory-item:b -->")
    assert rl.span("no lines", rl.ITEM) is None
