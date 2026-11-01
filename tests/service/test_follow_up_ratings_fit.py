"""Numbered, rated follow-ups (review round 1): the Deferred section stays within the
comment limits with every checkbox kept, and a changed rating reaches the consolidated
follow-up issue's line (its checkbox and "fixed in" status kept)."""

from __future__ import annotations

from functools import partial
from typing import Any

import pytest

from omnigent_factory.core import report_lines as rl
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.protocol import MAX_WHY_CHARS, validate_result
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.deferred import (
    SECTION_BUDGET,
    Deferrals,
    _deferred,
    item_key,
)
from omnigent_factory.service.directory import PublicationRenderer
from omnigent_factory.service.review_rounds import ReviewRounds
from omnigent_factory.service.status_comment import StatusComments
from omnigent_factory.store import discipline as ds
from omnigent_factory.testing.harness import HEAD
from tests.service.test_deferred import FU_A, FU_B, result
from tests.service.test_deferred import rig as deferred_rig
from tests.service.test_status_comment import effect
from tests.service.test_status_comment import rig as status_rig
from tests.test_follow_up_discipline import build, corr
from tests.test_protocol import build_result

#: GitHub's comment body limit.
GITHUB_LIMIT = 65_536


def biggest(n: int) -> dict[str, Any]:
    """A deferred item with every text field at its schema maximum."""
    return {
        "severity": "P2",
        "path": f"src/{n:02d}/" + "p" * 293,
        "title": f"T{n:02d} " + "t|" * 148,
        "thread_url": f"https://github.com/o/r/pull/7#discussion_r{n:02d}" + "9" * 256,
        "reason": "r" * 300,
        "outcome": "follow_up" if n % 2 else "advisory",
        "blocks": list(range(100, 120)),
        "rating": ("bug", "likely", "edge", "tidy")[n % 4],
        "why": "w|" * (MAX_WHY_CHARS // 2),
    }


ITEMS = [biggest(n) for n in range(50)]


def test_the_biggest_items_are_a_valid_result():
    raw = build(deferred=ITEMS, summary="s" * 4000)
    assert all(len(i["path"]) == len(i["title"]) == len(i["reason"]) == 300 for i in ITEMS)
    validate_result(raw, corr(depth=0))


def markers(text: str) -> set[str]:
    return rl.ids(text)


@pytest.mark.asyncio
async def test_fifty_biggest_items_keep_every_box_in_the_status_and_the_ready_report(
    service_config: ServiceConfig,
):
    async with status_rig(service_config) as r:
        rounds = ReviewRounds(r.service)
        deferred = Deferrals(r.service, r.client, None, rounds, str)  # type: ignore[arg-type]
        parcel = await r.parcel()
        assert parcel.readiness is not None
        rows = await r.service.db.call(
            partial(
                ds.record_deferred,
                repo_id=service_config.repo_id,
                source_issue=parcel.issue_number,
                pr_number=7,
                session_id=parcel.readiness.session_id,
                items=_deferred(ITEMS),
            )
        )
        expected = {i.action_id for i in rows}
        assert len(expected) == 50
        # The longest text before the section a valid result can give.
        r.write_result(
            parcel.readiness.session_id,
            build_result(head_sha=HEAD, summary="Summary. " * 900, eli5="Simple. " * 70),
        )
        section = await deferred.report_section(parcel)
        assert len(section) <= SECTION_BUDGET and markers(section) == expected
        lines = [line for line in section.splitlines() if line.startswith("- [ ] F")]
        assert len(lines) == 50 and all(" Fix now: P2 " in line for line in lines)
        assert "| F" in section  # a (compacted) table still fits

        # The status comment: every box, within the limit.
        status = StatusComments(
            r.service, r.client, r.directory, str, deferred_section=deferred.report_section
        )
        written = await status.publish(parcel.parcel_id)
        assert written.comment_id is not None
        body = r.server.comments[written.comment_id]["body"]
        assert len(body) < GITHUB_LIMIT and markers(body) == expected

        # The Ready report (the same section): every box, within the limit, and still
        # within it once the factory sets the longest status on every line.
        renderer = PublicationRenderer(r.directory, service_config)
        renderer.deferred_section = deferred.report_section
        ready = await renderer(
            effect(EffectKind.PUBLISH_REPORT, report="ready", pr_number=7, head_sha=HEAD)
        )
        assert ready is not None and markers(ready) == expected
        assert ready.startswith("### PR #7 is ready for review")
        marker = "\n\n<!-- omnigent-factory effect=ef_00000000000000000000000000 -->"
        assert len(ready + marker) < GITHUB_LIMIT
        refused = rl.with_status(ready, dict.fromkeys(expected, "not started: " + "x" * 120))
        assert markers(refused) == expected and len(refused + marker) < GITHUB_LIMIT


@pytest.mark.asyncio
async def test_a_section_over_budget_shrinks_the_table_before_any_box(
    service_config: ServiceConfig,
):
    async with deferred_rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(*ITEMS[:6]))
        parcel = await r.parcel()
        full = await r.deferred.report_section(parcel)
        assert "| # | Item | Rating | Why |" in full and "pp" * 90 in full  # full text
        boxes = markers(full)
        sizes = []
        for budget in (len(full) - 1, 3000, 2000, 1500, 600):
            section = await r.deferred.report_section(parcel, budget)
            assert markers(section) == boxes, budget  # never a box less
            sizes.append(len(section))
            assert len(section) <= budget or budget == 600
        # Shorter lines and cells first, then number and rating only, then a note.
        assert "| # | Rating |" in await r.deferred.report_section(parcel, 1500)
        tight = [await r.deferred.report_section(parcel, b) for b in range(1000, 500, -10)]
        assert any("ratings table is left out" in t and "| #" not in t for t in tight)
        assert all(markers(t) == boxes for t in tight)
        assert sizes == sorted(sizes, reverse=True)


# ------------------------------------------------------------------ consolidated issue


@pytest.mark.asyncio
async def test_a_changed_rating_rewrites_open_and_fixed_issue_lines_in_place(
    service_config: ServiceConfig,
):
    async with deferred_rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, FU_B))
        key_a = item_key(FU_A["severity"], FU_A["path"], FU_A["title"], FU_A["thread_url"])
        key_b = item_key(FU_B["severity"], FU_B["path"], FU_B["title"], FU_B["thread_url"])
        # FU_B was fixed by a rework (the factory ticked it with its status); the owner
        # added a line of their own.
        fixed = "fixed in #7 (`abc1234`)"
        body = rl.with_status(r.repo.issues[100]["body"], {key_b: fixed}, rl.ITEM)
        r.repo.issues[100]["body"] = body + "\n\nOwner note."
        before = rl.checked(r.repo.issues[100]["body"], rl.ITEM)
        assert before == {key_a: False, key_b: True}

        await r.deferred.record_result(
            await r.parcel(),
            "s2",
            result({**FU_A, "rating": "bug", "why": "seen live"}, {**FU_B, "rating": "tidy"}),
        )
        text = r.repo.issues[100]["body"]
        lines = {next(iter(rl.ids(ln, rl.ITEM))): ln for ln in text.splitlines() if "item:" in ln}
        assert lines[key_a].startswith("- [ ] P2 (Bug) `api/src/export.ts` Retry leaks")
        assert lines[key_a].endswith(f"Why: seen live {rl.marker(rl.ITEM, key_a)}")
        assert lines[key_b].startswith("- [x] P1 (Tidy) `api/src/retry.ts` Unbounded")
        assert lines[key_b].endswith(f"→ {fixed} {rl.marker(rl.ITEM, key_b)}")
        assert rl.checked(text, rl.ITEM) == before and text.endswith("Owner note.")
        assert r.repo.creates == 1

        # The same rating again changes nothing; a restart's retry neither.
        writes = len(r.repo.writes)
        await r.deferred.record_result(
            await r.parcel(), "s3", result({**FU_A, "rating": "bug", "why": "seen live"})
        )
        await r.restarted().retry_pending()
        assert len(r.repo.writes) == writes and r.repo.issues[100]["body"] == text


def test_with_text_replaces_only_the_named_lines_text():
    body = "\n".join(
        [
            "intro",
            "  " + rl.line(rl.ITEM, "a", "old a", ticked=True, status="fixed in `1234567`"),
            rl.line(rl.ITEM, "b", "old b"),
            rl.line(rl.ACTION, "a", "action a"),
        ]
    )
    new = rl.with_text(body, {"a": "new a", "c": "nothing"}, rl.ITEM)
    assert new.splitlines() == [
        "intro",
        "  - [x] new a → fixed in `1234567` <!-- factory-item:a -->",
        rl.line(rl.ITEM, "b", "old b"),
        rl.line(rl.ACTION, "a", "action a"),
    ]
    assert rl.texts(new, rl.ITEM) == {"a": "new a", "b": "old b"}


# ------------------------------------------------------------------ accumulated batches

ELI5 = "Simple words for the owner. " * 20


def batches(count: int) -> list[dict[str, Any]]:
    """``count`` valid 50-item results' items (distinct items: they accumulate)."""
    out = []
    for b in range(count):
        items = [{**biggest(n), "title": f"B{b:02d} T{n:02d} " + "t" * 290} for n in range(50)]
        validate_result(build(deferred=items), corr(depth=0))
        out += items
    return out


async def accumulate(r: Any, service_config: ServiceConfig, count: int) -> set[str]:
    parcel = await r.parcel()
    assert parcel.readiness is not None
    rows: list[ds.DeferredItem] = []
    for b in range(count):
        rows += await r.service.db.call(
            partial(
                ds.record_deferred,
                repo_id=service_config.repo_id,
                source_issue=parcel.issue_number,
                pr_number=7,
                session_id=f"s{b}",
                items=_deferred(batches(count)[b * 50 : (b + 1) * 50]),
            )
        )
    r.write_result(
        parcel.readiness.session_id,
        build_result(head_sha=HEAD, summary="Summary line.\n" * 200, eli5=ELI5),
    )
    return {i.action_id for i in rows}


def assert_whole_lines(body: str) -> None:
    """Every marker sits on a whole checkbox line (nothing cut mid-line or mid-marker)."""
    for line in body.splitlines():
        if "factory-action" in line or line.startswith("- ["):
            assert rl.ids(line), line
    assert body.count("<!--") == body.count("-->")


def assert_pr_block(body: str) -> None:
    assert f"#### ELI5\n\n{ELI5.strip()}" in body
    assert "**PR:** " in body and "**CI:** " in body


async def renderers(r: Any, service_config: ServiceConfig) -> tuple[Any, Any]:
    rounds = ReviewRounds(r.service)
    deferred = Deferrals(r.service, r.client, None, rounds, str)  # type: ignore[arg-type]
    status = StatusComments(
        r.service, r.client, r.directory, str, deferred_section=deferred.report_section
    )
    ready = PublicationRenderer(r.directory, service_config)
    ready.deferred_section = deferred.report_section
    return status, ready


@pytest.mark.asyncio
async def test_450_accumulated_items_keep_every_box_and_the_pr_block_in_both_comments(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    async with status_rig(service_config) as r:
        expected = await accumulate(r, service_config, 9)
        assert len(expected) == 450
        status, renderer = await renderers(r, service_config)
        parcel = await r.parcel()
        section = await status.deferred_section(parcel)
        assert len(section) <= SECTION_BUDGET and markers(section) == expected

        written = await status.publish(parcel.parcel_id)
        assert written.comment_id is not None
        body = r.server.comments[written.comment_id]["body"]
        assert len(body) <= 60_000 + 200 and markers(body) == expected
        assert_pr_block(body)
        assert_whole_lines(body)

        ready = await renderer(
            effect(EffectKind.PUBLISH_REPORT, report="ready", pr_number=7, head_sha=HEAD)
        )
        assert ready is not None and len(ready) <= 55_000 and markers(ready) == expected
        assert ready.startswith("### PR #7 is ready for review")
        assert_pr_block(ready)
        assert_whole_lines(ready)
        assert "more items - see" not in ready + body
        assert "checkbox lines left out" not in caplog.text


def test_status_keeps_the_pr_block_and_trims_other_parts_first():
    from omnigent_factory.core.types import BotState
    from omnigent_factory.service.status_comment import IssueView, Question, _fit_issue

    boxes = "\n".join(rl.line(rl.ACTION, f"d-{n:04d}", f"F{n} Fix now: P2 x") for n in range(400))
    deferred = "\n\n**Deferred** (tick an item to have it done in this PR):\n" + boxes
    pr = f"#### ELI5\n\n{ELI5.strip()}\n\n**PR:** https://x/pull/7 (head `abc`)\n\n**CI:** green"
    view = IssueView(
        issue=1,
        stage="Ready",
        bot=BotState.NEEDS_YOU,
        question=Question("Q " * 2000),
        triage="Triage detail. " * 600,
        checkpoint="Checkpoint. " * 700,
        pr=pr,
        pr_number=7,
        deferred=deferred,
    )
    history = [(n * 1_000_000, "Something happened " * 5) for n in range(300)]
    full = _fit_issue(view, "Ready", history, limit=10**9)
    assert len(full) > 60_000
    body = _fit_issue(view, "Ready", history)
    assert len(body) <= 60_000 and markers(body) == {f"d-{n:04d}" for n in range(400)}
    assert pr in body and body.startswith("### Ready") and body.endswith("[truncated]")
    assert "History" not in body  # older content left out before anything of the PR


@pytest.mark.asyncio
async def test_800_accumulated_items_fall_back_within_the_caps_with_a_more_items_line(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    async with status_rig(service_config) as r:
        expected = await accumulate(r, service_config, 16)
        assert len(expected) == 800
        status, renderer = await renderers(r, service_config)
        parcel = await r.parcel()
        section = await status.deferred_section(parcel)
        assert len(section) > SECTION_BUDGET  # beyond what the budget can hold
        order = [next(iter(rl.ids(line))) for line in section.splitlines() if rl.ids(line)]

        with caplog.at_level("WARNING"):
            written = await status.publish(parcel.parcel_id)
            ready = await renderer(
                effect(EffectKind.PUBLISH_REPORT, report="ready", pr_number=7, head_sha=HEAD)
            )
        assert written.comment_id is not None and ready is not None
        body = r.server.comments[written.comment_id]["body"]
        for text, cap in ((body, 60_000 + 200), (ready, 55_000)):
            assert len(text) <= cap < GITHUB_LIMIT
            assert_pr_block(text)
            assert_whole_lines(text)
            kept = [next(iter(rl.ids(line))) for line in text.splitlines() if rl.ids(line)]
            assert 0 < len(kept) < 800 and kept == order[: len(kept)]  # in number order
            more = [line for line in text.splitlines() if "more items - see" in line]
            assert more == [f"{800 - len(kept)} more items - see the follow-up issue"], more
        assert ready.startswith("### PR #7 is ready for review")
        assert caplog.text.count("checkbox lines left out") >= 2
