"""Owner-directed splits ("do these bits, file the rest as an issue"): the ``split_out``
result field on triage, plan, build and blocked results with its caps, one issue per item
filed by the factory (idempotent across restarts and unclear creates), no ``follow-up``
label and no lineage, the source's epic and blocked-by links, and the stage comment
listing them."""

from __future__ import annotations

import logging
from dataclasses import replace
from functools import partial
from typing import Any

import pytest

from omnigent_factory.core.protocol import (
    RESULT_SHAPES,
    Correlation,
    ResultError,
    validate_result,
)
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.store import discipline as ds
from tests.service.test_deferred import rig
from tests.service.test_follow_ups import gh_issue
from tests.test_follow_up_discipline import build

pytestmark = pytest.mark.asyncio

SPLIT = {
    "title": "Export CSV in the admin view",
    "body": "The owner asked to file the admin export separately.",
    "reason": "owner: do the API part now, file the admin view",
}


def corr(stage: str = "build") -> Correlation:
    return Correlation(
        parcel_id="I_747",
        stage_session_id="ss_1",
        dispatch_nonce="n",
        revision=1,
        stage=stage,  # type: ignore[arg-type]
        issue_number=747,
        follow_up_depth=1,  # even a follow-up's run may split work out at the owner's word
        follow_up_source=700,
    )


TRIAGE = {
    "kind": "triage",
    "summary": "s",
    "priority": "P2",
    "size": "S",
    "recommendation": "fix",
    "duplicate_issue": None,
    "labels": [],
    "missing_information": [],
}
PLAN = {
    "kind": "plan",
    "publication_kind": "contract",
    "approach": "a",
    "risks": [],
    "contract": {
        "goal": "g",
        "acceptance_criteria": [{"id": "AC-1", "criterion": "c", "verification": "v"}],
        "non_goals": [],
        "size": "S",
        "resolved_decisions": [],
    },
    "open_decision_ids": [],
}


# ------------------------------------------------------------------ the result field


async def test_split_out_is_accepted_on_triage_plan_build_and_blocked():
    for result, stage in (
        ({**TRIAGE, "split_out": [SPLIT]}, "triage"),
        ({**PLAN, "split_out": [SPLIT]}, "plan"),
        (build(split_out=[{**SPLIT, "relation": "after_source", "blocks": [12]}]), "build"),
        ({"kind": "blocked", "reason": "x", "done": [], "split_out": [SPLIT]}, "plan"),
    ):
        dumped = validate_result(result, corr(stage)).result.result.model_dump(mode="json")
        assert dumped["split_out"][0]["title"] == SPLIT["title"]
        assert dumped["split_out"][0]["relation"] in ("none", "after_source")


async def test_split_out_caps_are_enforced():
    many = [{**SPLIT, "title": f"part {n}"} for n in range(11)]
    for bad, message in (
        (many, "split_out"),
        ([{**SPLIT, "title": "x" * 201}], "split_out"),
        ([{**SPLIT, "body": "x" * 4001}], "split_out"),
        ([{**SPLIT, "reason": "x" * 301}], "split_out"),
        ([{**SPLIT, "relation": "depends_on"}], "split_out"),
        ([SPLIT, {**SPLIT, "title": "  export CSV in the ADMIN view"}], "each title at most once"),
        ([{**SPLIT, "blocks": [747]}], "use relation"),
        ([{**SPLIT, "extra": 1}], "split_out"),
    ):
        with pytest.raises(ResultError) as error:
            validate_result(build(split_out=bad), corr())
        assert message in " ".join(error.value.details), bad
    validate_result(build(split_out=many[:10]), corr())


async def test_split_relations_name_their_subject_and_legacy_values_are_still_accepted():
    for relation in ("none", "after_source", "before_source", "blocks", "blocked_by"):
        validate_result(build(split_out=[{**SPLIT, "relation": relation}]), corr())
    shape = RESULT_SHAPES["triage"]
    assert "after_source(new issue blocked by this one)" in shape
    assert "before_source(new issue blocks this one)" in shape
    assert "issues the new issue blocks" in shape


# ------------------------------------------------------------------ filing


def splitter(r: Any) -> Any:
    from omnigent_factory.service.links import NativeLinks
    from omnigent_factory.service.splits import Splits

    async def no_cards() -> list[Any]:
        return []

    native = NativeLinks(r.service, r.repo, lambda _sid: None, no_cards)  # type: ignore[arg-type]
    return Splits(r.service, native, str)


def result(*items: dict[str, Any]) -> dict[str, Any]:
    return {**TRIAGE, "split_out": list(items)}


async def test_one_issue_per_split_with_provenance_epic_and_links_and_no_follow_up_label(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        splits = splitter(r)
        parcel = await r.parcel()  # issue #1, a sub-issue of epic #40
        filed = await splits.record_result(
            parcel,
            "s1",
            result(
                {**SPLIT, "relation": "before_source", "blocks": [5]},
                {**SPLIT, "title": "Audit log for exports", "relation": "after_source"},
            ),
        )
        assert [s.issue_number for s in filed] == [100, 101]
        assert r.repo.creates == 2  # separate pieces of work: never consolidated
        first, second = r.repo.issues[100], r.repo.issues[101]
        assert first["title"] == SPLIT["title"]
        assert "Split out of #1 at the owner's direction" in first["body"]
        assert "<!-- factory-split source=1 key=" in first["body"]
        writes = r.repo.writes
        assert ("addSubIssue", (40, 100)) in writes and ("addSubIssue", (40, 101)) in writes
        assert ("addBlockedBy", (1, 100)) in writes  # before_source: #1 waits on #100
        assert ("addBlockedBy", (5, 100)) in writes
        assert ("addBlockedBy", (101, 1)) in writes  # after_source: #101 waits on #1
        assert not [w for w in writes if w[0] in ("addLabel", "createLabel")]
        assert second["labels"] == []
        config = r.service.config
        for number in (100, 101):
            assert (
                await r.service.db.call(lambda s, n=number: s.follow_up(config.repo_id, n)) is None
            )
            lineage = await r.service.db.call(
                lambda s, n=number: s.follow_up_lineage(config.repo_id, n)
            )
            assert lineage == (0, None)  # not a follow-up: its build may defer follow-ups
        section = await splits.section(parcel, "s1")
        assert "**Split out** (filed at the owner's direction):" in section
        assert f"- #100: {SPLIT['title']}" in section and "- #101: Audit log" in section
        assert await splits.section(parcel, "other-run") == ""


def links(r: Any) -> list[tuple[int, int]]:
    """The blocked-by links written: (blocked issue, blocking issue)."""
    return [w[1] for w in r.repo.writes if w[0] == "addBlockedBy"]


async def test_1150_replay_after_source_links_the_new_issue_blocked_by_the_source(
    service_config: ServiceConfig,
):
    # timesheets #1150: split #1159 was meant to come after #1150 but was linked
    # "#1150 blocked by #1159" (legacy "blocks" read from the new issue's side).
    async with rig(service_config) as r:
        r.repo.issues[1150] = gh_issue(1150, parent=40)
        r.repo.next_number = 1159
        parcel = replace(await r.parcel(), issue_number=1150)
        item = {
            **SPLIT,
            "title": "Approver history follow-on after the approvals rework",
            "relation": "after_source",
        }
        validate_result({**TRIAGE, "split_out": [item]}, corr("triage"))
        filed = await splitter(r).record_result(parcel, "s1", result(item))
        assert [s.issue_number for s in filed] == [1159]
        assert links(r) == [(1159, 1150)]  # #1159 blocked by #1150, never the reverse


async def test_before_source_links_the_source_blocked_by_the_new_issue(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await splitter(r).record_result(
            await r.parcel(), "s1", result({**SPLIT, "relation": "before_source"})
        )
        assert links(r) == [(1, 100)]  # #1 blocked by the new #100


@pytest.mark.parametrize("legacy", ["blocks", "blocked_by"])
async def test_a_legacy_relation_reads_as_after_source_with_a_deprecation_line(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture, legacy: str
):
    async with rig(service_config) as r:
        with caplog.at_level(logging.INFO, logger="omnigent_factory.service.splits"):
            [filed] = await splitter(r).record_result(
                await r.parcel(), "s1", result({**SPLIT, "relation": legacy})
            )
        assert filed.relation == "after_source"
        assert links(r) == [(100, 1)]  # the new #100 blocked by its source #1
        assert [m for m in caplog.messages if "deprecated split relation" in m] == [
            f"deprecated split relation={legacy} source=#1 read as after_source "
            "(use after_source or before_source)"
        ]


@pytest.mark.parametrize(
    ("legacy", "done"),
    [("blocks", "blocks:1"), ("blocks", "blocked_by:1"), ("blocked_by", "blocked_by:1")],
)
async def test_a_stored_legacy_split_whose_source_link_is_done_is_never_relinked(
    service_config: ServiceConfig, legacy: str, done: str
):
    async with rig(service_config) as r:
        repo_id = r.service.config.repo_id
        r.repo.issues[100] = gh_issue(100, parent=40)
        item = ds.NewSplit("k", SPLIT["title"], SPLIT["body"], SPLIT["reason"], legacy)
        for step in (
            partial(ds.record_splits, source_issue=1, session_id="s1", items=[item]),
            partial(
                ds.set_split_state, source_issue=1, item_key="k", state="created", issue_number=100
            ),
            partial(ds.mark_split_done, source_issue=1, item_key="k", action="parent"),
            partial(ds.mark_split_done, source_issue=1, item_key="k", action=done),
        ):
            await r.service.db.call(partial(step, repo_id=repo_id))
        [row] = await r.service.db.call(partial(ds.splits, repo_id=repo_id))
        assert row.relation == legacy and row.pending_links() == ()
        await splitter(r).retry_pending()  # after the upgrade
        assert links(r) == [] and r.repo.creates == 0


async def test_a_stored_legacy_split_with_its_link_pending_links_after_source(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        repo_id = r.service.config.repo_id
        item = ds.NewSplit("k", SPLIT["title"], SPLIT["body"], SPLIT["reason"], "blocks")
        await r.service.db.call(
            partial(
                ds.record_splits, repo_id=repo_id, source_issue=1, session_id="s1", items=[item]
            )
        )
        await splitter(r).retry_pending()
        assert links(r) == [(100, 1)]


async def test_splits_are_filed_once_across_resubmission_and_restart(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        parcel = await r.parcel()
        await splitter(r).record_result(parcel, "s1", result(SPLIT))
        writes = len(r.repo.writes)
        restarted = splitter(r)
        await restarted.record_result(parcel, "s2", result({**SPLIT, "body": "reworded"}))
        await restarted.retry_pending()
        assert r.repo.creates == 1 and len(r.repo.writes) == writes


async def test_an_unclear_create_is_found_by_its_marker(service_config: ServiceConfig):
    async with rig(service_config) as r:
        parcel = await r.parcel()
        r.repo.lose_create = True
        await splitter(r).record_result(parcel, "s1", result(SPLIT))
        rows = await r.service.db.call(partial(ds.splits, repo_id=r.service.config.repo_id))
        assert [(s.state, s.issue_number) for s in rows] == [("creating", None)]
        await splitter(r).retry_pending()  # after a restart
        rows = await r.service.db.call(partial(ds.splits, repo_id=r.service.config.repo_id))
        assert [(s.state, s.issue_number) for s in rows] == [("created", 100)]
        assert r.repo.creates == 1 and ("addSubIssue", (40, 100)) in r.repo.writes


async def test_a_split_lists_itself_in_the_stage_comment_filing_first_if_needed(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        parcel = await r.parcel()
        r.repo.issues[1] = gh_issue(1)  # no epic: no parent link
        splits = splitter(r)
        await r.service.db.call(
            partial(
                ds.record_splits,
                repo_id=r.service.config.repo_id,
                source_issue=1,
                session_id="s1",
                items=[ds.NewSplit("k", SPLIT["title"], SPLIT["body"], SPLIT["reason"])],
            )
        )
        # The comment renders before the background filing ran: it files first.
        section = await splits.section(parcel, "s1")
        assert f"- #100: {SPLIT['title']}" in section and r.repo.creates == 1
        assert not [w for w in r.repo.writes if w[0] == "addSubIssue"]


async def test_the_publication_renderer_adds_the_split_section(service_config: ServiceConfig):
    from types import SimpleNamespace

    from omnigent_factory.core.effects import EffectKind
    from omnigent_factory.service.directory import PublicationRenderer, ServiceDispatchDirectory

    async with rig(service_config) as r:
        directory = ServiceDispatchDirectory(r.service.db, r.service.config)
        renderer = PublicationRenderer(directory, r.service.config)
        seen: list[str] = []

        async def section(_parcel: Any, sid: str) -> str:
            seen.append(sid)
            return "\n\n**Split out** (filed at the owner's direction):\n- #100: x"

        renderer.split_section = section
        renderer._stored_result = lambda _e: dict(TRIAGE)  # type: ignore[method-assign]
        effect = SimpleNamespace(
            parcel_id="I_parcel_1",
            kind=EffectKind.PUBLISH_TRIAGE,
            args={"session_id": "s-triage"},
            preconditions=SimpleNamespace(session_id=None),
        )
        text = await renderer(effect)  # type: ignore[arg-type]
        assert text is not None and text.endswith("- #100: x") and seen == ["s-triage"]
