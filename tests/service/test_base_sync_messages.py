"""The first message of a stage run says how its start synced the worktree with main.

The sync outcome is persisted with the run's dispatch snapshot (once per run, so a
restarted daemon renders the same note and never syncs again). New template versions
carry the note; runs pinned to an older template keep rendering it without one.
"""

from __future__ import annotations

from importlib.resources import files
from typing import Any

import pytest

from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.types import SessionKind
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import ServiceDispatchDirectory
from tests.service.test_mcp import building, planning, started
from tests.service.test_pilot_677 import eventually

TEMPLATES = files("omnigent_factory.service") / "templates"
FAST_FORWARD = {
    "status": "fast_forwarded",
    "base": "main",
    "old_oid": "a" * 40,
    "new_oid": "b" * 40,
    "behind": 65,
    "reason": "",
}


def _first(rig: Any, session_id: str) -> Any:
    sends = [
        e
        for e in rig.executed(EffectKind.SEND_MESSAGE)
        if e.preconditions.session_id == session_id
        and e.args.get("purpose") == MessagePurpose.FIRST.value
    ]
    return sends[0] if sends else None


@pytest.mark.asyncio
async def test_a_plan_runs_first_message_says_the_worktree_was_fast_forwarded(
    service_config: ServiceConfig,
) -> None:
    async with started(service_config) as rig:
        await planning(rig)
        run = await rig.run()
        assert run.kind == SessionKind.PLAN
        await eventually(lambda: _first(rig, run.session_id) is not None)
        directory = rig.tools.directory
        assert await directory.stage_spec(run.session_id) is not None  # as preparation does
        assert await directory.base_sync(run.session_id) is None
        await directory.record_base_sync(run.session_id, FAST_FORWARD)
        # Recorded once: a later record (a retried preparation) never replaces it.
        await directory.record_base_sync(run.session_id, {**FAST_FORWARD, "behind": 1})
        # A restarted daemon reads the same record from the dispatch snapshot.
        restarted = ServiceDispatchDirectory(rig.service.db, service_config)
        assert await restarted.base_sync(run.session_id) == FAST_FORWARD
        text = await restarted.message_text(_first(rig, run.session_id))
    assert text is not None
    flat = " ".join(text.split())
    assert (
        "The factory fast-forwarded this issue worktree to origin/main "
        f"({'a' * 12} -> {'b' * 12}, 65 new commits)" in flat
    )
    assert "re-check the plan's assumptions against the current code" in flat


@pytest.mark.asyncio
async def test_a_build_runs_first_message_names_how_far_main_moved(
    service_config: ServiceConfig,
) -> None:
    async with started(service_config) as rig:
        await building(rig)
        run = await rig.run()
        assert run.kind == SessionKind.BUILD
        await eventually(lambda: _first(rig, run.session_id) is not None)
        directory = rig.tools.directory
        assert await directory.stage_spec(run.session_id) is not None
        own = {**FAST_FORWARD, "status": "own_commits", "behind": 17}
        await directory.record_base_sync(run.session_id, own)
        text = await directory.message_text(_first(rig, run.session_id))
        again = await directory.message_text(_first(rig, run.session_id))
    assert text is not None and again == text  # restart-stable text
    flat = " ".join(text.split())
    assert (
        "main has moved 17 commits since your branch base; merge origin/main first if "
        "relevant." in flat
    )
    assert 'If the approved plan no longer fits current main, submit "blocked"' in flat


def test_current_stage_templates_carry_the_sync_note_and_old_ones_are_kept() -> None:
    from omnigent_factory.service import directory as d

    current = {
        *d._FIRST_TEMPLATES.values(),
        d._RETRIAGE_TEMPLATE,
        d._EPIC_TRIAGE_TEMPLATE,
        d._REWORK_TEMPLATE,
        d._CONFLICT_TEMPLATE,
    }
    assert current == {
        "triage-v8.txt",
        "plan-v8.txt",
        "build-v10.txt",
        "triage-feedback-v5.txt",
        "epic-triage-v2.txt",
        "build-rework-v7.txt",
        "build-conflict-v2.txt",
    }
    for name in current:
        assert "{base_note}" in (TEMPLATES / name).read_text("utf-8"), name
    for name in ("build-v10.txt", "build-rework-v7.txt", "build-conflict-v2.txt"):
        text = " ".join((TEMPLATES / name).read_text("utf-8").split())
        assert 'no longer fits current {base_ref}, submit "blocked" with the reason' in text
        assert "never widen the scope" in text
    # In-flight runs stay pinned to the template bytes they were dispatched with.
    for old in (
        "triage-v7.txt",
        "plan-v7.txt",
        "build-v9.txt",
        "triage-feedback-v4.txt",
        "epic-triage-v1.txt",
        "build-rework-v6.txt",
        "build-conflict-v1.txt",
    ):
        assert "{base_note}" not in (TEMPLATES / old).read_text("utf-8"), old
