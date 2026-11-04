"""A PLAN/BUILD run's first message says the dependencies are installed only when the
worktree prepare state of its worktree is ``ok`` for the current dependency inputs
(``line_shown``); otherwise (off, failed, timed out, interrupted or stale) it is
byte-identical to what it was without the hook. No new template versions: the renderer
adds the line on the existing ``{base_note}`` line. The decision is refreshed by every
preparation until the first message renders, then pinned in the dispatch snapshot.
"""

from __future__ import annotations

from importlib.resources import files
from typing import Any

import pytest

from omnigent_factory.core.types import SessionKind
from omnigent_factory.credentials.worktree_prepare import PREPARED_LINE
from omnigent_factory.service import directory as d
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import ServiceDispatchDirectory, prepared_note
from tests.service.test_base_sync_messages import FAST_FORWARD, _first
from tests.service.test_mcp import building, planning, started
from tests.service.test_pilot_677 import eventually

TEMPLATES = files("omnigent_factory.service") / "templates"
LINE = "Dependencies are installed and prisma/renderer are generated; reinstall only if you change the lockfile."  # noqa: E501
OK = {"status": "ok", "path": "/w/factory-issue-1", "reason": "", "ran": True, "line_shown": True}
STALE = {**OK, "line_shown": False}  # ok for other dependency inputs
FAILED = {**OK, "status": "failed", "reason": "exit 1", "line_shown": False}
INTERRUPTED = {**OK, "status": "interrupted", "line_shown": False}


def test_the_line_is_exactly_the_owners_wording() -> None:
    assert PREPARED_LINE == LINE


async def _run(rig: Any, kind: SessionKind) -> Any:
    run = await rig.run()
    assert run.kind == kind
    await eventually(lambda: _first(rig, run.session_id) is not None)
    directory = rig.tools.directory
    assert await directory.stage_spec(run.session_id) is not None  # as preparation does
    return run


async def _text(rig: Any, run: Any, directory: Any = None) -> str:
    text = await (directory or rig.tools.directory).message_text(_first(rig, run.session_id))
    assert text is not None
    return text


@pytest.mark.asyncio
async def test_a_prepared_plan_run_says_so_and_the_decision_pins_at_first_render(
    service_config: ServiceConfig,
) -> None:
    async with started(service_config) as rig:
        await planning(rig)
        run = await _run(rig, SessionKind.PLAN)
        directory = rig.tools.directory
        assert await directory.worktree_prepare(run.session_id) is None
        before = await _text(rig, run)  # hook off: no record, nothing pinned
        assert "worktree_prepare_pinned" not in directory.dispatch_snapshot(run.session_id)
        # Refreshed while the first message has not rendered (a retried preparation of
        # a worktree recreated meanwhile).
        await directory.record_worktree_prepare(run.session_id, FAILED)
        await directory.record_worktree_prepare(run.session_id, OK)
        text = await _text(rig, run)
        snapshot = directory.dispatch_snapshot(run.session_id)
        assert snapshot["worktree_prepare"] == OK and snapshot["worktree_prepare_pinned"]
        # Pinned once rendered: a later record never changes what a restart sends.
        await directory.record_worktree_prepare(run.session_id, FAILED)
        restarted = ServiceDispatchDirectory(rig.service.db, service_config)
        again = await _text(rig, run, restarted)
    assert again == text
    assert text.count(LINE) == 1 and f"\n{LINE}\n" in text  # its own line
    assert text.replace(LINE, "") == before  # nothing else changed (it took the empty line)


@pytest.mark.asyncio
@pytest.mark.parametrize("record", [FAILED, STALE, INTERRUPTED], ids=["failed", "stale", "int"])
async def test_no_line_unless_ok_for_the_current_inputs_build_byte_identical(
    service_config: ServiceConfig, record: dict[str, Any]
) -> None:
    async with started(service_config) as rig:
        await building(rig)
        run = await _run(rig, SessionKind.BUILD)
        before = await _text(rig, run)
        await rig.tools.directory.record_worktree_prepare(run.session_id, record)
        text = await _text(rig, run)
        snapshot = rig.tools.directory.dispatch_snapshot(run.session_id)
        assert snapshot["worktree_prepare"]["line_shown"] is False
    assert text == before
    assert LINE not in text


@pytest.mark.asyncio
async def test_a_prepared_build_run_keeps_its_base_note_and_adds_the_line(
    service_config: ServiceConfig,
) -> None:
    async with started(service_config) as rig:
        await building(rig)
        run = await _run(rig, SessionKind.BUILD)
        directory = rig.tools.directory
        await directory.record_base_sync(run.session_id, FAST_FORWARD)
        synced = await _text(rig, run)
        await directory.record_worktree_prepare(run.session_id, {**OK, "ran": False})
        text = await _text(rig, run)
    note = d.base_note(FAST_FORWARD, SessionKind.BUILD)
    assert f"\n{note}\n{LINE}\n" in text
    assert text.replace(f"\n{LINE}", "", 1) == synced


def test_the_note_is_unchanged_without_a_current_successful_prepare() -> None:
    shown = {"worktree_prepare": OK}
    for kind in (SessionKind.PLAN, SessionKind.BUILD):
        assert prepared_note("", {}, kind) == ""
        for record in (FAILED, STALE, INTERRUPTED):
            assert prepared_note("x", {"worktree_prepare": record}, kind) == "x"
        assert prepared_note("", shown, kind) == LINE
        assert prepared_note("x", shown, kind) == f"x\n{LINE}"
        assert prepared_note("x", {**shown, "workspace_kind": "reference"}, kind) == "x"
    assert prepared_note("x", shown, SessionKind.TRIAGE) == "x"


def test_every_plan_and_build_first_template_carries_the_note_on_its_own_line() -> None:
    """Rework, items-rework and conflict reworks render through the same path."""
    current = {
        d._FIRST_TEMPLATES[SessionKind.PLAN],
        d._FIRST_TEMPLATES[SessionKind.BUILD],
        d._AUTOPILOT_PLAN_TEMPLATE,
        d._REPLAN_TEMPLATE,
        d._REWORK_TEMPLATE,
        d._ITEMS_REWORK_TEMPLATE,
        d._CONFLICT_TEMPLATE,
    }
    for name in current:
        assert "\n{base_note}\n" in (TEMPLATES / name).read_text("utf-8"), name
