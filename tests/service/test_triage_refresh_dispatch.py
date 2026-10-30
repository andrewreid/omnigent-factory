"""What a triage was made against (freshness), the refresh run's dispatch (template and
brief), the triage prompt families and the status comment's triage header."""

from __future__ import annotations

import hashlib
import json
from importlib.resources import files
from pathlib import Path
from typing import Any

import pytest

from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.types import BotState, SessionKind
from omnigent_factory.service import directory as module
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import (
    ServiceDispatchDirectory,
    TriageFreshness,
    triage_base_oid,
)
from omnigent_factory.service.status_comment import IssueView, render_issue
from tests.omnigent.support import intent
from tests.service.test_auto_retriage import finish_again, retriage_rig
from tests.service.test_auto_triage import triage_running
from tests.service.test_pilot_677 import eventually

TEMPLATES = files("omnigent_factory.service") / "templates"
OID = "a" * 40


def _write_result(config: ServiceConfig, session_id: str, result: dict[str, Any]) -> None:
    results = config.state_dir / "results"
    results.mkdir(mode=0o700, exist_ok=True)
    payload = {"item_id": "i", "factory_result": {"result": result}, "contract_canonical": None}
    (results / f"{session_id}-latest.json").write_text(json.dumps(payload))


def _receipt(store: Any, *, run_id: str, kind: str, at_us: int) -> None:
    with store._txn() as conn:
        conn.execute(
            "INSERT INTO mcp_receipts (receipt_key, parcel_id, run_id, tool, event_id, "
            "request_sha256, receipt_json, created_at_us) VALUES (?, ?, ?, 'submit', ?, ?, ?, ?)",
            (
                f"{run_id}:{kind}:{at_us}",
                "P",
                run_id,
                f"e{at_us}",
                "0" * 64,
                json.dumps({"kind": kind}),
                at_us,
            ),
        )


TRIAGE = {
    "kind": "triage",
    "summary": "s",
    "priority": "P2",
    "size": "S",
    "recommendation": "obsolete",
    "closure_evidence": "the module was removed",
    "duplicate_issue": None,
    "labels": [],
    "missing_information": [],
    "related": [{"issue": 12, "relation": "supersedes", "note": "n"}],
    "paths": ["src/a.py", "docs"],
}


def test_triage_base_oid_prefers_a_synced_head_then_the_create_oid():
    assert triage_base_oid(None) == ""
    assert triage_base_oid({"base_oid": OID}) == OID
    synced = {"status": "fast_forwarded", "new_oid": "b" * 40}
    assert triage_base_oid({"base_oid": OID, "base_sync": synced}) == "b" * 40
    up = {"status": "up_to_date", "new_oid": "c" * 40}
    assert triage_base_oid({"base_sync": up}) == "c" * 40
    own = {"status": "own_commits", "new_oid": "d" * 40}  # not on the base: not used
    assert triage_base_oid({"base_oid": OID, "base_sync": own}) == OID
    assert triage_base_oid({"base_sync": own}) == ""


@pytest.mark.asyncio
async def test_triage_freshness_reads_the_newest_accepted_triage(service_config: ServiceConfig):
    async with retriage_rig(service_config) as t:
        pid = await t.triaged(3)
        parcel = await t.parcel(pid)
        directory = ServiceDispatchDirectory(t.r.service.db, service_config)
        assert await directory.triage_freshness(parcel) is None  # no stored result yet
        [run] = [s for s in parcel.sessions if s.kind == SessionKind.TRIAGE]
        _write_result(service_config, run.session_id, TRIAGE)
        (directory.root).mkdir(parents=True, exist_ok=True)
        (directory.root / f"{run.session_id}.json").write_text(json.dumps({"base_oid": OID}))
        await t.r.service.db.call(
            lambda store: _receipt(store, run_id=run.session_id, kind="blocked", at_us=5)
        )
        await t.r.service.db.call(
            lambda store: _receipt(store, run_id=run.session_id, kind="triage", at_us=7)
        )
        fresh = await directory.triage_freshness(parcel)
        assert fresh == TriageFreshness(
            session_id=run.session_id,
            accepted_at_us=7,
            base_oid=OID,
            recommendation="obsolete",
            duplicate_issue=None,
            related=(12,),
            paths=("src/a.py", "docs"),
        )
        # An epic's triage is out of scope.
        _write_result(service_config, run.session_id, {"kind": "epic_triage", "summary": "s"})
        assert await directory.triage_freshness(parcel) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("lazy", [False, True])
async def test_a_refresh_run_pins_the_refresh_template_with_its_signals(
    service_config: ServiceConfig, lazy: bool
):
    config = service_config.model_copy(update={"lazy_issue_worktrees": lazy})
    async with retriage_rig(config, auto_retriage=True) as t:
        t.workspaces.changed = ("src/a.py",)
        pid = await t.triaged(3, paths=("src/a.py",))
        first = (await t.parcel(pid)).sessions[-1]
        _write_result(config, first.session_id, TRIAGE)
        assert await t.retriager.run_once() == "started #3"
        await triage_running(t.r, pid)
        run = (await t.parcel(pid)).current_session
        assert run is not None and run.session_id != first.session_id
        directory = ServiceDispatchDirectory(t.r.service.db, config)
        assert await directory.retriage_signals(
            (await t.parcel(pid)).authorization(run.authorization_id).source_event_id  # type: ignore[union-attr]
        ) == ("cited paths changed: src/a.py", "triaged 40 days ago")
        await directory.stage_spec(run.session_id)
        snapshot = directory.dispatch_snapshot(run.session_id)
        assert snapshot is not None
        name = "triage-refresh-v2.txt" if lazy else "triage-refresh-v1.txt"
        assert snapshot["template"] == name
        assert snapshot["brief"] == "- cited paths changed: src/a.py\n- triaged 40 days ago"
        assert snapshot["triage_comment_template"] == (
            "triage-comment-v5.txt" if lazy else "triage-comment-v4.txt"
        )
        text = await directory.message_text(intent(EffectKind.SEND_MESSAGE, run.session_id))
        assert text is not None
        assert "- cited paths changed: src/a.py" in text
        assert f'"{config.wrapper_bin_dir / "gh"}"' in text
        assert str(config.capability_dir / f"{run.session_id}.cap") in text
        assert ("shared read-only reference snapshot" in text) is lazy
        feedback = await directory.message_text(
            intent(EffectKind.SEND_MESSAGE, run.session_id, purpose="feedback")
        )
        assert feedback is not None and "closure_evidence" in feedback
        await finish_again(t.r, pid, "refresh")


@pytest.mark.asyncio
async def test_an_owner_retriage_keeps_the_feedback_template(service_config: ServiceConfig):
    from omnigent_factory.core import events as ev
    from omnigent_factory.core.types import Stage
    from omnigent_factory.testing.builders import EventFactory, snapshot

    async with retriage_rig(service_config) as t:
        pid = await t.triaged(3)
        first = (await t.parcel(pid)).sessions[-1]
        _write_result(service_config, first.session_id, TRIAGE)
        factory = EventFactory(pid, issue_number=3, start_us=t.r.clock.now_utc_us())
        applied = await t.r.service.apply_event(
            factory.make(
                ev.PlanFeedback(text_digest="d" * 64),  # an owner comment on the triage
                event_id=f"{pid}:owner-triage",
                evidence=snapshot(stage=Stage.TRIAGED, read_at_us=factory.now),
            )
        )
        assert applied.accepted, applied.reason
        result = (await t.parcel(pid)).authorizations[-1]
        assert result.source_event_id == f"{pid}:owner-triage"  # the owner's run

        async def new_run() -> bool:
            cur = (await t.parcel(pid)).current_session
            return cur is not None and cur.session_id != first.session_id

        await eventually(new_run)
        run = (await t.parcel(pid)).current_session
        assert run is not None
        directory = ServiceDispatchDirectory(t.r.service.db, service_config)
        await directory.stage_spec(run.session_id)
        snapshot = directory.dispatch_snapshot(run.session_id)
        assert snapshot is not None and snapshot["template"] == "triage-feedback-v10.txt"
        assert snapshot["brief"] == ""


# ------------------------------------------------------------------ prompt families

NEW = {
    # issue-worktree (flag off) -> reference snapshot variant
    "triage-v13.txt": "triage-v14.txt",
    "triage-feedback-v10.txt": "triage-feedback-v11.txt",
    "triage-comment-v4.txt": "triage-comment-v5.txt",
    "triage-refresh-v1.txt": "triage-refresh-v2.txt",
}
OLD = (
    "triage-v11.txt",
    "triage-v12.txt",
    "triage-feedback-v8.txt",
    "triage-feedback-v9.txt",
    "triage-comment-v2.txt",
    "triage-comment-v3.txt",
)


def test_constants_point_at_the_new_families_and_old_files_are_kept():
    assert module._FIRST_TEMPLATES[SessionKind.TRIAGE] == "triage-v13.txt"
    assert module._RETRIAGE_TEMPLATE == "triage-feedback-v10.txt"
    assert module._TRIAGE_COMMENT_TEMPLATE == "triage-comment-v4.txt"
    assert module._REFRESH_TEMPLATE == "triage-refresh-v1.txt"
    for issue_name, reference in NEW.items():
        assert module._REFERENCE_TEMPLATES[issue_name] == reference
    for name in OLD:
        assert (TEMPLATES / name).is_file(), name  # in-flight dispatches stay pinned


@pytest.mark.parametrize("name", [*NEW, *NEW.values()])
def test_new_triage_prompts_explain_closure_outcomes_paths_and_the_gh_wrapper(name: str):
    text = (TEMPLATES / name).read_text("utf-8")
    flat = " ".join(text.split())
    for word in ("already_fixed", "obsolete", "closure_evidence", "paths"):
        assert word in flat, (name, word)
    assert "never close an issue" in flat
    assert "factory gh wrapper" in flat or "factory wrapper" in flat
    if "comment" not in name or name == "triage-comment-v4.txt":
        assert '"{gh_wrapper}"' in text and "{capability_file}" in text
    if name.startswith(("triage-v", "triage-feedback", "triage-refresh")):
        assert "factory:close-candidate" in flat and "supersedes" in flat
        assert "always fill eli5" in flat
    if name.startswith("triage-refresh"):
        assert "{brief}" in text and "{base_note}" in text
    assert ("{reference_note}" in text) == (
        name in NEW.values() and name not in ("triage-comment-v5.txt",)
    )


def test_render_values_match_build_and_fill_every_placeholder(tmp_path: Path):
    import string

    values = {
        "repository": "o/r",
        "issue_number": 1,
        "run_id": "ss",
        "session_id": "root",
        "revision": 1,
        "granted_minutes": 5,
        "handoff": "",
        "base_note": "",
        "brief": "- x",
        "reference_note": "",
        "gh_wrapper": tmp_path / "gh",
        "capability_file": tmp_path / "ss.cap",
        "base_ref": "main",
    }
    build = (TEMPLATES / "build-v19.txt").read_text("utf-8")
    assert "{gh_wrapper}" in build and "{capability_file}" in build  # the same names
    for name in [*NEW, *NEW.values()]:
        text = (TEMPLATES / name).read_text("utf-8")
        fields = {f for _, f, _, _ in string.Formatter().parse(text) if f}
        assert fields <= set(values), (name, fields - set(values))
        text.format(**values)


def test_template_bytes_hash_is_pinned_per_dispatch():
    for name in NEW:
        data = (TEMPLATES / name).read_text("utf-8")
        assert (
            hashlib.sha256(module._template(name).encode()).hexdigest()
            == hashlib.sha256(data.encode()).hexdigest()
        )


# ------------------------------------------------------------------ status header


def test_status_comment_triage_section_starts_with_the_freshness_line():
    view = IssueView(
        issue=3,
        stage="Triaged",
        bot=BotState.IDLE,
        triage="**Recommendation:** obsolete",
        triage_header="Triaged 2026-10-01 against main@aaaaaaa · 52 commits behind",
    )
    body = render_issue(view, (), "Triaged · idle")
    assert (
        "#### Triage\n\n_Triaged 2026-10-01 against main@aaaaaaa · 52 commits behind_\n\n"
        "**Recommendation:** obsolete"
    ) in body
    plain = render_issue(
        IssueView(issue=3, stage="Triaged", bot=BotState.IDLE, triage="x"), (), "h"
    )
    assert "#### Triage\n\nx" in plain
