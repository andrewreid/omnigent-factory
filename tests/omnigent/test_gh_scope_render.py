"""Wrapper values exist for both issue and reference triage, including feedback."""

import hashlib
from dataclasses import replace
from types import SimpleNamespace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.types import Via
from omnigent_factory.service import directory as module
from tests.omnigent.support import intent
from tests.omnigent.test_lazy_worktrees import P, setup


@pytest.mark.asyncio
@pytest.mark.parametrize("reference", [False, True])
@pytest.mark.parametrize("variant", ["triage", "retriage", "epic", "epic_plan"])
async def test_all_triage_templates_can_render_wrapper_values(
    git_env, monkeypatch, reference, variant
):
    _, directory, h = setup(git_env, monkeypatch, enabled=reference)
    if variant == "retriage":
        monkeypatch.setattr(directory, "latest_triage", lambda _: {})
    if variant in ("epic", "epic_plan"):
        h.parcels[P] = replace(
            h.p(),
            links=SimpleNamespace(epic=True),
            autopilot=SimpleNamespace() if variant == "epic_plan" else None,
        )
    # Simulate the other worker's new template wording without changing released files.
    original = module._template

    def template(name):
        return original(name) + "\nwrapper={gh_wrapper}\ncapability={capability_file}\n"

    monkeypatch.setattr(module, "_template", template)
    run = h.cur()
    await directory.stage_spec(run.session_id)
    for purpose in ("first", "feedback"):
        text = await directory.message_text(
            intent(EffectKind.SEND_MESSAGE, run.session_id, purpose=purpose)
        )
        assert f"wrapper={directory.config.wrapper_bin_dir / 'gh'}" in text
        assert f"capability={directory.config.capability_dir / (run.session_id + '.cap')}" in text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "template", ["plan-v13.txt", "plan-autopilot-v5.txt", "plan-replan-v3.txt"]
)
async def test_plan_templates_are_pinned_and_render_wrapper(git_env, monkeypatch, template):
    _, directory, h = setup(git_env, monkeypatch, triaged=True)
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    monkeypatch.setitem(module._FIRST_TEMPLATES, h.cur().kind, template)
    run = h.cur()
    await directory.stage_spec(run.session_id)
    snapshot = directory.dispatch_snapshot(run.session_id)
    assert snapshot["template"] == template
    assert (
        snapshot["template_sha256"]
        == hashlib.sha256(module._template(template).encode()).hexdigest()
    )
    text = await directory.message_text(intent(EffectKind.SEND_MESSAGE, run.session_id))
    assert str(directory.config.wrapper_bin_dir / "gh") in text
    assert str(directory.config.capability_dir / f"{run.session_id}.cap") in text
