"""Status columns are keyed by option ID; display names come from host config."""

from __future__ import annotations

import tomllib
from typing import Any

import pytest
from pydantic import ValidationError

from omnigent_factory.core.types import Stage
from omnigent_factory.github.setup import render_setup
from omnigent_factory.ports.github import STATUS_OPTION_IDS
from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig
from omnigent_factory.service.doctor import DoctorReport, _check_project

RENAMED = {"Triaged": "Triage", "Scoped": "Planning"}


def _config(**overrides: Any) -> ServiceConfig:
    return ServiceConfig(repo_id="R", owners=frozenset({1}), **overrides)


class _Board:
    """A project whose Status options carry the given live names for the live IDs."""

    def __init__(self, config: ServiceConfig, status: dict[str, str]) -> None:
        self.config = config
        self.status = status  # option id -> live name

    async def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        del query, variables
        c = self.config
        fields = [
            {
                "id": c.status_field_node_id,
                "name": "Status",
                "options": [{"id": i, "name": n} for i, n in self.status.items()],
            },
            {
                "id": c.bot_field_node_id,
                "name": "Bot",
                "options": [{"id": i, "name": n} for n, i in c.bot_options.items()],
            },
            {"id": c.note_field_node_id, "name": "Factory note", "dataType": "TEXT"},
        ]
        return {"node": {"id": c.project_node_id, "fields": {"nodes": fields}}}


def _live(names: dict[str, str] | None = None) -> dict[str, str]:
    names = names or {}
    return {i: names.get(stage.value, stage.value) for stage, i in STATUS_OPTION_IDS.items()}


async def _doctor(config: ServiceConfig, status: dict[str, str]) -> DoctorReport:
    report = DoctorReport()
    await _check_project(config, _Board(config, status), report)  # type: ignore[arg-type]
    return report


def test_status_names_default_to_the_stage_names_and_merge_partial_overrides():
    assert _config().status_names == {stage.value: stage.value for stage in STATUS_OPTION_IDS}
    names = _config(status_names=RENAMED).status_names
    assert names["Triaged"] == "Triage" and names["Scoped"] == "Planning"
    assert names["Inbox"] == "Inbox" and set(names) == {s.value for s in STATUS_OPTION_IDS}


@pytest.mark.parametrize(
    "bad",
    [{"Triage": "Triage"}, {"Triaged": " "}, {"Triaged": "Inbox"}],
    ids=["unknown-stage", "blank", "duplicate"],
)
def test_status_names_reject_unknown_blank_or_duplicate(bad: dict[str, str]):
    with pytest.raises(ValidationError):
        _config(status_names=bad)


def test_status_names_are_hot_reloadable():
    assert "status_names" in HOT_RELOAD_KEYS


def test_status_names_load_from_the_host_config_section():
    raw = tomllib.loads(
        'repo_id = "R"\nowners = [1]\n[status_names]\nTriaged = "Triage"\nScoped = "Planning"\n'
    )
    assert ServiceConfig.model_validate(raw).status_names["Scoped"] == "Planning"


@pytest.mark.asyncio
async def test_doctor_passes_when_configured_names_match_the_live_board():
    report = await _doctor(_config(status_names=RENAMED), _live(RENAMED))
    assert report.ok and not report.warnings, report.errors
    assert "Triage" in report.checks["status_names"]
    assert report.checks["github_project"] == "project and live field/option IDs match"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("configured", "live"),
    [({}, RENAMED), (RENAMED, {})],
    ids=["board-renamed-first", "config-renamed-first"],
)
async def test_doctor_warns_but_passes_on_a_name_mismatch(
    configured: dict[str, str], live: dict[str, str]
):
    report = await _doctor(_config(status_names=configured), _live(live))
    assert report.ok, report.errors
    assert report.checks["github_project"] == "project and live field/option IDs match"
    [warning] = report.warnings
    assert warning.startswith("status_names: Triaged option 43889573 is ")
    assert "Scoped option 3a7f779a" in warning


@pytest.mark.asyncio
async def test_doctor_fails_when_a_configured_option_id_is_missing_live():
    status = _live()
    del status["43889573"]
    status["deadbeef"] = "Triaged"
    report = await _doctor(_config(), status)
    assert not report.ok
    assert report.errors == [
        "github_project: Status option IDs differ from configuration "
        "(missing ['43889573'], unexpected ['deadbeef'])"
    ]


def test_setup_renders_configured_names_and_preserves_every_option_id():
    names = {stage: RENAMED.get(stage.value, stage.value) for stage in STATUS_OPTION_IDS}
    bundle = render_setup(status_names=names)
    options = bundle.project_migration["update_status"]["input"]["singleSelectOptions"]
    assert [(o["id"], o["name"]) for o in options] == [
        (STATUS_OPTION_IDS[stage], names[stage]) for stage in STATUS_OPTION_IDS
    ]
    assert names[Stage.TRIAGED] == "Triage"
    assert options[1]["color"] == "BLUE" and options[2]["color"] == "PURPLE"
