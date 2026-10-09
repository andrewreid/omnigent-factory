"""The epic triage result: validated against the epic's sub-issues, stage and schema file."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import omnigent_factory.core as core_pkg
from omnigent_factory.core.protocol import (
    RESULT_SHAPES,
    Correlation,
    EpicTriageResult,
    ResultError,
    validate_result,
)
from tests.test_protocol import triage_result

SUBS = frozenset({822, 823, 824})


def epic_corr(subs: frozenset[int] | None = SUBS, stage: str = "triage") -> Correlation:
    return Correlation("P1", "S1", "N1", 3, stage, issue_number=821, sub_issues=subs)  # type: ignore[arg-type]


def epic_result(**over: object) -> dict:
    return {
        "kind": "epic_triage",
        "summary": "Eight sub-issues cover the worker move.",
        "coverage": {"gaps": ["no rollback plan"], "overlaps": []},
        "build_order": [{"issue": 823, "reason": "framework first"}, {"issue": 824, "reason": "x"}],
        "links": [{"issue": 824, "blocked_by": 823, "reason": "needs the framework"}],
        **over,
    }


def test_an_epic_triage_validates_without_priority_or_size():
    parsed = validate_result(epic_result(), epic_corr())
    body = parsed.result.result
    assert isinstance(body, EpicTriageResult)
    assert [(link.issue, link.blocked_by) for link in body.links] == [(824, 823)]
    assert not hasattr(body, "priority") and not hasattr(body, "size")
    assert "epic_triage" in RESULT_SHAPES


@pytest.mark.parametrize(
    ("result", "corr", "message"),
    [
        (epic_result(), epic_corr(subs=None), "only for an epic"),
        (epic_result(), epic_corr(stage="plan"), "non-triage stage"),
        (
            epic_result(build_order=[{"issue": 900, "reason": "x"}]),
            epic_corr(),
            "only this epic's sub-issues",
        ),
        (
            epic_result(links=[{"issue": 823, "blocked_by": 823, "reason": "x"}]),
            epic_corr(),
            "cannot block itself",
        ),
        (
            epic_result(build_order=[{"issue": 823, "reason": "x"}] * 2),
            epic_corr(),
            "at most once",
        ),
        (triage_result(), epic_corr(), "submit epic_triage"),
        (epic_result(priority="P1"), epic_corr(), "Extra inputs"),
    ],
)
def test_epic_triage_is_refused_where_it_does_not_fit(result, corr, message):
    with pytest.raises(ResultError) as error:
        validate_result(result, corr)
    assert message in str(error.value) + " ".join(error.value.details)


def test_an_incomplete_sub_issue_list_does_not_refuse_unknown_numbers():
    # More sub-issues than one read lists: membership cannot be judged.
    assert validate_result(
        epic_result(build_order=[{"issue": 900, "reason": "x"}]), epic_corr(frozenset())
    )


def test_schema_file_declares_the_epic_triage():
    schema = json.loads(
        (Path(core_pkg.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    epic = schema["$defs"]["epic_triage"]
    assert {"$ref": "#/$defs/epic_triage"} in schema["properties"]["result"]["oneOf"]
    assert set(epic["required"]) == {"kind", "summary", "coverage", "build_order"}
    assert "priority" not in epic["properties"] and "size" not in epic["properties"]
