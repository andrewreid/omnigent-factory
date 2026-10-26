"""Stage results of a bundle issue's runs: only they may eject members or attribute
findings and splits to one, and a bundle plan names every member in its acceptance
criteria."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import omnigent_factory.core as core_pkg
from omnigent_factory.core.protocol import Correlation, ResultError, validate_result
from omnigent_factory.testing.builders import contract

PLAN = Correlation("P1", "S1", "N1", 3, "plan", issue_number=9)
BUILD = Correlation("P1", "S1", "N1", 3, "build", issue_number=9)
MEMBERS = frozenset({12, 13})


def bundle(corr: Correlation) -> Correlation:
    from dataclasses import replace

    return replace(corr, bundle_members=MEMBERS)


def plan(*criteria: str, **over: object) -> dict:
    c = contract()
    c["acceptance_criteria"] = [
        {"id": f"ac{i}", "criterion": text, "verification": "test"}
        for i, text in enumerate(criteria, start=1)
    ]
    return {
        "kind": "plan",
        "publication_kind": "contract",
        "approach": "one PR",
        "risks": [],
        "contract": c,
        "open_decision_ids": [],
        **over,
    }


def blocked(**over: object) -> dict:
    return {"kind": "blocked", "reason": "r", "done": [], **over}


def test_a_bundle_plan_names_every_member_in_its_criteria():
    validate_result(plan("#12: the export works", "#13: the import works"), bundle(PLAN))
    with pytest.raises(ResultError, match="none name #13"):
        validate_result(plan("#12: the export works", "nothing else"), bundle(PLAN))


def test_an_ejected_member_needs_no_criteria():
    validate_result(
        plan("#12: the export works", ejected=[{"issue": 13, "reason": "too big"}]),
        bundle(PLAN),
    )


def test_only_a_bundle_run_ejects_or_attributes_members():
    with pytest.raises(ResultError, match="only a bundle issue's run"):
        validate_result(blocked(ejected=[{"issue": 12, "reason": "too big"}]), BUILD)
    item = {"title": "Later", "body": "b", "reason": "owner said so", "member": 12}
    with pytest.raises(ResultError, match="only a bundle issue's run"):
        validate_result(blocked(split_out=[item]), BUILD)
    validate_result(blocked(split_out=[item]), bundle(BUILD))


def test_ejected_and_member_name_members_only_once():
    with pytest.raises(ResultError, match="not members of this bundle: #14"):
        validate_result(blocked(ejected=[{"issue": 14, "reason": "x"}]), bundle(BUILD))
    deferred = {"severity": "P2", "title": "t", "reason": "r", "member": 15}
    with pytest.raises(ResultError, match="#15"):
        validate_result(blocked(deferred=[deferred]), bundle(BUILD))
    twice = [{"issue": 12, "reason": "x"}, {"issue": 12, "reason": "y"}]
    with pytest.raises(ResultError, match="each member at most once"):
        validate_result(blocked(ejected=twice), bundle(BUILD))


def test_schema_file_declares_ejected_and_member():
    schema = json.loads(
        (Path(core_pkg.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    defs = schema["$defs"]
    assert defs["ejected"]["required"] == ["issue", "reason"]
    for kind in ("plan", "build", "blocked"):
        assert "ejected" in defs[kind]["properties"]
        assert "ejected" not in defs[kind]["required"]
    assert "member" in defs["split_out"]["properties"]
    assert "member" in defs["deferred"]["properties"]
