"""Result schema v1 validation and cross-record constraints (§7.2)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import omnigent_factory.core as core_pkg
from omnigent_factory.core.canonical import contract_digest
from omnigent_factory.core.protocol import (
    MAX_RESULT_BYTES,
    Correlation,
    ResultError,
    validate_result,
)
from omnigent_factory.testing.builders import contract

CORR = Correlation("P1", "S1", "N1", 3, "plan")
SHA = "b" * 40


def envelope(result: dict, **over) -> dict:
    return {
        "version": 1,
        "parcel_id": "P1",
        "stage_session_id": "S1",
        "dispatch_nonce": "N1",
        "revision": 3,
        "result": result,
        **over,
    }


def plan_result(**over) -> dict:
    return {
        "kind": "plan",
        "publication_kind": "contract",
        "approach": "do it",
        "risks": [],
        "contract": contract(),
        "open_decision_ids": [],
        **over,
    }


def triage_result(**over) -> dict:
    return {
        "kind": "triage",
        "summary": "s",
        "priority": "P2",
        "size": "S",
        "recommendation": "fix",
        "duplicate_issue": None,
        "labels": ["area:api"],
        "missing_information": [],
        **over,
    }


def build_result(**over) -> dict:
    return {
        "kind": "build_ready",
        "pr_number": 7,
        "branch": "factory/issue-1",
        "head_sha": SHA,
        "summary": "done",
        "verification": [
            {"command": "pytest", "file_set": ["tests"], "outcome": "passed", "evidence": "ok"}
        ],
        "review": {
            "implementation_vendor": "anthropic",
            "review_vendor": "openai",
            "reviewed_head": SHA,
            "artifact_reference": "r.md",
            "artifact_sha256": "c" * 64,
            "accepted": True,
        },
        "findings": [],
        "remediation_batches_used": 0,
        "targeted_rechecks_used": 0,
        "release_readiness": "ready",
        **over,
    }


def test_schema_file_is_the_architecture_schema():
    schema = json.loads(
        (Path(core_pkg.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    assert schema["$id"] == "urn:omnigent-factory:result:v1"
    assert set(schema["$defs"]) >= {"triage", "plan", "build", "checkpoint", "contract"}


def test_plan_result_validates_and_canonicalizes():
    parsed = validate_result(plan_result(), CORR)
    assert parsed.contract_canonical is not None
    import hashlib

    assert hashlib.sha256(parsed.contract_canonical).hexdigest() == contract_digest(contract())
    # The envelope comes from the calling run, never from the agent.
    assert parsed.result.model_dump(exclude={"result"}) == {
        k: v for k, v in envelope({}).items() if k != "result"
    }


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_strict_json(value):
    with pytest.raises(ResultError):
        validate_result(plan_result(risks=[value]), CORR)


@pytest.mark.parametrize(
    "obj",
    [
        plan_result(extra=1),
        {**plan_result(), "kind": "unknown"},
        plan_result(contract={**contract(), "size": "XL"}),
        plan_result(risks="not a list"),
        [],
    ],
)
def test_schema_violations(obj):
    with pytest.raises(ResultError) as info:
        validate_result(obj, CORR)
    assert info.value.details


def test_oversized_result_rejected():
    big = plan_result(approach="x" * 16000, risks=["y" * 16000] * 9)
    with pytest.raises(ResultError):
        validate_result(big, CORR)
    assert MAX_RESULT_BYTES == 131072


def test_plan_stage_requires_contract_publication():
    with pytest.raises(ResultError):
        validate_result(plan_result(publication_kind="info"), CORR)


def test_build_info_plan_only_under_waiver():
    info = plan_result(publication_kind="info")
    waiver = Correlation("P1", "S1", "N1", 3, "build", waiver_build=True)
    assert validate_result(info, waiver).result.result.kind == "plan"
    with pytest.raises(ResultError):
        validate_result(info, Correlation("P1", "S1", "N1", 3, "build"))


def test_triage_constraints():
    corr = Correlation("P1", "S1", "N1", 3, "triage")
    assert validate_result(triage_result(), corr)
    for bad in (
        triage_result(recommendation="duplicate"),
        triage_result(duplicate_issue=5),
        triage_result(labels=["factory:build"]),
        triage_result(labels=["a", "a"]),
    ):
        with pytest.raises(ResultError):
            validate_result(bad, corr)
    ok_dup = triage_result(recommendation="duplicate", duplicate_issue=5)
    assert validate_result(ok_dup, corr)


def test_build_constraints():
    corr = Correlation("P1", "S1", "N1", 3, "build")
    assert validate_result(build_result(), corr)
    finding = {
        "id": "F",
        "source": "bot",
        "severity": "ADVISORY",
        "disposition": "advisory",
        "evidence": "e",
    }
    for bad in (
        build_result(findings=[finding, finding]),
        build_result(remediation_batches_used=2),
        build_result(head_sha="xyz"),
        build_result(review={**build_result()["review"], "review_vendor": "anthropic"}),
    ):
        with pytest.raises(ResultError):
            validate_result(bad, corr)
    with pytest.raises(ResultError):
        validate_result(build_result(), CORR)

    unresolved = {
        "id": "F2",
        "source": "bot",
        "severity": "SHOULD_FIX",
        "disposition": "unresolved",
        "evidence": "not handled",
    }
    with pytest.raises(ResultError, match="all be dispositioned"):
        validate_result(build_result(findings=[unresolved]), corr)


def test_checkpoint_only_inside_checkpoint():
    cp = {
        "kind": "checkpoint",
        "grant_id": "g",
        "head_sha": None,
        "done": [],
        "remaining": ["x"],
        "risks": [],
        "worktree_state": "clean",
        "elicitation_id": None,
    }
    with pytest.raises(ResultError):
        validate_result(cp, Correlation("P1", "S1", "N1", 3, "build"))
    corr = Correlation("P1", "S1", "N1", 3, "build", in_checkpoint=True)
    assert validate_result(cp, corr).contract_canonical is None


# ---------------------------------------------------------------- related (cross-issue)


def triage_corr() -> Correlation:
    return Correlation("P1", "S1", "N1", 3, "triage", issue_number=40)


def test_triage_related_is_optional_and_validated():
    assert validate_result(triage_result(), triage_corr()).result.result.related == []  # type: ignore[union-attr]
    related = [
        {"issue": 12, "relation": "overlaps", "note": "split: #12 keeps the API, this the UI"},
        {"issue": 9, "relation": "conflicts", "note": "#9 (Building) removes the endpoint"},
    ]
    parsed = validate_result(triage_result(related=related), triage_corr())
    body = parsed.result.result
    assert [(r.issue, r.relation) for r in body.related] == [(12, "overlaps"), (9, "conflicts")]  # type: ignore[union-attr]


@pytest.mark.parametrize(
    ("related", "message"),
    [
        ([{"issue": 12, "relation": "similar", "note": "x"}], "relation"),
        ([{"issue": 0, "relation": "overlaps", "note": "x"}], "issue"),
        ([{"issue": 12, "relation": "overlaps", "note": ""}], "note"),
        ([{"issue": 12, "relation": "overlaps", "note": "x" * 301}], "note"),
        ([{"issue": 12, "relation": "overlaps"}], "note"),
        ([{"issue": 12, "relation": "overlaps", "note": "x", "id": 1}], "id"),
        (
            [
                {"issue": 12, "relation": "overlaps", "note": "x"},
                {"issue": 12, "relation": "blocks", "note": "y"},
            ],
            "each issue at most once",
        ),
        ([{"issue": 40, "relation": "duplicate", "note": "x"}], "itself"),
        ([{"issue": n, "relation": "overlaps", "note": "x"} for n in range(1, 23)], "related"),
    ],
)
def test_invalid_related_is_refused_at_submit(related, message):
    valid = [{"issue": 12, "relation": "overlaps", "note": "x"}]
    validate_result(triage_result(related=valid), triage_corr())  # the field itself is fine
    with pytest.raises(ResultError) as error:
        validate_result(triage_result(related=related), triage_corr())
    assert message in " ".join(error.value.details)


def test_schema_file_declares_related():
    schema = json.loads(
        (Path(core_pkg.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    triage = schema["$defs"]["triage"]
    assert "related" in triage["properties"] and "related" not in triage["required"]
    assert schema["$defs"]["related"]["properties"]["relation"]["enum"] == [
        "duplicate",
        "overlaps",
        "conflicts",
        "depends_on",
        "blocks",
        "supersedes",
    ]


REPLAN = {"reason": "the owner's direction reverses part of the plan", "changes": ["x"]}


def blocked_result(**over) -> dict:
    return {"kind": "blocked", "reason": "plan must change", "done": [], **over}


def test_build_blocked_result_may_require_a_replan():
    build = Correlation("P1", "S1", "N1", 3, "build")
    parsed = validate_result(blocked_result(replan_required=REPLAN), build)
    assert parsed.result.result.replan_required.changes == ["x"]
    assert validate_result(blocked_result(), build).result.result.replan_required is None
    for bad in ({"reason": "r", "changes": []}, {"reason": "r"}, {**REPLAN, "extra": 1}):
        with pytest.raises(ResultError):
            validate_result(blocked_result(replan_required=bad), build)


@pytest.mark.parametrize(
    "corr",
    [
        CORR,
        Correlation("P1", "S1", "N1", 3, "triage"),
        Correlation("P1", "S1", "N1", 3, "build", in_checkpoint=True),
    ],
)
def test_only_a_build_outside_a_checkpoint_requires_a_replan(corr):
    with pytest.raises(ResultError, match="replan_required: only a build run"):
        validate_result(blocked_result(replan_required=REPLAN), corr)


def test_schema_file_declares_replan_required():
    schema = json.loads(
        (Path(core_pkg.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    blocked = schema["$defs"]["blocked"]
    assert "replan_required" not in blocked["required"]
    replan = blocked["properties"]["replan_required"]
    assert replan["required"] == ["reason", "changes"] and replan["additionalProperties"] is False


@pytest.mark.parametrize("kind", ["triage", "epic_triage", "plan", "build_ready"])
def test_eli5_is_optional_and_validated_without_changing_v1(kind):
    from tests.test_epic_triage_protocol import epic_corr, epic_result

    results = {
        "triage": (triage_result(), triage_corr()),
        "epic_triage": (epic_result(), epic_corr()),
        "plan": (plan_result(), CORR),
        "build_ready": (build_result(), Correlation("P1", "S1", "N1", 3, "build")),
    }
    result, corr = results[kind]
    old = validate_result(result, corr)
    assert old.result.version == 1 and old.result.result.eli5 is None
    for value in (None, "This makes the count right. Your hours will add up."):
        parsed = validate_result({**result, "eli5": value}, corr)
        assert parsed.result.result.eli5 == value
        assert parsed.contract_canonical == old.contract_canonical
    for value in ("", "x" * 16001, 42):
        with pytest.raises(ResultError, match="eli5"):
            validate_result({**result, "eli5": value}, corr)


def test_schema_file_declares_optional_eli5_outside_contract():
    schema = json.loads(
        (Path(core_pkg.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    from omnigent_factory.core.protocol import RESULT_SHAPES

    for kind in ("triage", "epic_triage", "plan", "build"):
        model = schema["$defs"][kind]
        assert model["properties"]["eli5"] == {
            "anyOf": [{"$ref": "#/$defs/text"}, {"type": "null"}]
        }
        assert "eli5" not in model["required"]
        assert '"eli5"?:str|null' in RESULT_SHAPES["build_ready" if kind == "build" else kind]
    assert "eli5" not in schema["$defs"]["contract"]["properties"]
