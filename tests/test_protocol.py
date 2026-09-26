"""Frozen result protocol v1 parsing and cross-record constraints (§7.1-§7.2)."""

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
    extract_result_block,
    parse_factory_result,
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


def message(obj: object, fence: str = "```") -> str:
    return f"Some prose.\n\nFACTORY_RESULT_V1\n{fence}factory-result\n{json.dumps(obj)}\n{fence}\n"


def test_schema_file_is_the_architecture_schema():
    schema = json.loads(
        (Path(core_pkg.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    assert schema["$id"] == "urn:omnigent-factory:result:v1"
    assert set(schema["$defs"]) >= {"triage", "plan", "build", "checkpoint", "contract"}


@pytest.mark.parametrize("fence", ["```", "~~~"])
def test_plan_result_parses_and_canonicalizes(fence):
    parsed = parse_factory_result(message(envelope(plan_result()), fence), CORR)
    assert parsed.contract_canonical is not None
    import hashlib

    assert hashlib.sha256(parsed.contract_canonical).hexdigest() == contract_digest(contract())


def test_quoted_and_duplicate_blocks_are_ignored_or_rejected():
    obj = envelope(plan_result())
    quoted = "> FACTORY_RESULT_V1\n> ```factory-result\n> {}\n> ```\n" + message(obj)
    assert parse_factory_result(quoted, CORR).result.revision == 3
    with pytest.raises(ResultError):
        extract_result_block(message(obj) + message(obj))
    with pytest.raises(ResultError):
        extract_result_block("```factory-result\n{}\n```")
    with pytest.raises(ResultError):
        extract_result_block("FACTORY_RESULT_V1\ntext\n```factory-result\n{}\n```")
    with pytest.raises(ResultError):
        extract_result_block("FACTORY_RESULT_V1\n```factory-result\n{}\n")


@pytest.mark.parametrize(
    ("text", "why"),
    [
        ('{"version": 1, "version": 1}', "duplicate key"),
        ('{"version": NaN}', "nan"),
    ],
)
def test_strict_json(text, why):
    msg = f"FACTORY_RESULT_V1\n```factory-result\n{text}\n```"
    with pytest.raises(ResultError):
        parse_factory_result(msg, CORR)
    assert why


@pytest.mark.parametrize(
    "obj",
    [
        envelope(plan_result(), extra=1),
        envelope(plan_result(extra=1)),
        envelope(plan_result(), version=2),
        envelope(plan_result(), revision=True),
        envelope(plan_result(), revision=-1),
        envelope({**plan_result(), "kind": "unknown"}),
        envelope(plan_result(contract={**contract(), "size": "XL"})),
    ],
)
def test_schema_violations(obj):
    with pytest.raises(ResultError):
        parse_factory_result(message(obj), CORR)


def test_oversized_result_rejected():
    big = plan_result(approach="x" * 16000, risks=["y" * 16000] * 9)
    with pytest.raises(ResultError):
        parse_factory_result(message(envelope(big)), CORR)
    assert MAX_RESULT_BYTES == 131072


def test_correlation_must_match():
    for field, value in [("parcel_id", "P2"), ("dispatch_nonce", "N2"), ("revision", 2)]:
        with pytest.raises(ResultError):
            parse_factory_result(message(envelope(plan_result(), **{field: value})), CORR)


def test_plan_stage_requires_contract_publication():
    with pytest.raises(ResultError):
        parse_factory_result(message(envelope(plan_result(publication_kind="info"))), CORR)


def test_build_info_plan_only_under_waiver():
    info = envelope(plan_result(publication_kind="info"))
    waiver = Correlation("P1", "S1", "N1", 3, "build", waiver_build=True)
    assert parse_factory_result(message(info), waiver).result.result.kind == "plan"
    with pytest.raises(ResultError):
        parse_factory_result(message(info), Correlation("P1", "S1", "N1", 3, "build"))


def test_triage_constraints():
    corr = Correlation("P1", "S1", "N1", 3, "triage")
    assert parse_factory_result(message(envelope(triage_result())), corr)
    for bad in (
        triage_result(recommendation="duplicate"),
        triage_result(duplicate_issue=5),
        triage_result(labels=["factory:build"]),
        triage_result(labels=["a", "a"]),
    ):
        with pytest.raises(ResultError):
            parse_factory_result(message(envelope(bad)), corr)
    ok_dup = triage_result(recommendation="duplicate", duplicate_issue=5)
    assert parse_factory_result(message(envelope(ok_dup)), corr)


def test_build_constraints():
    corr = Correlation("P1", "S1", "N1", 3, "build")
    assert parse_factory_result(message(envelope(build_result())), corr)
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
            parse_factory_result(message(envelope(bad)), corr)
    with pytest.raises(ResultError):
        parse_factory_result(message(envelope(build_result())), CORR)


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
        parse_factory_result(message(envelope(cp)), Correlation("P1", "S1", "N1", 3, "build"))
    corr = Correlation("P1", "S1", "N1", 3, "build", in_checkpoint=True)
    assert parse_factory_result(message(envelope(cp)), corr).contract_canonical is None
