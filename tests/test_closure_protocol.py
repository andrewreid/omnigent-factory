"""Triage closure outcomes, closure_evidence and paths in result schema v1."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import omnigent_factory.core as core_pkg
from omnigent_factory.core.protocol import (
    CLOSE_CANDIDATE_LABEL,
    CLOSURE_RECOMMENDATIONS,
    RESULT_SHAPES,
    Correlation,
    ResultError,
    validate_result,
)
from tests.test_protocol import triage_result

CORR = Correlation("P1", "S1", "N1", 3, "triage", issue_number=40)


def test_old_triage_results_still_validate_under_v1():
    parsed = validate_result(triage_result(), CORR)
    assert parsed.result.version == 1
    body = parsed.result.result
    assert body.closure_evidence is None and body.paths == []  # type: ignore[union-attr]
    for recommendation in ("wont_fix", "needs_info"):
        assert validate_result(triage_result(recommendation=recommendation), CORR)
    assert validate_result(triage_result(recommendation="duplicate", duplicate_issue=5), CORR)


@pytest.mark.parametrize("recommendation", ["already_fixed", "obsolete"])
def test_new_closure_outcomes_require_closure_evidence(recommendation):
    with pytest.raises(ResultError, match=f"closure_evidence is required .* {recommendation}"):
        validate_result(triage_result(recommendation=recommendation), CORR)
    evidence = "Commit abc1234 added the guard in src/api/routes.py (see #12)."
    parsed = validate_result(
        triage_result(recommendation=recommendation, closure_evidence=evidence), CORR
    )
    assert parsed.result.result.closure_evidence == evidence  # type: ignore[union-attr]
    # Still no duplicate_issue outside a duplicate.
    with pytest.raises(ResultError, match="duplicate_issue"):
        validate_result(
            triage_result(
                recommendation=recommendation, closure_evidence=evidence, duplicate_issue=5
            ),
            CORR,
        )


def test_closure_evidence_is_optional_elsewhere_and_bounded():
    assert validate_result(triage_result(recommendation="wont_fix", closure_evidence="x"), CORR)
    assert validate_result(
        triage_result(recommendation="duplicate", duplicate_issue=5, closure_evidence="#5"), CORR
    )
    assert validate_result(triage_result(closure_evidence=None), CORR)
    for bad in ("", "x" * 2001, 7):
        with pytest.raises(ResultError, match="closure_evidence"):
            validate_result(triage_result(recommendation="obsolete", closure_evidence=bad), CORR)


def test_superseded_is_obsolete_with_a_supersedes_relation():
    related = [{"issue": 12, "relation": "supersedes", "note": "#12 replaces this design"}]
    parsed = validate_result(
        triage_result(recommendation="obsolete", closure_evidence="#12", related=related), CORR
    )
    assert parsed.result.result.related[0].relation == "supersedes"  # type: ignore[union-attr]


def test_paths_are_optional_unique_and_bounded():
    paths = ["src/api/routes.py", "tests/test_routes.py"]
    parsed = validate_result(triage_result(paths=paths), CORR)
    assert parsed.result.result.paths == paths  # type: ignore[union-attr]
    assert validate_result(triage_result(paths=[f"f{n}.py" for n in range(20)]), CORR)
    for bad, message in (
        ([f"f{n}.py" for n in range(21)], "paths"),
        ([""], "paths"),
        (["x" * 301], "paths"),
        (["a.py", "a.py"], "paths must be unique"),
        ("src/a.py", "paths"),
    ):
        with pytest.raises(ResultError, match=message):
            validate_result(triage_result(paths=bad), CORR)


def test_closure_set_and_label():
    assert {"duplicate", "wont_fix", "already_fixed", "obsolete"} == CLOSURE_RECOMMENDATIONS
    assert CLOSE_CANDIDATE_LABEL == "factory:close-candidate"
    # The agent never sets factory labels itself; the factory applies this one.
    with pytest.raises(ResultError, match="control labels"):
        validate_result(triage_result(labels=[CLOSE_CANDIDATE_LABEL]), CORR)


def test_schema_file_and_shape_declare_the_new_fields():
    schema = json.loads(
        (Path(core_pkg.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    assert schema["properties"]["version"] == {"const": 1}
    triage = schema["$defs"]["triage"]
    assert triage["properties"]["recommendation"]["enum"] == [
        "fix",
        "wont_fix",
        "duplicate",
        "needs_info",
        "already_fixed",
        "obsolete",
    ]
    for name in ("closure_evidence", "paths"):
        assert name in triage["properties"] and name not in triage["required"]
    assert triage["properties"]["paths"]["maxItems"] == 20
    assert triage["properties"]["paths"]["items"]["maxLength"] == 300
    evidence = triage["properties"]["closure_evidence"]["anyOf"][0]
    assert evidence == {"type": "string", "minLength": 1, "maxLength": 2000}
    shape = RESULT_SHAPES["triage"]
    assert "already_fixed|obsolete" in shape and '"closure_evidence"?' in shape
    assert '"paths"?' in shape
