"""Canonicalization and digest golden cases (§2.3)."""

from __future__ import annotations

import hashlib

import pytest
from hypothesis import given
from hypothesis import strategies as st

from omnigent_factory.core.canonical import (
    CanonicalizationError,
    canonical_bytes,
    canonical_contract,
    canonical_issue_snapshot,
    contract_digest,
    issue_snapshot_digest,
    normalize_text,
    parse_json_strict,
    prefix_collisions,
    resolve_hash,
)
from omnigent_factory.testing.builders import contract

GOLDEN_CONTRACT_BYTES = (
    b'{"acceptance_criteria":[{"criterion":"Button exports CSV","id":"AC1",'
    b'"verification":"unit test"}],"goal":"Ship the export button",'
    b'"non_goals":["PDF export"],"resolved_decisions":[],"size":"M"}'
)
GOLDEN_CONTRACT_SHA = "5c78b0f13f42f1abdf10746cdf8df2fc57187aaed7966eced4bc0a0e351eb4ee"

UNICODE_CONTRACT = {
    "goal": "Ünïcode goal  \r\n  indented\t \rnext",
    "acceptance_criteria": [
        {"id": "AC2", "criterion": "b", "verification": "v"},
        {"id": "AC1", "criterion": "a  ", "verification": "v"},
    ],
    "non_goals": [],
    "size": "S",
    "resolved_decisions": [{"decision_id": "D1", "answer": "yes", "source_event_id": "E1"}],
}
GOLDEN_UNICODE_BYTES = (
    '{"acceptance_criteria":[{"criterion":"b","id":"AC2","verification":"v"},'
    '{"criterion":"a","id":"AC1","verification":"v"}],"goal":"Ünïcode goal\\n  indented\\nnext",'
    '"non_goals":[],"resolved_decisions":[{"answer":"yes","decision_id":"D1",'
    '"source_event_id":"E1"}],"size":"S"}'
).encode()
GOLDEN_UNICODE_SHA = "c66767836085e2cb884169aa979ff8aa7ae1cb94d9cc54f8e2c342d5c6e5e0bd"

GOLDEN_WAIVER_NULL_BODY = b'{"body":"","title":"Title"}'
GOLDEN_WAIVER_NULL_SHA = "e016a293ab96071a2ae6427299b55c900e5cd604c6fe0ccb92a39efbd9afe4b0"
GOLDEN_WAIVER_CRLF_SHA = "57263100753c5fea2867956c80f323b43c2c89d9551066b76467f3635ae0cce6"


def test_contract_golden_bytes_and_digest():
    assert canonical_contract(contract()) == GOLDEN_CONTRACT_BYTES
    assert contract_digest(contract()) == GOLDEN_CONTRACT_SHA
    assert hashlib.sha256(GOLDEN_CONTRACT_BYTES).hexdigest() == GOLDEN_CONTRACT_SHA


def test_unicode_crlf_trailing_whitespace_golden():
    # CRLF/CR -> LF, trailing spaces/tabs trimmed, leading whitespace and code points kept,
    # list order retained, keys sorted.
    assert canonical_contract(UNICODE_CONTRACT) == GOLDEN_UNICODE_BYTES
    assert contract_digest(UNICODE_CONTRACT) == GOLDEN_UNICODE_SHA


def test_waiver_snapshot_goldens():
    assert canonical_issue_snapshot("Title", None) == GOLDEN_WAIVER_NULL_BODY
    assert issue_snapshot_digest("Title", None) == GOLDEN_WAIVER_NULL_SHA
    assert issue_snapshot_digest("Title", "") == GOLDEN_WAIVER_NULL_SHA
    assert issue_snapshot_digest("Title  \r\n", "line\r\nx ") == GOLDEN_WAIVER_CRLF_SHA


def test_normalize_text_rules():
    assert normalize_text("a \t\r\n b\t\rc  ") == "a\n b\nc"
    assert normalize_text("  lead") == "  lead"


def test_line_ending_variants_hash_identically():
    base = contract()
    crlf = {**base, "goal": "Ship the export button\r\n"}
    cr = {**base, "goal": "Ship the export button\r"}
    lf = {**base, "goal": "Ship the export button\n"}
    assert contract_digest(crlf) == contract_digest(cr) == contract_digest(lf)


def test_list_order_is_significant():
    reordered = {**UNICODE_CONTRACT}
    reordered["acceptance_criteria"] = list(reversed(UNICODE_CONTRACT["acceptance_criteria"]))  # type: ignore[call-overload]
    assert contract_digest(reordered) != GOLDEN_UNICODE_SHA


@pytest.mark.parametrize(
    "mutate",
    [
        lambda c: c.pop("goal"),
        lambda c: c.update(extra="x"),
        lambda c: c.update(size="XL"),
        lambda c: c.update(goal=""),
        lambda c: c.update(acceptance_criteria=[]),
        lambda c: c.update(
            acceptance_criteria=[
                {"id": "A", "criterion": "x", "verification": "y"},
                {"id": "A", "criterion": "z", "verification": "y"},
            ]
        ),
        lambda c: c.update(
            resolved_decisions=[
                {"decision_id": "D", "answer": "a", "source_event_id": "e"},
                {"decision_id": "D", "answer": "b", "source_event_id": "e"},
            ]
        ),
        lambda c: c.update(non_goals=[1]),
        lambda c: c["acceptance_criteria"][0].update(extra=1),
    ],
)
def test_contract_validation_rejects(mutate):
    c = contract()
    mutate(c)
    with pytest.raises(CanonicalizationError):
        canonical_contract(c)


def test_non_finite_and_duplicate_keys_rejected():
    with pytest.raises(CanonicalizationError):
        canonical_bytes({"x": float("nan")})
    with pytest.raises(CanonicalizationError):
        canonical_bytes({"x": float("inf")})
    with pytest.raises(CanonicalizationError):
        parse_json_strict('{"a": 1, "a": 2}')
    with pytest.raises(CanonicalizationError):
        parse_json_strict('{"a": NaN}')
    with pytest.raises(CanonicalizationError):
        parse_json_strict('{"a": Infinity}')
    with pytest.raises(CanonicalizationError):
        canonical_bytes({1: "x"})
    with pytest.raises(CanonicalizationError):
        canonical_bytes({"x": object()})


def test_prefix_resolution_requires_unique_full_match():
    d1 = "abcdef012345" + "0" * 52
    d2 = "abcdef012345" + "1" * 52
    d3 = "ffffff000000" + "2" * 52
    assert resolve_hash("ffffff000000", [d1, d2, d3]) == d3
    assert resolve_hash("FFFFFF000000", [d1, d2, d3]) == d3
    assert resolve_hash("abcdef012345", [d1, d2, d3]) is None  # collision -> ambiguous
    assert resolve_hash("abcdef0123450", [d1, d2, d3]) == d1  # longer handle disambiguates
    assert resolve_hash("abcdef", [d1]) is None  # shorter than displayed prefix
    assert resolve_hash("zzzzzzzzzzzz", [d1]) is None
    assert resolve_hash("ffffff000000", [d3, d3]) == d3  # duplicates are one digest
    assert prefix_collisions([d1, d2, d3]) == {"abcdef012345"}


json_leaf = st.one_of(st.none(), st.booleans(), st.integers(), st.text())
json_values = st.recursive(
    json_leaf,
    lambda children: st.one_of(
        st.lists(children, max_size=4), st.dictionaries(st.text(), children, max_size=4)
    ),
    max_leaves=12,
)


@given(json_values)
def test_canonical_bytes_idempotent(value):
    once = canonical_bytes(value)
    again = canonical_bytes(parse_json_strict(once.decode("utf-8")))
    assert once == again


@given(st.text(), st.one_of(st.none(), st.text()))
def test_waiver_digest_stable_under_line_ending_rewrites(title, body):
    crlf_title = title.replace("\n", "\r\n")
    crlf_body = None if body is None else body.replace("\n", "\r\n")
    assert issue_snapshot_digest(title, body) == issue_snapshot_digest(crlf_title, crlf_body)
