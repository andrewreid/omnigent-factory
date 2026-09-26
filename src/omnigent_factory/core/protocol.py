"""Frozen stage-result protocol v1 (architecture §7.1-§7.2).

A root's final completed assistant response must contain a literal ``FACTORY_RESULT_V1``
line followed by exactly one fenced ``factory-result`` JSON object (backtick or tilde
fence). :func:`parse_factory_result` extracts it, parses strictly (no duplicate keys, no
non-finite numbers, ≤128 KiB), validates it against the models below (which mirror
``result_schema_v1.json`` exactly: unknown properties rejected) and then enforces the
cross-record constraints listed under the schema in §7.2.

The daemon computes hashes/timestamps itself; model-provided values never override them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError

from omnigent_factory.core.canonical import (
    CanonicalizationError,
    canonical_contract,
    parse_json_strict,
)

SENTINEL = "FACTORY_RESULT_V1"
MAX_RESULT_BYTES = 128 * 1024
MAX_ID_CHARS = 128

Text = Annotated[str, StringConstraints(min_length=1, max_length=16000)]
Texts = Annotated[list[Text], Field(max_length=100)]
Sha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SizeLit = Literal["S", "M", "L"]
Label = Annotated[str, StringConstraints(min_length=1, max_length=100)]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class Criterion(_Strict):
    id: Text
    criterion: Text
    verification: Text


class ResolvedDecision(_Strict):
    decision_id: Text
    answer: Text
    source_event_id: Text


class ContractModel(_Strict):
    goal: Text
    acceptance_criteria: Annotated[list[Criterion], Field(min_length=1, max_length=100)]
    non_goals: Texts
    size: SizeLit
    resolved_decisions: Annotated[list[ResolvedDecision], Field(max_length=100)]


class TriageResult(_Strict):
    kind: Literal["triage"]
    summary: Text
    priority: Literal["P0", "P1", "P2", "P3"]
    size: SizeLit
    recommendation: Literal["fix", "wont_fix", "duplicate", "needs_info"]
    duplicate_issue: Annotated[int, Field(ge=1)] | None
    labels: Annotated[list[Label], Field(max_length=20)]
    missing_information: Texts


class PlanResult(_Strict):
    kind: Literal["plan"]
    publication_kind: Literal["contract", "info"]
    approach: Text
    risks: Texts
    contract: ContractModel
    open_decision_ids: Texts


class Verification(_Strict):
    command: Text
    file_set: Texts
    outcome: Literal["passed", "failed", "not_run"]
    evidence: Text


class Finding(_Strict):
    id: Text
    source: Text
    severity: Literal["BLOCKING", "SHOULD_FIX", "FOLLOW_UP", "ADVISORY"]
    disposition: Literal["fixed", "follow_up", "advisory", "unresolved"]
    evidence: Text


class Review(_Strict):
    implementation_vendor: Text
    review_vendor: Text
    reviewed_head: Sha
    artifact_reference: Text
    artifact_sha256: Sha256
    accepted: bool


class BuildResult(_Strict):
    kind: Literal["build_ready"]
    pr_number: Annotated[int, Field(ge=1)]
    branch: Text
    head_sha: Sha
    summary: Text
    verification: Annotated[list[Verification], Field(min_length=1, max_length=100)]
    review: Review
    findings: Annotated[list[Finding], Field(max_length=200)]
    remediation_batches_used: Annotated[int, Field(ge=0, le=1)]
    targeted_rechecks_used: Annotated[int, Field(ge=0, le=1)]
    release_readiness: Literal["ready", "needs_owner"]


class CheckpointResult(_Strict):
    kind: Literal["checkpoint"]
    grant_id: Text
    head_sha: Sha | None
    done: Texts
    remaining: Texts
    risks: Texts
    worktree_state: Text
    elicitation_id: Annotated[str, StringConstraints(min_length=1)] | None


StageResult = Annotated[
    TriageResult | PlanResult | BuildResult | CheckpointResult, Field(discriminator="kind")
]


class FactoryResult(_Strict):
    version: Literal[1]
    parcel_id: Text
    stage_session_id: Text
    dispatch_nonce: Text
    revision: Annotated[int, Field(ge=0)]
    result: StageResult


class ResultError(ValueError):
    """The final response does not carry a valid factory result."""


@dataclass(frozen=True, slots=True)
class Correlation:
    """Expected correlation fields for the session that produced the result."""

    parcel_id: str
    stage_session_id: str
    dispatch_nonce: str
    revision: int
    stage: Literal["triage", "plan", "build"]
    waiver_build: bool = False
    in_checkpoint: bool = False


@dataclass(frozen=True, slots=True)
class ParsedResult:
    result: FactoryResult
    contract_canonical: bytes | None


_FENCE_OPEN = re.compile(r"^(?P<fence>`{3,}|~{3,})\s*factory-result\s*$")


def extract_result_block(final_text: str) -> str:
    """Return the JSON text of the single fenced block that follows the sentinel."""
    lines = [ln for ln in final_text.replace("\r\n", "\n").split("\n")]
    unquoted = [ln for ln in lines if not ln.lstrip().startswith(">")]
    if sum(1 for ln in unquoted if ln.strip() == SENTINEL) != 1:
        raise ResultError("expected exactly one sentinel line")
    if sum(1 for ln in unquoted if _FENCE_OPEN.match(ln.strip())) != 1:
        raise ResultError("expected exactly one factory-result fence")
    idx = next(i for i, ln in enumerate(unquoted) if ln.strip() == SENTINEL) + 1
    while idx < len(unquoted) and not unquoted[idx].strip():
        idx += 1
    if idx >= len(unquoted):
        raise ResultError("sentinel not followed by a fence")
    opener = _FENCE_OPEN.match(unquoted[idx].strip())
    if opener is None:
        raise ResultError("sentinel not followed by a factory-result fence")
    fence = opener.group("fence")
    body: list[str] = []
    for ln in unquoted[idx + 1 :]:
        if ln.strip() == fence:
            return "\n".join(body)
        body.append(ln)
    raise ResultError("unterminated factory-result fence")


def _check_ids(values: list[str], what: str) -> None:
    if len(set(values)) != len(values):
        raise ResultError(f"duplicate {what} id")


def parse_factory_result(final_text: str, expected: Correlation) -> ParsedResult:
    raw = extract_result_block(final_text)
    if len(raw.encode("utf-8")) > MAX_RESULT_BYTES:
        raise ResultError("result exceeds 128 KiB")
    try:
        data = parse_json_strict(raw)
        result = FactoryResult.model_validate(data)
    except (CanonicalizationError, ValidationError) as exc:
        raise ResultError(f"schema: {exc}") from exc
    for value in (result.parcel_id, result.stage_session_id, result.dispatch_nonce):
        if len(value) > MAX_ID_CHARS:
            raise ResultError("identifier too long")
    if (result.parcel_id, result.stage_session_id, result.dispatch_nonce, result.revision) != (
        expected.parcel_id,
        expected.stage_session_id,
        expected.dispatch_nonce,
        expected.revision,
    ):
        raise ResultError("correlation mismatch")
    r = result.result
    contract_bytes: bytes | None = None
    if isinstance(r, CheckpointResult):
        if not expected.in_checkpoint:
            raise ResultError("checkpoint result outside checkpoint")
    elif isinstance(r, TriageResult):
        if expected.stage != "triage":
            raise ResultError("triage result from non-triage stage")
        if (r.recommendation == "duplicate") != (r.duplicate_issue is not None):
            raise ResultError("duplicate_issue present iff recommendation is duplicate")
        if len(set(r.labels)) != len(r.labels):
            raise ResultError("labels must be unique")
        if any(label.lower().startswith("factory:") for label in r.labels):
            raise ResultError("factory:* labels are control labels")
    elif isinstance(r, PlanResult):
        if expected.stage == "plan" and r.publication_kind != "contract":
            raise ResultError("plan stage must publish a contract")
        if expected.stage == "build" and not (
            expected.waiver_build and r.publication_kind == "info"
        ):
            raise ResultError("build may only publish an informational plan under a waiver")
        if expected.stage == "triage":
            raise ResultError("plan result from triage stage")
        _check_ids(r.open_decision_ids, "open decision")
        try:
            contract_bytes = canonical_contract(r.contract.model_dump(mode="json"))
        except CanonicalizationError as exc:
            raise ResultError(str(exc)) from exc
    else:
        if expected.stage != "build":
            raise ResultError("build result from non-build stage")
        _check_ids([f.id for f in r.findings], "finding")
        if r.review.implementation_vendor == r.review.review_vendor:
            raise ResultError("independent review must use the opposite vendor")
    return ParsedResult(result=result, contract_canonical=contract_bytes)
