"""Stage-result schema v1 and its cross-record constraints (architecture §7.2).

Results arrive through ``factory_submit_result`` as a JSON object (the ``result`` member of
the v1 envelope). :func:`validate_result` wraps it in the envelope the service derives from
the calling run (parcel, run, nonce, revision: never taken from the agent), validates it
against the models below (which mirror ``result_schema_v1.json`` exactly: unknown
properties rejected, ≤128 KiB) and then enforces the cross-record constraints. Errors carry
compact ``location: message`` details the tool returns so the agent can fix the call in
the same turn.

The daemon computes hashes/timestamps itself; model-provided values never override them.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError

from omnigent_factory.core.canonical import (
    CanonicalizationError,
    canonical_contract,
    parse_json_strict,
)

MAX_RESULT_BYTES = 128 * 1024
#: The canonical contract is published verbatim in one GitHub issue comment (65,536
#: characters maximum) together with its header, fence and effect marker, and its hash
#: binds approval. A contract that cannot be published whole is an invalid result.
MAX_CONTRACT_CHARS = 60_000

Text = Annotated[str, StringConstraints(min_length=1, max_length=16000)]
Texts = Annotated[list[Text], Field(max_length=100)]
Sha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SizeLit = Literal["S", "M", "L"]
Label = Annotated[str, StringConstraints(min_length=1, max_length=100)]
#: An http(s) URL with no whitespace, quotes, brackets, angle brackets or ``@`` (no
#: userinfo; it is rendered as a bare link in the Ready report).
DEV_ENVIRONMENT_URL_PATTERN = r"""^https?://[^\s<>"'`()\[\]@]+$"""
DevEnvironmentUrl = Annotated[
    str, StringConstraints(max_length=500, pattern=DEV_ENVIRONMENT_URL_PATTERN)
]


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


Relation = Literal["duplicate", "overlaps", "conflicts", "depends_on", "blocks", "supersedes"]
RelatedNote = Annotated[str, StringConstraints(min_length=1, max_length=300)]


SplitTitle = Annotated[str, StringConstraints(min_length=1, max_length=200)]
SplitBody = Annotated[str, StringConstraints(min_length=1, max_length=4000)]


class SplitOut(_Strict):
    """Work the owner directed be filed as its own issue ("do these bits, file the rest
    as an issue"). The factory opens one issue per item (Inbox, no ``follow-up`` label),
    in the source issue's epic, linked per ``relation`` (the new issue to the source:
    ``after_source`` is blocked by it, ``before_source`` blocks it) and ``blocks`` (issues
    the new issue blocks). Legacy ``blocks``/``blocked_by`` relations are still accepted
    for one release and read as ``after_source``."""

    title: SplitTitle
    body: SplitBody
    reason: Annotated[str, StringConstraints(min_length=1, max_length=300)]
    relation: Literal["none", "after_source", "before_source", "blocks", "blocked_by"] = "none"
    blocks: Annotated[list[Annotated[int, Field(ge=1)]], Field(max_length=20)] = []
    #: A bundle run: the member issue the piece comes from (None: the bundle issue).
    member: Annotated[int, Field(ge=1)] | None = None


SplitOuts = Annotated[list[SplitOut], Field(max_length=10)]


class Ejected(_Strict):
    """A bundle run: a member that turned out bigger, riskier or in need of an owner
    decision, taken out of the bundle (back to Triage on its own)."""

    issue: Annotated[int, Field(ge=1)]
    reason: RelatedNote


Ejections = Annotated[list[Ejected], Field(max_length=5)]


class Related(_Strict):
    """This issue <relation> ``issue`` (another open issue found during triage)."""

    issue: Annotated[int, Field(ge=1)]
    relation: Relation
    note: RelatedNote


#: Why an issue may be closed (``closure_evidence``): a commit, PR or file that already
#: fixes it, or what made it obsolete.
ClosureEvidence = Annotated[str, StringConstraints(min_length=1, max_length=2000)]
#: A repository path the triage relied on (``paths``): auto-retriage watches them.
TriagePath = Annotated[str, StringConstraints(min_length=1, max_length=300)]

#: Recommendations that make the issue a closure candidate (``factory:close-candidate``).
CLOSURE_RECOMMENDATIONS = frozenset({"duplicate", "wont_fix", "already_fixed", "obsolete"})
#: Recommendations that require ``closure_evidence``.
EVIDENCED_RECOMMENDATIONS = frozenset({"already_fixed", "obsolete"})
#: The label the factory (never the agent) puts on a closure candidate's issue.
CLOSE_CANDIDATE_LABEL = "factory:close-candidate"
#: On every member issue of an open (pending or posted) bundle proposal, while the issue
#: is in Triage or Planning (``EffectKind.SET_BUNDLE_PROPOSED``).
BUNDLE_PROPOSED_LABEL = "factory:bundle-proposed"


class TriageResult(_Strict):
    kind: Literal["triage"]
    eli5: Text | None = None
    summary: Text
    priority: Literal["P0", "P1", "P2", "P3"]
    size: SizeLit
    recommendation: Literal[
        "fix", "wont_fix", "duplicate", "needs_info", "already_fixed", "obsolete"
    ]
    #: Defaults (R5): only a ``duplicate`` recommendation names an issue, and an empty
    #: list is exactly "nothing missing" / "no labels": omitting them changes nothing.
    duplicate_issue: Annotated[int, Field(ge=1)] | None = None
    labels: Annotated[list[Label], Field(max_length=20)] = []
    missing_information: Texts = []
    related: Annotated[list[Related], Field(max_length=20)] = []
    split_out: SplitOuts = []
    #: Required for ``already_fixed`` and ``obsolete``: the evidence the owner checks.
    closure_evidence: ClosureEvidence | None = None
    #: Files the triage relied on (auto-retriage re-runs when they change on the base).
    paths: Annotated[list[TriagePath], Field(max_length=20)] = []


class EpicOrder(_Strict):
    """One sub-issue in the proposed build order, with why it goes there."""

    issue: Annotated[int, Field(ge=1)]
    reason: RelatedNote


class EpicLink(_Strict):
    """Suggested native link: sub-issue ``issue`` is blocked by sub-issue ``blocked_by``."""

    issue: Annotated[int, Field(ge=1)]
    blocked_by: Annotated[int, Field(ge=1)]
    reason: RelatedNote


class EpicCoverage(_Strict):
    """What the epic body asks for that no sub-issue covers, and sub-issues that overlap."""

    gaps: Texts
    overlaps: Texts


GateKey = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9-]{0,39}$")]
PartText = Annotated[str, StringConstraints(min_length=1, max_length=1000)]
GateTitle = Annotated[str, StringConstraints(min_length=1, max_length=120)]


class EpicPart(_Strict):
    """One sub-issue's part of the epic: what its plan may cover."""

    issue: Annotated[int, Field(ge=1)]
    scope: PartText


class EpicGate(_Strict):
    """A step the factory cannot do (tenant, cloud, identity, secrets, DNS, ...): it
    becomes a sub-issue assigned to the owner, blocking the sub-issues in ``blocks``."""

    key: GateKey
    title: GateTitle
    steps: PartText
    blocks: Annotated[list[Annotated[int, Field(ge=1)]], Field(min_length=1, max_length=50)]


class EpicExternal(_Strict):
    """An issue outside the epic that sub-issues wait on (autopilot never starts it)."""

    issue: Annotated[int, Field(ge=1)]
    note: RelatedNote


class EpicPlanSection(_Strict):
    """The epic plan autopilot follows (required when the epic is on autopilot)."""

    parts: Annotated[list[EpicPart], Field(max_length=100)]
    coordination: Texts
    human_gates: Annotated[list[EpicGate], Field(max_length=20)] = []
    external_blockers: Annotated[list[EpicExternal], Field(max_length=50)] = []


class EpicTriageResult(_Strict):
    """The triage of an epic (an issue with sub-issues): no priority or size. With
    ``plan``, the epic plan autopilot follows once the owner approves it."""

    kind: Literal["epic_triage"]
    eli5: Text | None = None
    summary: Text
    coverage: EpicCoverage
    build_order: Annotated[list[EpicOrder], Field(max_length=100)]
    links: Annotated[list[EpicLink], Field(max_length=100)] = []
    missing_information: Texts = []
    related: Annotated[list[Related], Field(max_length=20)] = []
    plan: EpicPlanSection | None = None
    split_out: SplitOuts = []


class EpicFit(_Strict):
    """A sub-issue plan of an autopilot epic: does it stay within its part of the epic?"""

    within: bool
    note: RelatedNote


class PlanResult(_Strict):
    kind: Literal["plan"]
    eli5: Text | None = None
    publication_kind: Literal["contract", "info"]
    approach: Text
    risks: Texts
    contract: ContractModel
    #: The service always replaces it with the parcel's open questions: never the agent's.
    open_decision_ids: Texts = []
    related: Annotated[list[Related], Field(max_length=20)] = []
    epic_fit: EpicFit | None = None
    split_out: SplitOuts = []
    ejected: Ejections = []


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


class FollowUp(_Strict):
    """An issue the run filed (e.g. a review-bot finding resolved as a follow-up) and the
    issues it must come before: the factory labels it ``follow-up``, adds it to the
    source issue's epic and links each ``blocks`` issue as blocked by it."""

    issue: Annotated[int, Field(ge=1)]
    blocks: Annotated[list[Annotated[int, Field(ge=1)]], Field(max_length=20)] = []


FollowUps = Annotated[list[FollowUp], Field(max_length=20)]

Severity = Literal["P0", "P1", "P2", "P3"]
ShortText = Annotated[str, StringConstraints(max_length=300)]
#: How much a deferred finding needs fixing (the Ready report sorts by it): ``bug`` a real
#: defect that will hit or has hit, ``likely`` a plausible failure under real conditions,
#: ``edge`` hypothetical (needs an unusual combination), ``tidy`` cleanup/docs/cosmetic.
Rating = Literal["bug", "likely", "edge", "tidy"]
RATINGS: tuple[str, ...] = ("bug", "likely", "edge", "tidy")
#: One short line on how much a deferred finding actually needs fixing.
MAX_WHY_CHARS = 160
Why = Annotated[str, StringConstraints(min_length=1, max_length=MAX_WHY_CHARS)]


class Deferred(_Strict):
    """A review finding not fixed in this PR, with its outcome: ``follow_up`` (a real
    defect outside the plan or too large for this PR: the factory files every one of a
    PR in one consolidated follow-up issue) or ``advisory`` (nothing to file). Each gets
    a checkbox in the Ready report; the owner may tick it to have it fixed now. The
    optional ``rating`` and ``why`` say how much it needs fixing (the report's table)."""

    severity: Severity
    path: ShortText = ""
    title: RelatedNote
    thread_url: ShortText = ""
    reason: RelatedNote
    outcome: Literal["follow_up", "advisory"] = "follow_up"
    #: Issues the follow-up must come before (native blocked-by links).
    blocks: Annotated[list[Annotated[int, Field(ge=1)]], Field(max_length=20)] = []
    #: A bundle run: the member issue the finding belongs to (None: the bundle issue).
    member: Annotated[int, Field(ge=1)] | None = None
    rating: Rating | None = None
    why: Why | None = None


DeferredList = Annotated[list[Deferred], Field(max_length=50)]


class BuildResult(_Strict):
    kind: Literal["build_ready"]
    eli5: Text | None = None
    pr_number: Annotated[int, Field(ge=1)]
    branch: Text
    head_sha: Sha
    summary: Text
    verification: Annotated[list[Verification], Field(min_length=1, max_length=100)]
    review: Review
    findings: Annotated[list[Finding], Field(max_length=200)]
    #: Cumulative fix batches / targeted rechecks: 0 (none used) is the safe default; a
    #: value above the run's review-round budget is still refused.
    remediation_batches_used: Annotated[int, Field(ge=0, le=12)] = 0
    targeted_rechecks_used: Annotated[int, Field(ge=0, le=12)] = 0
    release_readiness: Literal["ready", "needs_owner"]
    related: Annotated[list[Related], Field(max_length=20)] = []
    #: Issues the run filed itself (kept for compatibility: the factory now files them).
    follow_ups: FollowUps = []
    deferred: DeferredList = []
    split_out: SplitOuts = []
    ejected: Ejections = []
    #: The dev environment the run started and left running for the owner's review
    #: (shown in the Ready report); the factory tears it down when the issue is done.
    dev_environment_url: DevEnvironmentUrl | None = None


class CheckpointResult(_Strict):
    kind: Literal["checkpoint"]
    grant_id: Text
    head_sha: Sha | None
    done: Texts
    remaining: Texts
    risks: Texts
    worktree_state: Text
    elicitation_id: Annotated[str, StringConstraints(min_length=1)] | None


class ReplanRequired(_Strict):
    """A build's report that the owner's direction goes beyond the approved plan: the
    factory replans (the owner approves the revised plan)."""

    reason: Text
    #: What the revised plan must change.
    changes: Annotated[list[Text], Field(min_length=1, max_length=30)]


class BlockedResult(_Strict):
    """The agent could not finish (a tool, credential or permission refused) and says so.

    Honest failure instead of invented values: the daemon shows Blocked with ``reason``.
    A build whose owner direction goes beyond the approved plan says so in
    ``replan_required``: the factory then replans instead.
    """

    kind: Literal["blocked"]
    reason: Text
    done: Texts
    related: Annotated[list[Related], Field(max_length=20)] = []
    follow_ups: FollowUps = []
    deferred: DeferredList = []
    split_out: SplitOuts = []
    replan_required: ReplanRequired | None = None
    ejected: Ejections = []


def blocked_on(r: BlockedResult) -> tuple[int, ...]:
    """The issues a ``blocked`` result names as its only obstacle: every ``related`` entry
    is ``depends_on`` (any other relation, or none, is not a wait on other issues)."""
    if not r.related or any(item.relation != "depends_on" for item in r.related):
        return ()
    return tuple(sorted({item.issue for item in r.related}))


StageResult = Annotated[
    TriageResult | EpicTriageResult | PlanResult | BuildResult | CheckpointResult | BlockedResult,
    Field(discriminator="kind"),
]


class FactoryResult(_Strict):
    version: Literal[1]
    parcel_id: Text
    stage_session_id: Text
    dispatch_nonce: Text
    revision: Annotated[int, Field(ge=0)]
    result: StageResult


class ResultError(ValueError):
    """The submitted result is not a valid factory result for this run.

    ``details`` are compact ``location: message`` lines safe to show the agent and the
    owner: field locations and validator messages only, never the rejected input values.
    """

    def __init__(self, message: str, details: tuple[str, ...] = ()) -> None:
        super().__init__(message)
        self.details = details or (message,)


#: Bounds for error details relayed to the agent or published in a status comment.
MAX_ERROR_DETAILS = 100
MAX_ERROR_DETAIL_CHARS = 300
_RESULT_KINDS = frozenset({"triage", "epic_triage", "plan", "build_ready", "checkpoint", "blocked"})


#: Result model per ``kind`` (the validator's own models: shapes, examples and the typed
#: submit tools' input schemas are all generated from these, so they cannot drift).
RESULT_MODELS: dict[str, type[_Strict]] = {
    "triage": TriageResult,
    "epic_triage": EpicTriageResult,
    "plan": PlanResult,
    "build_ready": BuildResult,
    "checkpoint": CheckpointResult,
    "blocked": BlockedResult,
}

#: How to read a shape (quoted once wherever shapes are shown).
SHAPE_LEGEND = (
    '"field"?: optional (may be omitted); str: 1-16000 characters; str(<=N): at most N '
    "characters; [T](<=N): a list of at most N items; a|b: exactly one of these values "
    "(a note in parentheses explains a value, it is not part of it); |null: may be null."
)

#: Notes appended to a field's shape: (model, field) -> note.
_FIELD_NOTES: dict[tuple[str, str], str] = {
    ("TriageResult", "duplicate_issue"): "the issue to keep; set iff recommendation is duplicate",
    ("TriageResult", "closure_evidence"): "required for already_fixed and obsolete",
    ("TriageResult", "paths"): "repository paths the triage relied on, no duplicates",
    ("TriageResult", "labels"): "no factory:* labels, no duplicates",
    ("SplitOut", "blocks"): "issues the new issue blocks",
    ("SplitOut", "member"): "bundle runs: the member it comes from",
    ("Deferred", "member"): "bundle runs: the member it belongs to",
    ("Deferred", "blocks"): "issues the follow-up must come before",
    ("Deferred", "why"): "one short line: how much it needs fixing",
    ("Ejected", "issue"): "a bundle member",
    ("BuildResult", "remediation_batches_used"): "at most min(12, review_rounds.budget)",
    ("BuildResult", "targeted_rechecks_used"): "at most min(12, review_rounds.budget)",
    ("BuildResult", "dev_environment_url"): "a dev environment you left running",
    ("Review", "review_vendor"): "must differ from implementation_vendor",
    ("Finding", "severity"): "the findings scale; never P0-P3",
    ("Finding", "disposition"): "build_ready: never unresolved",
    ("Deferred", "severity"): "the deferred scale; never BLOCKING etc.",
    ("ReplanRequired", "changes"): "build only: what the plan must change",
    ("PlanResult", "open_decision_ids"): "the factory fills it",
    ("EpicTriageResult", "plan"): "required when the epic is on autopilot",
}
#: Notes on individual enum values: (model, field) -> {value: note}.
_VALUE_NOTES: dict[tuple[str, str], dict[str, str]] = {
    ("SplitOut", "relation"): {
        "after_source": "new issue blocked by this one",
        "before_source": "new issue blocks this one",
        "blocks": "legacy, read as after_source",
        "blocked_by": "legacy, read as after_source",
    },
    ("Deferred", "rating"): {
        "bug": "real defect: will hit or has hit",
        "likely": "plausible under real conditions",
        "edge": "hypothetical: needs an unusual combination",
        "tidy": "cleanup, docs, cosmetic",
    },
}
#: Named string formats (by pattern) shown instead of the raw pattern.
_PATTERN_NAMES = {
    Sha.__metadata__[0].pattern: "sha40",  # type: ignore[attr-defined]
    Sha256.__metadata__[0].pattern: "sha256",  # type: ignore[attr-defined]
    GateKey.__metadata__[0].pattern: "slug(a-z, 0-9, -; <=40)",  # type: ignore[attr-defined]
    DEV_ENVIRONMENT_URL_PATTERN: "url(http/https, <=500)",
}
#: Example values where the first enum value or a placeholder would mislead.
_EXAMPLE_VALUES: dict[tuple[str, str], object] = {
    ("TriageResult", "priority"): "P2",
    ("TriageResult", "size"): "S",
    ("ContractModel", "size"): "S",
    ("BuildResult", "head_sha"): "0123456789abcdef0123456789abcdef01234567",
    ("Review", "reviewed_head"): "0123456789abcdef0123456789abcdef01234567",
    ("Review", "artifact_sha256"): "0123456789abcdef" * 4,
    ("BuildResult", "pr_number"): 1,
}
#: Optional fields every example shows anyway (the prompts ask for them).
_EXAMPLE_ALWAYS = frozenset({"eli5"})


def _ref(
    schema: Mapping[str, object], defs: Mapping[str, object]
) -> tuple[str, Mapping[str, object]]:
    ref = schema.get("$ref")
    if isinstance(ref, str):
        name = ref.rsplit("/", 1)[-1]
        target = defs[name]
        assert isinstance(target, Mapping)  # noqa: S101 - pydantic's own schema
        return name, target
    return str(schema.get("title") or ""), schema


def _bounds(low: object, high: object, unit: str = "") -> str:
    if isinstance(high, int) and isinstance(low, int) and low > 1:
        return f"{low}..{high}{unit}"
    if isinstance(high, int):
        return f"<={high}{unit}"
    if isinstance(low, int) and low > 1:
        return f">={low}{unit}"
    return ""


def _shape(
    schema: Mapping[str, object], defs: Mapping[str, object], owner: str = "", field: str = ""
) -> str:
    """Compact type of one JSON-schema node (pydantic's schema of the result models)."""
    if "$ref" in schema:
        name, target = _ref(schema, defs)
        return _object_shape(name, target, defs)
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        options = [o for o in any_of if isinstance(o, Mapping) and o.get("type") != "null"]
        nullable = len(options) != len(any_of)
        text = "|".join(_shape(o, defs, owner, field) for o in options)
        return text + ("|null" if nullable else "")
    if "const" in schema:
        return json.dumps(schema["const"])
    enum = schema.get("enum")
    if isinstance(enum, list):
        notes = _VALUE_NOTES.get((owner, field), {})
        return '"' + "|".join(f"{v}({notes[v]})" if v in notes else str(v) for v in enum) + '"'
    kind = schema.get("type")
    if kind == "string":
        pattern = schema.get("pattern")
        if isinstance(pattern, str):
            return _PATTERN_NAMES.get(pattern, f"str(matching {pattern})")
        low, high = schema.get("minLength", 0), schema.get("maxLength")
        if low == 1 and high == 16000:
            return "str"
        bound = _bounds(low, high)
        return f"str({bound})" if bound else "str(may be empty)" if low == 0 else "str"
    if kind == "integer":
        low, high = schema.get("minimum"), schema.get("maximum")
        if isinstance(low, int) and isinstance(high, int):
            return f"int({low}..{high})"
        return f"int>={low}" if isinstance(low, int) else "int"
    if kind == "boolean":
        return "bool"
    if kind == "array":
        items = schema.get("items")
        inner = _shape(items, defs, owner, field) if isinstance(items, Mapping) else "any"
        low, high = schema.get("minItems", 0), schema.get("maxItems")
        bound = f"{low}..{high}" if isinstance(low, int) and low >= 1 else _bounds(low, high)
        return f"[{inner}]" + (f"({bound})" if bound else "")
    if kind == "object":
        return _object_shape(owner, schema, defs)
    return "any"


def _object_shape(name: str, schema: Mapping[str, object], defs: Mapping[str, object]) -> str:
    props = schema.get("properties")
    required = schema.get("required")
    req = set(required) if isinstance(required, list) else set()
    parts: list[str] = []
    for key, sub in props.items() if isinstance(props, Mapping) else ():
        assert isinstance(sub, Mapping)  # noqa: S101 - pydantic's own schema
        text = _shape(sub, defs, name, key)
        note = _FIELD_NOTES.get((name, key))
        if note:
            text += f" ({note})"
        parts.append(f'"{key}"{"" if key in req else "?"}:{text}')
    return "{" + ",".join(parts) + "}"


def result_shape(kind: str) -> str:
    """The compact field shape of result ``kind``, generated from its model."""
    model = RESULT_MODELS[kind]
    schema = model.model_json_schema()
    defs = schema.get("$defs", {})
    return _object_shape(model.__name__, schema, defs)


def _example(
    schema: Mapping[str, object], defs: Mapping[str, object], owner: str, field: str
) -> object:
    if (owner, field) in _EXAMPLE_VALUES:
        return _EXAMPLE_VALUES[(owner, field)]
    if "$ref" in schema:
        name, target = _ref(schema, defs)
        return _object_example(name, target, defs)
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        return None
    if "const" in schema:
        return schema["const"]
    enum = schema.get("enum")
    if isinstance(enum, list):
        return enum[0]
    kind = schema.get("type")
    if kind == "string":
        if isinstance(schema.get("pattern"), str):
            raise ValueError(f"{owner}.{field}: give a pattern field an example value")
        return f"<{field}>"
    if kind == "integer":
        low = schema.get("minimum")
        return low if isinstance(low, int) else 0
    if kind == "boolean":
        return True
    if kind == "array":
        items = schema.get("items")
        count = schema.get("minItems", 0)
        assert isinstance(items, Mapping) and isinstance(count, int)  # noqa: S101
        return [_example(items, defs, owner, field) for _ in range(count)]
    if kind == "object":
        return _object_example(owner, schema, defs)
    raise ValueError(f"{owner}.{field}: no example for {schema}")


def _object_example(
    name: str, schema: Mapping[str, object], defs: Mapping[str, object]
) -> dict[str, object]:
    props = schema.get("properties")
    required = schema.get("required")
    req = set(required) if isinstance(required, list) else set()
    out: dict[str, object] = {}
    for key, sub in props.items() if isinstance(props, Mapping) else ():
        if key in req or key in _EXAMPLE_ALWAYS:
            assert isinstance(sub, Mapping)  # noqa: S101 - pydantic's own schema
            value = _example(sub, defs, name, key)
            if key in _EXAMPLE_ALWAYS and value is None:
                value = f"<{key}>"
            out[key] = value
    return out


def result_example(kind: str) -> dict[str, object]:
    """A minimal valid result of ``kind`` (its required fields, plus eli5), generated
    from its model: every string value is a placeholder to replace."""
    model = RESULT_MODELS[kind]
    schema = model.model_json_schema()
    return _object_example(model.__name__, schema, schema.get("$defs", {}))


#: Compact field types per result kind, quoted in tool errors, prompts and status.
RESULT_SHAPES: dict[str, str] = {kind: result_shape(kind) for kind in RESULT_MODELS}
RESULT_EXAMPLES: dict[str, dict[str, object]] = {
    kind: result_example(kind) for kind in RESULT_MODELS
}


#: Longest list of item positions quoted on one grouped error line (then "+N more").
MAX_ERROR_INDEX_CHARS = 80


def _positions(where: list[tuple[int, ...]]) -> str:
    """Item positions as compact ranges (``0-67, 70``; nested ``3.1``), bounded, with
    any positions left out counted explicitly."""
    parts: list[str] = []
    if all(len(w) == 1 for w in where):
        values = sorted({w[0] for w in where})
        start = prev = values[0]
        for value in [*values[1:], None]:
            if value is not None and value == prev + 1:
                prev = value
                continue
            parts.append(str(start) if start == prev else f"{start}-{prev}")
            if value is not None:
                start = prev = value
    else:
        parts = [".".join(str(i) for i in w) for w in where]
    shown: list[str] = []
    for part in parts:
        if len(", ".join([*shown, part])) > MAX_ERROR_INDEX_CHARS:
            break
        shown.append(part)
    text = ", ".join(shown)
    left = len(parts) - len(shown)
    return text + (f", +{left} more" if left else "")


def validation_details(exc: ValidationError) -> tuple[str, ...]:
    """Every schema error in one rejection, grouped by field (R4), without input values.

    ``loc: msg`` per field; the same message at several list positions is one line,
    message first (``findings.*.severity: Input should be ... [items 0-67, 70]``), the
    positions compressed into ranges and bounded with an explicit "+N more". Lines are
    ordered by top-level field; discriminator tags are dropped. At most
    :data:`MAX_ERROR_DETAILS` lines.
    """
    groups: dict[tuple[str, str], list[tuple[int, ...]]] = {}
    for error in exc.errors(include_input=False, include_url=False, include_context=False):
        loc = [str(part) for part in error["loc"]]
        if len(loc) > 1 and loc[0] == "result" and loc[1] in _RESULT_KINDS:
            del loc[1]
        indices = tuple(int(part) for part in loc if part.isdigit())
        pattern = ".".join("*" if part.isdigit() else part for part in loc) or "(root)"
        groups.setdefault((pattern, error["msg"]), []).append(indices)
    lines: list[str] = []
    for (pattern, msg), where in sorted(groups.items(), key=lambda item: item[0][0].split(".")[:2]):
        if len(where) == 1:
            exact = iter(where[0])
            name = ".".join(
                str(next(exact)) if part == "*" else part for part in pattern.split(".")
            )
            line = f"{name}: {msg}"[:MAX_ERROR_DETAIL_CHARS]
        else:
            items = f" [items {_positions(where)}]"
            line = f"{pattern}: {msg}"[: MAX_ERROR_DETAIL_CHARS - len(items)] + items
        lines.append(line)
    extra = len(lines) - MAX_ERROR_DETAILS
    if extra > 0:
        lines = [*lines[:MAX_ERROR_DETAILS], f"... and {extra} more"]
    return tuple(lines)


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
    #: The parcel's own issue number (a triage may not relate the issue to itself).
    issue_number: int | None = None
    #: The issue is an epic: its sub-issue numbers (empty when not all could be read).
    #: None: not an epic (an epic is triaged with ``epic_triage``, never ``triage``).
    sub_issues: frozenset[int] | None = None
    #: The epic is on autopilot: its epic triage must carry the epic ``plan``.
    epic_plan: bool = False
    #: The issue is a sub-issue epic autopilot drives: its plan must say ``epic_fit``.
    epic_part: bool = False
    #: Follow-up lineage: how many follow-up hops separate this issue from an ordinary
    #: one (0: not a follow-up) and its source issue. At ``follow_up_max_depth`` or more,
    #: the run may not defer anything as a follow-up (fix it here or mark it advisory).
    follow_up_depth: int = 0
    follow_up_source: int | None = None
    follow_up_max_depth: int = 1
    #: A bundle issue: its members still in the bundle (None: not a bundle). Only a
    #: bundle run may eject members or attribute findings and splits to one, and its
    #: plan names each member in an acceptance criterion.
    bundle_members: frozenset[int] | None = None
    #: Trusted run context, never supplied by the agent. Legacy callers retain one batch.
    review_rounds_budget: int = 1


@dataclass(frozen=True, slots=True)
class ParsedResult:
    result: FactoryResult
    contract_canonical: bytes | None


def _check_ids(values: list[str], what: str) -> None:
    if len(set(values)) != len(values):
        raise ResultError(f"duplicate {what} id")


def _check_epic_triage(r: EpicTriageResult, expected: Correlation) -> None:
    if expected.stage != "triage":
        raise ResultError("epic_triage result from non-triage stage")
    if expected.sub_issues is None:
        raise ResultError("epic_triage is only for an epic (an issue with sub-issues)")
    known = expected.sub_issues
    order = [item.issue for item in r.build_order]
    if len(set(order)) != len(order):
        raise ResultError("build_order: each sub-issue at most once")
    pairs = [(item.issue, item.blocked_by) for item in r.links]
    if len(set(pairs)) != len(pairs):
        raise ResultError("links: each pair at most once")
    if any(a == b for a, b in pairs):
        raise ResultError("links: an issue cannot block itself")
    named = set(order) | {n for pair in pairs for n in pair}
    plan = r.plan
    if plan is not None:
        parts = [item.issue for item in plan.parts]
        if len(set(parts)) != len(parts):
            raise ResultError("plan.parts: each sub-issue at most once")
        keys = [gate.key for gate in plan.human_gates]
        if len(set(keys)) != len(keys):
            raise ResultError("plan.human_gates: each key at most once")
        named |= set(parts) | {n for gate in plan.human_gates for n in gate.blocks}
        if known and any(item.issue in known for item in plan.external_blockers):
            raise ResultError("plan.external_blockers: only issues outside this epic")
    elif expected.epic_plan:
        raise ResultError(
            "this epic is on autopilot: include plan (parts, coordination, human_gates, "
            "external_blockers)"
        )
    outside = sorted(named - known) if known else []
    if outside:
        raise ResultError(
            "build_order/links/plan: only this epic's sub-issues ("
            + ", ".join(f"#{n}" for n in outside[:10])
            + " are not)"
        )


def _check_follow_ups(items: list[FollowUp], own: int | None) -> None:
    numbers = [item.issue for item in items]
    if len(set(numbers)) != len(numbers):
        raise ResultError("follow_ups: each issue at most once")
    if own is not None and own in numbers:
        raise ResultError("follow_ups: not this issue itself")
    for item in items:
        if len(set(item.blocks)) != len(item.blocks):
            raise ResultError(f"follow_ups: #{item.issue} blocks each issue at most once")
        if item.issue in item.blocks:
            raise ResultError(f"follow_ups: #{item.issue} cannot block itself")


def _check_split_out(items: list[SplitOut], own: int | None) -> None:
    titles = [" ".join(item.title.lower().split()) for item in items]
    if len(set(titles)) != len(titles):
        raise ResultError("split_out: each title at most once")
    for item in items:
        if own is not None and own in item.blocks:
            raise ResultError('split_out: blocks names this issue: use relation "before_source"')
        if len(set(item.blocks)) != len(item.blocks):
            raise ResultError("split_out: blocks each issue at most once")


def _check_deferred(r: BuildResult | BlockedResult, expected: Correlation) -> None:
    for item in r.deferred:
        if expected.issue_number is not None and expected.issue_number in item.blocks:
            raise ResultError("deferred: a follow-up cannot block this issue itself")
        if len(set(item.blocks)) != len(item.blocks):
            raise ResultError("deferred: blocks each issue at most once")
    filed = [item for item in r.deferred if item.outcome == "follow_up"]
    if expected.follow_up_depth >= expected.follow_up_max_depth and (filed or r.follow_ups):
        source = (
            f" (a follow-up of #{expected.follow_up_source})"
            if expected.follow_up_source is not None
            else ""
        )
        raise ResultError(
            f"deferred: this issue is itself a follow-up{source}, so nothing more may be "
            "deferred as a follow-up: fix each of these findings in this PR, or give it "
            'outcome "advisory" with a one-line reason (no follow_ups either)'
        )


_MEMBER_REF = re.compile(r"(?<![\w/&#])#(\d+)\b")


def _check_bundle(r: object, expected: Correlation) -> None:
    ejected = [item.issue for item in getattr(r, "ejected", [])]
    attributed = [
        item.member
        for name in ("split_out", "deferred")
        for item in getattr(r, name, [])
        if item.member is not None
    ]
    members = expected.bundle_members
    if members is None:
        if ejected or attributed:
            raise ResultError("ejected/member: only a bundle issue's run names bundle members")
        return
    if len(set(ejected)) != len(ejected):
        raise ResultError("ejected: each member at most once")
    unknown = sorted((set(ejected) | set(attributed)) - members)
    if unknown:
        raise ResultError(
            "ejected/member: not members of this bundle: "
            + ", ".join(f"#{n}" for n in unknown[:10])
            + " (members: "
            + ", ".join(f"#{n}" for n in sorted(members))
            + ")"
        )
    if isinstance(r, PlanResult) and expected.stage == "plan":
        named: set[int] = set()
        for c in r.contract.acceptance_criteria:
            named |= {int(n) for n in _MEMBER_REF.findall(f"{c.id} {c.criterion}")}
        missing = sorted(members - set(ejected) - named)
        if missing:
            raise ResultError(
                "contract.acceptance_criteria: give each bundle member its own acceptance "
                'criteria naming it (e.g. "#12: ..."); none name '
                + ", ".join(f"#{n}" for n in missing)
            )


def epic_plan_hash(result: Mapping[str, object]) -> str:
    """sha256 of an epic plan (a validated ``epic_triage`` result as JSON): binds approval."""
    text = json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


#: Most sub-issues an epic example names (enough to show the form).
_EXAMPLE_SUB_ISSUES = 3


def context_requirements(kind: str, expected: Correlation) -> tuple[str, ...]:
    """What this run's context adds to ``kind``'s shape, in the validator's own terms
    (each line mirrors a check in :func:`validate_result` or its helpers)."""
    notes: list[str] = []
    if expected.in_checkpoint:
        notes.append('at a checkpoint only kind "blocked" (done, and what remains as reason)')
    if kind == "plan" and expected.stage == "build" and expected.waiver_build:
        notes.append('under a planning waiver a build publishes publication_kind "info"')
    if kind == "plan" and expected.stage == "plan" and expected.epic_part:
        notes.append(
            "part of an autopilot epic: include epic_fit (within: does the plan stay within "
            "its part of the epic plan; note: why)"
        )
    if kind == "plan" and expected.stage == "plan" and expected.bundle_members:
        members = ", ".join(f"#{n}" for n in sorted(expected.bundle_members))
        notes.append(
            f"a bundle plan gives each member ({members}) acceptance criteria naming it "
            '(e.g. "#12: ...") unless it is in ejected'
        )
    if kind == "epic_triage" and expected.epic_plan:
        notes.append(
            "this epic is on autopilot: include plan (parts, coordination, human_gates, "
            "external_blockers)"
        )
    if kind == "epic_triage" and expected.sub_issues:
        notes.append("build_order, links and plan name only this epic's sub-issues")
    if (
        kind in ("build_ready", "blocked")
        and expected.follow_up_depth >= expected.follow_up_max_depth
    ):
        notes.append(
            'this issue is itself a follow-up: deferred items only with outcome "advisory", '
            "and no follow_ups"
        )
    return tuple(notes)


def run_example(kind: str, expected: Correlation) -> dict[str, object] | None:
    """The minimal example of ``kind`` made valid for this run's context (the fields the
    context requires, per :func:`context_requirements`), checked with
    :func:`validate_result` under ``expected``; None when no valid example can be built
    (the shape and the requirements are then shown instead)."""
    example: dict[str, Any] = json.loads(json.dumps(RESULT_EXAMPLES[kind]))
    if kind == "plan":
        if expected.stage == "build" and expected.waiver_build:
            example["publication_kind"] = "info"
        if expected.stage == "plan" and expected.epic_part:
            example["epic_fit"] = {"within": True, "note": "<note>"}
        if expected.stage == "plan" and expected.bundle_members:
            example["contract"]["acceptance_criteria"] = [
                {"id": f"#{n}", "criterion": f"#{n}: <criterion>", "verification": "<verification>"}
                for n in sorted(expected.bundle_members)
            ]
    if kind == "epic_triage":
        subs = sorted(expected.sub_issues or ())[:_EXAMPLE_SUB_ISSUES]
        example["build_order"] = [{"issue": n, "reason": "<reason>"} for n in subs]
        if expected.epic_plan:
            example["plan"] = {
                "parts": [{"issue": n, "scope": "<scope>"} for n in subs],
                "coordination": [],
            }
    try:
        validate_result(example, expected)
    except ResultError:
        return None
    return example


def validate_result(result: Mapping[str, object], expected: Correlation) -> ParsedResult:
    """Validate one submitted stage result for the run described by ``expected``."""
    if not isinstance(result, Mapping):
        raise ResultError("result: must be a JSON object")
    try:
        raw = json.dumps(result, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ResultError("result: must be plain JSON (no NaN/Infinity)") from exc
    if len(raw.encode("utf-8")) > MAX_RESULT_BYTES:
        raise ResultError("result exceeds 128 KiB")
    try:
        # Strict re-parse: no duplicate keys or non-finite numbers reach the models.
        inner = parse_json_strict(raw)
    except CanonicalizationError as exc:
        raise ResultError(str(exc)[:MAX_ERROR_DETAIL_CHARS]) from exc
    data = {
        "version": 1,
        "parcel_id": expected.parcel_id,
        "stage_session_id": expected.stage_session_id,
        "dispatch_nonce": expected.dispatch_nonce,
        "revision": expected.revision,
        "result": inner,
    }
    try:
        result_model = FactoryResult.model_validate(data)
    except ValidationError as exc:
        details = validation_details(exc)
        raise ResultError("schema: " + "; ".join(details), details) from exc
    r = result_model.result
    related = [item.issue for item in getattr(r, "related", [])]
    if len(set(related)) != len(related):
        raise ResultError("related: each issue at most once")
    if expected.issue_number is not None and expected.issue_number in related:
        raise ResultError("related: an issue cannot relate to itself")
    _check_follow_ups(getattr(r, "follow_ups", []), expected.issue_number)
    if isinstance(r, BuildResult | BlockedResult):
        _check_deferred(r, expected)
    _check_split_out(getattr(r, "split_out", []), expected.issue_number)
    _check_bundle(r, expected)
    contract_bytes: bytes | None = None
    if isinstance(r, BlockedResult):
        # Any stage may report that it could not finish; only a build asks for a replan.
        if r.replan_required is not None and (expected.stage != "build" or expected.in_checkpoint):
            raise ResultError("replan_required: only a build run reports it (not at a checkpoint)")
    elif isinstance(r, CheckpointResult):
        if not expected.in_checkpoint:
            raise ResultError("checkpoint result outside checkpoint")
    elif isinstance(r, EpicTriageResult):
        _check_epic_triage(r, expected)
    elif isinstance(r, TriageResult):
        if expected.stage != "triage":
            raise ResultError("triage result from non-triage stage")
        if expected.sub_issues is not None:
            raise ResultError("this issue is an epic (it has sub-issues): submit epic_triage")
        if (r.recommendation == "duplicate") != (r.duplicate_issue is not None):
            raise ResultError("duplicate_issue present iff recommendation is duplicate")
        if r.recommendation in EVIDENCED_RECOMMENDATIONS and r.closure_evidence is None:
            raise ResultError(
                f"closure_evidence is required when recommendation is {r.recommendation}"
            )
        if len(set(r.paths)) != len(r.paths):
            raise ResultError("paths must be unique")
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
        if expected.stage == "plan" and expected.epic_part and r.epic_fit is None:
            raise ResultError(
                "this issue is part of an autopilot epic: include epic_fit (within: does the "
                "plan stay within its part of the epic plan; note: why)"
            )
        try:
            contract_bytes = canonical_contract(r.contract.model_dump(mode="json"))
        except CanonicalizationError as exc:
            raise ResultError(str(exc)) from exc
        if len(contract_bytes.decode("utf-8")) > MAX_CONTRACT_CHARS:
            raise ResultError("contract exceeds the publishable comment size")
    else:
        if expected.stage != "build":
            raise ResultError("build result from non-build stage")
        for field in ("remediation_batches_used", "targeted_rechecks_used"):
            if getattr(r, field) > expected.review_rounds_budget:
                raise ResultError(
                    f"{field}: exceeds this run's review-round budget "
                    f'({expected.review_rounds_budget}); submit kind "blocked" (Needs you) '
                    "if a required local fix cannot be completed within the allowance"
                )
        _check_ids([f.id for f in r.findings], "finding")
        if any(f.disposition == "unresolved" for f in r.findings):
            raise ResultError("build-ready findings must all be dispositioned")
        if r.review.implementation_vendor == r.review.review_vendor:
            raise ResultError("independent review must use the opposite vendor")
    return ParsedResult(result=result_model, contract_canonical=contract_bytes)
