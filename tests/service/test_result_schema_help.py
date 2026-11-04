"""First-attempt submits: result shapes up front, grouped errors, the unread-feedback and
plan-read gates, safe defaults and the typed per-kind submit tools (R1-R7)."""

from __future__ import annotations

import json
import re
import typing
from dataclasses import replace
from importlib.resources import files
from typing import Any

import pytest
from pydantic import BaseModel

from omnigent_factory.core import events as ev
from omnigent_factory.core import protocol
from omnigent_factory.core.effects import (
    Ack,
    AmbiguousWrite,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    MessagePurpose,
    Preconditions,
)
from omnigent_factory.core.protocol import (
    RESULT_EXAMPLES,
    RESULT_MODELS,
    RESULT_SHAPES,
    Correlation,
    ResultError,
    validate_result,
)
from omnigent_factory.core.types import SessionKind, Via
from omnigent_factory.service import directory as d
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.mcp import (
    TOOL_NAMES,
    FactoryToolError,
    build_mcp_server,
    typed_submit_schema,
)
from omnigent_factory.store.sqlite import DeliveryRecord
from tests.service.test_mcp import (
    PLAN,
    TRIAGE,
    P,
    Rig,
    build_ready,
    building,
    planning,
    started,
    triaged,
)
from tests.service.test_pilot_677 import eventually

TEMPLATES = files("omnigent_factory.service") / "templates"
SUBMITTED = ("triage", "epic_triage", "plan", "build_ready", "blocked")


# ------------------------------------------------------------------ R1/R2: shapes, examples


def _corr(kind: str) -> Correlation:
    stage = {"triage": "triage", "epic_triage": "triage", "plan": "plan"}.get(kind, "build")
    return Correlation(
        "P1",
        "S1",
        "N1",
        0,
        stage,  # type: ignore[arg-type]
        in_checkpoint=kind == "checkpoint",
        sub_issues=frozenset({2}) if kind == "epic_triage" else None,
    )


@pytest.mark.parametrize("kind", sorted(RESULT_MODELS))
def test_every_generated_example_validates_against_its_model_and_the_validator(kind: str):
    example = RESULT_EXAMPLES[kind]
    RESULT_MODELS[kind].model_validate(example)
    assert validate_result(example, _corr(kind)).result.result.kind == kind
    # Minimal: only required fields (and eli5, which every prompt asks for).
    model = RESULT_MODELS[kind]
    required = {n for n, f in model.model_fields.items() if f.is_required()}
    assert set(example) - required <= {"eli5"}
    assert required <= set(example)


def _literals(annotation: Any) -> list[tuple[str, ...]]:
    """Every Literal's values inside a (possibly nested) annotation."""
    origin = typing.get_origin(annotation)
    if origin is typing.Literal:
        return [tuple(str(a) for a in typing.get_args(annotation))]
    out: list[tuple[str, ...]] = []
    for arg in typing.get_args(annotation):
        out += _literals(arg)
    return out


def _models(model: type[BaseModel]) -> set[type[BaseModel]]:
    """``model`` and every model nested in its fields."""
    seen = {model}
    for field in model.model_fields.values():
        stack = [field.annotation]
        while stack:
            tp = stack.pop()
            if isinstance(tp, type) and issubclass(tp, BaseModel) and tp not in seen:
                seen |= _models(tp)
            stack += list(typing.get_args(tp))
    return seen


@pytest.mark.parametrize("kind", sorted(RESULT_MODELS))
def test_rendered_shapes_match_the_models(kind: str):
    shape = RESULT_SHAPES[kind]
    assert shape == protocol.result_shape(kind)  # generated, never hand-written
    top = RESULT_MODELS[kind]
    for name, field in top.model_fields.items():
        mark = f'"{name}":' if field.is_required() else f'"{name}"?:'
        assert mark in shape, (kind, name)
    for model in _models(top):
        for name, field in model.model_fields.items():
            assert f'"{name}"' in shape, (kind, model.__name__, name)
            for values in _literals(field.annotation):
                if len(values) > 1:
                    # Every allowed value is listed (some carry a note in parentheses).
                    rendered = re.sub(r"\([^()]*\)", "", shape)
                    assert "|".join(values) in rendered, (kind, model.__name__, name)
    # Length and count limits come from the model too.
    if kind == "build_ready":
        assert '"title":str(<=300)' in shape and '"why"?:str(<=160)' in shape
        assert '"acceptance_criteria"' not in shape
        assert '"severity":"BLOCKING|SHOULD_FIX|FOLLOW_UP|ADVISORY' in shape
        assert '"disposition":"fixed|follow_up|advisory|unresolved' in shape
        assert '"severity":"P0|P1|P2|P3' in shape  # deferred items
    if kind == "plan":
        assert '"acceptance_criteria":[{"id":str,"criterion":str,"verification":str}](1..100)' in (
            shape
        )
        assert '"risks":[str](<=100)' in shape  # required (R5 keeps it)


def test_shapes_list_enum_values_and_the_triage_relations():
    triage = RESULT_SHAPES["triage"]
    assert '"relation":"duplicate|overlaps|conflicts|depends_on|blocks|supersedes"' in triage
    assert '"recommendation":"fix|wont_fix|duplicate|needs_info|already_fixed|obsolete"' in triage
    assert '"duplicate_issue"?:' in triage and '"missing_information"?:' in triage


# ------------------------------------------------------------------ R4: grouped errors


def test_every_error_is_reported_grouped_by_field():
    corr = Correlation("P1", "S1", "N1", 0, "build")
    findings = [
        {"id": str(i), "source": "bot", "severity": "P1", "disposition": "fixed", "evidence": "e"}
        for i in range(30)
    ]
    bad = {**build_ready(), "findings": findings, "pr_number": "7", "summary": ""}
    with pytest.raises(ResultError) as error:
        validate_result({**bad, "kind": "build_ready"}, corr)
    details = error.value.details
    assert not any("more" in line for line in details)  # no cap at 10
    [severity] = [line for line in details if ".severity" in line]
    assert severity == (
        "result.findings.*.severity: Input should be 'BLOCKING', 'SHOULD_FIX', "
        "'FOLLOW_UP' or 'ADVISORY' [items 0-29]"
    )
    assert any(line.startswith("result.pr_number:") for line in details)
    assert any(line.startswith("result.summary:") for line in details)


def test_a_single_error_keeps_its_exact_location():
    corr = Correlation("P1", "S1", "N1", 0, "build")
    finding = {
        "id": "f",
        "source": "bot",
        "severity": "P2",
        "disposition": "fixed",
        "evidence": "e",
    }
    with pytest.raises(ResultError) as error:
        validate_result(
            {**build_ready(), "branch": "b", "kind": "build_ready", "findings": [finding]}, corr
        )
    assert error.value.details[0].startswith("result.findings.0.severity:")


# ------------------------------------------------------------------ R5: defaults


def test_safe_defaults_and_required_risks():
    triage = {
        k: v
        for k, v in TRIAGE.items()
        if k not in ("duplicate_issue", "missing_information", "labels")
    }
    parsed = validate_result({**triage, "kind": "triage"}, _corr("triage")).result.result
    assert parsed.duplicate_issue is None and parsed.missing_information == []  # type: ignore[union-attr]
    build = {
        k: v
        for k, v in {**build_ready(), "branch": "b"}.items()
        if k not in ("remediation_batches_used", "targeted_rechecks_used")
    }
    parsed = validate_result({**build, "kind": "build_ready"}, _corr("build_ready")).result.result
    assert parsed.remediation_batches_used == 0 and parsed.targeted_rechecks_used == 0  # type: ignore[union-attr]
    plan = {k: v for k, v in PLAN.items() if k != "risks"}
    with pytest.raises(ResultError, match="risks"):
        validate_result({**plan, "kind": "plan", "publication_kind": "contract"}, _corr("plan"))
    # duplicate still needs its issue; review/findings/verification stay required.
    with pytest.raises(ResultError, match="duplicate_issue"):
        validate_result(
            {**triage, "kind": "triage", "recommendation": "duplicate"}, _corr("triage")
        )
    for field in ("review", "findings", "verification", "release_readiness"):
        with pytest.raises(ResultError, match=field):
            validate_result(
                {**{k: v for k, v in build.items() if k != field}, "kind": "build_ready"},
                _corr("build_ready"),
            )


def test_schema_file_mirrors_the_defaults():
    from pathlib import Path

    schema = json.loads(
        (Path(protocol.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    defs = schema["$defs"]
    for name, model in (("triage", "triage"), ("build", "build_ready"), ("plan", "plan")):
        required = {n for n, f in RESULT_MODELS[model].model_fields.items() if f.is_required()}
        assert set(defs[name]["required"]) == required, name


# ------------------------------------------------------------------ templates (R1/R2/R7)


def _live_first_templates() -> set[str]:
    return {
        *d._FIRST_TEMPLATES.values(),
        *(
            d._REFERENCE_TEMPLATES[n]
            for n in d._REFERENCE_TEMPLATES
            if n.startswith(
                (
                    "triage-v15",
                    "triage-feedback-v12",
                    "triage-refresh-v3",
                    "epic-triage-v7",
                    "epic-plan-v6",
                )
            )
        ),
        d._RETRIAGE_TEMPLATE,
        d._REFRESH_TEMPLATE,
        d._EPIC_TRIAGE_TEMPLATE,
        d._EPIC_PLAN_TEMPLATE,
        d._AUTOPILOT_PLAN_TEMPLATE,
        d._REPLAN_TEMPLATE,
        d._REWORK_TEMPLATE,
        d._ITEMS_REWORK_TEMPLATE,
        d._CONFLICT_TEMPLATE,
    }


def _live_templates() -> set[str]:
    return {
        *_live_first_templates(),
        d._READINESS_WAKE_TEMPLATE,
        d._FINDINGS_WAKE_TEMPLATE,
        d._CONFLICT_WAKE_TEMPLATE,
        d._ANSWER_TEMPLATE,
        d._ANSWER_OTHER_TEMPLATE,
    }


def test_every_live_first_message_carries_the_generated_result_contract():
    for name in _live_first_templates():
        text = (TEMPLATES / name).read_text("utf-8")
        assert "{result_contract}" in text, name
        assert "factory_submit_result (kind" not in text, name  # points at typed tools
        assert re.search(r"factory_submit_(triage|epic_triage|plan|build_ready)\b", text), name


def test_templates_never_teach_values_the_schema_rejects():
    """P-levels belong to deferred items (and a review bot's badges), never to findings."""
    rendered_rounds = d.rounds_line(1, 3)
    for name, text in [
        *((n, (TEMPLATES / n).read_text("utf-8")) for n in _live_templates()),
        ("rounds_line", rendered_rounds),
    ]:
        flat = " ".join(text.split())
        for sentence in re.split(r"(?<=[.;])\s", flat):
            if re.search(r"\bP[0-3]s?\b", sentence) and "priority" not in sentence:
                assert "badge" in sentence or "Deferred" in sentence, (name, sentence)
        assert "P1 finding" not in flat and "P1s" not in flat and "P2/P3" not in flat, name
    for name in (
        d._FIRST_TEMPLATES[SessionKind.BUILD],
        d._REWORK_TEMPLATE,
        d._ITEMS_REWORK_TEMPLATE,
        d._CONFLICT_TEMPLATE,
        d._READINESS_WAKE_TEMPLATE,
        d._FINDINGS_WAKE_TEMPLATE,
    ):
        flat = " ".join((TEMPLATES / name).read_text("utf-8").split())
        assert "severity is BLOCKING, SHOULD_FIX, FOLLOW_UP or ADVISORY" in flat, name
        assert "Deferred items use their own scale: severity P0, P1, P2 or P3" in flat, name
    # The old P-level wording stays only in the pinned old versions.
    assert "only P1 findings" in (TEMPLATES / "build-v21.txt").read_text("utf-8").replace("\n", " ")


def test_contract_lists_shape_example_and_last_result():
    text = d.result_contract(
        ("triage", "blocked"), {"kind": "triage", "summary": "old"}, _corr("triage")
    )
    assert RESULT_SHAPES["triage"] in text and RESULT_SHAPES["blocked"] in text
    assert json.dumps(RESULT_EXAMPLES["triage"], separators=(",", ":")) in text
    assert "factory_submit_triage" in text and "factory_submit_blocked" in text
    assert '{"kind":"triage","summary":"old"}' in text
    huge = {"kind": "build_ready", "summary": "x" * (d.MAX_RESULT_ECHO_CHARS + 1)}
    assert "last_accepted_result" in d.result_contract(("build_ready",), huge, _corr("x"))


# ------------------------------------------------------------------ service rigs


def _first(rig: Rig, session_id: str) -> EffectIntent:
    return EffectIntent(
        effect_id=f"ef_first_{session_id}",
        kind=EffectKind.SEND_MESSAGE,
        parcel_id=P,
        target=session_id,
        preconditions=Preconditions(1, 0, session_id=session_id),
        args={"purpose": MessagePurpose.FIRST.value},
    )


@pytest.mark.asyncio
async def test_first_messages_and_status_expose_the_run_shapes(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await triaged(rig)
        run = await rig.run()
        await rig.tools.directory.stage_spec(run.session_id)
        text = await rig.tools.directory.message_text(_first(rig, run.session_id))
        assert text is not None and RESULT_SHAPES["triage"] in text
        assert RESULT_SHAPES["epic_triage"] not in text  # not an epic
        assert "Your last accepted result" not in text  # a first triage
        status = await rig.tools.get_status(root)
        assert status["result_kinds"] == ["triage", "blocked"]
        assert status["result_fields"]["triage"]["example"] == RESULT_EXAMPLES["triage"]
        assert status["result_fields"]["triage"]["tool"] == "factory_submit_triage"
        assert status["last_accepted_result"] is None
        await rig.tools.get_feedback(root)
        await rig.submit(root, "triage", TRIAGE)
        status = await rig.tools.get_status(root)
        assert status["last_accepted_result"]["summary"] == TRIAGE["summary"]


@pytest.mark.asyncio
async def test_a_rerun_echoes_the_last_accepted_result(service_config: ServiceConfig):
    from tests.service.test_owner_feedback_mcp import say

    async with started(service_config) as rig:
        root = await triaged(rig)
        await rig.submit(root, "triage", TRIAGE)
        await rig.quiesce()
        await say(rig, "this is a P1, please re-triage")
        await rig.active(SessionKind.TRIAGE)
        run = await rig.run()
        await rig.tools.directory.stage_spec(run.session_id)
        snapshot = rig.tools.directory.dispatch_snapshot(run.session_id)
        assert snapshot["template"] == d._RETRIAGE_TEMPLATE
        text = await rig.tools.directory.message_text(_first(rig, run.session_id))
        assert text is not None and "Your last accepted result for this stage" in text
        assert '"summary":"Guard is untested."' in text


def _sent_first(rig: Rig, session_id: str) -> EffectIntent | None:
    """The first-message effect the executor actually ran for ``session_id``."""
    sends = [
        e
        for e in rig.executed(EffectKind.SEND_MESSAGE)
        if e.preconditions.session_id == session_id
        and e.args.get("purpose") == MessagePurpose.FIRST.value
    ]
    return sends[0] if sends else None


async def _build_with_first_send(rig: Rig, *outcomes: Any) -> tuple[str, str, Any]:
    """A build run whose first message gets ``outcomes`` (default: acknowledged)."""
    root = await planning(rig)
    rig.github.script(
        EffectKind.PUBLISH_CONTRACT,
        Ack(remote_id="c-1", detail={"verified": True, "posted_at_us": 1}),
    )
    receipt = await rig.submit(root, "plan", PLAN)

    async def published() -> bool:
        return (await rig.parcel()).current_contract is not None

    await eventually(published)
    if outcomes:
        rig.omnigent.script(EffectKind.SEND_MESSAGE, *outcomes)
    await rig.send(ev.ApprovePlan(via=Via.DRAG))
    await rig.quiesce()

    async def sent() -> bool:
        run = await rig.run()
        return (
            run is not None
            and run.kind == SessionKind.BUILD
            and (_sent_first(rig, run.session_id) is not None)
        )

    await eventually(sent)
    return await rig.root(), receipt["plan_hash"], await rig.run()


async def _render(rig: Rig, effect: EffectIntent) -> str:
    sid = effect.preconditions.session_id
    assert sid is not None
    await rig.tools.directory.stage_spec(sid)
    text = await rig.tools.directory.message_text(effect)
    assert text is not None
    return text


@pytest.mark.asyncio
async def test_a_delivered_first_message_hash_satisfies_the_plan_read_gate(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root, plan_hash, run = await _build_with_first_send(rig)
        first = _sent_first(rig, run.session_id)
        assert first is not None
        text = await _render(rig, first)  # as the adapter renders it for the send
        assert f"The approved plan hash is {plan_hash}" in text
        assert RESULT_SHAPES["build_ready"] in text and "Result scales" in text
        # The fake acknowledged that exact effect: the hash was given to the run.
        assert await rig.tools.directory.plan_hashes_given(run.session_id) == (plan_hash,)
        # A restarted daemon (a fresh directory) reads the same evidence.
        restarted = ServiceDispatchDirectory(rig.service.db, service_config)
        assert await restarted.plan_hashes_given(run.session_id) == (plan_hash,)
        # Absent and mismatched hashes are still refused (current approved hash check).
        with pytest.raises(FactoryToolError, match="plan_hash: required"):
            await rig.submit(root, "build_ready", build_ready())
        with pytest.raises(FactoryToolError, match="does not match the current approved plan"):
            await rig.submit(root, "build_ready", build_ready(), plan_hash="0" * 64)
        await rig.tools.get_feedback(root)
        # No factory_get_plan call: the hash this run was given is enough.
        receipt = await rig.submit(root, "build_ready", build_ready(), plan_hash=plan_hash)
        assert receipt["accepted"] is True


@pytest.mark.asyncio
async def test_a_rendered_but_unsent_first_message_gives_no_credit_across_restart(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root, plan_hash, run = await _build_with_first_send(rig)
        # Render a first-message effect that is never sent (no ack, no reconciliation).
        text = await _render(rig, _first(rig, run.session_id))
        assert f"The approved plan hash is {plan_hash}" in text
        restarted = ServiceDispatchDirectory(rig.service.db, service_config)
        assert await restarted.plan_hashes_given(run.session_id) == ()
        await rig.tools.get_feedback(root)
        with pytest.raises(FactoryToolError, match="call factory_get_plan in this run first"):
            await rig.submit(root, "build_ready", build_ready(), plan_hash=plan_hash)


@pytest.mark.asyncio
async def test_a_failed_first_send_gives_no_credit(service_config: ServiceConfig):
    async with started(service_config) as rig:
        _, _, run = await _build_with_first_send(rig, DefinitiveFailure("denied"))
        first = _sent_first(rig, run.session_id)
        assert first is not None
        await _render(rig, first)
        assert await rig.tools.directory.plan_hashes_given(run.session_id) == ()


async def _reconcile(rig: Rig, effect: EffectIntent, *, delivered: bool) -> Any:
    return await rig.service.apply_event(
        rig.factory.make(
            ev.EffectReconciled(
                effect_id=effect.effect_id,
                session_id=effect.preconditions.session_id,
                delivered=delivered,
                item_id="item-r" if delivered else "",
            ),
            provenance=ev.Provenance.ADAPTER,
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("delivered", [False, True])
async def test_an_ambiguous_first_send_counts_only_once_reconciled_delivered(
    service_config: ServiceConfig, delivered: bool
):
    async with started(service_config) as rig:
        _, plan_hash, run = await _build_with_first_send(rig, AmbiguousWrite("timeout"))
        first = _sent_first(rig, run.session_id)
        assert first is not None
        await _render(rig, first)
        # Unknown, not reconciled: no credit.
        assert await rig.tools.directory.plan_hashes_given(run.session_id) == ()

        async def unknown() -> bool:
            return any(u.effect_id == first.effect_id for u in (await rig.parcel()).unknown_effects)

        await eventually(unknown)
        assert (await _reconcile(rig, first, delivered=delivered)).accepted
        restarted = ServiceDispatchDirectory(rig.service.db, service_config)
        expected = (plan_hash,) if delivered else ()
        assert await restarted.plan_hashes_given(run.session_id) == expected


# ------------------------------------------------------------------ R3: relayed answers


async def say(rig: Rig, text: str, comment_id: int) -> None:
    rig.factory.tick()
    guid = f"d-comment-{comment_id}"
    payload = {"action": "created", "comment": {"id": comment_id, "body": text}}
    await rig.service.db.call(
        lambda store: store.append_delivery(
            DeliveryRecord(guid, "issue_comment", json.dumps(payload).encode(), {})
        )
    )
    event = rig.factory.make(
        ev.PlanFeedback(text_digest=text), event_id=f"github:comment:{comment_id}:created"
    )
    result = await rig.service.apply_event(replace(event, delivery_guid=guid))
    assert result.accepted


def reactions(rig: Rig) -> list[tuple[int, str]]:
    return [
        (e.args["comment_id"], e.args["content"]) for e in rig.executed(EffectKind.REACT_COMMENT)
    ]


async def _asked_and_answered(rig: Rig, comment_id: int) -> tuple[str, Any]:
    root = await planning(rig)
    await rig.tools.get_feedback(root)
    asked = await rig.ask(root, "Keep the v1 API?", options=["keep", "drop"])
    await rig.quiesce()
    rig.factory.tick(10_000_000)
    await say(rig, "Keep it.", comment_id)

    async def acked() -> bool:
        decision = (await rig.parcel()).decision(asked["decision_id"])
        return decision is not None and decision.relay_acked

    await eventually(acked)
    return root, asked


@pytest.mark.asyncio
async def test_an_acknowledged_answer_relay_is_the_read_of_its_only_unread_comment(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root, _ = await _asked_and_answered(rig, 7001)

        async def thumbs() -> bool:
            return (7001, "+1") in reactions(rig)

        await eventually(thumbs)  # 👍 at relay time, not only after factory_get_feedback
        parcel = await rig.parcel()
        assert parcel.unread_feedback == ()
        # The plan resubmits directly, with no factory_get_feedback call.
        assert (await rig.submit(root, "plan", PLAN))["accepted"] is True
        # A later read finds nothing unread: no second 👍, and the 👀 was posted once.
        await rig.tools.get_feedback(root)
        assert reactions(rig).count((7001, "+1")) == 1
        # 👀 posts at most once and never after the 👍 (a not-yet-sent 👀 is superseded).
        seen = reactions(rig)
        assert seen.count((7001, "eyes")) <= 1
        if (7001, "eyes") in seen:
            assert seen.index((7001, "eyes")) < seen.index((7001, "+1"))


@pytest.mark.asyncio
async def test_another_unread_comment_still_gates_and_is_quoted_before_validation(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root = await planning(rig)
        await rig.tools.get_feedback(root)
        asked = await rig.ask(root, "Keep the v1 API?")
        await rig.quiesce()
        rig.factory.tick(10_000_000)
        await say(rig, "Keep it.", 7101)

        async def acked() -> bool:
            decision = (await rig.parcel()).decision(asked["decision_id"])
            return decision is not None and decision.relay_acked

        await eventually(acked)
        await say(rig, "and rename the flag", 7102)  # a second, unread comment
        # Gate first: even an invalid payload hears about the unread comment.
        with pytest.raises(FactoryToolError) as error:
            await rig.submit(root, "plan", {"approach": ""})
        message = str(error.value)
        assert "factory_get_feedback" in message and "invalid result" not in message
        assert "and rename the flag" in message and "BEGIN OWNER COMMENT" in message
        # The relayed answer was read at its ack (it was then the only unread comment);
        # the later comment still gates (tests/test_answer_relay_read.py covers the ack
        # arriving after a second comment).
        await rig.tools.get_feedback(root)
        assert (await rig.submit(root, "plan", PLAN))["accepted"] is True

        async def both() -> bool:
            return {(7101, "+1"), (7102, "+1")} <= set(reactions(rig))

        await eventually(both)
        assert reactions(rig).count((7101, "+1")) == 1


@pytest.mark.asyncio
async def test_an_unacknowledged_relay_does_not_count_as_read(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await planning(rig)
        await rig.tools.get_feedback(root)
        await say(rig, "use the helper", 7201)  # an ordinary comment, never relayed
        with pytest.raises(FactoryToolError, match="factory_get_feedback"):
            await rig.submit(root, "plan", PLAN)


def test_answer_templates_point_at_feedback_when_other_comments_may_be_unread():
    for name in (d._ANSWER_TEMPLATE, d._ANSWER_OTHER_TEMPLATE):
        flat = " ".join((TEMPLATES / name).read_text("utf-8").split())
        assert "If other owner comments may be unread" in flat, name
        assert "call factory_get_feedback first" in flat, name
    assert d._ANSWER_TEMPLATE == "answer-v4.txt" and d._ANSWER_OTHER_TEMPLATE == "answer-v5.txt"


# ------------------------------------------------------------------ R7: typed tools


@pytest.mark.asyncio
async def test_typed_tools_advertise_the_real_models(service_config: ServiceConfig):
    async with started(service_config) as rig:
        server = build_mcp_server(rig.tools, service_config)
        listed = {tool.name: tool for tool in await server.list_tools()}
    for kind in SUBMITTED:
        name = f"factory_submit_{kind}"
        assert name in TOOL_NAMES and name in listed
        schema = listed[name].inputSchema
        assert schema == typed_submit_schema(kind)
        model = RESULT_MODELS[kind]
        result = schema["$defs"][model.__name__]
        assert set(result["properties"]) == set(model.model_fields)
        assert "kind" not in result["required"]
        assert set(schema["required"]) >= {"session_id", "run_id", "result"}
    build = listed["factory_submit_build_ready"].inputSchema
    assert build["$defs"]["Finding"]["properties"]["severity"]["enum"] == [
        "BLOCKING",
        "SHOULD_FIX",
        "FOLLOW_UP",
        "ADVISORY",
    ]
    assert "plan_hash" in build["required"]
    generic = listed["factory_submit_result"].description or ""
    for kind in SUBMITTED:
        assert f"factory_submit_{kind}" in generic


@pytest.mark.asyncio
async def test_typed_and_generic_submits_share_one_path(service_config: ServiceConfig):
    async with started(service_config) as rig:
        server = build_mcp_server(rig.tools, service_config)
        root = await triaged(rig)
        run_id = await rig.run_id(root)
        await rig.tools.get_feedback(root)

        async def call(name: str, args: dict[str, Any]) -> Any:
            return await server.call_tool(name, {"session_id": root, "run_id": run_id, **args})

        # Same validation: the typed tool reports the generic tool's grouped errors.
        with pytest.raises(Exception, match="invalid result") as typed_error:
            await call("factory_submit_triage", {"result": {"summary": ""}})
        with pytest.raises(FactoryToolError) as generic_error:
            await rig.submit(root, "triage", {"summary": ""})
        assert str(generic_error.value) in str(typed_error.value)
        # Same gate: wrong kind for the run.
        with pytest.raises(Exception, match="not allowed for this triage run"):
            await call("factory_submit_plan", {"result": PLAN})
        # Same mutation and receipt: an exact retry through the other tool replays it.
        await call("factory_submit_triage", {"result": TRIAGE})
        retry = await rig.submit(root, "triage", TRIAGE)
        assert retry["accepted"] is True and retry["replayed"] is True
        assert retry["slot"] == "triage" and retry["kind"] == "triage"
        # A kind field that contradicts the tool is refused as for the generic tool.
        with pytest.raises(Exception, match=r"result\.kind differs from kind"):
            await call(
                "factory_submit_blocked", {"result": {"kind": "triage", "reason": "x", "done": []}}
            )


# ------------------------------------------------------------------ F3: run-context examples


def _ctx(stage: str, **kw: Any) -> Correlation:
    return Correlation("P1", "S1", "N1", 0, stage, issue_number=40, **kw)  # type: ignore[arg-type]


#: Every run context that changes what a kind's result must contain (validate_result):
#: (kind, context, requirement fragment). Others (follow-up depth, round budget,
#: checkpoint) only restrict optional fields the minimal examples leave out.
CONTEXTS = [
    ("plan", _ctx("build", waiver_build=True), 'publication_kind "info"'),
    ("plan", _ctx("plan", epic_part=True), "include epic_fit"),
    ("epic_triage", _ctx("triage", sub_issues=frozenset({41, 42}), epic_plan=True), "plan"),
    ("plan", _ctx("plan", bundle_members=frozenset({12, 13})), "#12, #13"),
]


@pytest.mark.parametrize(("kind", "corr", "needs"), CONTEXTS)
def test_examples_are_valid_for_each_run_context(kind: str, corr: Correlation, needs: str):
    with pytest.raises(ResultError):
        validate_result(RESULT_EXAMPLES[kind], corr)  # the plain example would be refused
    example = protocol.run_example(kind, corr)
    assert example is not None
    validate_result(example, corr)
    assert any(needs in note for note in protocol.context_requirements(kind, corr))
    text = d.result_contract((kind, "blocked"), None, corr)
    assert json.dumps(example, separators=(",", ":")) in text
    assert json.dumps(RESULT_EXAMPLES[kind], separators=(",", ":")) not in text
    fields = d.result_fields((kind,), corr)[kind]
    assert fields["example"] == example and fields["requires"]


@pytest.mark.parametrize(
    "corr",
    [
        _ctx("build", follow_up_depth=1, follow_up_max_depth=1, review_rounds_budget=2),
        _ctx("build", in_checkpoint=True),
        _ctx("triage", sub_issues=frozenset()),  # an epic whose list is incomplete
    ],
)
def test_restricting_contexts_keep_valid_examples_and_say_so(corr: Correlation):
    kinds = (
        ("epic_triage", "blocked") if corr.sub_issues is not None else ("build_ready", "blocked")
    )
    if corr.in_checkpoint:
        kinds = ("blocked",)
    for kind in kinds:
        example = protocol.run_example(kind, corr)
        assert example is not None, kind
        validate_result(example, corr)
    if corr.follow_up_depth:
        assert any(
            "itself a follow-up" in n for n in protocol.context_requirements("blocked", corr)
        )


def test_a_context_without_a_valid_example_shows_the_requirements(monkeypatch):
    monkeypatch.setattr(d, "run_example", lambda kind, corr: None)
    corr = _ctx("plan", epic_part=True)
    text = d.result_contract(("plan",), None, corr)
    assert "No minimal example fits this run's context" in text
    assert "This run: part of an autopilot epic: include epic_fit" in text
    assert "Minimal valid example" not in text


@pytest.mark.asyncio
async def test_status_examples_validate_under_the_submission_correlation(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root, _ = await building(rig)
        status = await rig.tools.get_status(root)
        parcel = await rig.parcel()
        run = await rig.run()
        corr = await rig.tools._correlation(parcel, run, in_checkpoint=False)
        for kind in status["result_kinds"]:
            example = status["result_fields"][kind]["example"]
            assert example is not None, kind
            validate_result(example, corr)


def test_grouped_errors_keep_the_message_at_the_list_limit():
    corr = Correlation("P1", "S1", "N1", 0, "build")
    findings = [
        {"id": str(i), "source": "bot", "severity": "P1", "disposition": "fixed", "evidence": "e"}
        for i in range(200)
    ]
    for i in range(0, 200, 3):  # scattered valid items: many ranges
        findings[i]["severity"] = "BLOCKING"
    bad = {**build_ready(), "branch": "b", "kind": "build_ready", "findings": findings}
    with pytest.raises(ResultError) as error:
        validate_result(bad, corr)
    [line] = error.value.details
    assert line.startswith(
        "result.findings.*.severity: Input should be 'BLOCKING', 'SHOULD_FIX', "
        "'FOLLOW_UP' or 'ADVISORY' [items 1-2, 4-5,"
    )
    assert re.search(r", \+\d+ more\]$", line)  # omitted positions are counted
    assert len(line) <= protocol.MAX_ERROR_DETAIL_CHARS
    # Every finding wrong: one contiguous range.
    for item in findings:
        item["severity"] = "P2"
    with pytest.raises(ResultError) as error:
        validate_result(bad, corr)
    assert error.value.details[0].endswith("[items 0-199]")
