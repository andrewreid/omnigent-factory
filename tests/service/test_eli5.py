"""Plain-English explanations lead publications and their sections in the status dashboard."""

from dataclasses import replace

import pytest

from omnigent_factory.core.contract_view import extract_contract_section, marker_hash
from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.protocol import epic_plan_hash
from omnigent_factory.service.directory import (
    _decision_text,
    _public_result,
    _ready_text,
    ready_text,
    stage_result_text,
)
from omnigent_factory.service.mcp import FactoryToolError
from omnigent_factory.testing.harness import HEAD
from tests.service.test_comment_style import _plain, _render
from tests.service.test_mcp import eventually, planning, started
from tests.service.test_plan_comment import FIXTURE
from tests.service.test_plan_comment import _render as render_plan
from tests.service.test_status_comment import CTX, ISSUE, TRIAGE, effect, rig, triage_harness
from tests.test_epic_triage_protocol import epic_result
from tests.test_protocol import build_result, plan_result, triage_result

ELI5 = "We will fix how the total is counted. This will make the number match your hours."
LONG_ELI5 = "These hours belong in the total. " * 17 + "Every hour will count."


@pytest.mark.parametrize(
    "result",
    [triage_result(), epic_result(), epic_result(plan={}), plan_result(), build_result()],
)
def test_eli5_leads_each_public_result_and_absence_preserves_detail(result):
    old = _public_result(result)
    text = _public_result({**result, "eli5": ELI5})
    section = f"#### ELI5\n\n{ELI5}\n\n"
    if result["kind"] == "build_ready":
        assert text.startswith(section)
    else:
        assert text.split("\n\n", 1)[1].startswith(section)
    if result.get("plan") is not None:
        # Epic approval binds the whole result, including the explanation.
        old = old.replace(
            epic_plan_hash(result)[:12], epic_plan_hash({**result, "eli5": ELI5})[:12]
        )
    assert text.replace(section, "", 1) == old
    without_text = _public_result({**result, "eli5": None})
    original = _public_result(result)
    if result.get("plan") is not None:
        original = original.replace(
            epic_plan_hash(result)[:12], epic_plan_hash({**result, "eli5": None})[:12]
        )
    assert without_text == original
    assert stage_result_text({**result, "eli5": ELI5}) == text


def test_contract_eli5_is_before_and_outside_the_approved_section(service_config):
    old = render_plan(service_config)
    text = render_plan(service_config, {**FIXTURE["plan_result"], "eli5": ELI5})
    section = f"#### ELI5\n\n{ELI5}\n\n"
    assert text.split("\n\n", 1)[1].startswith(section)
    assert text.replace(section, "", 1) == old
    assert extract_contract_section(text) == extract_contract_section(old)
    assert marker_hash(text) == marker_hash(old)
    unsafe = render_plan(service_config, {**FIXTURE["plan_result"], "eli5": "@owner <!-- x -->"})
    assert "@\u200bowner &lt;!-- x -->" in unsafe


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "result", "args"),
    [
        (EffectKind.PUBLISH_TRIAGE, triage_result(), {}),
        (EffectKind.PUBLISH_TRIAGE, epic_result(), {}),
        (EffectKind.PUBLISH_TRIAGE, epic_result(plan={}), {}),
        (EffectKind.PUBLISH_REPORT, plan_result(), {"report": "info-plan"}),
        (
            EffectKind.PUBLISH_REPORT,
            build_result(head_sha=HEAD),
            {"report": "ready", "pr_number": 7, "head_sha": HEAD},
        ),
    ],
)
async def test_published_eli5_survives_long_detail_and_is_neutralised(
    service_config, kind, result, args
):
    result.update(eli5=ELI5 + " @owner <!-- x -->", summary="long " * 3000, approach="long " * 3000)
    text = await _render(service_config, kind, result, **args)
    assert text.split("\n\n", 1)[1].startswith(f"#### ELI5\n\n{ELI5}")
    assert "@\u200bowner &lt;!-- x -->" in text
    assert len(text) <= 8000 and text.endswith("[truncated]")
    _plain(text)


def test_ready_report_eli5_matches_head_and_is_shared_with_status():
    args = {"pr_number": 7, "head_sha": HEAD}
    result = build_result(head_sha=HEAD, eli5=ELI5)
    text = _ready_text(args, result, "green", "o/r")
    assert text.startswith(f"### PR #7 is ready for review\n\n#### ELI5\n\n{ELI5}\n\n")
    assert ready_text(args, result, "green", "o/r") == text
    assert "ELI5" not in _ready_text({**args, "head_sha": "f" * 40}, result, "green", "o/r")


def test_question_eli5_survives_4000_character_cap():
    text = _decision_text({"eli5": ELI5, "summary": "@owner " * 1000})
    assert text.startswith(f"#### ELI5\n\n{ELI5}\n\n")
    assert len(text) <= 4000
    assert "@owner" not in text
    assert _decision_text({"summary": "Which one?"}) == "Which one?"
    assert "ELI5" not in _decision_text({"summary": "Run it?", "eli5": ELI5, "permission": True})


@pytest.mark.asyncio
@pytest.mark.parametrize("explanation", [ELI5, LONG_ELI5])
async def test_mcp_question_keeps_eli5_separate_and_ignores_it_for_dedup(
    service_config, explanation
):
    async with started(service_config) as r:
        root = await planning(r)
        first = await r.ask(
            root, "Which total?", eli5=explanation, options=["all hours", "work hours"]
        )
        again = await r.ask(
            root, "Which total?", eli5="Another explanation.", options=["all hours", "work hours"]
        )
        assert again == {**first, "replayed": True}

        async def posted():
            return any(
                e.args.get("template") == "decision" for e in r.executed(EffectKind.POST_COMMENT)
            )

        await eventually(posted)
        [comment] = [
            e for e in r.executed(EffectKind.POST_COMMENT) if e.args.get("template") == "decision"
        ]
        assert comment.args["eli5"] == explanation
        assert comment.args["summary"] == "Which total?\n\n- all hours\n- work hours"
        from omnigent_factory.service.directory import PublicationRenderer
        from omnigent_factory.service.status_comment import (
            StatusComments,
            issue_headline,
            render_issue,
        )

        text = await PublicationRenderer(r.tools.directory, service_config)(comment)
        assert text == f"#### ELI5\n\n{explanation}\n\nWhich total?\n\n- all hours\n- work hours"
        # _question reads the stored effect, exactly as the live dashboard does.
        status = StatusComments(r.service, None, r.tools.directory, str)
        question = await status._question(await r.parcel())
        assert question is not None and question.summary == comment.args["summary"]
        assert explanation not in question.summary
        view = await status._issue_view(await r.parcel())
        dashboard = render_issue(view, [], issue_headline(view))
        assert "**Question:**" in dashboard and "Which total?" in dashboard
        assert f"**Question:**\n\n{text}" in dashboard
        assert issue_headline(view) == "Waiting on you: Which total? - all hours - work hours"


@pytest.mark.asyncio
@pytest.mark.parametrize("eli5", ["", " ", "x" * 601])
async def test_mcp_rejects_invalid_question_eli5(service_config, eli5):
    async with started(service_config) as r:
        root = await planning(r)
        with pytest.raises(FactoryToolError, match="eli5:"):
            await r.ask(root, "Which total?", eli5=eli5)


@pytest.mark.asyncio
async def test_triage_eli5_is_folded_into_status_without_a_separate_comment(service_config):
    h, sid = triage_harness()
    async with rig(service_config, h) as r:
        r.write_result(sid, {**TRIAGE, "eli5": ELI5})
        outcome = await r.adapter().execute(effect(EffectKind.PUBLISH_TRIAGE, sid), CTX)
        assert isinstance(outcome, Ack)
        [status] = r.server.status()
        assert r.server.posts() == [status["body"]]
        assert outcome.remote_id == str(status["id"])
        assert f"#### Triage\n\n#### ELI5\n\n{ELI5}\n\n{TRIAGE['summary']}" in status["body"]


@pytest.mark.asyncio
async def test_ready_notification_and_status_both_have_eli5(service_config):
    async with rig(service_config) as r:
        parcel = await r.parcel()
        assert parcel.readiness is not None
        sid = parcel.readiness.session_id
        r.write_result(sid, build_result(head_sha=HEAD, eli5=ELI5))
        publication = effect(EffectKind.PUBLISH_REPORT, report="ready", pr_number=7, head_sha=HEAD)
        outcome = await r.adapter().execute(publication, CTX)
        assert isinstance(outcome, Ack)
        status, notification = r.server.posts()
        assert f"#### Pull request\n\n#### ELI5\n\n{ELI5}\n\n" in status
        [comment] = r.server.status(ISSUE)
        assert notification.startswith(
            f"PR #7 is ready for review — {comment['html_url']}\n\n#### ELI5\n\n{ELI5}"
        )
        assert isinstance(await r.adapter().execute(publication, CTX), Ack)
        assert len(r.server.posts()) == 2
        assert (
            await r.status.publication_eli5(
                replace(publication, args={**publication.args, "head_sha": "f" * 40})
            )
            == ""
        )


@pytest.mark.asyncio
async def test_checkpoint_eli5_is_folded_when_present_in_the_stored_report(service_config):
    # Checkpoint's protocol is unchanged; render a stored explanation when supplied.
    async with rig(service_config) as r:
        sid = (await r.parcel()).current_session_id or ""
        r.write_result(
            sid,
            {
                "kind": "checkpoint",
                "eli5": LONG_ELI5,
                "done": ["Counted the hours."],
                "remaining": ["Check the total."],
            },
        )
        publication = effect(EffectKind.PUBLISH_REPORT, sid, report="checkpoint")
        assert isinstance(await r.adapter().execute(publication, CTX), Ack)
        [status] = r.server.status()
        assert r.server.posts() == [status["body"]]
        assert f"#### Last checkpoint\n\n#### ELI5\n\n{LONG_ELI5}\n\n**Done:**" in status["body"]


def test_epic_waiting_question_keeps_long_eli5_outside_the_short_line():
    from omnigent_factory.service.directory import eli5_text
    from omnigent_factory.service.status_comment import Question, epic_headline, render_epic
    from tests.service.test_status_comment import epic_view

    view = epic_view()
    question = Question("Which hours?", eli5=eli5_text({"eli5": LONG_ELI5}))
    view = replace(
        view, subs=tuple(replace(s, question=question) if s.number == 46 else s for s in view.subs)
    )
    text = render_epic(view, [], epic_headline(view))
    assert f"#46 Part 46: Which hours?\n\n#### ELI5\n\n{LONG_ELI5}" in text


@pytest.mark.asyncio
async def test_long_triage_eli5_precedes_detail_in_the_status(service_config):
    h, sid = triage_harness()
    async with rig(service_config, h) as r:
        r.write_result(sid, {**TRIAGE, "eli5": LONG_ELI5, "summary": "detail " * 2500})
        assert isinstance(
            await r.adapter().execute(effect(EffectKind.PUBLISH_TRIAGE, sid), CTX), Ack
        )
        [status] = r.server.status()
        assert f"#### Triage\n\n#### ELI5\n\n{LONG_ELI5}\n\ndetail" in status["body"]


@pytest.mark.asyncio
async def test_status_body_cap_keeps_ready_eli5_before_large_deferred_section(service_config):
    async with rig(service_config) as r:
        parcel = await r.parcel()
        assert parcel.readiness is not None
        r.write_result(parcel.readiness.session_id, build_result(head_sha=HEAD, eli5=LONG_ELI5))
        r.section[0] = "Details. " * 10000
        assert (await r.status.refresh(parcel.parcel_id, urgent=True)).ok
        [status] = r.server.status()
        body, marker = status["body"].rsplit("\n\n<!-- omnigent-factory status", 1)
        assert marker and len(body) <= 60000
        assert "[truncated]" in status["body"]
        assert f"#### Pull request\n\n#### ELI5\n\n{LONG_ELI5}\n\n" in status["body"]
