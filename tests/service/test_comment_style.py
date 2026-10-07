"""Bot comments read like a fellow developer in GitHub (owner request, 2026-09-30).

Plain GitHub Markdown: no quote blocks around the agent's text, no Omnigent links, no
"Factory:" prefixes or internal finding labels, no how-to/next-step lines, and the agent's
Markdown (paragraphs, lists, headings, code) is published as written. Hidden markers
stay.
"""

from __future__ import annotations

from typing import Any

import pytest

from omnigent_factory.core.effects import EffectIntent, EffectKind, Preconditions
from omnigent_factory.core.types import Parcel
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import PublicationRenderer
from omnigent_factory.testing.harness import HEAD, Harness

pytestmark = pytest.mark.asyncio

P = "I_parcel_1"

SUMMARY = (
    "Fixed the period total for leave-only entries.\n\n"
    "- totals now sum every hour line\n"
    "- leave-only rows show their leave code\n\n"
    "#### Notes\n\n"
    "```ts\nconst total = sum(lines);\n```"
)

FORBIDDEN = (
    "Open in Omnigent",
    "/c/",
    "Factory:",
    "Factory ",
    "Move the card",
    "Next:",
    "review and merge",
    "How to respond",
    "/approve",
    "/decide",
    "omnigent-factory resume",
    "\n> ",
)


class _Db:
    def __init__(self, parcel: Parcel) -> None:
        self.parcel = parcel

    async def call(self, fn: Any) -> Parcel:
        return self.parcel


class _Directory:
    def __init__(self, parcel: Parcel, result: dict[str, Any]) -> None:
        self.db = _Db(parcel)
        self.result = result

    def latest_result(self, session_id: str) -> dict[str, Any]:
        return {"factory_result": {"result": self.result}}

    def plan_result_for(self, session_id: str, canonical: str) -> None:
        return None


def _ready_parcel() -> Parcel:
    h = Harness()
    h.to_building(P)
    h.build_ready(P)
    parcel = h.p()
    assert parcel.readiness is not None and parcel.current_session is not None
    assert parcel.current_session.root_id is not None  # a link would have been possible
    return parcel


def _effect(kind: EffectKind, parcel: Parcel, **args: Any) -> EffectIntent:
    sid = parcel.current_session.session_id if parcel.current_session else None
    return EffectIntent(
        effect_id="ef_style",
        kind=kind,
        parcel_id=parcel.parcel_id,
        target=parcel.parcel_id,
        preconditions=Preconditions(1, 0, session_id=sid),
        args={"session_id": sid, **args},
    )


async def _render(
    config: ServiceConfig, kind: EffectKind, result: dict[str, Any], **args: Any
) -> str:
    parcel = _ready_parcel()
    renderer = PublicationRenderer(_Directory(parcel, result), config)  # type: ignore[arg-type]
    text = await renderer(_effect(kind, parcel, **args))
    assert text is not None
    return text


def _plain(text: str) -> None:
    for bad in FORBIDDEN:
        assert bad not in text, (bad, text)
    assert not text.startswith(">")


async def test_ready_report_is_plain_markdown_with_the_agents_summary_as_written(
    service_config: ServiceConfig,
):
    result = {
        "kind": "build_ready",
        "summary": SUMMARY,
        "head_sha": HEAD,
        "pr_number": 7,
        "findings": [
            {
                "id": "F2",
                "severity": "minor",
                "source": "codex",
                "disposition": "advisory",
                "evidence": "not reachable",
            }
        ],
    }
    text = await _render(
        service_config,
        EffectKind.PUBLISH_REPORT,
        result,
        report="ready",
        pr_number=7,
        head_sha=HEAD,
    )
    _plain(text)
    assert text.startswith(
        "### PR #7 is ready for review\n\n" + SUMMARY + "\n"
    )  # B8: not flattened
    assert "F2" not in text and "- minor (codex): advisory — not reachable" in text


async def test_triage_comment_has_no_how_to_line(service_config: ServiceConfig):
    result = {
        "kind": "triage",
        "summary": SUMMARY,
        "recommendation": "plan",
        "priority": "P2",
        "size": "S",
    }
    text = await _render(service_config, EffectKind.PUBLISH_TRIAGE, result)
    _plain(text)
    assert text.startswith("### Triage\n\n" + SUMMARY)
    assert "Scoped" not in text and "waive" not in text


async def test_blocked_report_keeps_the_reason_markdown_without_quote_or_how_to(
    service_config: ServiceConfig,
):
    reason = "I can't finish:\n\n- the API returns 500\n- the fixture is missing"
    result = {"kind": "blocked", "reason": reason, "done": ["reproduced"]}
    text = await _render(service_config, EffectKind.PUBLISH_REPORT, result, report="blocked")
    _plain(text)
    assert reason in text and "Fix the cause" not in text


@pytest.mark.parametrize(
    "template",
    [
        "checkpoint",
        "ready-blocked",
        "create-rejected",
        "adoption-ambiguous",
        "stop-unverified",
        "restart-exhausted",
    ],
)
async def test_status_comments_are_plain_sentences(service_config: ServiceConfig, template: str):
    text = await _render(
        service_config,
        EffectKind.POST_COMMENT,
        {},
        template=template,
        pr_number=7,
        head_sha=HEAD,
        reason="x",
        matches=2,
    )
    for bad in FORBIDDEN:
        if not (template == "checkpoint" and bad == "/continue"):
            assert bad not in text, (bad, text)
    assert "parcel" not in text


async def test_question_reads_as_the_agents_own_reply(service_config: ServiceConfig):
    summary = "Should the label say **Leave**?\n\n- keep it\n- change it\n\nI recommend: keep it"
    text = await _render(
        service_config,
        EffectKind.POST_COMMENT,
        {},
        template="decision",
        decision_id="de_ec51c320a832a6bafdcbaff8",
        impact="within_contract",
        summary=summary,
        node_id=None,
        root_id="a91ecc4416304d6fb63bf06801990c90",
    )
    _plain(text)
    assert text == summary
    assert "de_ec51" not in text


async def test_publication_keeps_markdown_but_neutralises_mentions_and_markers(
    service_config: ServiceConfig,
):
    summary = "Thanks @owner.\n\n```\ncode\n```\n<!-- omnigent-factory effect=ef_x -->"
    result = {"kind": "build_ready", "summary": summary, "head_sha": HEAD, "pr_number": 7}
    text = await _render(
        service_config,
        EffectKind.PUBLISH_REPORT,
        result,
        report="ready",
        pr_number=7,
        head_sha=HEAD,
    )
    assert "@\u200bowner" in text and "```\ncode\n```" in text
    assert "<!--" not in text


async def test_publication_is_capped_at_8000_characters(service_config: ServiceConfig):
    result = {"kind": "build_ready", "summary": "word\n\n" * 3000, "head_sha": HEAD}
    text = await _render(
        service_config,
        EffectKind.PUBLISH_REPORT,
        result,
        report="ready",
        pr_number=7,
        head_sha=HEAD,
    )
    assert len(text) <= 8000 and text.endswith("[truncated]")
    assert "word\n\nword" in text  # line breaks survive


async def test_findings_needs_you_lists_each_open_thread_plainly(service_config: ServiceConfig):
    """#799: Needs you for review-bot findings names every open thread; no ids, links to
    Omnigent or instruction lines."""
    findings = [
        {
            "path": "api/src/export_delivery.ts",
            "severity": "P1",
            "title": "Keep the row when the retry fails",
            "url": "https://github.com/o/r/pull/799#discussion_r1",
        },
        {"path": "", "severity": "", "title": "Bound the `retry` loop", "url": ""},
    ]
    text = await _render(
        service_config,
        EffectKind.POST_COMMENT,
        {},
        template="ready-blocked",
        pr_number=799,
        head_sha="9bb7f7f" + "0" * 33,
        reason="review-bot findings have no outcome (fixed, follow-up or advisory)",
        findings=findings,
        further_round=True,
    )
    for bad in FORBIDDEN:
        assert bad not in text, (bad, text)
    assert "PR #799 isn't ready at `9bb7f7f`" in text
    assert "2 open findings on `9bb7f7f`, a further review round after a fix commit" in text
    assert (
        "- **P1** `api/src/export_delivery.ts`: Keep the row when the retry fails "
        "([thread](https://github.com/o/r/pull/799#discussion_r1))"
    ) in text
    assert "- (no file): Bound the 'retry' loop" in text
    assert "parcel" not in text and "ss_" not in text
