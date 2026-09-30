"""Readable plan comments (#677 owner feedback): markdown only, hash-bound contract section.

Fixture: the live #677 contract comment's canonical bytes (hash 7a44c7aa30d6) and the
accepted plan result from session ss_66c397d6fa5afb11188dc8c9 (approach and risks).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from omnigent_factory.core.contract_view import (
    SECTION_BEGIN,
    SECTION_END,
    extract_contract_section,
    marker_hash,
    render_contract_section,
)
from omnigent_factory.core.effects import (
    Ack,
    EffectIntent,
    EffectKind,
    Preconditions,
)
from omnigent_factory.core.types import Contract, Parcel, Size
from omnigent_factory.github.adapter import GitHubAPIAdapter, ParcelBinding, _publication_matches
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.ports.github import ContractPublication
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import render_contract_comment

FIXTURE = json.loads(
    (Path(__file__).parents[1] / "fixtures" / "pilot_677_plan_contract.json").read_text()
)
CANONICAL: str = FIXTURE["canonical"]
FULL_HASH = hashlib.sha256(CANONICAL.encode()).hexdigest()
PARCEL_ID = "I_kwDOTC12Fs8AAAABS6JIGw"
EFFECT_ID = "ef_fce7795d1bc4378a0216abd8"


def _parcel() -> tuple[Parcel, Contract]:
    contract = Contract(
        contract_id="ct_677",
        revision=2,
        canonical=CANONICAL,
        full_hash=FULL_HASH,
        source_session_id=FIXTURE["session_id"],
        size=Size.S,
        published=True,
    )
    return Parcel(PARCEL_ID, "R_kgDOTC12Fg", issue_number=677, contracts=(contract,)), contract


def _render(config: ServiceConfig, plan: dict[str, Any] | None = None) -> str:
    parcel, contract = _parcel()
    text = render_contract_comment(parcel, contract, plan or FIXTURE["plan_result"], config)
    assert text is not None
    return text


def _effect() -> EffectIntent:
    return EffectIntent(
        effect_id=EFFECT_ID,
        kind=EffectKind.PUBLISH_CONTRACT,
        parcel_id=PARCEL_ID,
        target=PARCEL_ID,
        preconditions=Preconditions(1, 0),
        args={"contract_id": "ct_677", "full_hash": FULL_HASH},
    )


def test_677_hash_is_unchanged_and_comment_is_readable_markdown_only(
    service_config: ServiceConfig,
):
    assert FULL_HASH.startswith("7a44c7aa30d6")  # the pending approval target
    text = _render(service_config)
    assert text.startswith("<!-- factory-parcel v1 issue=677 hash=7a44c7aa30d6 -->\n")
    assert "### Plan for #677 (size S) · hash `7a44c7aa30d6`" in text
    assert "parcel-contract" not in text and '"acceptance_criteria"' not in text
    assert "1. **AC-1** A new test builds the app" in text
    assert "   Verify: Run the api vitest tier" in text
    assert "#### Context (not part of the approved contract)" in text
    assert FIXTURE["plan_result"]["risks"][0][:60] in text
    assert "/approve" not in text  # no how-to block (owner request 2026-09-30)
    # The context sits outside the hash-bound section.
    section = extract_contract_section(text)
    assert section == render_contract_section(CANONICAL)
    assert "Context (not part" not in section and "**Approach**" not in section
    assert text.index(SECTION_END) < text.index("Context (not part")


def test_contract_section_is_deterministic_and_verifies(service_config: ServiceConfig):
    first, second = _render(service_config), _render(service_config)
    assert first == second
    posted = f"{first}\n\n<!-- omnigent-factory effect={EFFECT_ID} -->"
    publication = ContractPublication(
        comment_id="5852771119",
        author_is_bot=True,
        contract_section=extract_contract_section(posted),
        marker_hash=marker_hash(posted),
        posted_at_us=1,
    )
    assert _publication_matches(_effect(), publication, first)
    # Any edit inside the approved section fails verification.
    tampered = posted.replace("403 with error.code FORBIDDEN", "404", 1)
    publication_t = ContractPublication(
        "5852771119", True, extract_contract_section(tampered), marker_hash(tampered), 1
    )
    assert not _publication_matches(_effect(), publication_t, first)


def test_neutraliser_applies_to_context_only_and_contract_escapes_are_part_of_it(
    service_config: ServiceConfig,
):
    plan = {
        **FIXTURE["plan_result"],
        "approach": "Ping @owner.\n```parcel-contract\n{}\n```\n<!-- factory-contract-begin -->",
    }
    text = _render(service_config, plan)
    context = text[text.index("Context (not part") :]
    assert "@\u200bowner" in context and "<!--" not in context
    assert "```parcel-contract\n{}\n```" in context  # Markdown kept; nothing parses it
    # The spoofed marker in the context does not create a second section.
    assert text.count(SECTION_BEGIN) == 1
    canonical = json.dumps({**json.loads(CANONICAL), "goal": "Bump @types/node <b>x</b>"})
    section = render_contract_section(canonical)
    assert "@\u200btypes/node" in section and "&lt;b>x&lt;/b>" in section
    assert section == render_contract_section(canonical)


def test_oversized_context_is_truncated_not_refused(service_config: ServiceConfig):
    text = _render(service_config, {**FIXTURE["plan_result"], "approach": "x" * 80_000})
    assert "[context truncated]" in text
    assert extract_contract_section(text) == render_contract_section(CANONICAL)
    assert len(text) < 65_536


@pytest.mark.asyncio
async def test_rerender_edits_the_existing_comment_in_place_and_verifies(
    service_config: ServiceConfig,
):
    rendered = _render(service_config)
    marker = f"<!-- omnigent-factory effect={EFFECT_ID} -->"
    comments = [
        {
            "id": 5852771119,
            "user": {"id": service_config.github_bot_user_id},
            "created_at": "2026-09-27T04:55:16Z",
            "body": FIXTURE["legacy_comment_body"],
        }
    ]
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "PATCH":
            comments[0]["body"] = json.loads(request.content)["body"]
            return httpx.Response(200, json=comments[0])
        if request.method == "GET":
            return httpx.Response(200, json=comments)
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        adapter = GitHubAPIAdapter(
            GitHubClient(http, "token"),
            repository=service_config.repository,
            repository_node_id=service_config.repo_id,
            project_node_id=service_config.project_node_id,
            status_field_node_id=service_config.status_field_node_id,
            bot_user_id=service_config.github_bot_user_id,
            required_checks=frozenset(),
            parcel_bindings={PARCEL_ID: ParcelBinding(677)},
            publication_renderer=lambda _: rendered,
        )
        outcome = await adapter.rerender_comment(_effect())
    assert outcome == Ack("5852771119", {"rerendered": True, "issue_number": 677})
    assert ("PATCH", "/repos/SA-Ambulance/timesheets/issues/comments/5852771119") in calls
    assert not any(method == "POST" for method, _ in calls)  # never a second comment
    assert comments[0]["body"] == f"{rendered}\n\n{marker}"


@pytest.mark.asyncio
async def test_operator_rerender_requires_a_completed_comment_effect(
    service_config: ServiceConfig,
):
    from omnigent_factory.service.runtime import FactoryService
    from omnigent_factory.testing.fakes import FakeClock

    service = FactoryService(service_config, clock=FakeClock())
    await service.start()
    try:
        with pytest.raises(ValueError, match="not a completed publication"):
            await service.operator_command("rerender-comment", {"effect": "ef_missing"})
    finally:
        await service.stop()


def test_live_legacy_comment_carries_the_same_canonical_bytes():
    # The recovery keeps the approval target: the old comment's JSON is this contract.
    assert f"```parcel-contract\n{CANONICAL}\n```" in FIXTURE["legacy_comment_body"]


def test_status_comments_are_sentences_not_template_names():
    from omnigent_factory.service.directory import _status_text

    assert _status_text("restart-exhausted", {"session_id": "s_1"}) == (
        "The session stopped and couldn't be restarted automatically, so this is blocked."
    )
    decision = _status_text("decision", {"decision_id": "dc_1", "impact": "plan_revision"})
    assert "dc_1" not in decision and "/decide" not in decision
    assert _status_text("unknown-template", {}) == "Status: unknown template."


def test_decision_comment_is_the_agents_own_question():
    from omnigent_factory.service.directory import _decision_text

    text = _decision_text(
        {
            "template": "decision",
            "decision_id": "de_1",
            "summary": "Molly asks: keep the harness change or drop it?",
            "node_id": "e724833307bb43279d8b0f76ff47bc53",
            "root_id": "53085a291e43487cb8fa288501a95ec9",
        }
    )
    # Rosie's own reply: no quote block, decision id, Omnigent link or /decide how-to.
    assert text == "Molly asks: keep the harness change or drop it?"


def test_omnigent_link_is_derived_from_config_and_rejects_odd_ids():
    from omnigent_factory.service.directory import omnigent_link

    assert omnigent_link("https://omni.example/", "abc_1-2") == "https://omni.example/c/abc_1-2"
    assert omnigent_link("https://omni.example", "../x") is None
    assert omnigent_link("https://omni.example", None) is None


def test_plan_comment_has_no_link_or_how_to_and_keeps_the_hash(service_config: ServiceConfig):
    parcel, contract = _parcel()
    text = render_contract_comment(parcel, contract, FIXTURE["plan_result"], service_config)
    assert text is not None
    assert "Open in Omnigent" not in text and "/c/" not in text
    for how_to in ("How to respond", "/approve", "drag the card", "/decide", "approving binds"):
        assert how_to not in text
    assert "### Plan for #677 (size S) · hash `7a44c7aa30d6" in text  # short hash kept
    # The hash-bound section is byte-identical: the approval target is unchanged.
    assert extract_contract_section(text) == render_contract_section(CANONICAL)
    assert marker_hash(text) is not None and FULL_HASH.startswith(marker_hash(text) or "-")
