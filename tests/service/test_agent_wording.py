"""Owner-facing text names the configured agent, never a hard-coded one, and the build
prompts carry the agreed review-bot disposition rule."""

from __future__ import annotations

from importlib.resources import files

from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import (
    _STATUS_TEXT,
    _public_result,
    _status_text,
    agent_display_name,
)


def test_agent_name_comes_from_config():
    base = {"repo_id": "R", "owners": frozenset({1})}
    assert agent_display_name(ServiceConfig(**base, omnigent_agent_name="rosie")) == "Rosie"
    assert agent_display_name(ServiceConfig(**base, omnigent_agent_name=" ")) == (
        "The factory agent"
    )


def test_blocked_report_and_status_comments_name_the_configured_agent():
    blocked = _public_result({"kind": "blocked", "reason": "needs a decision"}, "Rosie")
    assert blocked.startswith("### Rosie is blocked")
    args = {"decision_id": "d1", "impact": "within_contract", "pr_number": 1, "head_sha": "a"}
    texts = [_status_text(name, {**args, "reason": "x"}, "Rosie") for name in _STATUS_TEXT]
    assert any("I'm wrapping up" in text for text in texts)  # the agent's own voice
    assert not any("Molly" in text or "Factory" in text for text in [blocked, *texts])
    assert "The factory agent is blocked" in _public_result({"kind": "blocked", "reason": "x"})


def test_build_prompts_carry_the_review_bot_disposition_rule():
    root = files("omnigent_factory.service") / "templates"
    for name in ("build-v6.txt", "readiness-wake-v5.txt"):
        text = " ".join((root / name).read_text("utf-8").split())
        assert "follow-up issue" in text and "resolve" in text, name
        assert "Only resolve threads you have replied to." in text, name
    wake = " ".join((root / "readiness-wake-v5.txt").read_text("utf-8").split())
    assert "submit build_ready, not blocked" in wake


def test_stage_prompts_keep_internal_ids_out_of_github_prose():
    """Decision/effect IDs (de_..., ef_...) are factory internals, not owner-facing text."""
    root = files("omnigent_factory.service") / "templates"
    for name in (
        "triage-v5.txt",
        "triage-feedback-v3.txt",
        "plan-v5.txt",
        "build-v6.txt",
        "build-rework-v3.txt",
    ):
        text = " ".join((root / name).read_text("utf-8").split())
        assert "no internal labels or IDs (de_..., ef_...)" in text, name


def test_readiness_wake_says_not_ready_for_review_not_the_column_name():
    wake = " ".join(
        (files("omnigent_factory.service") / "templates" / "readiness-wake-v5.txt")
        .read_text("utf-8")
        .split()
    )
    assert "is not ready for review:" in wake and "not Ready" not in wake


def test_readiness_wake_says_red_checks_outside_the_change_are_build_ready_not_blocked():
    """#461: Rosie reported "blocked" for red checks from main and a flaky test."""
    wake = " ".join(
        (files("omnigent_factory.service") / "templates" / "readiness-wake-v5.txt")
        .read_text("utf-8")
        .split()
    )
    assert "red for a cause outside this change" in wake
    assert "name the check and the cause in a PR comment and in the build_ready summary" in wake
    assert 'Submit kind "blocked" only if a required defect in this change' in wake
