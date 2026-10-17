"""Follow-up discipline in the core: the ``deferred`` result field and the follow-up depth
limit, Ready report checkbox lines, the review-round budget in readiness (no further wait
once it is used up, the round note) and the rework the owner's ticks start."""

from __future__ import annotations

from dataclasses import replace
from importlib.resources import files
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.protocol import Correlation, ResultError, validate_result
from omnigent_factory.core.types import MICROS_PER_MINUTE, QueueStatus, Stage
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID, config
from omnigent_factory.testing.harness import Harness
from tests.test_late_review_findings import FIX, GREEN, PR, evidence, green, submit
from tests.test_rework import ready

MIN = MICROS_PER_MINUTE
P = "I_parcel_1"


# ------------------------------------------------------------------ result protocol


def build(**over: Any) -> dict[str, Any]:
    return {
        "kind": "build_ready",
        "pr_number": 7,
        "branch": "factory/issue-747",
        "head_sha": "a" * 40,
        "summary": "done",
        "verification": [
            {"command": "pytest", "file_set": ["tests"], "outcome": "passed", "evidence": "ok"}
        ],
        "review": {
            "implementation_vendor": "anthropic",
            "review_vendor": "openai",
            "reviewed_head": "a" * 40,
            "artifact_reference": "r",
            "artifact_sha256": "b" * 64,
            "accepted": True,
        },
        "findings": [],
        "remediation_batches_used": 0,
        "targeted_rechecks_used": 0,
        "release_readiness": "ready",
        **over,
    }


def corr(depth: int = 0, max_depth: int = 1) -> Correlation:
    return Correlation(
        parcel_id="I_747",
        stage_session_id="ss_1",
        dispatch_nonce="n",
        revision=1,
        stage="build",
        issue_number=747,
        follow_up_depth=depth,
        follow_up_source=700 if depth else None,
        follow_up_max_depth=max_depth,
    )


FOLLOW_UP = {
    "severity": "P2",
    "path": "api/src/export.ts",
    "title": "Retry leaks the delivery row",
    "thread_url": "https://github.com/o/r/pull/7#discussion_r1",
    "reason": "outside the plan: the retry module is #820's",
    "blocks": [882],
}
ADVISORY = {
    "severity": "P3",
    "title": "Guard against a clock going backwards",
    "reason": "no realistic trigger: the host clock is NTP-disciplined",
    "outcome": "advisory",
}


def test_build_and_blocked_results_carry_deferred_findings():
    parsed = validate_result(build(deferred=[FOLLOW_UP, ADVISORY]), corr())
    dumped = parsed.result.result.model_dump(mode="json")
    assert [d["outcome"] for d in dumped["deferred"]] == ["follow_up", "advisory"]
    assert dumped["deferred"][1]["blocks"] == [] and dumped["deferred"][1]["path"] == ""
    blocked = {"kind": "blocked", "reason": "x", "done": [], "deferred": [ADVISORY]}
    assert validate_result(blocked, corr()).result.result.model_dump()["deferred"]
    with pytest.raises(ResultError, match="this issue itself"):
        validate_result(build(deferred=[{**FOLLOW_UP, "blocks": [747]}]), corr())
    with pytest.raises(ResultError):
        validate_result(build(deferred=[{**FOLLOW_UP, "severity": "BLOCKING"}]), corr())


def test_a_follow_up_of_a_follow_up_is_refused_with_an_actionable_error():
    for result in (
        build(deferred=[FOLLOW_UP]),
        build(follow_ups=[{"issue": 904}]),  # the legacy field counts too
        {"kind": "blocked", "reason": "x", "done": [], "deferred": [FOLLOW_UP]},
    ):
        with pytest.raises(ResultError) as error:
            validate_result(result, corr(depth=1))
        message = " ".join(error.value.details)
        assert "itself a follow-up (a follow-up of #700)" in message
        assert "fix each of these findings in this PR" in message and "advisory" in message
    # Fixing everything, or advisories only, is fine at any depth.
    validate_result(build(deferred=[ADVISORY]), corr(depth=1))
    validate_result(build(), corr(depth=3))
    # The limit is configurable: depth 2 allows one more generation.
    validate_result(build(deferred=[FOLLOW_UP]), corr(depth=1, max_depth=2))
    with pytest.raises(ResultError, match="itself a follow-up"):
        validate_result(build(deferred=[FOLLOW_UP]), corr(depth=2, max_depth=2))


def test_the_schema_file_declares_deferred():
    import json
    from pathlib import Path

    from omnigent_factory import core as core_pkg

    schema = json.loads(
        (Path(core_pkg.__file__).parent / "result_schema_v1.json").read_text("utf-8")
    )
    for kind in ("build", "blocked"):
        assert schema["$defs"][kind]["properties"]["deferred"]["items"] == {
            "$ref": "#/$defs/deferred"
        }
    assert schema["$defs"]["deferred"]["properties"]["outcome"]["enum"] == [
        "follow_up",
        "advisory",
    ]


# ------------------------------------------------------------------ checkbox lines


def report() -> str:
    from omnigent_factory.core import report_lines as rl

    return "\n".join(
        [
            "### PR #7 is ready for review",
            rl.line(rl.ACTION, "d-a", "Fix now: P2 `a.ts` A (in #100)"),
            rl.line(rl.ACTION, "d-b", "Fix now: P3 B (advisory: style)"),
            rl.line(rl.ACTION, "round-2", "One more Codex review round"),
        ]
    )


def tick(body: str, *idents: str) -> str:
    """``body`` as the owner leaves it after ticking ``idents`` in the GitHub UI."""
    from omnigent_factory.core import report_lines as rl

    markers = [rl.marker(rl.ACTION, ident) for ident in idents]
    return "\n".join(
        line.replace("- [ ]", "- [x]", 1) if any(m in line for m in markers) else line
        for line in body.splitlines()
    )


def test_ticks_are_found_by_marker_and_diffed():
    from omnigent_factory.core import report_lines as rl

    REPORT = report()
    owner = tick(REPORT, "d-b")
    assert rl.newly_ticked(REPORT, owner) == ["d-b"]
    assert rl.newly_ticked(owner, tick(owner, "round-2")) == ["round-2"]
    assert rl.newly_ticked(owner, REPORT) == []  # an untick is no action
    # The text is not the identity: a reworded line with the same marker is the same box.
    reworded = owner.replace("Fix now: P3 B", "Fix now: P3 renamed")
    assert rl.newly_ticked(owner, reworded) == []


def test_a_rewrite_keeps_ticks_it_has_not_processed():
    from omnigent_factory.core import report_lines as rl

    REPORT = report()
    current = tick(REPORT, "d-a", "round-2")  # d-a processed, round-2 the owner's new tick
    updated = rl.with_status(current, {"d-a": "fixing (rework started)"})
    assert "Fix now: P2 `a.ts` A (in #100) → fixing (rework started)" in updated
    assert rl.checked(updated) == {"d-a": True, "d-b": False, "round-2": True}
    # A status is replaced, never stacked; a re-rendered report keeps the owner's ticks.
    again = rl.with_status(updated, {"d-a": "fixed in `abc1234`"})
    assert "→ fixed in `abc1234`" in again and "fixing" not in again
    assert rl.checked(rl.keep_ticks(current, REPORT)) == rl.checked(current)


# ------------------------------------------------------------------ review rounds in readiness


def harness() -> Harness:
    cfg = replace(config(), review_grace_us=10 * MIN, review_ack_us=5 * MIN, review_cap_us=45 * MIN)
    return Harness(cfg=cfg)


def test_with_the_rounds_used_up_ready_does_not_wait_for_another_review():
    h = harness()
    h.to_building()
    submit(h)
    pushed = h.f(P).now
    # Rounds left: the bot may still review this head, so the green read waits (grace).
    h.send(
        P,
        green(
            h,
            review_bot_pending_since_us=pushed,
            review_rounds_used=1,
            review_rounds_budget=2,
            review_rounds_note="Codex round 1/2",
        ),
    )
    assert h.p().stage == Stage.BUILDING
    # Budget spent: the same pending trigger is not waited for; Ready at once.
    h.send(
        P,
        green(
            h,
            review_bot_pending_since_us=pushed,
            review_rounds_used=2,
            review_rounds_budget=2,
            review_rounds_note="Codex round 2/2",
        ),
    )
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready
    assert p.note == "Codex round 2/2"
    assert p.readiness.review_bot == "no further review awaited (2/2 rounds used)"


def test_a_readiness_wake_tells_the_run_its_rounds():
    h = harness()
    h.to_building()
    submit(h)
    r = h.send(
        P,
        evidence(
            h,
            checks=ev.ChecksState.GREEN,
            checks_summary=GREEN,
            findings_open=True,
            review_bot_pending_since_us=0,
            review_rounds_used=2,
            review_rounds_budget=2,
        ),
    )
    [wake] = [e for e in r.effects if e.kind == EffectKind.SEND_MESSAGE]
    assert wake.args["purpose"] == MessagePurpose.READINESS_WAKE.value
    assert (wake.args["rounds_used"], wake.args["rounds_budget"]) == (2, 2)
    assert h.p().readiness.review_bot.endswith("· round 2/2")


# ------------------------------------------------------------------ rework from ticks


def test_ticked_items_start_one_rework_the_factory_moves_the_card_for():
    h = ready()
    approval = h.p().current_approval_id
    r = h.send(P, ev.RequestRework(batch_id="b1"), actor=OWNER_ID, event_id="report-items:b1")
    assert r.audit.accepted
    p = h.p()
    assert p.stage == Stage.BUILDING and p.note == "Rework: Ready report items ticked"
    assert p.current_approval_id == approval  # the plan approval stands
    [move] = Harness.of(r, EffectKind.MOVE_CARD)  # not a drag: the factory moves the card
    assert move.args["to"] == "Building"
    assert h.admission.queue_entry(P).status == QueueStatus.QUEUED
    assert p.authorizations[-1].rework and p.authorizations[-1].source_event_id == (
        "report-items:b1"
    )
    # Only the owner's authenticated tick, and only from Ready.
    h2 = ready()
    assert not h2.send(P, ev.RequestRework(batch_id="b"), actor=OTHER_USER_ID).audit.accepted
    assert not h2.send(
        P, ev.RequestRework(batch_id="b"), actor=OWNER_ID, provenance=Provenance.SCHEDULER
    ).audit.accepted
    assert h2.p().stage == Stage.READY
    assert not h.send(P, ev.RequestRework(batch_id="b2"), actor=OWNER_ID).audit.accepted


# ------------------------------------------------------------------ stage instructions


def template(name: str) -> str:
    root = files("omnigent_factory.service") / "templates"
    return " ".join((root / name).read_text("utf-8").split())


def test_current_build_instructions_carry_the_outcome_rule_and_file_no_issues():
    from omnigent_factory.service import directory as d

    current = (
        d._FIRST_TEMPLATES[d.SessionKind.BUILD],
        d._REWORK_TEMPLATE,
        d._READINESS_WAKE_TEMPLATE,
        d._FINDINGS_WAKE_TEMPLATE,
        d._ITEMS_REWORK_TEMPLATE,
    )
    for name in current:
        text = template(name)
        assert "deferred" in text and "Never open issues" in text, name
        assert "follow_ups" not in text and "open a follow-up issue" not in text, name
    for name in current[:4]:
        text = template(name)
        assert "exactly one outcome" in text, name
        for outcome in ("fix in this pr", "follow-up", "advisory"):
            assert outcome in text.lower(), (name, outcome)
    assert "review_rounds" in template(current[0]) and "strict_from_round" in template(current[0])
    for name in current[2:4]:
        assert "{rounds}" in template(name), name
    assert "{brief}" in template(current[4])


def test_rounds_line_scales_the_strict_round_with_the_budget():
    from omnigent_factory.service.directory import rounds_line

    assert rounds_line(0, 0) == ""
    assert "from round 2 on only P1" in rounds_line(1, 2)
    assert "from round 4 on only P1" in rounds_line(1, 4)  # a raised budget stays strict late
    spent = rounds_line(2, 2)
    assert "budget is spent" in spent and "do not ask the review bot" in spent


def test_the_findings_wake_renders_with_its_rounds_line():
    from omnigent_factory.service.directory import _FINDINGS_WAKE_TEMPLATE, _template

    text = _template(_FINDINGS_WAKE_TEMPLATE).format(
        pr_number=PR, head_sha=FIX, reason="findings", run_id="r", rounds="Review rounds: x"
    )
    text = " ".join(text.split())
    assert f"PR #{PR} at head `{FIX}`" in text and "Review rounds: x" in text
