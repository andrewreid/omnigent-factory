"""The Ready report's factory lines stay current; Ready never shows the bot working.

#694 (2026-10-01): the Ready report embedded the agent's summary written before CI and
the review bot had finished ("CI at submission: still running", "Review bot: reacted
👀, no verdict yet") under a factory footer saying all checks were green. The agent's
summary no longer carries live status; the factory adds a verified review-bot line next
to its CI line and edits those lines in place while the card stays in Ready on the same
head (a +1 reaction sends no webhook: the reconcile loop keeps reading until the line is
final). #461: a Ready card showed Bot Working while CI re-ran after an owner merge.
"""

from __future__ import annotations

from dataclasses import replace
from importlib.resources import files

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent, EffectKind
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.projection import ready_bot_ok
from omnigent_factory.core.types import MICROS_PER_MINUTE, BotState, Stage
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import _ready_text, review_bot_name
from omnigent_factory.testing.builders import config, result_candidate
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"
PR = 714
NEW = "b" * 40
GREEN = "18 checks: 16 success, 2 skipped"
THUMBS = f"👍 on `{HEAD[:7]}`"
SILENT = "no response within the grace window"
HOUR = 60 * MICROS_PER_MINUTE


def kinds(result) -> list[EffectKind]:
    return [e.kind for e in result.effects]


def harness() -> Harness:
    return Harness(cfg=replace(config(), review_grace_us=10 * MICROS_PER_MINUTE))


def evidence(h: Harness, head: str = HEAD, **kw) -> ev.ReadinessEvidence:
    r = h.p().readiness
    assert r is not None
    return ev.ReadinessEvidence(session_id=r.session_id, pr_number=PR, head_sha=head, **kw)


def green(h: Harness, head: str = HEAD, **kw) -> ev.ReadinessEvidence:
    kw.setdefault("checks_summary", GREEN)
    return evidence(h, head, verified=True, checks=ev.ChecksState.GREEN, **kw)


def built(h: Harness) -> None:
    h.to_building()
    s = h.cur()
    assert s.root_id is not None
    r = h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=PR,
            head_sha=HEAD,
        ),
    )
    assert r.audit.accepted, r.audit.reason
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=HEAD, bot_authored=True, parcel_branch=True))
    h.quiesce(P, h.cur().session_id)


def ready_on_grace_expiry(h: Harness) -> EffectIntent:
    """The review bot stays silent; Ready comes from a green read after the grace."""
    built(h)
    pushed = h.f(P).now
    h.f(P).tick(11 * MICROS_PER_MINUTE)
    r = h.send(P, green(h, review_bot_pending_since_us=pushed))
    assert h.p().stage == Stage.READY and h.p().readiness.ready
    [report] = Harness.of(r, EffectKind.PUBLISH_REPORT)
    return report


# ------------------------------------------------------------ the report's lines


def test_ready_report_carries_the_review_bot_line_from_the_ready_read():
    h = harness()
    built(h)
    r = h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    [report] = Harness.of(r, EffectKind.PUBLISH_REPORT)
    assert report.args["review_bot"] == THUMBS
    assert report.args["checks_summary"] == GREEN
    assert h.p().readiness.report_effect_id == report.effect_id


def test_grace_expiry_reports_no_response():
    h = harness()
    report = ready_on_grace_expiry(h)
    assert report.args["review_bot"] == SILENT


def test_no_review_bot_configured_means_no_line():
    h = harness()
    built(h)
    h.f(P).tick(11 * MICROS_PER_MINUTE)
    r = h.send(P, green(h))  # review_bot_pending_since_us None: unknown or no bot
    [report] = Harness.of(r, EffectKind.PUBLISH_REPORT)
    assert report.args["review_bot"] == ""


def test_rendered_report_lines():
    head = "f7493c8" + "0" * 33
    base = {"report": "ready", "pr_number": PR, "head_sha": head, "checks_summary": GREEN}
    result = {"head_sha": head, "summary": "Adds the thing."}

    def render(bot: str) -> str:
        return _ready_text({**base, "review_bot": bot}, result, "", "o/r", "Codex")

    text = render("👍 on `f7493c8`")
    lines = text.splitlines()
    ci = lines.index(f"**CI:** {GREEN}")
    assert lines[ci + 1] == "**Review bot:** Codex: 👍 on `f7493c8`"
    assert "Adds the thing." in text
    findings = render("reviewed `abc1234`, 2 findings, all with outcomes")
    assert "**Review bot:** Codex: reviewed `abc1234`, 2 findings, all with outcomes" in findings
    assert f"**Review bot:** Codex: {SILENT}" in render(SILENT)
    assert "Review bot" not in render("")
    cfg = ServiceConfig(repo_id="R", owners=frozenset({1}), review_bot_mention="@codex")
    assert review_bot_name(cfg) == "Codex"


# --------------------------------------------------------- keeping them current


def test_reaction_only_verdict_is_picked_up_by_reconcile_and_edits_once():
    """No webhook for a +1: the reconcile read finds it and the report is edited once."""
    h = harness()
    report = ready_on_grace_expiry(h)
    r = h.send(P, ev.ReconcileDue())
    assert EffectKind.FETCH_PR_EVIDENCE in kinds(r)  # the line is not final yet
    # The read the reconcile asked for: the bot's +1 on the head, no other change.
    r = h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    [edit] = Harness.of(r, EffectKind.EDIT_REPORT)
    assert edit.args["report_effect_id"] == report.effect_id
    assert edit.args["review_bot"] == THUMBS and edit.args["checks_summary"] == GREEN
    assert EffectKind.POST_COMMENT not in kinds(r) and EffectKind.PUBLISH_REPORT not in kinds(r)
    # Unchanged reads edit nothing; the line is final, so reconcile stops reading.
    r = h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    assert EffectKind.EDIT_REPORT not in kinds(r)
    assert EffectKind.FETCH_PR_EVIDENCE not in kinds(h.send(P, ev.ReconcileDue()))


def test_unchanged_reads_never_edit():
    h = harness()
    ready_on_grace_expiry(h)
    pushed = h.p().readiness.settle_at_us - 10 * MICROS_PER_MINUTE
    for _ in range(3):  # still silent on the same trigger
        r = h.send(P, green(h, review_bot_pending_since_us=pushed))
        assert EffectKind.EDIT_REPORT not in kinds(r)


def test_a_changed_check_summary_on_the_same_head_is_edited_in():
    h = harness()
    built(h)
    h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    r = h.send(
        P,
        green(
            h,
            review_bot_pending_since_us=0,
            review_bot_verdict=THUMBS,
            checks_summary="19 checks: 17 success, 2 skipped",
        ),
    )
    [edit] = Harness.of(r, EffectKind.EDIT_REPORT)
    assert edit.args["checks_summary"] == "19 checks: 17 success, 2 skipped"


def test_red_then_green_on_the_same_head_edits_the_red_line_away():
    h = harness()
    built(h)
    h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    red = evidence(
        h,
        checks=ev.ChecksState.FAILED,
        checks_summary="18 checks: 17 success, 1 failure",
        failing_checks="api / audit",
        review_bot_pending_since_us=0,
        review_bot_verdict=THUMBS,
    )
    r = h.send(P, red)
    [edit] = Harness.of(r, EffectKind.EDIT_REPORT)
    assert edit.args["red_checks"] == "api / audit"
    assert h.p().bot == BotState.BLOCKED
    r = h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    [edit] = Harness.of(r, EffectKind.EDIT_REPORT)
    assert "red_checks" not in edit.args and edit.args["checks_summary"] == GREEN


def test_pending_re_run_does_not_edit():
    h = harness()
    built(h)
    h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    r = h.send(P, evidence(h, checks=ev.ChecksState.PENDING, review_bot_pending_since_us=0))
    assert EffectKind.EDIT_REPORT not in kinds(r)


def test_a_new_head_never_edits_the_old_report():
    h = harness()
    ready_on_grace_expiry(h)
    h.send(P, evidence(h, observed_head_sha=NEW, checks=ev.ChecksState.PENDING))
    assert h.p().readiness.report_effect_id == ""
    h.f(P).tick(11 * MICROS_PER_MINUTE)
    r = h.send(
        P, green(h, NEW, review_bot_pending_since_us=0, review_bot_verdict="👍 on `bbbbbbb`")
    )
    assert h.p().stage == Stage.READY and h.p().readiness.ready
    assert EffectKind.EDIT_REPORT not in kinds(r)
    assert EffectKind.PUBLISH_REPORT not in kinds(r)  # unchanged: no second report


def test_reconcile_stops_polling_a_silent_bot_after_a_day():
    h = harness()
    ready_on_grace_expiry(h)
    assert EffectKind.FETCH_PR_EVIDENCE in kinds(h.send(P, ev.ReconcileDue()))
    h.f(P).tick(25 * HOUR)
    assert EffectKind.FETCH_PR_EVIDENCE not in kinds(h.send(P, ev.ReconcileDue()))


def test_final_ready_cards_get_no_extra_polling():
    h = harness()
    built(h)
    h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    assert EffectKind.FETCH_PR_EVIDENCE not in kinds(h.send(P, ev.ReconcileDue()))


# ------------------------------------------------- Ready is never Bot Working (#461)


def test_ready_with_checks_re_running_is_idle_with_a_note_then_cleared():
    h = harness()
    built(h)
    h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    h.send(P, ev.ChecksChanged(pr_number=PR, head_sha=HEAD, state=ev.ChecksState.PENDING))
    h.send(P, evidence(h, checks=ev.ChecksState.PENDING, review_bot_pending_since_us=0))
    p = h.p()
    assert p.stage == Stage.READY and p.bot == BotState.IDLE
    assert p.board_note == f"Checks running on `{HEAD[:7]}`"
    h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    p = h.p()
    assert p.bot == BotState.IDLE and p.board_note == ""


def test_owner_feedback_on_a_ready_card_moves_it_to_building_with_the_work():
    h = harness()
    built(h)
    h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict=THUMBS))
    r = h.send(P, ev.PlanFeedback(text_digest="please rename it", pr_number=PR))
    p = h.p()
    assert p.stage == Stage.BUILDING
    assert p.bot in (BotState.QUEUED, BotState.WORKING)
    [move] = Harness.of(r, EffectKind.MOVE_CARD)
    assert move.args["to"] == "Building"


def test_ready_bot_ok_is_the_projection_invariant():
    h = harness()
    built(h)
    p = h.p()
    assert not ready_bot_ok(replace(p, stage=Stage.READY), BotState.WORKING)
    assert not ready_bot_ok(replace(p, stage=Stage.READY), BotState.QUEUED)
    assert not ready_bot_ok(replace(p, stage=Stage.READY), BotState.CHECKPOINT)
    for bot in (BotState.IDLE, BotState.BLOCKED, BotState.NEEDS_YOU):
        assert ready_bot_ok(replace(p, stage=Stage.READY), bot)
    assert ready_bot_ok(p, BotState.WORKING)  # Building


# ------------------------------------------------------------ agent instructions


def test_build_summaries_carry_no_live_status():
    root = files("omnigent_factory.service") / "templates"
    for name in ("build-v7.txt", "build-rework-v4.txt", "readiness-wake-v6.txt"):
        text = " ".join((root / name).read_text("utf-8").split())
        assert "no CI or review-bot status" in text or "Leave out CI and review-bot" in text, name
        assert "the factory reports" in text, name
    for gone in ("build-v6.txt", "build-rework-v3.txt", "readiness-wake-v5.txt"):
        assert not (root / gone).is_file()


def test_operator_note_to_a_waiting_run_in_ready_is_refused():
    """The owner dragged the card to Ready before the evidence read: the run only waits
    on checks (Idle). An operator note would put it to work in Ready, so it is refused."""
    h = harness()
    built(h)
    h.send(P, ev.ColumnObserved(stage=Stage.READY), provenance=Provenance.WEBHOOK)
    assert h.p().stage == Stage.READY and h.p().bot == BotState.IDLE  # waits on checks
    r = h.send(P, ev.OperatorResume(text="go on"), provenance=Provenance.OPERATOR)
    assert not r.audit.accepted and r.audit.reason == "card-in-ready"
    assert EffectKind.SEND_MESSAGE not in kinds(r)
