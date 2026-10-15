"""A red required check on a just-pushed head is shown only once confirmed (#891).

#891 (15 Oct, 14:33:53 ACDT): a fresh PR head 405f756 read one required check red; the
card in Ready went Bot Blocked for ~85 s, then the next read saw the checks pending and
it went back to Idle. A first red read on a head seen less than ``red_confirm_us`` ago
now keeps showing the checks as running until a second red read confirms it or the
window ends (a confirming read is scheduled for the window's end). Display only: the
readiness record, Ready decisions and the Building path's fix wake are unchanged.
"""

from __future__ import annotations

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, RetryableReadFailure
from omnigent_factory.core.types import BotState, Stage
from omnigent_factory.testing.builders import config, result_candidate
from omnigent_factory.testing.harness import Harness
from tests.github.test_adapter import CTX, adapter, effect

P = "I_parcel_1"
PR = 7
#: #891's fresh head.
FRESH = "405f756" + "0" * 33
CHECK = "web / Typecheck, test, lint"
WINDOW_US = 120 * 1_000_000


def harness() -> Harness:
    return Harness(cfg=config(red_confirm_us=WINDOW_US))


def evidence(h: Harness, head: str, **kw) -> ev.ReadinessEvidence:
    r = h.p().readiness
    assert r is not None
    return ev.ReadinessEvidence(session_id=r.session_id, pr_number=PR, head_sha=head, **kw)


def red(h: Harness, head: str = FRESH) -> ev.ReadinessEvidence:
    return evidence(
        h,
        head,
        checks=ev.ChecksState.FAILED,
        checks_summary="20 checks: 19 success, 1 failure",
        failing_checks=CHECK,
        base_sync=True,
    )


def pending(h: Harness, head: str = FRESH) -> ev.ReadinessEvidence:
    return evidence(h, head, checks=ev.ChecksState.PENDING, base_sync=True)


def ready_then_push(h: Harness) -> int:
    """Ready, then a new head (the owner's "Update branch"); returns when it was seen."""
    h.to_building()
    h.build_ready()
    assert h.p().stage == Stage.READY and h.p().readiness.ready
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=FRESH, bot_authored=True, parcel_branch=True))
    current = h.p().readiness.head_sha
    h.send(P, evidence(h, current, observed_head_sha=FRESH, checks=ev.ChecksState.PENDING))
    r = h.p().readiness
    assert r.head_sha == FRESH and r.head_seen_at_us
    assert h.p().bot == BotState.IDLE
    return r.head_seen_at_us


def bot_writes(h: Harness) -> list[str]:
    return [
        str(e.args["bot"])
        for _, result in h.log
        for e in result.effects
        if e.kind == EffectKind.SET_BOT
    ]


def assert_checks_running(h: Harness) -> None:
    p = h.p()
    assert p.stage == Stage.READY
    assert p.bot == BotState.IDLE
    assert p.board_note == f"Checks running on `{FRESH[:7]}`"


def test_red_on_a_fresh_head_stays_idle_and_schedules_the_confirming_read():
    h = harness()
    seen = ready_then_push(h)
    before = len(bot_writes(h))
    h.f().tick(30 * 1_000_000)
    r = h.send(P, red(h))
    assert r.audit.accepted
    assert_checks_running(h)
    assert len(bot_writes(h)) == before  # no Blocked write
    # The readiness record is what it is without damping (decisions unchanged).
    rd = h.p().readiness
    assert rd.sync_red and not rd.ready and rd.red_checks == CHECK
    assert rd.red_unconfirmed_until_us == seen + WINDOW_US
    reads = h.of(r, EffectKind.FETCH_PR_EVIDENCE)
    assert [e.args.get("not_before_us") for e in reads] == [seen + WINDOW_US]


def test_a_second_red_read_confirms_it_blocked():
    h = harness()
    ready_then_push(h)
    h.send(P, red(h))
    assert_checks_running(h)
    r = h.send(P, red(h))
    assert [e.args["bot"] for e in h.of(r, EffectKind.SET_BOT)] == ["Blocked"]
    p = h.p()
    assert p.bot == BotState.BLOCKED
    assert p.board_note == f"Required check red: {CHECK}"
    assert p.readiness.red_unconfirmed_until_us == 0


def test_red_then_pending_is_never_blocked():
    h = harness()
    ready_then_push(h)
    h.send(P, red(h))
    h.f().tick(60 * 1_000_000)
    h.send(P, pending(h))
    assert_checks_running(h)
    # The scheduled read after the window and a reconcile: still pending, still Idle.
    h.f().tick(90 * 1_000_000)
    h.send(P, pending(h))
    h.send(P, ev.ReconcileDue())
    assert_checks_running(h)
    assert "Blocked" not in bot_writes(h)


def test_the_window_ending_without_a_second_read_shows_blocked():
    h = harness()
    seen = ready_then_push(h)
    h.send(P, red(h))
    assert_checks_running(h)
    h.f().tick(seen + WINDOW_US - h.f().now)  # the next event lands at the window's end
    r = h.send(P, ev.ReconcileDue())
    assert [e.args["bot"] for e in h.of(r, EffectKind.SET_BOT)] == ["Blocked"]
    assert h.p().board_note == f"Required check red: {CHECK}"


def test_red_on_a_head_seen_before_the_window_is_blocked_at_once():
    h = harness()
    ready_then_push(h)
    h.f().tick(WINDOW_US + 1_000_000)
    r = h.send(P, red(h))
    assert [e.args["bot"] for e in h.of(r, EffectKind.SET_BOT)] == ["Blocked"]
    assert h.p().readiness.red_unconfirmed_until_us == 0
    assert all("not_before_us" not in e.args for e in h.of(r, EffectKind.FETCH_PR_EVIDENCE))


def test_without_the_setting_red_is_blocked_at_once():
    h = Harness(cfg=config(red_confirm_us=0))
    ready_then_push(h)
    r = h.send(P, red(h))
    assert [e.args["bot"] for e in h.of(r, EffectKind.SET_BOT)] == ["Blocked"]


def test_the_building_fix_wake_is_not_deferred():
    """Building path: a red read on a fresh head still wakes the build as before."""
    h = harness()
    h.to_building()
    s = h.cur()
    assert s.root_id is not None
    h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=PR,
            head_sha=FRESH,
        ),
    )
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=FRESH, bot_authored=True, parcel_branch=True))
    h.quiesce(P, h.cur().session_id)
    r = h.send(
        P,
        evidence(
            h,
            FRESH,
            checks=ev.ChecksState.FAILED,
            failing_checks=CHECK,
        ),
    )
    purposes = [e.args.get("purpose") for e in r.effects if e.kind == EffectKind.SEND_MESSAGE]
    assert purposes == ["readiness_wake"]
    assert h.p().stage == Stage.BUILDING


def test_891_replay():
    """14:33:53 red on the fresh head 405f756; ~85 s later the next read: pending."""
    h = harness()
    ready_then_push(h)
    h.f().tick(5 * 1_000_000)
    h.send(P, red(h))  # 14:33:53
    assert_checks_running(h)
    h.f().tick(85 * 1_000_000)
    h.send(P, ev.ChecksChanged(pr_number=PR, head_sha=FRESH, state=ev.ChecksState.PENDING))
    h.send(P, pending(h))  # 14:35:18
    assert_checks_running(h)
    # The confirming read scheduled for the window's end still sees the checks pending.
    h.f().tick(40 * 1_000_000)
    h.send(P, pending(h))
    assert_checks_running(h)
    assert "Blocked" not in bot_writes(h)
    # Later the checks go green: Ready as before.
    h.send(P, evidence(h, FRESH, verified=True, checks=ev.ChecksState.GREEN, base_sync=True))
    assert h.p().readiness.ready and h.p().bot == BotState.IDLE
    assert "Blocked" not in bot_writes(h)


@pytest.mark.asyncio
async def test_the_confirming_read_waits_for_its_time_without_a_request():
    """The adapter defers a scheduled read (the executor retries it then)."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        a = adapter(http)  # its clock reads 123 s
        outcome = await a.execute(
            effect(
                EffectKind.FETCH_PR_EVIDENCE,
                pr_number=PR,
                head_sha=FRESH,
                not_before_us=150_000_000,
            ),
            CTX,
        )
    assert isinstance(outcome, RetryableReadFailure)
    assert outcome.retry_after_us == 27_000_000
    assert requests == []
