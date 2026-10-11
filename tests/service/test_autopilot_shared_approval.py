"""Two autopilot builds under one epic plan approval (the 2026-10-11 auto-build outage).

One owner approval of the epic plan authorises every sub-issue build of that epic, but
the store keeps ``approvals.source_event_id`` unique: the second sub-issue's start raised
``IntegrityError`` in every pass, which the loop logged without a traceback while no
auto-build started at all. Also: one failing mark no longer starves the marks behind it,
and every swallowed background-loop failure logs its (redacted) traceback.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import pytest

from omnigent_factory.core.types import IssueSnapshot, SessionKind, Stage
from omnigent_factory.ports.github import IssueRef, RankingCard
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.redaction import SecretRedactionFilter
from omnigent_factory.service.runtime import FactoryService
from tests.service.test_epic_autopilot import S1, S2, history, on, rig
from tests.test_epic_autopilot_reducer import EPIC, auto_build, claimed, queue


def rank_card(pid: str, number: int, rank: float | None) -> RankingCard:
    return RankingCard(
        node_id=pid,
        item_id=f"PVTI_{number}",
        number=number,
        title=f"Issue {number}",
        stage=Stage.SCOPED,
        created_at_us=number,
        rank=rank,
        priority=None,
    )


def two_queued_subs():
    """Epic #1 on Full autopilot, sub-issues #2 (S1) and #3 (S2) both queued by it."""
    h = history("Full")
    for pid in (S1, S2):
        claimed(h, pid)
        r = queue(h, pid)
        assert r.audit.accepted, r.audit.reason
    return h


def one_built_one_queued():
    """As live (#854 then #896 under epic #878): the epic's first autopilot build started
    and was admitted long ago (its approval is stored), the next sub-issue is queued."""
    h = history("Full")
    claimed(h, S1)
    assert queue(h, S1).audit.accepted
    plan = h.cur(S1)
    r = auto_build(h, S1)
    assert r.audit.accepted, r.audit.reason
    h.quiesce(S1, plan.session_id)
    assert h.admit(S1).kind == SessionKind.BUILD
    claimed(h, S2)
    assert queue(h, S2).audit.accepted
    return h


@pytest.mark.asyncio
async def test_a_second_build_under_the_same_epic_approval_starts(
    service_config: ServiceConfig,
):
    cards = {2: rank_card(S1, 2, 2.0), 3: rank_card(S2, 3, None)}  # #3 unranked, as #851
    # The live switches and capacity: auto-build on, 2 auto-build slots, 4 build slots.
    config = on(service_config, auto_build=True, auto_build_concurrency=2, max_building=4)
    async with rig(config, one_built_one_queued(), cards=cards) as r:
        status = await r.builder.status()
        assert status["can_start"] is True, status
        assert status["queue"] == [{"issue": 3, "rank": None, "eligible": True, "blocker": None}]
        assert await r.builder.run_once() == "started #3"  # was: IntegrityError, every pass
        s1, s2 = await r.parcel(S1), await r.parcel(S2)
        assert s1.stage == s2.stage == Stage.BUILDING
        a1, a2 = s1.current_approval, s2.current_approval
        assert a1 is not None and a2 is not None
        epic = (await r.parcel(EPIC)).autopilot
        assert epic is not None
        # Both carry the owner's epic plan approval; each has its own source event.
        assert a1.owner_id == a2.owner_id == epic.approved_by
        assert a1.source_event_id != a2.source_event_id
        assert a2.source_event_id.startswith(f"auto-build:{S2}:")


@pytest.mark.asyncio
async def test_one_failing_mark_does_not_starve_the_marks_behind_it(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    cards = {2: rank_card(S1, 2, None), 3: rank_card(S2, 3, 3.0)}
    config = on(service_config, auto_build_concurrency=2, max_building=4)
    async with rig(config, two_queued_subs(), cards=cards) as r:
        read = r.builder.snapshot

        async def flaky(ref: IssueRef) -> Any:
            if ref.parcel_id == S2:
                raise RuntimeError("read exploded token=sekrit-value")
            snap: IssueSnapshot = await read(ref)
            return snap

        r.builder.snapshot = flaky
        with caplog.at_level(logging.WARNING, logger="omnigent_factory.service.auto_build"):
            assert await r.builder.run_once() == "started #2"
        [record] = [x for x in caplog.records if "auto-build start failed" in x.getMessage()]
        assert "issue=#3" in record.getMessage()
        assert "RuntimeError: read exploded" in traceback_of(record)


def traceback_of(record: logging.LogRecord) -> str:
    """The record's traceback (the redaction filter may already have rendered it)."""
    if record.exc_info and record.exc_info[0] is not None:
        return logging.Formatter().formatException(record.exc_info)
    return record.exc_text or ""


class _Boom:
    def __init__(self) -> None:
        self.calls = 0

    async def run_once(self) -> str:
        self.calls += 1
        raise RuntimeError("pass exploded token=sekrit-value")


@pytest.mark.parametrize(
    ("loop", "attr", "message"),
    [
        ("_auto_build_loop", "auto_builder", "auto-build pass failed"),
        ("_auto_build_loop", "epic_autopilot", "epic autopilot pass failed"),
        ("_auto_triage_loop", "auto_triager", "auto-triage pass failed"),
        ("_ranking_loop", "ranker", "ranking pass failed"),
    ],
)
@pytest.mark.asyncio
async def test_a_failed_loop_pass_logs_its_traceback(
    service_config: ServiceConfig,
    caplog: pytest.LogCaptureFixture,
    loop: str,
    attr: str,
    message: str,
):
    service = FactoryService(service_config)
    boom = _Boom()
    service.auto_builder = _Boom() if attr != "auto_builder" else boom
    service.auto_triager = service.ranker = None
    setattr(service, attr, boom)
    service.accepting_admission = True
    waits = 0

    async def one_pass(_seconds: float) -> None:
        nonlocal waits
        waits += 1
        if waits > 1:
            service._stop.set()

    service._wait = one_pass  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="omnigent_factory.service.runtime"):
        await getattr(service, loop)()
    assert boom.calls == 1
    [record] = [x for x in caplog.records if x.getMessage() == message]
    assert "RuntimeError: pass exploded" in traceback_of(record)


@pytest.mark.asyncio
async def test_a_background_failure_logs_its_traceback(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    service = FactoryService(service_config)

    async def no_wait(_seconds: float) -> None:
        return None

    service._wait = no_wait  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING, logger="omnigent_factory.service.runtime"):
        try:
            raise ValueError("admission exploded")
        except ValueError:
            assert await service._background_error("admission", 0) == 1
    [record] = [x for x in caplog.records if "background task failed" in x.getMessage()]
    assert "ValueError: admission exploded" in traceback_of(record)


def test_a_logged_traceback_is_redacted():
    try:
        raise RuntimeError("call failed with Bearer ghs_live_secret token=abc123")
    except RuntimeError:
        exc_info = sys.exc_info()
    record = logging.LogRecord("x", logging.WARNING, __file__, 1, "pass failed", (), exc_info)
    assert SecretRedactionFilter().filter(record)
    text = logging.Formatter("%(message)s").format(record)
    assert "Traceback (most recent call last)" in text
    assert "RuntimeError: call failed" in text
    assert "ghs_live_secret" not in text and "abc123" not in text
    assert "token=[REDACTED]" in text
