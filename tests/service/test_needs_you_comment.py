"""Needs you when a PR isn't ready and no fix attempt is left reads like a colleague
asking for direction (owner request, 2026-10-14).

#889 / PR #1022: an omni-host restart interrupted the build run's woken turn, so it ended
without reporting back. The comment then said only "required checks failed (23 checks:
18 success, 4 skipped, 1 failure); review-bot findings have no outcome (fixed, follow-up
or advisory); no accepted cross-vendor review of head 30613e90c0d2. I have no automatic
fix attempt left, so I need your call." It named neither the failing check nor the
threads, and didn't say the run had stopped. Now one renderer serves every readiness
Needs you: counts, the failing checks with links, each open thread, the stopped run and
a plain question.
"""

from __future__ import annotations

from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent, EffectKind, Preconditions
from omnigent_factory.core.types import BotState, Hold
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import PublicationRenderer, needs_you_text
from omnigent_factory.testing.builders import result_candidate
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
PR = 1022
HEAD_889 = "30613e90c0d2" + "a" * 28
CHECK_URL = "https://github.com/o/r/actions/runs/1/job/2"
QUESTION = (
    "Should I keep working on these, or do you want to take a look yourself? "
    "Reply here and I'll keep going."
)
STOPPED = "My last run stopped before it reported back (it may have been interrupted)."
FORBIDDEN = (
    "Factory:",
    "readiness_failed",
    "readiness-failed",
    "ss_",
    "ef_",
    "I_parcel",
    "build_ready",
    "/continue",
    "/decide",
    "need your call",
    "\n> ",
)

THREADS_889 = (
    ev.FindingRef(
        path="api/src/approvals/history.ts",
        severity="P1",
        title="Count export failures per transition",
        url="https://github.com/o/r/pull/1022#discussion_r11",
    ),
    ev.FindingRef(
        path="api/src/approvals/repository.ts",
        severity="P2",
        title="Chunk the anchor query at 2000 ids",
        url="https://github.com/o/r/pull/1022#discussion_r12",
    ),
    ev.FindingRef(
        path="web/app/routes/approvals.tsx",
        severity="P2",
        title="Keep the `status` filter on refresh",
        url="https://github.com/o/r/pull/1022#discussion_r13",
    ),
    ev.FindingRef(path="", severity="", title="Document the new envelope", url=""),
)


def _args(**kw: Any) -> dict[str, Any]:
    args: dict[str, Any] = {
        "pr_number": PR,
        "head_sha": HEAD_889,
        "reason": "internal reason text",
        "checks_failed": False,
        "failing_checks": [],
        "findings_open": False,
        "review_missing": False,
        "closes_issue": True,
        "issue": 889,
        "conflict": "",
        "run_stopped": False,
    }
    args.update(kw)
    return args


def _thread(i: int) -> dict[str, str]:
    return {
        "path": f"src/file_{i}.py",
        "severity": "P2",
        "title": f"Finding {i}",
        "url": f"https://github.com/o/r/pull/1022#discussion_r{i}",
    }


def _plain(text: str) -> None:
    for bad in FORBIDDEN:
        assert bad not in text, (bad, text)
    assert not any(line.startswith(">") for line in text.splitlines())


# ------------------------------------------------------------------- renderer


def test_headline_counts_every_gap_with_the_short_head():
    text = needs_you_text(
        _args(
            checks_failed=True,
            failing_checks=[{"name": "api / Test", "url": CHECK_URL}],
            findings_open=True,
            findings=[_thread(1), _thread(2), _thread(3), _thread(4)],
            review_missing=True,
        ),
        "Codex",
    )
    _plain(text)
    assert text.splitlines()[0] == (
        "PR #1022 isn't ready: 4 Codex threads are unresolved, 1 required check is failing "
        "and no cross-vendor review covers this head yet (head `30613e9`)."
    )
    assert "internal reason text" not in text
    assert text.endswith(QUESTION)


def test_singular_and_plural_counts_and_other_gaps():
    one = needs_you_text(_args(findings_open=True, findings=[_thread(1)]), "Codex")
    assert (
        one.splitlines()[0]
        == "PR #1022 isn't ready: 1 Codex thread is unresolved (head `30613e9`)."
    )
    two = needs_you_text(
        _args(
            checks_failed=True,
            failing_checks=[{"name": "a", "url": ""}, {"name": "b", "url": ""}],
            closes_issue=False,
            conflict="main",
        ),
        "Codex",
    )
    assert two.splitlines()[0] == (
        "PR #1022 isn't ready: it has a merge conflict with `main`, 2 required checks are "
        "failing and the PR doesn't close #889 (head `30613e9`)."
    )


def test_failing_checks_are_named_and_linked():
    text = needs_you_text(
        _args(
            checks_failed=True,
            failing_checks=[
                {"name": "api / DB-tier Test", "url": CHECK_URL},
                {"name": "web / Typecheck, test, lint", "url": ""},
                {"name": "odd [name]", "url": CHECK_URL},
            ],
        ),
        "Codex",
    )
    _plain(text)
    assert "**Failing required checks**" in text
    assert f"- [api / DB-tier Test]({CHECK_URL})" in text
    assert "- `web / Typecheck, test, lint`" in text  # no link reported: the name alone
    assert f"- [odd (name)]({CHECK_URL})" in text  # brackets cannot break the link
    assert "3 required checks are failing" in text


def test_failing_checks_without_names_still_count_in_the_headline():
    text = needs_you_text(_args(checks_failed=True), "Codex")
    assert "required checks are failing" in text and "**Failing required checks**" not in text


def test_each_thread_is_one_line_with_severity_path_title_and_link():
    text = needs_you_text(
        _args(
            findings_open=True,
            findings=[
                _thread(1),
                {"path": "a`b.py", "severity": "", "title": "Use `x`\nnot y", "url": ""},
            ],
            further_round=True,
        ),
        "Codex",
    )
    _plain(text)
    assert "**Unresolved Codex threads** (a further review round after a fix commit)" in text
    assert (
        "- **P2** `src/file_1.py`: Finding 1 "
        "([thread](https://github.com/o/r/pull/1022#discussion_r1))"
    ) in text
    assert "- `a'b.py`: Use 'x' not y" in text  # no link: the line alone


def test_threads_are_capped_at_ten_then_and_n_more():
    text = needs_you_text(
        _args(findings_open=True, findings=[_thread(i) for i in range(13)]), "Codex"
    )
    assert "13 Codex threads are unresolved" in text
    assert "Finding 9" in text and "Finding 10" not in text
    assert "- and 3 more" in text
    # A full read (20, the cap) may hide more threads: never an exact count.
    full = needs_you_text(
        _args(findings_open=True, findings=[_thread(i) for i in range(20)]), "Codex"
    )
    assert "20+ Codex threads are unresolved" in full and "- and 10+ more" in full


def test_open_findings_without_thread_details_still_say_so():
    text = needs_you_text(_args(findings_open=True), "Codex")
    assert "Codex threads are unresolved" in text and "**Unresolved" not in text


def test_a_run_that_stopped_without_reporting_back_is_said_plainly():
    stopped = needs_you_text(_args(checks_failed=True, run_stopped=True), "Codex")
    assert STOPPED in stopped
    assert STOPPED not in needs_you_text(_args(checks_failed=True), "Codex")


def test_effects_recorded_before_the_gaps_keep_their_reason():
    """A comment effect emitted by the previous version (pending across the upgrade)."""
    text = needs_you_text(
        {
            "pr_number": 7,
            "head_sha": "9bb7f7f" + "0" * 33,
            "reason": "merge conflict with main",
        },
        "Codex",
    )
    _plain(text)
    assert text.splitlines()[0] == "PR #7 isn't ready (head `9bb7f7f`): merge conflict with main."
    assert text.endswith(QUESTION)


# ------------------------------------------------------------------- #889 replay


def _evidence_889(h: Harness) -> ev.ReadinessEvidence:
    r = h.p().readiness
    assert r is not None
    return ev.ReadinessEvidence(
        session_id=r.session_id,
        pr_number=PR,
        head_sha=HEAD_889,
        checks=ev.ChecksState.FAILED,
        checks_summary="23 checks: 18 success, 4 skipped, 1 failure",
        failing_checks="api / DB-tier Test",
        failing_check_links=(ev.CheckRef(name="api / DB-tier Test", url=CHECK_URL),),
        findings_open=True,
        open_findings=THREADS_889,
        review_accepted=False,
    )


def _comments(result: Any) -> list[EffectIntent]:
    return [
        e
        for e in result.effects
        if e.kind == EffectKind.POST_COMMENT and e.args.get("template") == "ready-blocked"
    ]


def _replay_889() -> tuple[Harness, EffectIntent]:
    h = Harness()
    h.to_building()
    s = h.cur(P)
    assert s.root_id is not None
    r = h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=PR,
            head_sha=HEAD_889,
        ),
    )
    assert r.audit.accepted, r.audit.reason
    h.quiesce(P, h.cur(P).session_id)
    # The read: a red check, Codex threads, no review of this head: the one wake.
    r = h.send(P, _evidence_889(h))
    assert [e.kind for e in r.effects if e.kind == EffectKind.SEND_MESSAGE]
    # The omni-host restart interrupts the woken turn: it ends idle, no result.
    h.quiesce(P, h.cur(P).session_id)
    r = h.send(P, _evidence_889(h))
    [comment] = _comments(r)
    assert Hold.READINESS_FAILED in h.p().holds and h.p().bot == BotState.NEEDS_YOU
    return h, comment


EXPECTED_889 = f"""\
PR #1022 isn't ready: 4 Codex threads are unresolved, 1 required check is failing and no \
cross-vendor review covers this head yet (head `30613e9`).

**Failing required checks**

- [api / DB-tier Test]({CHECK_URL})

**Unresolved Codex threads**

- **P1** `api/src/approvals/history.ts`: Count export failures per transition \
([thread](https://github.com/o/r/pull/1022#discussion_r11))
- **P2** `api/src/approvals/repository.ts`: Chunk the anchor query at 2000 ids \
([thread](https://github.com/o/r/pull/1022#discussion_r12))
- **P2** `web/app/routes/approvals.tsx`: Keep the 'status' filter on refresh \
([thread](https://github.com/o/r/pull/1022#discussion_r13))
- (no file): Document the new envelope

{STOPPED}

{QUESTION}"""


class _Db:
    def __init__(self, h: Harness) -> None:
        self.h = h

    async def call(self, fn: Any) -> Any:
        return self.h.p()


class _Directory:
    def __init__(self, h: Harness) -> None:
        self.db = _Db(h)

    def latest_result(self, session_id: str) -> None:
        return None


@pytest.mark.asyncio
async def test_889_replay_names_the_check_threads_stopped_run_and_asks(
    service_config: ServiceConfig,
):
    h, comment = _replay_889()
    assert comment.args["run_stopped"] is True
    effect = EffectIntent(
        "ef_889",
        comment.kind,
        P,
        P,
        Preconditions(1, 0),
        dict(comment.args),
    )
    renderer = PublicationRenderer(_Directory(h), service_config)  # type: ignore[arg-type]
    text = await renderer(effect)
    assert text == EXPECTED_889
    _plain(text)
    # The same state again (or a restart's re-read): no second comment.
    r = h.send(P, _evidence_889(h))
    assert not _comments(r)
