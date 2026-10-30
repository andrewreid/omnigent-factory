"""The ``factory:close-candidate`` label as durable effect work (``SET_CLOSE_CANDIDATE``):
one per published triage generation, a removal when the card leaves Triage, retried by
the effect-recovery machinery, never re-added after an owner's removal."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    Ack,
    AmbiguousWrite,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    Preconditions,
    RetryableReadFailure,
)
from omnigent_factory.core.protocol import CLOSE_CANDIDATE_LABEL
from omnigent_factory.core.types import Hold, Stage, Via
from omnigent_factory.github.client import GitHubClient, GitHubRejected
from omnigent_factory.github.links import remove_label
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from tests.service.test_auto_triage import finish, triage_running
from tests.service.test_pilot_677 import (
    CTX,
    ISSUE,
    PARCEL,
    SESSION,
    TRIAGE,
    _context_adapter,
    _effect,
    _GitHubServer,
    eventually,
)

LABEL_PATH = "/labels/factory%3Aclose-candidate"


class _LabelServer(_GitHubServer):
    """The pilot server plus the label endpoints. ``statuses`` scripts answers per kind:
    ``label`` (GET/POST repository label), ``add`` (POST the close-candidate to the
    issue), ``info`` (POST informational labels), ``delete``."""

    def __init__(self, config: ServiceConfig, **kwargs: Any) -> None:
        self.label_exists = kwargs.pop("label_exists", False)
        self.on_issue = kwargs.pop("on_issue", False)
        self.statuses: dict[str, list[int]] = kwargs.pop("statuses", {})
        super().__init__(config, **kwargs)

    def _status(self, kind: str, default: int) -> int:
        queue = self.statuses.get(kind)
        return queue.pop(0) if queue else default

    def __call__(self, request: httpx.Request) -> httpx.Response:
        repo = f"/repos/{self.config.repository}"
        path = request.url.raw_path.decode().split("?")[0]
        body = json.loads(request.content) if request.content else None
        if path == f"{repo}{LABEL_PATH}" and request.method == "GET":
            self.requests.append((request.method, path, None))
            return httpx.Response(self._status("label", 200 if self.label_exists else 404))
        if path == f"{repo}/labels" and request.method == "POST":
            self.requests.append((request.method, path, body))
            self.label_exists = True
            return httpx.Response(self._status("label", 201), json=body)
        if path == f"{repo}/issues/{ISSUE}/labels" and request.method == "POST":
            self.requests.append((request.method, path, body))
            kind = "add" if body == {"labels": [CLOSE_CANDIDATE_LABEL]} else "info"
            status = self._status(kind, 200)
            if kind == "add" and status == 200:
                self.on_issue = True
            return httpx.Response(status, json=[])
        if path == f"{repo}/issues/{ISSUE}{LABEL_PATH}" and request.method == "DELETE":
            self.requests.append((request.method, path, None))
            status = self._status("delete", 200 if self.on_issue else 404)
            if status in (200, 404):
                self.on_issue = False
            return httpx.Response(status, json=[])
        return super().__call__(request)

    def label_calls(self) -> list[tuple[str, str, Any]]:
        return [
            (m, p, b)
            for m, p, b in self.requests
            if "close-candidate" in p or (m == "POST" and p.endswith("/labels"))
        ]


def _store_result(config: ServiceConfig, result: dict[str, Any]) -> None:
    results = config.state_dir / "results"
    results.mkdir(mode=0o700, exist_ok=True)
    payload = {"item_id": "item", "factory_result": {"result": result}, "contract_canonical": None}
    (results / f"{SESSION}-latest.json").write_text(json.dumps(payload))


def _label_effect(action: str) -> EffectIntent:
    return EffectIntent(
        effect_id=f"ef_close_{action}",
        kind=EffectKind.SET_CLOSE_CANDIDATE,
        parcel_id=PARCEL,
        target=PARCEL,
        preconditions=Preconditions(parcel_version=1, eligibility_epoch=0),
        args={"action": action, "session_id": SESSION},
    )


async def _execute(
    config: ServiceConfig, server: _LabelServer, result: dict[str, Any], *effects: EffectIntent
) -> list[Any]:
    adapter, service, http = await _context_adapter(config, server)
    _store_result(config, result)
    try:
        return [await adapter.execute(effect, CTX) for effect in effects]
    finally:
        await http.aclose()
        await service.stop()


ALREADY_FIXED = {
    **TRIAGE,
    "labels": ["area:api"],
    "recommendation": "already_fixed",
    "closure_evidence": "Commit abc1234 already guards the route.",
}


# ------------------------------------------------------------------ the adapter


@pytest.mark.asyncio
@pytest.mark.parametrize("recommendation", ["already_fixed", "obsolete", "wont_fix", "duplicate"])
async def test_a_closure_triage_creates_and_adds_the_label(
    service_config: ServiceConfig, recommendation: str
):
    server = _LabelServer(service_config)
    result = {**ALREADY_FIXED, "recommendation": recommendation}
    if recommendation == "duplicate":
        result["duplicate_issue"] = 5
    [outcome] = await _execute(service_config, server, result, _label_effect("triage"))
    assert isinstance(outcome, Ack) and outcome.detail["close_candidate"] == "added"
    repo = f"/repos/{service_config.repository}"
    calls = server.label_calls()
    assert calls[0] == ("GET", f"{repo}{LABEL_PATH}", None)  # absent: created first
    assert calls[1][:2] == ("POST", f"{repo}/labels")
    assert calls[1][2]["name"] == CLOSE_CANDIDATE_LABEL
    assert calls[2] == (
        "POST",
        f"{repo}/issues/{ISSUE}/labels",
        {"labels": [CLOSE_CANDIDATE_LABEL]},
    )


@pytest.mark.asyncio
async def test_an_existing_label_is_not_created_again(service_config: ServiceConfig):
    server = _LabelServer(service_config, label_exists=True)
    [outcome] = await _execute(service_config, server, ALREADY_FIXED, _label_effect("triage"))
    assert isinstance(outcome, Ack)
    repo = f"/repos/{service_config.repository}"
    assert not [p for m, p, _ in server.label_calls() if m == "POST" and p == f"{repo}/labels"]


@pytest.mark.asyncio
@pytest.mark.parametrize("on_issue", [True, False])
@pytest.mark.parametrize("action", ["triage", "remove"])
async def test_a_work_triage_or_leaving_triage_removes_the_label_404_is_success(
    service_config: ServiceConfig, action: str, on_issue: bool
):
    server = _LabelServer(service_config, on_issue=on_issue)
    work = {**TRIAGE, "recommendation": "needs_info"}
    [outcome] = await _execute(service_config, server, work, _label_effect(action))
    assert isinstance(outcome, Ack) and outcome.detail["close_candidate"] == "removed"
    repo = f"/repos/{service_config.repository}"
    assert [(m, p) for m, p, _ in server.label_calls()] == [
        ("DELETE", f"{repo}/issues/{ISSUE}{LABEL_PATH}")
    ]


@pytest.mark.asyncio
async def test_an_epic_triage_leaves_the_label_alone(service_config: ServiceConfig):
    server = _LabelServer(service_config)
    epic = {"kind": "epic_triage", "summary": "s"}
    [outcome] = await _execute(service_config, server, epic, _label_effect("triage"))
    assert isinstance(outcome, Ack) and outcome.detail["close_candidate"] == "unchanged"
    assert server.label_calls() == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("statuses", "kind"),
    [
        ({"add": [502]}, RetryableReadFailure),
        ({"label": [503]}, RetryableReadFailure),
        ({"delete": [500]}, RetryableReadFailure),
        ({"add": [422]}, DefinitiveFailure),  # terminal: a failed effect, shown in recovery
        ({"delete": [403]}, DefinitiveFailure),
    ],
)
async def test_errors_map_to_the_effect_recovery_classes(
    service_config: ServiceConfig, statuses: dict[str, list[int]], kind: type
):
    server = _LabelServer(service_config, statuses=statuses)
    result = TRIAGE if "delete" in statuses else ALREADY_FIXED
    [outcome] = await _execute(service_config, server, result, _label_effect("triage"))
    assert isinstance(outcome, kind), outcome


@pytest.mark.asyncio
async def test_a_rate_limit_retries_after_github_asks(service_config: ServiceConfig):
    class _Limited(_LabelServer):
        def __call__(self, request: httpx.Request) -> httpx.Response:
            if "close-candidate" in request.url.raw_path.decode():
                return httpx.Response(429, headers={"retry-after": "30"})
            return super().__call__(request)

    [outcome] = await _execute(
        service_config, _Limited(service_config), ALREADY_FIXED, _label_effect("triage")
    )
    assert isinstance(outcome, RetryableReadFailure)
    assert outcome.retry_after_us == 30_000_000


@pytest.mark.asyncio
async def test_an_informational_label_refusal_never_skips_the_close_candidate(
    service_config: ServiceConfig,
):
    """The triage comment's informational labels (422) and the close-candidate label are
    separate work: the publication acks, the label effect still converges."""
    server = _LabelServer(service_config, statuses={"info": [422]})
    published, labelled = await _execute(
        service_config,
        server,
        ALREADY_FIXED,
        _effect(EffectKind.PUBLISH_TRIAGE, session_id=SESSION),
        _label_effect("triage"),
    )
    assert isinstance(published, Ack) and published.detail["fields_error"] == "HTTP 422"
    assert isinstance(labelled, Ack) and labelled.detail["close_candidate"] == "added"
    assert server.on_issue


async def _remove(status: int) -> list[str]:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(f"{request.method} {request.url.raw_path.decode()}")
        return httpx.Response(status, json=[])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await remove_label(GitHubClient(http, "t"), "o/r", 7, CLOSE_CANDIDATE_LABEL)
    return seen


@pytest.mark.asyncio
async def test_remove_label_helper_treats_404_as_success():
    assert await _remove(200) == ["DELETE /repos/o/r/issues/7/labels/factory%3Aclose-candidate"]
    assert await _remove(404) == ["DELETE /repos/o/r/issues/7/labels/factory%3Aclose-candidate"]
    with pytest.raises(GitHubRejected):
        await _remove(403)


# ------------------------------------------------------------------ the executor


@asynccontextmanager
async def running(
    config: ServiceConfig, clock: FakeClock, github: FakeGitHub
) -> AsyncIterator[FactoryService]:
    service = FactoryService(
        config.model_copy(
            update={"effect_recovery_backoff_seconds": 1.0, "effect_recovery_max_attempts": 5}
        ),
        adapters=(github, FakeOmnigent(), FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        yield service
    finally:
        await service.stop()


async def _label_rows(service: FactoryService) -> list[tuple[str, int]]:
    rows = await service.db.call(
        lambda store: store.query(
            "SELECT state, attempts FROM effects WHERE kind = ? ORDER BY rowid",
            (EffectKind.SET_CLOSE_CANDIDATE.value,),
        )
    )
    return [(str(r[0]), int(r[1])) for r in rows]


async def _triaged(service: FactoryService, clock: FakeClock, github: FakeGitHub) -> str:
    from tests.service.test_auto_triage import Rig

    pid = "I_cc_1"
    github.snapshots[pid] = snapshot(stage=Stage.TRIAGED, read_at_us=clock.now_utc_us())
    factory = EventFactory(pid, issue_number=ISSUE, start_us=clock.now_utc_us())
    await service.apply_event(
        factory.make(
            ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now), event_id=f"{pid}:s"
        )
    )
    await service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG), event_id=f"{pid}:t"))
    rig = Rig(service, github, clock, [], None)  # type: ignore[arg-type]
    await triage_running(rig, pid)
    await finish(rig, pid)

    async def posted() -> bool:
        p = await service.db.call(lambda store: store.load_parcel(pid))
        return p is not None and Hold.AWAITING_OWNER in p.holds

    await eventually(posted)
    return pid


def _calls(github: FakeGitHub) -> int:
    return len(github.executed(EffectKind.SET_CLOSE_CANDIDATE))


@pytest.mark.asyncio
async def test_a_5xx_then_a_rate_limit_then_success_is_retried_with_backoff(
    service_config: ServiceConfig,
):
    clock, github = FakeClock(), FakeGitHub()
    github.script(
        EffectKind.SET_CLOSE_CANDIDATE,
        RetryableReadFailure("GitHub POST returned HTTP 502"),
        RetryableReadFailure("rate limited", 30_000_000),
        Ack(None, {"close_candidate": "added"}),
    )
    async with running(service_config, clock, github) as service:
        await _triaged(service, clock, github)
        await eventually(lambda: _calls(github) >= 1)
        assert (await _label_rows(service))[0][0] == "pending"  # scheduled, not failed
        clock.advance(2_000_000)  # past the first backoff
        await eventually(lambda: _calls(github) >= 2)
        clock.advance(31_000_000)  # past GitHub's Retry-After
        await eventually(lambda: _calls(github) >= 3)

        async def done() -> bool:
            return (await _label_rows(service))[0][0] == "done"

        await eventually(done)
        assert _calls(github) == 3


@pytest.mark.asyncio
async def test_a_pending_label_effect_survives_a_restart(service_config: ServiceConfig):
    clock = FakeClock()
    first = FakeGitHub()
    first.script(EffectKind.SET_CLOSE_CANDIDATE, RetryableReadFailure("down", 3_600_000_000))
    async with running(service_config, clock, first) as service:
        await _triaged(service, clock, first)
        await eventually(lambda: _calls(first) >= 1)
        assert (await _label_rows(service))[0][0] == "pending"
    clock.reboot()
    clock.advance(3_700_000_000)
    second = FakeGitHub()
    async with running(service_config, clock, second) as service:
        await eventually(lambda: _calls(second) >= 1)
        [effect] = second.executed(EffectKind.SET_CLOSE_CANDIDATE)
        assert effect.args["action"] == "triage"

        async def done() -> bool:
            return (await _label_rows(service))[0][0] == "done"

        await eventually(done)


@pytest.mark.asyncio
async def test_an_owner_removal_sticks_until_the_next_triage(service_config: ServiceConfig):
    clock, github = FakeClock(), FakeGitHub()
    async with running(service_config, clock, github) as service:
        pid = await _triaged(service, clock, github)
        await eventually(lambda: _calls(github) >= 1)
        [publish] = github.executed(EffectKind.PUBLISH_TRIAGE)
        # The owner removes the label; a repeated acknowledgement of that publication
        # (or any later event) never issues the label again for this triage.
        factory = EventFactory(pid, issue_number=ISSUE, start_us=clock.now_utc_us())
        again = await service.apply_event(
            factory.make(
                ev.PublicationAcked(
                    effect_id=publish.effect_id,
                    effect_kind=EffectKind.PUBLISH_TRIAGE.value,
                    session_id=publish.preconditions.session_id,
                    comment_id="c-again",
                ),
                event_id=f"{pid}:ack-again",
            )
        )
        assert again.accepted
        assert [e for e in again.effects if e.kind == EffectKind.SET_CLOSE_CANDIDATE] == []
        assert len(await _label_rows(service)) == 1


@pytest.mark.asyncio
async def test_leaving_triage_is_a_durable_removal_however_it_is_seen(
    service_config: ServiceConfig,
):
    clock, github = FakeClock(), FakeGitHub()
    async with running(service_config, clock, github) as service:
        pid = await _triaged(service, clock, github)
        await eventually(lambda: _calls(github) >= 1)
        factory = EventFactory(pid, issue_number=ISSUE, start_us=clock.now_utc_us())
        # The owner's drag to Planning, as any accepted event that leaves Triaged.
        moved = await service.apply_event(
            factory.make(ev.RequestPlan(via=Via.DRAG), event_id=f"{pid}:plan")
        )
        assert moved.accepted, moved.reason
        removal = [e for e in moved.effects if e.kind == EffectKind.SET_CLOSE_CANDIDATE]
        assert [e.args["action"] for e in removal] == ["remove"]

        async def removed() -> bool:
            return any(
                e.args["action"] == "remove"
                for e in github.executed(EffectKind.SET_CLOSE_CANDIDATE)
            )

        await eventually(removed)
        assert [r[0] for r in await _label_rows(service)] == ["done", "done"]


# ------------------------------------------------------------------ owner removal


async def _owner_unlabels(service: FactoryService, clock: FakeClock, pid: str, tag: str) -> None:
    clock.advance(1_000_000)
    factory = EventFactory(pid, issue_number=ISSUE, start_us=clock.now_utc_us())
    result = await service.apply_event(
        factory.make(
            ev.CloseCandidateRemoved(),
            event_id=f"{pid}:unlabel:{tag}",
            evidence=snapshot(stage=Stage.TRIAGED, read_at_us=clock.now_utc_us()),
        )
    )
    assert result.accepted, result.reason


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first",
    [
        RetryableReadFailure("GitHub POST returned HTTP 502"),  # pending retry
        AmbiguousWrite("ReadTimeout"),  # the add landed, its acknowledgement was lost
    ],
)
async def test_an_owner_removal_cancels_this_generations_retried_add(
    service_config: ServiceConfig, first: Any
):
    clock, github = FakeClock(), FakeGitHub()
    github.script(EffectKind.SET_CLOSE_CANDIDATE, first)
    async with running(service_config, clock, github) as service:
        pid = await _triaged(service, clock, github)
        await eventually(lambda: _calls(github) >= 1)
        assert (await _label_rows(service))[0][0] == "pending"  # automatic recovery
        await _owner_unlabels(service, clock, pid, "1")
        clock.advance(3_600_000_000)  # well past every backoff

        async def settled() -> bool:
            return (await _label_rows(service))[0][0] == "done"  # superseded, not failed

        await eventually(settled)
        assert _calls(github) == 1  # never re-added
        parcel = await service.db.call(lambda store: store.load_parcel(pid))
        assert parcel is not None and parcel.unknown_effects == ()


@pytest.mark.asyncio
async def test_the_next_triage_generation_converges_again(service_config: ServiceConfig):
    from tests.service.test_auto_retriage import finish_again
    from tests.service.test_auto_triage import Rig

    clock, github = FakeClock(), FakeGitHub()
    github.script(EffectKind.SET_CLOSE_CANDIDATE, RetryableReadFailure("down", 3_600_000_000))
    async with running(service_config, clock, github) as service:
        pid = await _triaged(service, clock, github)
        await eventually(lambda: _calls(github) >= 1)
        await _owner_unlabels(service, clock, pid, "1")
        # The owner comments: a new triage is published (a new generation).
        factory = EventFactory(pid, issue_number=ISSUE, start_us=clock.now_utc_us())
        await service.apply_event(
            factory.make(
                ev.PlanFeedback(text_digest="d" * 64),
                event_id=f"{pid}:comment",
                evidence=snapshot(stage=Stage.TRIAGED, read_at_us=clock.now_utc_us()),
            )
        )
        rig = Rig(service, github, clock, [], None)  # type: ignore[arg-type]
        await finish_again(rig, pid, "second")

        async def second() -> bool:
            return (
                len({e.args["session_id"] for e in github.executed(EffectKind.SET_CLOSE_CANDIDATE)})
                == 2
            )

        await eventually(second)
        newest = github.executed(EffectKind.SET_CLOSE_CANDIDATE)[-1]
        assert newest.args["action"] == "triage"
        parcel = await service.db.call(lambda store: store.load_parcel(pid))
        assert parcel is not None
        assert newest.args["session_id"] == parcel.close_candidate_session
        assert parcel.close_candidate_removed != parcel.close_candidate_session
