"""Adapter protocol conformance of the in-memory fakes, codec round trips, projection, CLI."""

from __future__ import annotations

from dataclasses import replace

import pytest

from omnigent_factory import cli
from omnigent_factory.core import codec
from omnigent_factory.core.effects import (
    Ack,
    AmbiguousWrite,
    CredentialProfile,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    Preconditions,
)
from omnigent_factory.core.events import BODY_TYPES
from omnigent_factory.core.projection import project_bot
from omnigent_factory.core.types import BotState, FenceKind, Hold, Lifecycle, UnknownEffect
from omnigent_factory.ports.adapter import EffectAdapter
from omnigent_factory.ports.clock import Clock, SystemClock
from omnigent_factory.ports.credentials import (
    CREDENTIAL_EFFECT_KINDS,
    CredentialBroker,
    TokenGrant,
    TokenRefusal,
)
from omnigent_factory.ports.github import GITHUB_EFFECT_KINDS, GitHubAdapter, IssueRef
from omnigent_factory.ports.omnigent import OMNIGENT_EFFECT_KINDS, OmnigentAdapter
from omnigent_factory.ports.scheduler import SCHEDULER_EFFECT_KINDS
from omnigent_factory.ports.workspace import WORKSPACE_EFFECT_KINDS
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
    FakeScheduler,
)
from omnigent_factory.testing.harness import Harness

CTX = ExecutionContext(boot_id="b", lease_epoch=1, parcel_version=1, attempt=1)


def intent(kind: EffectKind, session_id: str | None = "s1", **args) -> EffectIntent:
    return EffectIntent("e1", kind, "P", "t", Preconditions(1, 0, session_id=session_id), args=args)


def test_effect_kinds_partitioned_across_adapters():
    groups = [
        GITHUB_EFFECT_KINDS,
        OMNIGENT_EFFECT_KINDS,
        CREDENTIAL_EFFECT_KINDS,
        SCHEDULER_EFFECT_KINDS,
        WORKSPACE_EFFECT_KINDS,
    ]
    union = frozenset().union(*groups)
    assert union == frozenset(EffectKind)
    assert sum(len(g) for g in groups) == len(EffectKind)


def test_no_forbidden_effect_kinds():
    names = {k.value for k in EffectKind}
    for forbidden in ("merge", "close_issue", "delete_branch", "bypass", "admin", "ruleset"):
        assert not any(forbidden in n for n in names)


def test_fakes_satisfy_protocols():
    assert isinstance(FakeClock(), Clock) and isinstance(SystemClock(), Clock)
    assert isinstance(FakeGitHub(), GitHubAdapter)
    assert isinstance(FakeOmnigent(), OmnigentAdapter)
    assert isinstance(FakeCredentialBroker(), CredentialBroker)
    assert isinstance(FakeScheduler(), EffectAdapter)


def test_fake_clock():
    c = FakeClock(start_us=10)
    c.advance(5)
    assert (c.now_utc_us(), c.monotonic_us()) == (15, 5)
    c.reboot()
    assert (c.now_utc_us(), c.monotonic_us()) == (15, 0)
    with pytest.raises(ValueError):
        c.advance(-1)


@pytest.mark.asyncio
async def test_fake_github_scripted_outcomes_and_reads():
    gh = FakeGitHub()
    gh.script(EffectKind.POST_COMMENT, AmbiguousWrite("timeout"))
    assert isinstance(await gh.execute(intent(EffectKind.POST_COMMENT), CTX), AmbiguousWrite)
    assert isinstance(await gh.execute(intent(EffectKind.POST_COMMENT), CTX), Ack)
    assert len(gh.executed(EffectKind.POST_COMMENT)) == 2
    ref = IssueRef("R", 1, "P")
    assert (await gh.issue_snapshot(ref)).__class__.__name__ == "RetryableReadFailure"
    gh.snapshots["P"] = snapshot()
    assert (await gh.issue_snapshot(ref)).eligible
    with pytest.raises(ValueError):
        await gh.execute(intent(EffectKind.CREATE_SESSION), CTX)


@pytest.mark.asyncio
async def test_fake_omnigent_adoption_by_nonce_and_scan():
    om = FakeOmnigent()
    out = await om.execute(intent(EffectKind.CREATE_SESSION, nonce="n1"), CTX)
    assert isinstance(out, Ack)
    [match] = await om.find_by_nonce("n1")
    assert match.root_id == out.remote_id
    assert await om.find_by_nonce("other") == []
    scan = await om.scan_tree("root")
    assert scan.complete and not scan.busy


@pytest.mark.asyncio
async def test_fake_broker_denies_by_default_and_follows_issuance_effects():
    broker = FakeCredentialBroker()
    assert isinstance(await broker.request_token("s1", "cap", broker.repository), TokenRefusal)
    await broker.execute(intent(EffectKind.ENABLE_ISSUANCE, profile="build"), CTX)
    grant = await broker.request_token("s1", "cap", broker.repository)
    assert isinstance(grant, TokenGrant) and grant.profile == CredentialProfile.BUILD
    assert isinstance(await broker.request_token("s1", "cap", "other/repo"), TokenRefusal)
    await broker.execute(intent(EffectKind.DISABLE_ISSUANCE), CTX)
    assert not broker.issuance_enabled("s1")


def test_codec_round_trips_every_event_body_and_aggregate():
    f = EventFactory()
    for body_type in BODY_TYPES.values():
        event = f.make(body_type(), evidence=snapshot())  # type: ignore[call-arg]
        assert codec.event_from_json(codec.event_to_json(event)) == event
    h = Harness()
    h.to_building()
    p = h.p()
    assert codec.parcel_from_json(codec.parcel_to_json(p)) == p
    assert codec.admission_from_data(codec.admission_to_data(h.admission)) == h.admission
    for _, result in h.log:
        for e in result.effects:
            assert codec.effect_from_json(codec.effect_to_json(e)) == e
    with pytest.raises(ValueError):
        codec.parcel_from_json('{"schema_version": 99}')


def test_bot_projection_precedence():
    h = Harness()
    b = h.to_building()
    p = h.p()
    assert project_bot(p) == BotState.WORKING
    s = replace(b, lifecycle=Lifecycle.CHECKPOINT_WAIT)
    cp = replace(p, sessions=tuple(s if x.session_id == s.session_id else x for x in p.sessions))
    assert project_bot(cp) == BotState.CHECKPOINT
    assert project_bot(replace(cp, holds=frozenset({Hold.STOP_UNVERIFIED}))) == BotState.BLOCKED
    assert project_bot(replace(p, holds=frozenset({Hold.AWAITING_OWNER}))) == BotState.NEEDS_YOU
    fenced = replace(b, lifecycle=Lifecycle.FENCED, fences=frozenset({FenceKind.STOPPED}))
    idle = replace(
        p, sessions=tuple(fenced if x.session_id == b.session_id else x for x in p.sessions)
    )
    assert project_bot(idle) == BotState.IDLE
    assert (
        project_bot(replace(idle, unknown_effects=(UnknownEffect("e", "post_comment"),)))
        == BotState.BLOCKED
    )


def test_cli_version_and_db_init(tmp_path, capsys):
    assert cli.main(["version"]) == 0
    assert capsys.readouterr().out.strip() == "0.1.0"
    assert cli.main(["db", "init", str(tmp_path / "s.db")]) == 0
    assert "schema version 5" in capsys.readouterr().out
