"""#1576: real adapter reads/409, durable reducer, and disposable Git orphan binding."""

from dataclasses import replace

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    Ack,
    AmbiguousWrite,
    DefinitiveFailure,
    EffectKind,
    RetryableReadFailure,
)
from omnigent_factory.core.types import Stage, Via
from omnigent_factory.omnigent.outcomes import observations
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.fakes import FakeClock
from tests.credentials.repos import git
from tests.omnigent.fake_server import FakeSession
from tests.omnigent.support import CTX, intent, make_rig, spec
from tests.test_store import P, store_harness
from tests.test_store_durable import claim
from tests.test_unknown_create_research import refresh

pytestmark = pytest.mark.asyncio


async def test_409_empty_search_budget_then_new_nonce_binds_orphan(
    git_env, tmp_path, monkeypatch, caplog
):
    rig = make_rig(git_env)
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h = store_harness(store)
        h.eligible(P)
        result = h.send(P, ev.RequestTriage(via=Via.DRAG))
        [create] = h.of(result, EffectKind.CREATE_SESSION)
        old = h.cur(P)
        rig.directory.specs[old.session_id] = spec(old.session_id, nonce=old.nonce, kind=old.kind)
        rig.server.faults[("POST", "/v1/sessions")].append("worktree-then-409")
        outcome = await rig.adapter.execute(create, CTX)
        assert isinstance(outcome, AmbiguousWrite)
        assert "status=409 error_code=host_offline" in caplog.text
        assert "Worktree host unavailable" in caplog.text
        assert not rig.server.sessions
        claim(store, create.effect_id)
        [unknown] = observations(create, outcome)
        assert store.record_effect_outcome(
            create.effect_id,
            "unknown",
            from_states=("claimed",),
            event=h.f(P).make(unknown),
            config=h.cfg,
            reason=outcome.reason,
        ).recorded
        refresh(h)
        h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))
        h.send(P, ev.RequestTriage(via=Via.DRAG))
        for index in range(4):
            run = h.p(P).session(old.session_id)
            [pending] = [
                e
                for e in store.pending_effects(now_us=2**62)
                if e.effect.kind == EffectKind.RECONCILE_SESSION
                and e.effect.args.get("search") == index + 1
            ]
            search = pending.effect
            rig.clock.advance(max(run.create_search_not_before_us - rig.clock.now_utc_us(), 0))
            h.f(P).tick(max(run.create_search_not_before_us - h.f(P).now, 0))
            if index == 1:
                # Rate limit and ReadError are incomplete searches, not absence evidence.
                for fault in (429, "timeout-before"):
                    rig.server.faults[("GET", "/v1/sessions")].append(fault)
                    incomplete = await rig.adapter.execute(search, CTX)
                    assert isinstance(incomplete, RetryableReadFailure)
                    assert incomplete.retry_after_us == 60_000_000
                    assert observations(search, incomplete) == ()
                    assert store.load_parcel(P).session(old.session_id).create_searches == index
            ack = await rig.adapter.execute(search, CTX)
            assert isinstance(ack, Ack) and ack.detail["matches"] == 0
            [body] = observations(search, ack)
            claim(store, search.effect_id)
            assert store.record_effect_outcome(
                search.effect_id,
                "done",
                from_states=("claimed",),
                event=h.f(P).make(body, event_id=f"effect:{search.effect_id}:ack"),
                config=h.cfg,
            ).recorded
            refresh(h)
            if index < 3:
                assert h.cur(P).session_id == old.session_id
                assert rig.server.count("POST", "/v1/sessions") == 1
        fresh = h.cur(P)
        assert fresh.nonce != old.nonce and h.p(P).session(old.session_id).execution_closed
        rig.directory.specs[fresh.session_id] = spec(
            fresh.session_id, nonce=fresh.nonce, kind=fresh.kind
        )
        # Exercise the production local ownership predicate, with the test store as its source.
        directory = object.__new__(ServiceDispatchDirectory)

        async def parcel_for_session(_sid):
            return store.load_parcel(P)

        monkeypatch.setattr(directory, "_parcel_for_session", parcel_for_session)
        monkeypatch.setattr(
            rig.directory, "orphan_repair_allowed", directory.orphan_repair_allowed, raising=False
        )
        [new_create] = [
            e.effect
            for e in store.pending_effects(now_us=2**62)
            if e.effect.kind == EffectKind.CREATE_SESSION and e.effect.target == fresh.session_id
        ]
        repaired = await rig.adapter.execute(new_create, CTX)
        assert isinstance(repaired, Ack)
        assert rig.server.requests[-1][2]["git"]["existing_worktree"] is True
        assert "binding clean orphan worktree" in caplog.text
        assert len(rig.server.sessions) == 1
        assert rig.server.count("POST", "/v1/sessions") == 2

        # A late old-nonce root can appear after the fresh POST. The durable watch
        # verifies its original identity, then archives it without any preparation.
        remote = rig.server.sessions[repaired.remote_id]
        late = rig.server.add(
            replace(remote, id="late-old-root", labels={"factory.dispatch": old.nonce})
        )
        [watch] = [
            e.effect
            for e in store.pending_effects(now_us=2**62)
            if e.effect.kind == EffectKind.RECONCILE_SESSION and e.effect.target == old.session_id
        ]
        due = int(watch.args["not_before_us"])
        rig.clock.advance(max(due - rig.clock.now_utc_us(), 0))
        h.f(P).tick(max(due - h.f(P).now, 0))
        found = await rig.adapter.execute(watch, CTX)
        assert isinstance(found, Ack) and found.detail["root_id"] == late.id
        [adopt] = observations(watch, found)
        claim(store, watch.effect_id)
        assert store.record_effect_outcome(
            watch.effect_id,
            "done",
            from_states=("claimed",),
            event=h.f(P).make(adopt),
            config=h.cfg,
        ).recorded
        refresh(h)
        effects = [e.effect for e in store.pending_effects(now_us=2**62)]
        [archive] = [
            e for e in effects if e.kind == EffectKind.CLOSE_SESSION and e.target == late.id
        ]
        assert isinstance(await rig.adapter.execute(archive, CTX), Ack)
        assert late.archived and not remote.archived
        assert h.cur(P).session_id == fresh.session_id
        assert not any(
            e.kind == EffectKind.PREPARE_SESSION and e.target == old.session_id for e in effects
        )
    finally:
        store.close()
        await rig.rest.aclose()


@pytest.mark.parametrize(
    "unsafe", ["dirty", "ahead", "live-root", "local-owner", "read-error", "unowned-name"]
)
async def test_orphan_binding_preserves_unsafe_or_unowned_worktree(git_env, monkeypatch, unsafe):
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    rig.server.faults[("POST", "/v1/sessions")].append("worktree-then-409")
    assert isinstance(
        await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX),
        AmbiguousWrite,
    )
    path = git_env.worktrees / "factory-issue-42-g1"
    if unsafe == "dirty":
        (path / "untracked").write_text("keep me")
    elif unsafe == "ahead":
        git(
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--allow-empty",
            "-m",
            "work",
            cwd=path,
        )
    elif unsafe == "live-root":
        rig.server.add(
            FakeSession(
                id="live",
                agent_id="other-agent",
                workspace=str(path),
                git_branch="factory/issue-42-g1",
            )
        )
    elif unsafe == "read-error":
        rig.server.fail_paths.add("/v1/sessions")
    elif unsafe == "unowned-name":
        moved = git_env.worktrees / "owner-checkout"
        git("worktree", "move", str(path), str(moved), cwd=git_env.source)
        path = moved
    before = git("rev-parse", "HEAD", cwd=path)

    async def allowed(_sid):
        return unsafe != "local-owner"

    monkeypatch.setattr(rig.directory, "orphan_repair_allowed", allowed, raising=False)
    rig.directory.specs["S2"] = spec("S2", nonce="new")
    result = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, "S2", nonce="new"), CTX)
    if unsafe == "read-error":
        assert isinstance(result, RetryableReadFailure)
    else:
        assert isinstance(result, DefinitiveFailure) and "branch-already-exists" in result.reason
    assert rig.server.count("POST", "/v1/sessions") == 1
    assert path.is_dir() and git("rev-parse", "HEAD", cwd=path) == before
    await rig.rest.aclose()


async def test_search_not_before_defers_remote_read(git_env):
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    effect = intent(
        EffectKind.RECONCILE_SESSION,
        nonce="nonce-abc",
        not_before_us=rig.clock.now_utc_us() + 60_000_000,
    )
    outcome = await rig.adapter.execute(effect, CTX)
    assert isinstance(outcome, RetryableReadFailure) and outcome.retry_after_us == 60_000_000
    assert rig.server.requests == []
    rig.clock.advance(60_000_000)
    assert isinstance(await rig.adapter.execute(effect, CTX), Ack)
    await rig.rest.aclose()


async def test_nonce_search_honors_retry_after(git_env):
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    rig.server.faults[("GET", "/v1/sessions")].append(
        httpx.Response(429, headers={"Retry-After": "180"})
    )
    effect = intent(EffectKind.RECONCILE_SESSION, nonce="nonce-abc")
    outcome = await rig.adapter.execute(effect, CTX)
    assert isinstance(outcome, RetryableReadFailure)
    assert outcome.retry_after_us == 180_000_000 and observations(effect, outcome) == ()
    await rig.rest.aclose()
