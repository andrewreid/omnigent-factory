"""#1576: adapter + executor + durable store, with disposable Git and paged HTTP."""

import asyncio
from dataclasses import replace

import pytest
import pytest_asyncio

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectKind, PreSendRetryable
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.predicates import settled
from omnigent_factory.core.types import BotState, Lifecycle, Stage, Via
from omnigent_factory.omnigent.outcomes import observations
from omnigent_factory.service.db import StoreWorker
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.executor import EffectExecutor, ParcelSerializers
from omnigent_factory.testing.builders import snapshot
from tests.credentials.repos import git
from tests.omnigent.fake_server import FakeSession
from tests.omnigent.support import CTX, intent, make_rig, spec
from tests.test_store import P
from tests.test_unknown_create_research import complete_search, refresh, unknown_triage

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def pending_create(git_env, tmp_path, monkeypatch):
    rig = make_rig(git_env, page_limit=100)
    db = StoreWorker(tmp_path / "state.db", rig.clock)
    await db.start()
    try:

        def setup(store):
            h, _ = unknown_triage(store)
            h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))
            h.send(P, ev.RequestTriage(via=Via.DRAG))
            for _ in range(4):
                complete_search(h)
            run = h.cur(P)
            [create] = [
                e.effect
                for e in store.pending_effects(now_us=2**62)
                if e.effect.kind == EffectKind.CREATE_SESSION and e.effect.target == run.session_id
            ]
            return h, create, run

        h, create, run = await db.call(setup)
        rig.clock.advance(max(h.f(P).now - rig.clock.now_utc_us(), 0))
        rig.directory.specs[run.session_id] = spec(run.session_id, nonce=run.nonce, kind=run.kind)
        directory = object.__new__(ServiceDispatchDirectory)

        async def parcel_for_session(_sid):
            return await db.call(lambda store: store.load_parcel(P))

        monkeypatch.setattr(directory, "_parcel_for_session", parcel_for_session)
        monkeypatch.setattr(
            rig.directory, "orphan_repair_allowed", directory.orphan_repair_allowed, raising=False
        )
        branch = "factory/issue-42-g1"
        path = git_env.worktrees / branch.replace("/", "-")
        git("worktree", "add", "-b", branch, str(path), "origin/main", cwd=git_env.source)
        executor = EffectExecutor(
            db,
            h.cfg,
            rig.clock,
            [rig.adapter],
            ParcelSerializers(),
            poll_seconds=0.01,
            boot_id="boot",
        )
        yield rig, db, h, create, run, path, executor
    finally:
        await rig.rest.aclose()
        await db.close()


async def test_1576_timeout_retries_same_nonce_then_binds_orphan(
    pending_create, monkeypatch, caplog
):
    rig, db, _h, create, run, path, executor = pending_create
    rig.adapter.config = replace(rig.adapter.config, orphan_ownership_timeout_s=0.03)
    get_json = rig.rest.get_json
    cancelled = asyncio.Event()

    async def slow_inventory(url, params=None):
        if url == "/v1/sessions":
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        return await get_json(url, params)

    monkeypatch.setattr(rig.rest, "get_json", slow_inventory)
    await executor._execute(create.effect_id)
    await asyncio.wait_for(cancelled.wait(), 1)
    pending = await db.call(lambda store: store.get_effect(create.effect_id))
    assert pending.state == "pending" and pending.attempts == 1
    assert pending.effect == create
    assert pending.next_at_us == rig.clock.now_utc_us() + 120_000_000
    parcel = await db.call(lambda store: store.load_parcel(P))
    assert parcel.current_session_id == run.session_id
    assert parcel.current_session.nonce == run.nonce
    assert parcel.current_session.authorization_id == run.authorization_id
    assert parcel.bot == BotState.WORKING and "Blocked" not in parcel.board_note
    assert parcel.unknown_effects == ()
    assert parcel.current_session.create_searches == 0
    searches = await db.call(lambda store: store.pending_effects(now_us=2**62))
    assert not any(
        e.effect.kind == EffectKind.RECONCILE_SESSION and e.effect.target == run.session_id
        for e in searches
    )
    assert rig.server.count("POST", "/v1/sessions") == 0
    assert "reason=orphan ownership search timed out" in caplog.text

    # Pending proof survives reopen; claimed-without-proof remains unsafe (below).
    await db.close()
    db = StoreWorker(db._path, rig.clock)
    await db.start()
    try:
        executor = EffectExecutor(
            db,
            _h.cfg,
            rig.clock,
            [rig.adapter],
            ParcelSerializers(),
            poll_seconds=0.01,
            boot_id="boot",
        )

        # Reconnect the production ownership predicate to the reopened store.
        directory = object.__new__(ServiceDispatchDirectory)

        async def parcel_for_session(_sid):
            return await db.call(lambda store: store.load_parcel(P))

        monkeypatch.setattr(directory, "_parcel_for_session", parcel_for_session)
        monkeypatch.setattr(rig.directory, "orphan_repair_allowed", directory.orphan_repair_allowed)
        assert await db.call(lambda store: store.recover_claimed()) == []
        page_queries = []

        async def paged_inventory(url, params=None):
            if url == "/v1/sessions":
                assert params["limit"] == 1000
                page_queries.append(dict(params))
            return await get_json(url, params)

        monkeypatch.setattr(rig.rest, "get_json", paged_inventory)
        rig.adapter.config = replace(rig.adapter.config, orphan_ownership_timeout_s=180)
        # 4,501 unrelated roots/children, including archives, must all be traversed.
        for n in range(4501):
            rig.server.add(
                FakeSession(
                    id=f"unrelated-{n}",
                    agent_id="other-agent",
                    archived=n % 2 == 0,
                    parent_session_id="other-root" if n % 3 == 0 else None,
                )
            )
        rig.clock.advance(pending.next_at_us - rig.clock.now_utc_us())
        await executor._execute(create.effect_id)
        done = await db.call(lambda store: store.get_effect(create.effect_id))
        assert done.state == "done" and done.attempts == 2
        assert rig.server.count("GET", "/v1/sessions") == len(page_queries) == 5
        assert all(query.get("after") for query in page_queries[1:])
        assert rig.server.count("POST", "/v1/sessions") == 1
        body = rig.server.requests[-1][2]
        assert body["labels"]["factory.dispatch"] == run.nonce
        assert body["workspace"] == str(path)
        assert body["git"]["existing_worktree"] is True
        assert "binding clean orphan worktree" in caplog.text
    finally:
        await db.close()


async def test_1576_exhaustion_releases_slot_and_owner_starts_fresh(pending_create, caplog):
    rig, db, h, create, run, _path, executor = pending_create
    rig.server.fail_paths.add("/v1/sessions")
    reason = "orphan ownership search incomplete: GET /v1/sessions: HTTP 503"
    for attempt, delay in enumerate((120, 240, 480, 960, None), 1):
        await executor._execute(create.effect_id)
        row = await db.call(lambda store: store.get_effect(create.effect_id))
        assert row.attempts == attempt
        if delay is not None:
            assert row.state == "pending"
            assert row.next_at_us == rig.clock.now_utc_us() + delay * 1_000_000
            rig.clock.advance(delay * 1_000_000)
    assert row.state == "failed"
    assert caplog.text.count(f"reason={reason}") == 6  # five attempts + definitive failure
    parcel = await db.call(lambda store: store.load_parcel(P))
    retired = parcel.session(run.session_id)
    assert retired.lifecycle == Lifecycle.RETIRED and retired.execution_closed
    assert not retired.issuance_enabled and not retired.create_unconfirmed
    assert parcel.unknown_effects == ()
    assert parcel.create_rejected_reason == reason
    assert parcel.board_note == f"Blocked: could not start the session: {reason}"
    admission = await db.call(lambda store: store.load_admission(h.cfg.repo_id))
    assert P not in admission.triage_runs
    assert rig.server.count("POST", "/v1/sessions") == 0

    def owner_retry(store):
        refresh(h)
        h.f(P).tick(max(rig.clock.now_utc_us() - h.f(P).now, 0))
        h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))
        result = h.send(
            P,
            ev.RequestTriage(via=Via.DRAG),
            evidence=snapshot(stage=Stage.INBOX, read_at_us=h.f(P).now),
        )
        [new] = h.of(result, EffectKind.CREATE_SESSION)
        return new, h.cur(P)

    fresh, successor = await db.call(owner_retry)
    assert fresh.effect_id != create.effect_id and successor.nonce != run.nonce
    rig.directory.specs[successor.session_id] = spec(
        successor.session_id,
        nonce=successor.nonce,
        kind=successor.kind,
    )
    rig.server.fail_paths.clear()
    await executor._execute(fresh.effect_id)
    assert (await db.call(lambda store: store.get_effect(fresh.effect_id))).state == "done"
    assert rig.server.count("POST", "/v1/sessions") == 1


@pytest.mark.parametrize("fault", ["timeout-after", "500-after", "timeout-before"])
async def test_ambiguous_post_after_presend_retry_never_reposts(pending_create, fault):
    rig, db, _h, create, run, _path, executor = pending_create
    rig.server.fail_paths.add("/v1/sessions")
    await executor._execute(create.effect_id)
    row = await db.call(lambda store: store.get_effect(create.effect_id))
    rig.clock.advance(row.next_at_us - rig.clock.now_utc_us())
    rig.server.fail_paths.clear()
    rig.server.faults[("POST", "/v1/sessions")].append(fault)
    await executor._execute(create.effect_id)
    assert (await db.call(lambda store: store.get_effect(create.effect_id))).state == "unknown"
    await executor._execute(create.effect_id)
    assert rig.server.count("POST", "/v1/sessions") == 1
    parcel = await db.call(lambda store: store.load_parcel(P))
    assert parcel.session(run.session_id).create_search_pending


async def test_claimed_presend_retry_crash_becomes_unknown(pending_create):
    rig, db, _h, create, _run, _path, executor = pending_create
    rig.server.fail_paths.add("/v1/sessions")
    await executor._execute(create.effect_id)
    lease = await db.call(lambda store: store.acquire_lease(P, "boot"))
    assert await db.call(lambda store: store.claim_effect(create.effect_id, lease))
    [recovered] = await db.call(lambda store: store.recover_claimed())
    assert recovered.state == "unknown"
    await executor._execute(create.effect_id)
    assert rig.server.count("POST", "/v1/sessions") == 0


async def test_presend_retry_after_and_bounded_reason(pending_create, caplog):
    rig, db, _h, create, _run, _path, executor = pending_create
    lease = await db.call(lambda store: store.acquire_lease(P, "boot"))
    claimed = await db.call(lambda store: store.claim_effect(create.effect_id, lease))
    outcome = PreSendRetryable("inventory unavailable " + "x" * 1000, 600_000_000)
    assert observations(create, outcome) == ()
    await executor._finish(claimed, outcome)
    row = await db.call(lambda store: store.get_effect(create.effect_id))
    assert row.state == "pending" and row.next_at_us == rig.clock.now_utc_us() + 600_000_000
    records = [r for r in caplog.records if "effect not sent" in r.message]
    assert len(records) == 1
    assert records[0].message.split("reason=", 1)[1] == outcome.reason[:500]


@pytest.mark.parametrize("owner", ["branch-child", "workspace-root", "archived-busy", "none"])
async def test_orphan_inventory_later_page_preserves_all_owners(git_env, monkeypatch, owner):
    rig = make_rig(git_env)
    branch = "factory/issue-42-g1"
    path = git_env.worktrees / branch.replace("/", "-")
    git("worktree", "add", "-b", branch, str(path), "origin/main", cwd=git_env.source)
    rig.directory.specs["S1"] = spec()

    async def allowed(_sid):
        return True

    monkeypatch.setattr(rig.directory, "orphan_repair_allowed", allowed, raising=False)
    for n in range(2000):
        rig.server.add(FakeSession(id=f"unrelated-{n}", agent_id="different"))
    rig.server.add(
        FakeSession(
            id="last-owner",
            agent_id="different",
            created_at=0,
            git_branch=branch if owner == "branch-child" else None,
            parent_session_id="elsewhere" if owner == "branch-child" else None,
            workspace=str(path) if owner in ("workspace-root", "archived-busy") else None,
            archived=owner == "archived-busy",
            status="busy" if owner == "archived-busy" else "idle",
        )
    )
    try:
        outcome = await rig.adapter.execute(
            intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX
        )
        assert isinstance(outcome, Ack) == (owner == "none")
        assert rig.server.count("POST", "/v1/sessions") == (owner == "none")
        assert rig.server.count("GET", "/v1/sessions") == 3
        if owner == "archived-busy":
            assert rig.server.count("GET", "/v1/sessions/last-owner") == 1
    finally:
        await rig.rest.aclose()


@pytest.mark.parametrize("attempts", [1, 4])
@pytest.mark.parametrize("request_before_cancel", [False, True])
async def test_owner_move_cancels_deferred_create_without_waiting(
    pending_create, attempts, request_before_cancel
):
    rig, db, h, create, run, _path, executor = pending_create
    rig.server.fail_paths.add("/v1/sessions")
    for attempt in range(attempts):
        await executor._execute(create.effect_id)
        waiting = await db.call(lambda store: store.get_effect(create.effect_id))
        if attempt + 1 < attempts:
            rig.clock.advance(waiting.next_at_us - rig.clock.now_utc_us())
    now = rig.clock.now_utc_us()
    assert waiting.next_at_us == now + (120 if attempts == 1 else 960) * 1_000_000

    def unrelated_event(store):
        refresh(h)
        h.f(P).tick(max(now - h.f(P).now, 0))
        h.send(P, ev.ReconcileDue())

    await db.call(unrelated_event)
    assert (await db.call(lambda store: store.get_effect(create.effect_id))).next_at_us == (
        waiting.next_at_us
    )
    assert create.effect_id not in {
        e.effect.effect_id for e in await db.call(lambda store: store.pending_effects())
    }

    def request():
        return h.send(
            P,
            ev.RequestTriage(via=Via.DRAG),
            evidence=snapshot(stage=Stage.INBOX, read_at_us=h.f(P).now),
        )

    def owner_move(store):
        refresh(h)
        h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))
        if request_before_cancel:
            assert request().audit.accepted

    await db.call(owner_move)
    # Use the production outbox's due-row filter: direct _execute alone would hide F1.
    assert create.effect_id in {
        e.effect.effect_id for e in await db.call(lambda store: store.pending_effects())
    }
    reads = rig.server.count("GET", "/v1/sessions")
    await executor._execute(create.effect_id)
    cancelled = await db.call(lambda store: store.get_effect(create.effect_id))
    assert cancelled.state == "cancelled" and rig.clock.now_utc_us() == now
    parcel = await db.call(lambda store: store.load_parcel(P))
    assert settled(parcel.session(run.session_id))
    assert parcel.session(run.session_id).lifecycle in (Lifecycle.FENCED, Lifecycle.RETIRED)
    if not request_before_cancel:
        admission = await db.call(lambda store: store.load_admission(h.cfg.repo_id))
        assert P not in admission.triage_runs
        assert parcel.bot != BotState.WORKING

        def owner_retry(store):
            refresh(h)
            return request()

        assert (await db.call(owner_retry)).audit.accepted
    parcel = await db.call(lambda store: store.load_parcel(P))
    successor = parcel.current_session
    assert successor.session_id != run.session_id and successor.nonce != run.nonce
    due = await db.call(lambda store: store.pending_effects())
    assert any(
        e.effect.kind == EffectKind.CREATE_SESSION and e.effect.target == successor.session_id
        for e in due
    )
    assert rig.server.count("POST", "/v1/sessions") == 0
    assert rig.server.count("GET", "/v1/sessions") == reads


@pytest.mark.parametrize("kind", [EffectKind.RESOLVE_ELICITATION, EffectKind.SEND_MESSAGE])
async def test_owner_move_cancels_deferred_answer_or_message(pending_create, kind):
    rig, db, h, _create, run, _path, executor = pending_create

    def setup(store):
        refresh(h)
        h.create_ok(P)
        if kind == EffectKind.RESOLVE_ELICITATION:
            h.send(P, ev.ElicitationOpened(session_id=run.session_id, elicitation_id="el-1"))
            [decision] = h.p(P).open_decisions
            result = h.send(P, ev.Decide(decision_id=decision.decision_id, answer="yes"))
            [effect] = h.of(result, kind)
        else:
            [effect] = [
                e.effect
                for e in store.pending_effects()
                if e.effect.kind == kind and e.effect.target == run.session_id
            ]
        assert effect_still_valid(h.p(P), effect) is None
        lease = store.acquire_lease(P, "boot")
        return store.claim_effect(effect.effect_id, lease)

    claimed = await db.call(setup)
    effect = claimed.effect
    await executor._finish(claimed, PreSendRetryable("read incomplete", 960_000_000))
    waiting = await db.call(lambda store: store.get_effect(effect.effect_id))
    assert waiting.next_at_us == rig.clock.now_utc_us() + 960_000_000

    def unrelated_event(store):
        refresh(h)
        h.send(P, ev.ReconcileDue())

    await db.call(unrelated_event)
    assert (await db.call(lambda store: store.get_effect(effect.effect_id))).next_at_us == (
        waiting.next_at_us
    )

    def owner_move(store):
        refresh(h)
        h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))

    await db.call(owner_move)
    assert effect.effect_id in {
        e.effect.effect_id for e in await db.call(lambda store: store.pending_effects())
    }
    await executor._execute(effect.effect_id)
    assert (await db.call(lambda store: store.get_effect(effect.effect_id))).state == "cancelled"
    assert rig.server.requests == []


async def test_deferred_cancellation_wake_commits_with_owner_move(pending_create, monkeypatch):
    rig, db, h, create, run, _path, executor = pending_create
    rig.server.fail_paths.add("/v1/sessions")
    await executor._execute(create.effect_id)
    waiting = await db.call(lambda store: store.get_effect(create.effect_id))
    move = h.f(P).make(ev.LeftwardMove(to_stage=Stage.INBOX))

    def crash(point):
        if point == "after-effects":
            raise RuntimeError("injected transaction failure")

    def interrupted_move(store):
        with monkeypatch.context() as patch:
            patch.setattr(store, "_fault", crash)
            store.apply_event(move, h.cfg)

    with pytest.raises(RuntimeError, match="injected transaction failure"):
        await db.call(interrupted_move)
    assert (await db.call(lambda store: store.get_effect(create.effect_id))).next_at_us == (
        waiting.next_at_us
    )
    parcel = await db.call(lambda store: store.load_parcel(P))
    assert parcel.session(run.session_id).lifecycle == run.lifecycle
    assert not parcel.session(run.session_id).fences
    await db.call(lambda store: store.apply_event(move, h.cfg))
    assert (await db.call(lambda store: store.get_effect(create.effect_id))).next_at_us is None
