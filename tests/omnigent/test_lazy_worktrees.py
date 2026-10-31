"""Lazy triage: real Git, durable dispatches, adapter and reducer transitions."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from omnigent.policies.builtins.cel import cel_policy

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, AmbiguousWrite, DefinitiveFailure, EffectKind
from omnigent_factory.core.types import Lifecycle, SessionKind, Via
from omnigent_factory.credentials.worktree import CLONE_LOCK, REFERENCE_META, WorktreeError
from omnigent_factory.omnigent import policies as pol
from omnigent_factory.service.cleanup import WorkspaceCleaner, issue_of
from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig
from omnigent_factory.service.directory import ServiceDispatchDirectory, _atomic_json, base_note
from omnigent_factory.service.worktree_gc import WorktreeGC
from omnigent_factory.testing.harness import Harness
from tests.credentials.repos import git
from tests.omnigent.fake_server import FakeSession
from tests.omnigent.support import AGENT, CTX, HOST, PROJECT, intent, make_rig
from tests.omnigent.test_read_only_git_761 import MOVES_HEAD, _event, _result

P = "I_parcel_1"


def setup(git_env, monkeypatch, *, triaged=False, enabled=True):
    h = Harness()
    if triaged:
        h.triage()
    else:
        h.eligible()
        h.send(P, ev.RequestTriage(via=Via.DRAG))
    rig = make_rig(git_env)
    config = ServiceConfig(
        repo_id="R_test",
        owners=frozenset({1}),
        state_dir=git_env.source.parent / "dispatch-state",
        worktree_root=git_env.worktrees,
        source_clone=git_env.source,
        lazy_issue_worktrees=enabled,
    )
    store = SimpleNamespace(
        query=lambda *args: [(P,)],
        load_parcel=lambda _: h.p(),
        effects_in_state=lambda _: [],
    )

    async def call(fn):
        return fn(store)

    directory = ServiceDispatchDirectory(SimpleNamespace(call=call), config)

    async def parcel(_):
        return h.p()

    async def evidence(_):
        return None

    monkeypatch.setattr(directory, "_parcel_for_session", parcel)
    monkeypatch.setattr(directory, "issue_evidence", evidence)
    rig.adapter.directory = directory
    return rig, directory, h


async def create(rig, h):
    run = h.cur()
    return await rig.adapter.execute(
        intent(EffectKind.CREATE_SESSION, run.session_id, nonce=run.nonce), CTX
    )


def advance(git_env, message="next"):
    seed = git_env.remote.parent / "seed"
    (seed / "README.md").write_text(message)
    git("add", "README.md", cwd=seed)
    git("commit", "-m", message, cwd=seed)
    git("push", "origin", "HEAD:main", cwd=seed)


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_ack", [False, True])
async def test_reference_create_pins_before_post_and_adopts_once(git_env, monkeypatch, lost_ack):
    rig, directory, h = setup(git_env, monkeypatch)
    run = h.cur()
    post = rig.rest.post_json

    async def checked_post(path, body):
        snap = directory.dispatch_snapshot(run.session_id)
        assert snap["workspace_kind"] == "reference" and snap["branch"] == ""
        assert snap["workspace"] == body["workspace"] and not snap["bind_worktree"]
        assert "git" not in body
        assert Path(body["workspace"]) in await directory.reference_pins()
        return await post(path, body)

    monkeypatch.setattr(rig.rest, "post_json", checked_post)
    if lost_ack:
        rig.server.faults[("POST", "/v1/sessions")].append("timeout-after")
    result = await create(rig, h)
    assert isinstance(result, AmbiguousWrite if lost_ack else Ack)
    assert not rig.adapter.workspaces.branch_exists("factory/issue-1")
    outcome = await rig.adapter.execute(
        intent(EffectKind.RECONCILE_SESSION, run.session_id, nonce=run.nonce), CTX
    )
    assert isinstance(outcome, Ack) and outcome.detail["verified"] == 1
    assert rig.server.count("POST", "/v1/sessions") == 1
    root = next(iter(rig.server.sessions.values()))
    assert root.git_branch is None
    # Matching nonce and host still cannot adopt another snapshot path.
    advance(git_env)
    other = rig.adapter.workspaces.select_reference(git_env.worktrees, 0, 2_000_000_000)
    root.workspace = str(other.path)
    outcome = await rig.adapter.execute(
        intent(EffectKind.RECONCILE_SESSION, run.session_id, nonce=run.nonce), CTX
    )
    assert outcome.detail["verified"] == 0
    root.workspace = directory.dispatch_snapshot(run.session_id)["workspace"]
    await rig.adapter.execute(intent(EffectKind.CLOSE_SESSION, root_id=root.id), CTX)
    assert Path(root.workspace).is_dir()
    assert rig.server.requests[-1] == ("PATCH", f"/v1/sessions/{root.id}", {"archived": True})


@pytest.mark.asyncio
async def test_reference_prepare_never_repairs_syncs_or_wires_and_flag_flip_is_pinned(
    git_env,
    monkeypatch,
):
    rig, directory, h = setup(git_env, monkeypatch)
    run = h.cur()
    created = await create(rig, h)
    assert isinstance(created, Ack)
    path = Path(created.detail["workspace"])
    ws = rig.adapter.workspaces
    before = (ws.admin_dir(path) / "config.worktree").read_bytes()

    def forbidden(*args, **kw):
        pytest.fail("reference preparation attempted issue-worktree mutation")

    for method in ("restore_branch", "sync_with_base", "configure", "recreate_worktree"):
        monkeypatch.setattr(ws, method, forbidden)
    directory.config = directory.config.model_copy(update={"lazy_issue_worktrees": False})
    prepared = await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, run.session_id, root_id=created.remote_id), CTX
    )
    assert prepared.detail["ok"] is True
    assert (ws.admin_dir(path) / "config.worktree").read_bytes() == before
    assert ws.effective_helpers(path) == []
    assert not ws._config_entries(path, r"^factory\.(stagesession|capabilityfile)$")
    assert rig.broker.capabilities.get(run.session_id) is not None
    wanted = rig.adapter._static_policies(
        await directory.stage_spec(run.session_id), created.remote_id
    )
    assert pol.factory_cel_policy(read_only=True, reference=True) in wanted
    text = await directory.message_text(intent(EffectKind.SEND_MESSAGE, run.session_id))
    assert "shared read-only reference snapshot" in text
    assert (
        "OMNIGENT_FACTORY_CAPABILITY_FILE" in text
        and str(directory.config.wrapper_bin_dir / "gh") in text
    )
    # The original issue decision likewise survives enabling the feature.
    _other, old_directory, old_h = setup(git_env, monkeypatch, enabled=False)
    # Same deterministic harness IDs: use a distinct dispatch root for this check.
    old_directory.root = old_directory.root.parent / "legacy"
    old_spec = await old_directory.stage_spec(old_h.cur().session_id)
    legacy_path = old_directory.root / f"{old_h.cur().session_id}.json"
    legacy = json.loads(legacy_path.read_text())
    legacy.pop("workspace_kind")
    _atomic_json(legacy_path, legacy)
    old_directory.config = old_directory.config.model_copy(update={"lazy_issue_worktrees": True})
    assert (await old_directory.stage_spec(old_h.cur().session_id)).workspace_kind == "issue"
    assert old_spec.branch and old_spec.bind_worktree is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", [SessionKind.PLAN, SessionKind.BUILD])
async def test_branchless_root_unusable_for_engineering(git_env, monkeypatch, kind):
    rig, directory, h = setup(git_env, monkeypatch, triaged=True)
    triage = h.cur()
    triage_spec = await directory.stage_spec(triage.session_id)
    reference = await directory.reserve_reference(triage_spec, rig.adapter.workspaces, 1000)
    rig.server.add(
        FakeSession(
            id=triage.root_id,
            agent_id=AGENT,
            host_id=HOST,
            project_id=PROJECT,
            workspace=str(reference.path),
            labels={"factory.dispatch": triage.nonce},
        )
    )
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    run = h.cur()
    spec = await directory.stage_spec(run.session_id)
    assert spec.branch and spec.bind_worktree is None  # reference never bound
    # BUILD has the same replacement check even when no plan precedes it.
    monkeypatch.setattr(rig.adapter, "_spec", async_value(replace(spec, kind=kind)))
    prepared = await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, run.session_id, root_id=triage.root_id, reuse=True), CTX
    )
    assert prepared.detail["unusable"] is True
    assert prepared.detail["reason"] == "issue session has no issue worktree"
    r = h.send(
        P,
        ev.Prepared(
            session_id=run.session_id,
            ok=False,
            unusable=True,
            reason=prepared.detail["reason"],
        ),
    )
    assert not Harness.of(r, EffectKind.CLOSE_SESSION)  # archive only after adoption
    assert len(Harness.of(r, EffectKind.CREATE_SESSION)) == 1
    created = await create(rig, h)
    assert isinstance(created, Ack)
    body = [b for m, p, b in rig.server.requests if m == "POST" and p == "/v1/sessions"][-1]
    assert body["git"] == {"branch_name": spec.branch, "base_branch": "origin/main"}
    assert Path(created.detail["workspace"]) != reference.path
    adopted = h.send(
        P,
        ev.SessionCreated(
            session_id=run.session_id,
            root_id=created.remote_id,
            nonce=run.nonce,
        ),
    )
    assert h.p().issue_session.generation == 2
    assert Harness.of(adopted, EffectKind.CLOSE_SESSION)[0].args["root_id"] == triage.root_id
    assert "moved to its own worktree" in directory._handoff(h.p(), run.session_id)
    assert directory._handoff(h.p(), triage.session_id) == ""
    again = await create(rig, h)
    assert isinstance(again, DefinitiveFailure) and "branch-already-exists" in again.reason


def async_value(value):
    async def get(*args):
        return value

    return get


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "dirty", "metadata"])
async def test_reused_triage_with_unusable_snapshot_replaces_session(git_env, monkeypatch, damage):
    rig, directory, h = setup(git_env, monkeypatch, triaged=True)
    old = h.cur()
    old_spec = await directory.stage_spec(old.session_id)
    ref = await directory.reserve_reference(old_spec, rig.adapter.workspaces, 1000)
    rig.server.add(
        FakeSession(
            id=old.root_id,
            agent_id=AGENT,
            host_id=HOST,
            project_id=PROJECT,
            workspace=str(ref.path),
            labels={"factory.dispatch": old.nonce},
        )
    )
    if damage == "missing":
        rig.adapter.workspaces.remove_worktree(ref.path, force=True)
    elif damage == "metadata":
        (rig.adapter.workspaces.admin_dir(ref.path) / REFERENCE_META).unlink()
    else:
        (ref.path / "untracked").write_text("poison")
    h.send(P, ev.PlanFeedback(text_digest="retriage"))
    run = h.cur()
    prepared = await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, run.session_id, root_id=old.root_id, reuse=True), CTX
    )
    assert prepared.detail["unusable"] is True
    assert "reference snapshot unusable" in prepared.detail["reason"]
    assert "workspace" not in directory.dispatch_snapshot(run.session_id)
    h.send(P, ev.Prepared(session_id=run.session_id, ok=False, unusable=True, reason="snapshot"))
    created = await create(rig, h)
    assert isinstance(created, Ack) and created.remote_id != old.root_id


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", [SessionKind.TRIAGE, SessionKind.PLAN, SessionKind.BUILD])
async def test_missing_previous_issue_worktree_selects_reference_only_for_triage(
    git_env,
    monkeypatch,
    kind,
):
    _rig, directory, h = setup(git_env, monkeypatch, triaged=True)
    old = h.cur()
    _atomic_json(
        directory.root / f"{old.session_id}.json",
        {
            "branch": "factory/issue-1",
            "workspace": str(git_env.worktrees / "gone"),
            # legacy dispatch records have no workspace_kind
        },
    )
    next_run = replace(old, session_id="next-run", kind=kind)
    snap = await directory._make_snapshot(h.p(), next_run)
    if kind == SessionKind.TRIAGE:
        assert snap["workspace_kind"] == "reference" and snap["bind_worktree"] is None
    else:
        assert snap["workspace_kind"] == "issue" and snap["bind_worktree"]
    (git_env.worktrees / "gone").mkdir()
    snap = await directory._make_snapshot(h.p(), replace(next_run, kind=SessionKind.TRIAGE))
    assert snap["workspace_kind"] == "issue" and snap["bind_worktree"]


def test_snapshot_immutable_age_reuse_dirty_poison_and_wiring(git_env, monkeypatch):
    ws = git_env.workspaces()
    original_fetch = ws.fetch_base

    def fetch(base="main"):
        assert CLONE_LOCK._is_owned()
        return original_fetch(base)

    monkeypatch.setattr(ws, "fetch_base", fetch)
    first = ws.select_reference(git_env.worktrees, 6, 1000)
    cfg = (ws.admin_dir(first.path) / "config.worktree").read_bytes()
    assert ws.effective_helpers(first.path) == []
    assert (first.path / ".claude/settings.json").read_text() == '{"autoMemoryEnabled": false}\n'
    assert not (first.path / "node_modules").exists() and issue_of(first.path.name) is None
    advance(git_env)
    reused = ws.select_reference(git_env.worktrees, 6, 2000)
    assert reused == first and ws.commits_behind(first.path) == 1
    assert (ws.admin_dir(first.path) / "config.worktree").read_bytes() == cfg
    fresh = ws.select_reference(git_env.worktrees, 6, 1000 + 6 * 3600)
    assert fresh.path != first.path
    assert git("rev-parse", "HEAD", cwd=first.path) == first.head_oid
    (fresh.path / "README.md").write_text("dirty")
    (fresh.path / "scratch").write_text("untracked")
    replacement = ws.select_reference(git_env.worktrees, 6, 1000 + 6 * 3600 + 1)
    assert replacement.path != fresh.path and replacement.head_oid == fresh.head_oid
    assert (fresh.path / "README.md").read_text() == "dirty"
    assert (fresh.path / "scratch").exists()
    # Even manually restoring cleanliness cannot clear the poison record.
    (fresh.path / "README.md").write_text("next")
    (fresh.path / "scratch").unlink()
    with pytest.raises(WorktreeError, match="poisoned"):
        ws.verify_reference(fresh.path)


@pytest.mark.asyncio
async def test_gc_pins_newest_poison_retention_no_teardown_and_status(git_env, monkeypatch):
    rig, directory, h = setup(git_env, monkeypatch)
    ws = rig.adapter.workspaces
    run = h.cur()
    spec = await directory.stage_spec(run.session_id)
    oldest = await directory.reserve_reference(spec, ws, 1000)
    advance(git_env)
    middle = ws.select_reference(git_env.worktrees, 0, 2000)
    advance(git_env, "third")
    newest = ws.select_reference(git_env.worktrees, 0, 3000)
    (oldest.path / "poison").write_text("dirty but pinned")
    (middle.path / "poison").write_text("dirty, unpinned")
    cleaner = WorkspaceCleaner(directory, ws, git_env.worktrees, teardown=("must-not-run",))
    service = SimpleNamespace(
        config=directory.config,
        db=directory.db,
        clock=SimpleNamespace(now_utc_us=lambda: int(1e12)),
    )
    gc = WorktreeGC(service, cleaner, ws, async_value({}))
    result = await gc.sweep_once()
    assert result["reference_snapshots"][str(oldest.path)] == "pinned"
    assert result["reference_snapshots"][str(newest.path)] == "newest"
    assert not middle.path.exists() and oldest.path.exists() and newest.path.exists()
    assert (await gc.status())["reference_snapshots"] == 2
    # No longer live: poisoned oldest can be removed, newest is always retained.
    h.parcels[P] = replace(h.p(), sessions=(replace(run, lifecycle=Lifecycle.RETIRED),))
    await gc.sweep_once()
    assert not oldest.path.exists() and newest.path.exists()
    # Only the newest usable snapshot is protected; clean non-newest respects retention.
    advance(git_env, "fourth")
    last = ws.select_reference(git_env.worktrees, 0, 4000)
    assert ws.collect_references(git_env.worktrees, set(), 4001, 72)[str(newest.path)] == "retained"
    assert ws.collect_references(git_env.worktrees, set(), 4001 + 72 * 3600, 72)[
        str(newest.path)
    ].startswith("removed")
    (last.path / "poison").write_text("newest")
    assert (
        ws.collect_references(git_env.worktrees, set(), 1e9, 0)[str(last.path)]
        == "removed poisoned"
    )


@pytest.mark.asyncio
async def test_gc_waits_for_selection_and_pin(git_env, monkeypatch):
    rig, directory, h = setup(git_env, monkeypatch)
    spec = await directory.stage_spec(h.cur().session_id)
    ws = rig.adapter.workspaces
    await directory.reserve_reference(spec, ws, 1000)
    service = SimpleNamespace(config=directory.config, db=directory.db, clock=rig.clock)
    gc = WorktreeGC(
        service, WorkspaceCleaner(directory, ws, git_env.worktrees), ws, async_value({})
    )
    async with directory.reference_lock:
        sweep = asyncio.create_task(gc.sweep_once())
        await asyncio.sleep(0.03)
        assert not sweep.done()
    await sweep


@pytest.fixture(scope="module")
def reference_policy():
    return cel_policy(**pol.factory_cel_policy(read_only=True, reference=True).factory_params)


@pytest.mark.parametrize(
    "command",
    [
        *MOVES_HEAD,
        "npm install",
        "npm ci",
        "npm --prefix x install",
        "pnpm install",
        "yarn install",
        "yarn add x",
        "./dev-environment.sh up",
        "bash scripts/dev-environment.sh",
        "git fetch origin main",
        "git -C /x fetch",
        "git push origin HEAD:refs/heads/x",
        "git clean -fd",
        "git add .",
    ],
)
def test_reference_cel_denies_mutation(reference_policy, command):
    assert _result(reference_policy(_event(command))) == "DENY"


@pytest.mark.parametrize(
    "command", ["git show origin/main:path", "git status", "git log -3", "npm --version"]
)
def test_reference_cel_allows_reads(reference_policy, command):
    assert _result(reference_policy(_event(command))) == "ALLOW"


def test_plan_policy_unchanged_and_snapshot_defaults_hot_reload():
    plan = cel_policy(**pol.factory_cel_policy(read_only=True).factory_params)
    for command in ("git fetch origin main", "npm install", "./dev-environment.sh up"):
        assert _result(plan(_event(command))) == "ALLOW"
    cfg = ServiceConfig(repo_id="R_test", owners=frozenset({1}))
    assert cfg.lazy_issue_worktrees is False
    assert (
        cfg.reference_snapshot_max_age_hours == 6 and cfg.reference_snapshot_retention_hours == 72
    )
    assert {
        "lazy_issue_worktrees",
        "reference_snapshot_max_age_hours",
        "reference_snapshot_retention_hours",
    } <= HOT_RELOAD_KEYS
    assert "4 commits behind" in base_note({"status": "reference", "behind": 4}, SessionKind.TRIAGE)


@pytest.mark.asyncio
async def test_template_pins_old_bytes_and_reference_ack_cannot_move_pin(git_env, monkeypatch):
    rig, directory, h = setup(git_env, monkeypatch)
    run = h.cur()
    await create(rig, h)
    before = directory.dispatch_snapshot(run.session_id)
    await directory.record_create(
        run.session_id, Ack(detail={"workspace": "/wrong", "branch": "wrong"})
    )
    assert directory.dispatch_snapshot(run.session_id) == before
    template_root = Path(__file__).parents[2] / "src/omnigent_factory/service/templates"
    old = "triage-v11.txt"
    path = directory.root / f"{run.session_id}.json"
    _atomic_json(
        path,
        {
            **before,
            "template": old,
            "template_sha256": hashlib.sha256((template_root / old).read_bytes()).hexdigest(),
        },
    )
    text = await directory.message_text(intent(EffectKind.SEND_MESSAGE, run.session_id))
    assert text and "shared read-only reference snapshot" not in text
    # Tampered pins fail closed, even with the feature enabled.
    data = json.loads(path.read_text())
    data["template_sha256"] = "wrong"
    _atomic_json(path, data)
    assert await directory.message_text(intent(EffectKind.SEND_MESSAGE, run.session_id)) is None


@pytest.mark.asyncio
async def test_reference_fetch_failure_retries_without_post(git_env, monkeypatch):
    from omnigent_factory.core.effects import PreSendRetryable

    rig, _directory, h = setup(git_env, monkeypatch)

    def fail(*args):
        raise WorktreeError("offline")

    monkeypatch.setattr(rig.adapter.workspaces, "fetch_base", fail)
    assert isinstance(await create(rig, h), PreSendRetryable)
    assert rig.server.count("POST", "/v1/sessions") == 0


def test_manually_deleted_snapshot_registration_does_not_block_creation(git_env):
    import shutil

    ws = git_env.workspaces()
    first = ws.select_reference(git_env.worktrees, 6, 1000)
    shutil.rmtree(first.path)
    assert ws.select_reference(git_env.worktrees, 6, 2000).path == first.path


@pytest.mark.asyncio
async def test_previous_worktree_ignores_reference_even_with_matching_branch(git_env, monkeypatch):
    _rig, directory, h = setup(git_env, monkeypatch, triaged=True)
    old = h.cur()
    _atomic_json(
        directory.root / f"{old.session_id}.json",
        {
            "workspace_kind": "reference",
            "branch": "factory/issue-1",
            "workspace": str(git_env.worktrees / "factory-ref-deadbeef0000"),
        },
    )
    run = replace(old, session_id="plan-run", kind=SessionKind.PLAN)
    assert await directory._previous_worktree(h.p(), run, "factory/issue-1") is None


@pytest.mark.asyncio
async def test_retriage_shares_snapshot_with_distinct_capability_and_done_keeps_it(
    git_env,
    monkeypatch,
):
    from omnigent_factory.core.effects import CredentialProfile, profile_for
    from omnigent_factory.service.doctor import DoctorReport, _check_worktrees

    rig, directory, h = setup(git_env, monkeypatch, triaged=True)
    old = h.cur()
    ws = rig.adapter.workspaces
    ref = await directory.reserve_reference(await directory.stage_spec(old.session_id), ws, 1000)
    old_cap = await rig.adapter.provisioner.provision(old.session_id)
    before = (ws.admin_dir(ref.path) / "config.worktree").read_bytes()
    rig.server.add(
        FakeSession(
            id=old.root_id,
            agent_id=AGENT,
            host_id=HOST,
            project_id=PROJECT,
            workspace=str(ref.path),
            labels={"factory.dispatch": old.nonce},
        )
    )
    h.send(P, ev.PlanFeedback(text_digest="retriage"))
    run = h.cur()
    assert run.session_id != old.session_id
    prepared = await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, run.session_id, root_id=old.root_id, reuse=True), CTX
    )
    assert prepared.detail["ok"] is True
    cap = rig.broker.capabilities.get(run.session_id)
    assert cap.path != old_cap.path and cap.capability_id != old_cap.capability_id
    assert profile_for(run.kind) == CredentialProfile.READ_ONLY
    assert (ws.admin_dir(ref.path) / "config.worktree").read_bytes() == before
    assert directory.dispatch_snapshot(run.session_id)["reference_reuse_workspace"] == str(ref.path)
    assert rig.server.count("POST", "/v1/sessions") == 0
    cleaner = WorkspaceCleaner(directory, ws, git_env.worktrees, teardown=("must-not-run",))
    paths, branches = cleaner._candidates(h.p())
    assert ref.path not in paths and "" not in branches
    cleaner._cleanup(h.p(), merged=True)
    assert ref.path.is_dir()
    report = DoctorReport()
    _check_worktrees(directory.config, report)
    assert "1 reference snapshots" in str(report)


@pytest.mark.parametrize(
    "damage",
    [
        "helper",
        "stage",
        "capability",
        "attached",
        "subdir",
        "foreign",
        "symlink",
        "settings",
        "settings-bytes",
        "node_modules",
    ],
)
def test_reference_verification_fails_closed(git_env, damage):
    ws = git_env.workspaces()
    ref = ws.select_reference(git_env.worktrees, 6, 1000)
    path = ref.path
    if damage == "helper":
        git("config", "--worktree", "--add", "credential.helper", "bad-helper", cwd=path)
    elif damage in ("stage", "capability"):
        key = "factory.stageSession" if damage == "stage" else "factory.capabilityFile"
        git("config", "--worktree", key, "unsafe", cwd=path)
    elif damage == "attached":
        git("switch", "-c", "bad", cwd=path)
    elif damage == "subdir":
        path = path / ".claude"
    elif damage == "foreign":
        path = git_env.worktrees / "factory-ref-000000000000"
        git("clone", str(git_env.remote), str(path), cwd=git_env.worktrees)
    elif damage == "symlink":
        link = git_env.worktrees / "factory-ref-000000000000"
        link.symlink_to(path, target_is_directory=True)
        path = link
    elif damage == "settings":
        (path / ".claude/settings.json").write_text('{"autoMemoryEnabled": true}\n')
    elif damage == "settings-bytes":
        (path / ".claude/settings.json").write_bytes(b"\xff")
    elif damage == "node_modules":
        (path / "node_modules").mkdir()
    with pytest.raises(WorktreeError):
        ws.verify_reference(path)


@pytest.mark.asyncio
async def test_feedback_template_pin_is_restart_stable_and_legacy_defaults_to_v2(
    git_env, monkeypatch
):
    rig, directory, h = setup(git_env, monkeypatch)
    run = h.cur()
    await create(rig, h)
    path = directory.root / f"{run.session_id}.json"
    data = directory.dispatch_snapshot(run.session_id)
    effect = intent(EffectKind.SEND_MESSAGE, run.session_id, purpose="feedback")
    text = await directory.message_text(effect)
    assert "reference snapshot" in text and "origin/main:path" in text
    assert "{base_ref}" not in text
    _atomic_json(path, {**data, "triage_comment_sha256": "wrong"})
    assert await directory.message_text(effect) is None
    data.pop("triage_comment_template")
    data.pop("triage_comment_sha256")
    _atomic_json(path, data)
    assert "reference snapshot" not in await directory.message_text(effect)


@pytest.mark.asyncio
async def test_poison_after_reuse_pin_can_replace_without_retargeting_any_pin(git_env, monkeypatch):
    from omnigent_factory.core.effects import RetryableReadFailure

    rig, directory, h = setup(git_env, monkeypatch, triaged=True)
    old = h.cur()
    ws = rig.adapter.workspaces
    ref = await directory.reserve_reference(await directory.stage_spec(old.session_id), ws, 1000)
    rig.server.add(
        FakeSession(
            id=old.root_id,
            agent_id=AGENT,
            host_id=HOST,
            project_id=PROJECT,
            workspace=str(ref.path),
            labels={"factory.dispatch": old.nonce},
        )
    )
    h.send(P, ev.PlanFeedback(text_digest="retriage"))
    run = h.cur()
    prep = intent(EffectKind.PREPARE_SESSION, run.session_id, root_id=old.root_id, reuse=True)
    rig.server.faults[("GET", f"/v1/sessions/{old.root_id}/policies")].append(500)
    assert isinstance(await rig.adapter.execute(prep, CTX), RetryableReadFailure)
    reused = directory.dispatch_snapshot(run.session_id)["reference_reuse_workspace"]
    assert reused == str(ref.path)
    (ref.path / "scratch").write_text("poison after pin")
    unusable = await rig.adapter.execute(prep, CTX)
    assert unusable.detail["unusable"] is True
    h.send(P, ev.Prepared(session_id=run.session_id, ok=False, unusable=True, reason="poisoned"))
    created = await create(rig, h)
    assert isinstance(created, Ack)
    data = directory.dispatch_snapshot(run.session_id)
    assert data["reference_reuse_workspace"] == reused
    assert data["workspace"] != reused
    assert {Path(reused), Path(data["workspace"])} <= await directory.reference_pins()
    outcome = await rig.adapter.execute(
        intent(EffectKind.RECONCILE_SESSION, run.session_id, nonce=run.nonce), CTX
    )
    assert outcome.detail["verified"] == 1
