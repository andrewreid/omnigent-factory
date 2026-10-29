"""Checkpoint regressions: poison recovery, incomplete creation, and GC isolation."""

from types import SimpleNamespace

import pytest

from omnigent_factory.credentials.worktree import REFERENCE_META, WorktreeError
from omnigent_factory.service.cleanup import WorkspaceCleaner, issue_of
from omnigent_factory.service.worktree_gc import WorktreeGC
from tests.credentials.repos import git
from tests.omnigent.test_lazy_worktrees import advance, async_value, setup


@pytest.mark.asyncio
async def test_poisoned_current_oid_gets_fresh_generation_without_touching_pin(
    git_env, monkeypatch
):
    from omnigent_factory.service.doctor import DoctorReport, _check_worktrees

    rig, directory, _ = setup(git_env, monkeypatch)
    ws = rig.adapter.workspaces
    old = ws.select_reference(git_env.worktrees, 6, 1000)
    (old.path / "scratch").write_text("keep pinned bytes")
    fresh = ws.select_reference(git_env.worktrees, 6, 1001)
    assert fresh.path != old.path and fresh.head_oid == old.head_oid
    assert ws.verify_reference(fresh.path) == fresh
    assert issue_of(fresh.path.name) is None
    gc = WorktreeGC(
        SimpleNamespace(config=directory.config, db=directory.db, clock=rig.clock),
        WorkspaceCleaner(directory, ws, git_env.worktrees),
        ws,
        async_value({}),
    )
    assert (await gc.status())["reference_snapshots"] == 2
    report = DoctorReport()
    _check_worktrees(directory.config, report)
    assert "2 reference snapshots" in str(report)
    outcomes = ws.collect_references(git_env.worktrees, {old.path}, 1e9, 0)
    assert outcomes[str(old.path)] == "pinned"
    assert outcomes[str(fresh.path)] == "newest"
    assert (old.path / "scratch").read_text() == "keep pinned bytes"
    assert (
        ws.collect_references(git_env.worktrees, set(), 1e9, 0)[str(old.path)] == "removed poisoned"
    )
    advance(git_env)
    later = ws.select_reference(git_env.worktrees, 0, 2000)
    (later.path / "scratch").touch()
    outcomes = ws.collect_references(git_env.worktrees, set(), 1e9, 0)
    assert outcomes[str(later.path)] == "removed poisoned"
    assert outcomes[str(fresh.path)] == "newest"  # older, but newest usable


def test_poisoned_newest_is_collectable_even_without_a_usable_snapshot(git_env):
    ws = git_env.workspaces()
    old = ws.select_reference(git_env.worktrees, 6, 1000)
    (old.path / "scratch").touch()
    assert (
        ws.collect_references(git_env.worktrees, set(), 1001, 72)[str(old.path)]
        == "removed poisoned"
    )
    assert not old.path.exists()
    assert ws.verify_reference(ws.select_reference(git_env.worktrees, 6, 1002).path)


def test_crash_after_worktree_add_self_heals(git_env, monkeypatch):
    ws = git_env.workspaces()
    run_git = ws._git

    def crash(args, *rest, **kwargs):
        result = run_git(args, *rest, **kwargs)
        if args[:2] == ["worktree", "add"]:
            raise WorktreeError("simulated process death after registration")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(ws, "_git", crash)
        with pytest.raises(WorktreeError, match="simulated process death"):
            ws.select_reference(git_env.worktrees, 6, 1000)
    [incomplete] = ws.references(git_env.worktrees)
    assert not (ws.admin_dir(incomplete) / REFERENCE_META).exists()
    fresh = ws.select_reference(git_env.worktrees, 6, 1001)
    assert fresh.path != incomplete
    assert ws.verify_reference(fresh.path) == fresh


@pytest.mark.parametrize("metadata", [None, "not json", "{}", '{"created_at": true}'])
def test_invalid_creation_record_is_skipped_and_collected_only_when_unpinned(git_env, metadata):
    ws = git_env.workspaces()
    old = ws.select_reference(git_env.worktrees, 6, 1000)
    record = ws.admin_dir(old.path) / REFERENCE_META
    if metadata is None:
        record.unlink()
    else:
        record.write_text(metadata)
    assert ws.collect_references(git_env.worktrees, {old.path}, 1001, 0)[str(old.path)] == "pinned"
    fresh = ws.select_reference(git_env.worktrees, 6, 1002)
    assert fresh.path != old.path
    outcomes = ws.collect_references(git_env.worktrees, set(), 1003, 72)
    assert outcomes[str(old.path)] == "removed poisoned"
    assert outcomes[str(fresh.path)] == "newest"


@pytest.mark.asyncio
async def test_snapshot_gc_error_does_not_stop_closed_issue_sweep(git_env, monkeypatch, caplog):
    rig, directory, _ = setup(git_env, monkeypatch)
    ws = rig.adapter.workspaces
    ws.select_reference(git_env.worktrees, 6, 1000)
    issue = git_env.worktrees / "factory-issue-99"
    git("worktree", "add", "-b", "factory/issue-99", str(issue), "origin/main", cwd=git_env.source)
    cleaner = WorkspaceCleaner(directory, ws, git_env.worktrees)
    service = SimpleNamespace(config=directory.config, db=directory.db, clock=rig.clock)
    gc = WorktreeGC(service, cleaner, ws, async_value({99: False}))
    monkeypatch.setattr(gc, "_parcels", async_value({}))
    monkeypatch.setattr(gc, "_unknown_effect_parcels", async_value(set()))
    monkeypatch.setattr(gc, "_guard", lambda *args, **kwargs: lambda: None)

    def fail(*args):
        raise WorktreeError("simulated snapshot sweep failure")

    monkeypatch.setattr(ws, "collect_references", fail)
    result = await gc.sweep_once()
    assert not issue.exists()
    assert result["reference_snapshot_count"] == 1
    assert "simulated snapshot sweep failure" in caplog.text


# SHA-256 of the exact template bytes at base 5036dee3 (not the snapshot versions).
LEGACY_TEMPLATES = {
    "triage": (
        "triage-v11.txt",
        "aca96b79734e026d05b1f45c016eea4326a5e11700128aea71c028aa09d4658e",
    ),
    "feedback": (
        "triage-feedback-v8.txt",
        "3c9b18092ac5008e15d3ab97c6bde9665b2ee57d53acaefd8322b0aa701567ea",
    ),
    "epic": (
        "epic-triage-v5.txt",
        "6fc185e355847f8d505674ef00ac7ed458a11f53051e2bfc1de217bd832539bc",
    ),
    "autopilot": (
        "epic-plan-v4.txt",
        "b5666ed060760145c4177168e16325dcfe4361d9664577ca61bb7656df0f38a0",
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["off", "live_issue", "reference"])
@pytest.mark.parametrize("variant", list(LEGACY_TEMPLATES))
async def test_only_reference_dispatches_use_new_template_bytes(
    git_env, monkeypatch, mode, variant
):
    import hashlib
    from dataclasses import replace

    from omnigent_factory.core.effects import EffectKind
    from omnigent_factory.core.types import MICROS_PER_MINUTE
    from omnigent_factory.service import directory as module
    from tests.omnigent.support import intent
    from tests.omnigent.test_lazy_worktrees import P

    _, directory, h = setup(git_env, monkeypatch, enabled=mode != "off")
    if variant == "feedback":
        monkeypatch.setattr(directory, "latest_triage", lambda _: {})
    if variant in ("epic", "autopilot"):
        h.parcels[P] = replace(
            h.p(),
            links=SimpleNamespace(epic=True),
            autopilot=SimpleNamespace() if variant == "autopilot" else None,
        )
    if mode == "live_issue":
        monkeypatch.setattr(directory, "_previous_worktree", async_value(str(git_env.worktrees)))
    run = h.cur()
    spec = await directory.stage_spec(run.session_id)
    snapshot = directory.dispatch_snapshot(run.session_id)
    old_name, old_hash = LEGACY_TEMPLATES[variant]
    assert hashlib.sha256(module._template(old_name).encode()).hexdigest() == old_hash
    name = module._REFERENCE_TEMPLATES[old_name] if mode == "reference" else old_name
    assert snapshot["template"] == name
    assert (
        snapshot["template_sha256"] == hashlib.sha256(module._template(name).encode()).hexdigest()
    )
    first = intent(EffectKind.SEND_MESSAGE, run.session_id)
    feedback = intent(EffectKind.SEND_MESSAGE, run.session_id, purpose="feedback")
    first_text = await directory.message_text(first)
    feedback_text = await directory.message_text(feedback)
    if mode != "reference":
        assert first_text == module._template(old_name).format(
            repository=directory.config.repository,
            issue_number=h.p().issue_number,
            run_id=run.session_id,
            session_id=spec.root_id or "",
            handoff="",
            base_note="",
            granted_minutes=max(1, spec.granted_us // MICROS_PER_MINUTE),
        )
        assert snapshot["triage_comment_template"] == "triage-comment-v2.txt"
        assert (
            snapshot["triage_comment_sha256"]
            == "55dbee8191c4375267ef6c4aedc9e56826befe1fc8ef35f7201335628226c0d9"
        )
        assert feedback_text == module._template("triage-comment-v2.txt").format(
            run_id=run.session_id
        )
    else:
        assert "shared read-only reference snapshot" in first_text
        assert snapshot["triage_comment_template"] == "triage-comment-v3.txt"
        assert "snapshot" in feedback_text
    directory.config = directory.config.model_copy(update={"lazy_issue_worktrees": mode == "off"})
    assert await directory.message_text(first) == first_text
    assert await directory.message_text(feedback) == feedback_text


@pytest.fixture(scope="module")
def snapshot_cel():
    from omnigent.policies.builtins.cel import cel_policy

    from omnigent_factory.omnigent.policies import factory_cel_policy

    return cel_policy(**factory_cel_policy(read_only=True, reference=True).factory_params)


@pytest.mark.parametrize(
    "command",
    [
        "yarn",
        "cd /repo && yarn",
        "yarn --frozen-lockfile",
        "bun install",
        "bun i",
        "pnpm i",
        "npm i",
        "npm ci",
        "pnpm --dir /repo i",
        "npm --prefix /repo ci",
        "git remote update",
        "git remote update --prune",
        "git remote prune origin",
        "git pull",
        "git -C /repo pull origin main",
        "git fetch --all",
        "git fetch-pack origin",
        "git update-ref refs/heads/main HEAD",
        "git symbolic-ref HEAD refs/heads/main",
        "git branch new HEAD",
        "git tag new HEAD",
        "git remote set-head origin main",
        "git pack-refs --all",
        "git prune",
        "git gc",
        "git maintenance run",
        "git reflog expire --all",
        "git notes add -m note",
        "git replace HEAD HEAD~1",
        "git am change.patch",
        "git bisect start",
        "git filter-branch --all",
        "git apply change.patch",
        "git read-tree HEAD",
        "git update-index --add file",
        "git checkout-index -a",
        "git sparse-checkout set src",
        "git submodule update --init",
        "git fast-import",
        "git receive-pack .",
        "git config alias.f fetch",
        "git -c alias.f=fetch f",
        "git -c 'alias.p=pull origin main' p",
    ],
)
def test_snapshot_policy_denies_install_and_ref_update_equivalents(snapshot_cel, command):
    from tests.omnigent.test_read_only_git_761 import _event, _result

    assert _result(snapshot_cel(_event(command))) == "DENY"
