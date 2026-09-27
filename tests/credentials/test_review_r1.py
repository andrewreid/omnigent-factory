"""Regressions for review t3/initial (sha256 90bdc5c5...): findings 2 and 4, plus the
FOLLOW_UP guard. Each test fails against candidate tree 1d285c01 and passes after."""

from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path

import pytest

from omnigent_factory.core.effects import CredentialProfile, EffectKind
from omnigent_factory.credentials import client
from omnigent_factory.credentials.capabilities import read_capability_file
from omnigent_factory.credentials.worktree import StageWiring, WorktreeError
from tests.credentials.repos import BOT, REPO, GitEnv, git
from tests.credentials.test_broker import CTX, _intent
from tests.credentials.test_identity_e2e import Stack, credential_fill, password

# ------------------------------------------------------------ 2. transport bypass


def _worktree(git_env: GitEnv, name: str = "t"):
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    wt = git_env.worktrees / name
    git("worktree", "add", "-b", f"factory/{name}", str(wt), "origin/main", cwd=git_env.source)
    return ws, wt


def _configure(git_env: GitEnv, ws, wt: Path) -> None:
    ws.configure(
        ws.verify_worktree(wt, f"factory/{wt.name}"),
        StageWiring("S", git_env.runtime / "c", git_env.runtime / "s", REPO),
        BOT,
    )


def _config_value(key: str, cwd: Path) -> str:
    proc = subprocess.run(  # noqa: S603 - test-controlled argv
        ["git", "config", "--get-all", key],  # noqa: S607
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.stdout.strip()


def _git_config(*args: str, cwd: Path) -> None:
    subprocess.run(["git", "config", *args], cwd=cwd, check=True)  # noqa: S603, S607


@pytest.mark.parametrize(
    ("scope", "key", "value"),
    [
        # Reviewer's repro: generic rewrite of every https:// URL to an SSH host.
        ("--global", "url.ssh://git@evil.invalid/.insteadOf", "https://"),
        ("--global", "url.ssh://git@evil.invalid/.pushInsteadOf", "https://"),
        ("--global", "url.https://owner-token@github.com/.insteadOf", "https://github.com/"),
        # Path-specific inherited Authorization header.
        (
            "--global",
            "http.https://github.com/SA-Ambulance/.extraHeader",
            "Authorization: Bearer OWNER",
        ),
        # Worktree-specific credential-bearing push URL.
        (
            "--worktree",
            "remote.origin.pushurl",
            "https://owner-token@github.com/other/repo.git",
        ),
        ("--local", "remote.origin.pushurl", "https://github.com/other/repo.git"),
    ],
)
def test_transport_bypass_is_rejected(git_env: GitEnv, scope: str, key: str, value: str) -> None:
    ws, wt = _worktree(git_env)
    cwd = git_env.source if scope != "--worktree" else wt
    if scope == "--worktree":
        _git_config("--local", "extensions.worktreeConfig", "true", cwd=git_env.source)
    _git_config(scope, key, value, cwd=cwd)
    with pytest.raises(WorktreeError):
        _configure(git_env, ws, wt)


def test_reviewer_repro_combined_leaves_no_usable_bypass(git_env: GitEnv) -> None:
    ws, wt = _worktree(git_env)
    _git_config("--global", "url.ssh://git@evil.invalid/.insteadOf", "https://", cwd=git_env.source)
    with pytest.raises(WorktreeError, match="rewrite"):
        _configure(git_env, ws, wt)
    assert _config_value("factory.stageSession", wt) == ""


def test_clean_worktree_still_wires_and_effective_urls_are_exact(git_env: GitEnv) -> None:
    ws, wt = _worktree(git_env)
    _git_config("--global", "http.extraHeader", "X-Owner: 1", cwd=git_env.source)  # generic: reset
    _configure(git_env, ws, wt)
    url = f"https://github.com/{REPO}.git"
    assert git("remote", "get-url", "--push", "--all", "origin", cwd=wt) == url
    assert git("ls-remote", "--get-url", "origin", cwd=wt) == url
    raw = subprocess.run(
        ["git", "config", "--get-all", "http.extraHeader"],  # noqa: S607
        cwd=wt,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert raw.removesuffix("\n").split("\n")[-1] == ""  # reset is the last value


# ------------------------------------------------------------ 4. worker registration


async def _dispatch_register(stack: Stack, cap_path: Path, path: Path, branch: str) -> dict:
    cap = read_capability_file(cap_path)
    raw = json.dumps(
        {
            "op": "register_worktree",
            "session_id": cap.session_id,
            "secret": cap.secret,
            "worker_id": "w",
            "path": str(path),
            "branch": branch,
        }
    ).encode()
    return dict(await stack.server.dispatch(raw))


async def _stage(stack: Stack, branch: str, sid: str, profile: CredentialProfile) -> Path:
    wt = stack.worktree(branch)
    await stack.wire(wt, branch, sid, profile)
    return Path(git("config", "--get", "factory.capabilityFile", cwd=wt))


@pytest.mark.asyncio
async def test_invented_prefix_branch_is_refused(git_env: GitEnv) -> None:
    stack = Stack(git_env)
    branch = "factory/issue-20-g1"
    cap = await _stage(stack, branch, "B20", CredentialProfile.BUILD)
    invented = git_env.worktrees / "invented"
    git("worktree", "add", "-b", f"{branch}--anything", str(invented), branch, cwd=git_env.source)
    reply = await _dispatch_register(stack, cap, invented, f"{branch}--anything")
    assert reply["ok"] is False
    assert _config_value("factory.stageSession", invented) == ""


@pytest.mark.asyncio
async def test_cross_parcel_prefix_collision_is_refused(git_env: GitEnv) -> None:
    stack = Stack(git_env)
    cap_a = await _stage(stack, "factory/issue-1-g1", "A", CredentialProfile.BUILD)
    # Another parcel's legitimately wired worktree whose branch falls under A's old prefix.
    other = "factory/issue-1-g1--b"
    b_wt = stack.worktree(other)
    await stack.wire(b_wt, other, "B", CredentialProfile.BUILD)
    reply = await _dispatch_register(stack, cap_a, b_wt, other)
    assert reply["ok"] is False
    assert git("config", "--get", "factory.stageSession", cwd=b_wt) == "B"


@pytest.mark.asyncio
async def test_registration_rechecks_the_execution_gate(git_env: GitEnv) -> None:
    stack = Stack(git_env)
    branch = "factory/issue-21-g1"
    cap = await _stage(stack, branch, "B21", CredentialProfile.BUILD)
    worker = git_env.worktrees / "gated"
    git("worktree", "add", "-b", f"{branch}--w", str(worker), branch, cwd=git_env.source)
    stack.gate.close("B21", "fenced:stopped")  # issuance flag still on; gate closed
    reply = await _dispatch_register(stack, cap, worker, f"{branch}--w")
    assert reply == {"ok": False, "reason": "fenced:stopped"}


@pytest.mark.asyncio
async def test_read_only_worker_under_build_stage_gets_read_only_token(git_env: GitEnv) -> None:
    from omnigent_factory.credentials.server import WorkerGrant

    stack = Stack(git_env)
    await stack.server.start()
    try:
        branch = "factory/issue-22-g1"
        cap_path = await _stage(stack, branch, "B22", CredentialProfile.BUILD)
        reviewer = git_env.worktrees / "reviewer"
        git("worktree", "add", "-b", f"{branch}--r", str(reviewer), branch, cwd=git_env.source)
        await stack.server.authorize_worker(
            "B22", WorkerGrant("r", reviewer, f"{branch}--r", CredentialProfile.READ_ONLY)
        )
        cap = read_capability_file(cap_path)
        # Registered with the wrong branch / wrong worker id: refused.
        wrong = await asyncio.to_thread(client.register_worktree, cap, "r", reviewer, branch)
        assert not wrong.ok
        ok = await asyncio.to_thread(client.register_worktree, cap, "r", reviewer, f"{branch}--r")
        assert ok.ok, ok.reason
        _, out, _ = await credential_fill(reviewer)
        assert password(out) is not None
        assert stack.minter.minted[-1][1]["contents"] == "read"
        assert all(v == "read" for v in stack.minter.minted[-1][1].values())
        # The stage root still gets its build profile.
        _, out_root, _ = await credential_fill(stack.env.worktrees / "factory-issue-22-g1")
        assert password(out_root) is not None
        assert stack.minter.minted[-1][1]["contents"] == "write"
        # Stage fence refuses the worker too; disabling revokes the worker's cached token.
        stack.gate.close("B22", "fenced:safety")
        code, out2, _ = await credential_fill(reviewer)
        assert code != 0 and password(out2) is None
        await stack.broker.execute(_intent(EffectKind.DISABLE_ISSUANCE, "B22"), CTX)
        assert password(out) in stack.minter.revoked
    finally:
        await stack.server.close()


@pytest.mark.asyncio
async def test_worker_profile_cannot_exceed_read_only_stage(git_env: GitEnv) -> None:
    from omnigent_factory.credentials.server import WorkerGrant

    stack = Stack(git_env)
    branch = "factory/issue-23-g1"
    cap_path = await _stage(stack, branch, "P23", CredentialProfile.READ_ONLY)
    w = git_env.worktrees / "w23"
    git("worktree", "add", "-b", f"{branch}--w", str(w), branch, cwd=git_env.source)
    await stack.server.authorize_worker(
        "P23", WorkerGrant("w", w, f"{branch}--w", CredentialProfile.BUILD)
    )
    reply = await _dispatch_register(stack, cap_path, w, f"{branch}--w")
    assert reply["ok"] is True
    worker_cap = read_capability_file(Path(git("config", "--get", "factory.capabilityFile", cwd=w)))
    grant = await stack.broker.request_token(worker_cap.session_id, worker_cap.secret, REPO)
    assert getattr(grant, "reason", None) == "worker-profile-exceeds-stage"


def test_volatile_capability_registry_cannot_be_wired_silently(tmp_path: Path) -> None:
    from omnigent_factory.credentials.capabilities import CapabilityFileError, CapabilityRegistry

    with pytest.raises(CapabilityFileError):
        CapabilityRegistry(tmp_path, tmp_path / "s", REPO, volatile_ok=False)
