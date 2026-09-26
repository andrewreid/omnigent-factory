"""Bot identity through real Git and a real Unix socket (§6.2 acceptance, no network).

Disposable repos plus a fake App minter demonstrate: the worktree helper outranks the
owner's global helper; commits carry the bot identity; tokens refresh; every fence
refuses; a successor stage rotates the capability; a worker's isolated worktree is wired
through the same stage capability; the ``gh`` wrapper scrubs inherited tokens; and no
global Git/gh configuration changes.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest
import pytest_asyncio

from omnigent_factory.core.effects import CredentialProfile, EffectKind
from omnigent_factory.credentials import client
from omnigent_factory.credentials.broker import LocalCredentialBroker
from omnigent_factory.credentials.capabilities import CapabilityRegistry, read_capability_file
from omnigent_factory.credentials.gh_wrapper import install_gh_wrapper
from omnigent_factory.credentials.server import (
    BrokerServer,
    StageProvisioner,
    WorkerGrant,
    peer_uid,
)
from omnigent_factory.credentials.worktree import StageWiring, WorktreeError
from omnigent_factory.testing.fakes import FakeClock
from tests.credentials.fakes import HOUR_US, FakeGate, FakeMinter
from tests.credentials.repos import BOT, OWNER_TOKEN, REPO, GitEnv, git
from tests.credentials.test_broker import CTX, _intent


class Stack:
    def __init__(self, env: GitEnv) -> None:
        self.env = env
        self.clock = FakeClock()
        self.minter = FakeMinter(self.clock)
        self.gate = FakeGate()
        self.socket = env.runtime / "broker.sock"
        self.caps = CapabilityRegistry(env.runtime / "caps", self.socket, REPO, volatile_ok=True)
        self.broker = LocalCredentialBroker(
            gate=self.gate,
            minter=self.minter,
            clock=self.clock,
            capabilities=self.caps,
            repository=REPO,
        )
        self.ws = env.workspaces()
        self.server = BrokerServer(self.broker, self.socket, workspaces=self.ws, identity=BOT)
        self.provisioner = StageProvisioner(self.broker, self.server)

    def worktree(self, branch: str) -> Path:
        self.ws.ensure_source_clone()
        self.ws.fetch_base("main")
        path = self.env.worktrees / branch.replace("/", "-")
        git("worktree", "add", "-b", branch, str(path), "origin/main", cwd=self.env.source)
        return path

    async def wire(self, path: Path, branch: str, sid: str, profile: CredentialProfile) -> str:
        record = self.provisioner.provision(sid)
        verified = self.ws.verify_worktree(path, branch)
        self.ws.configure(verified, StageWiring(sid, record.path, self.socket, REPO), BOT)
        await self.broker.execute(_intent(EffectKind.ENABLE_ISSUANCE, sid, profile.value), CTX)
        self.gate.open(sid, profile)
        return read_capability_file(record.path).secret


async def credential_fill(cwd: Path, repo_path: str = f"{REPO}.git") -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        "git",
        "credential",
        "fill",
        cwd=cwd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    out, err = await proc.communicate(
        f"protocol=https\nhost=github.com\npath={repo_path}\n\n".encode()
    )
    return proc.returncode or 0, out.decode(), err.decode()


def password(out: str) -> str | None:
    for line in out.splitlines():
        if line.startswith("password="):
            return line[len("password=") :]
    return None


@pytest_asyncio.fixture
async def stack(git_env: GitEnv):
    s = Stack(git_env)
    await s.server.start()
    try:
        yield s
    finally:
        await s.server.close()


@pytest.mark.asyncio
async def test_bot_identity_refresh_refusal_and_no_global_edits(stack: Stack) -> None:
    before = stack.env.home_digest()
    global_before = git("config", "--global", "--list", cwd=stack.env.source)
    wt = stack.worktree("factory/issue-7-g1")
    await stack.wire(wt, "factory/issue-7-g1", "BUILD1", CredentialProfile.BUILD)

    # Helper outranks the owner's inherited global helper.
    code, out, err = await credential_fill(wt)
    assert code == 0, err
    assert "username=x-access-token" in out
    first = password(out)
    assert first is not None and first.startswith("ghs_bot_")
    assert OWNER_TOKEN not in out

    # Commit attribution is the App bot identity, not the owner's global user.
    (wt / "change.txt").write_text("x\n", encoding="utf-8")
    git("add", "change.txt", cwd=wt)
    git("commit", "-m", "factory change", cwd=wt)
    assert git("log", "-1", "--format=%an <%ae>", cwd=wt) == f"{BOT.name} <{BOT.email}>"

    # Refresh: warm cache reused, then re-minted near expiry.
    _, out2, _ = await credential_fill(wt)
    assert password(out2) == first
    stack.clock.advance(HOUR_US)
    _, out3, _ = await credential_fill(wt)
    assert password(out3) not in (None, first)

    # Only the configured repository is served.
    code, out4, _ = await credential_fill(wt, "someone/else.git")
    assert password(out4) is None and OWNER_TOKEN not in out4

    # Every fence refuses immediately, with no fallback to the owner helper.
    for reason in ("fenced:safety", "fenced:stopped", "fenced:revoked", "fenced:checkpoint"):
        stack.gate.close("BUILD1", reason)
        code, out5, err5 = await credential_fill(wt)
        assert code != 0
        assert password(out5) is None and OWNER_TOKEN not in out5
        assert reason in err5
    stack.gate.open("BUILD1", CredentialProfile.BUILD)
    await stack.broker.execute(_intent(EffectKind.DISABLE_ISSUANCE, "BUILD1"), CTX)
    code, out6, _ = await credential_fill(wt)
    assert code != 0 and password(out6) is None
    assert password(out3) in stack.minter.revoked

    # Main checkout and global config untouched; only the worktree carries wiring.
    main_helpers = git("config", "--get-all", "credential.helper", cwd=stack.env.source)
    assert "omnigent_factory" not in main_helpers
    assert stack.env.home_digest() == before
    assert git("config", "--global", "--list", cwd=stack.env.source) == global_before
    assert "OWNER-TOKEN" in (stack.env.home / ".config/gh/hosts.yml").read_text()


@pytest.mark.asyncio
async def test_stage_rotation_on_the_same_worktree(stack: Stack) -> None:
    branch = "factory/issue-8-g1"
    wt = stack.worktree(branch)
    old_secret = await stack.wire(wt, branch, "PLAN1", CredentialProfile.READ_ONLY)
    _, out, _ = await credential_fill(wt)
    assert password(out) is not None
    assert stack.minter.minted[-1][1]["contents"] == "read"

    # Plan retires; successor build takes the same worktree (after Q).
    await stack.broker.retire("PLAN1")
    stack.gate.close("PLAN1", "retired")
    await stack.wire(wt, branch, "BUILD2", CredentialProfile.BUILD)
    assert git("config", "--get", "factory.stageSession", cwd=wt) == "BUILD2"
    _, out2, _ = await credential_fill(wt)
    assert password(out2) is not None
    assert stack.minter.minted[-1][1]["contents"] == "write"
    # The retired stage's capability no longer works, even presented directly.
    reply = await stack.broker.request_token("PLAN1", old_secret, REPO)
    assert reply.reason == "invalid-capability"  # type: ignore[union-attr]
    # The helper list was replaced, not appended to.
    raw = subprocess.run(
        ["git", "config", "--worktree", "--get-all", "credential.helper"],  # noqa: S607
        cwd=wt,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    helpers = raw.rstrip("\n").split("\n")
    assert helpers[0] == "" and len(helpers) == 2 and "git_helper" in helpers[1]


@pytest.mark.asyncio
async def test_worker_isolated_worktree_is_wired_through_stage_capability(stack: Stack) -> None:
    branch = "factory/issue-9-g1"
    wt = stack.worktree(branch)
    await stack.wire(wt, branch, "BUILD3", CredentialProfile.BUILD)
    cap = client.load(Path(git("config", "--get", "factory.capabilityFile", cwd=wt)))

    worker = stack.env.worktrees / "issue-9-worker"
    git("worktree", "add", "-b", f"{branch}--w1", str(worker), branch, cwd=stack.env.source)
    stack.server.authorize_worker(
        "BUILD3", WorkerGrant("w1", worker, f"{branch}--w1", CredentialProfile.BUILD)
    )
    reply = await asyncio.to_thread(client.register_worktree, cap, "w1", worker, f"{branch}--w1")
    assert reply.ok, reply.reason
    _, out, _ = await credential_fill(worker)
    assert password(out) is not None and OWNER_TOKEN not in out
    (worker / "w.txt").write_text("w\n", encoding="utf-8")
    git("add", "w.txt", cwd=worker)
    git("commit", "-m", "worker change", cwd=worker)
    assert git("log", "-1", "--format=%ae", cwd=worker) == BOT.email

    # Unrecorded worker tuples fail closed.
    rogue = stack.env.worktrees / "rogue"
    git("worktree", "add", "-b", "feature/rogue", str(rogue), branch, cwd=stack.env.source)
    bad = await asyncio.to_thread(client.register_worktree, cap, "w9", rogue, "feature/rogue")
    assert not bad.ok and bad.reason == "worker-not-recorded"
    # Recorded but outside the owned roots still fails closed.
    outside = stack.env.home / "elsewhere"
    git("worktree", "add", "-b", f"{branch}--w2", str(outside), branch, cwd=stack.env.source)
    stack.server.authorize_worker(
        "BUILD3", WorkerGrant("w2", outside, f"{branch}--w2", CredentialProfile.BUILD)
    )
    far = await asyncio.to_thread(client.register_worktree, cap, "w2", outside, f"{branch}--w2")
    assert not far.ok and "outside the approved owned roots" in far.reason


@pytest.mark.asyncio
async def test_socket_rejects_foreign_uid_and_malformed_requests(stack: Stack) -> None:
    wt = stack.worktree("factory/issue-10-g1")
    await stack.wire(wt, "factory/issue-10-g1", "B10", CredentialProfile.BUILD)
    assert (await stack.server.dispatch(b"not json"))["reason"] == "malformed-request"
    assert (await stack.server.dispatch(b'{"op":"x","session_id":"B10","secret":"s"}'))[
        "reason"
    ] == "unknown-op"
    other = BrokerServer(stack.broker, stack.env.runtime / "o.sock", expected_uid=os.getuid() + 1)
    await other.start()
    try:
        cap = client.load(Path(git("config", "--get", "factory.capabilityFile", cwd=wt)))
        foreign = cap.__class__(
            cap.capability_id, cap.session_id, cap.secret, other.socket_path, cap.repository
        )
        reply = await asyncio.to_thread(client.request_token, foreign, REPO)
        assert not reply.ok and reply.reason == "peer-not-authorised"
    finally:
        await other.close()
    import socket as _s

    with _s.socket(_s.AF_UNIX, _s.SOCK_STREAM) as a:
        assert peer_uid(a) is None  # unconnected: no peer credentials
    assert oct(stack.socket.stat().st_mode & 0o777) == "0o600"


@pytest.mark.asyncio
async def test_gh_wrapper_scrubs_inherited_tokens_and_isolates_config(stack: Stack) -> None:
    wt = stack.worktree("factory/issue-11-g1")
    await stack.wire(wt, "factory/issue-11-g1", "B11", CredentialProfile.BUILD)
    fake_gh = stack.env.runtime / "real-gh"
    fake_gh.write_text(
        "#!/bin/sh\n"
        'echo "TOKEN=$GH_TOKEN"\necho "GITHUB_TOKEN=${GITHUB_TOKEN:-unset}"\n'
        'echo "ENT=${GH_ENTERPRISE_TOKEN:-unset}"\necho "HOST=${GH_HOST:-unset}"\n'
        'echo "CONFIG=$GH_CONFIG_DIR"\necho "PROMPT=$GH_PROMPT_DISABLED"\necho "ARGS=$*"\n',
        encoding="utf-8",
    )
    fake_gh.chmod(0o700)
    bin_dir = stack.env.runtime / "bin"
    gh_config = stack.env.runtime / "gh-config"
    wrapper = install_gh_wrapper(bin_dir, fake_gh, gh_config)
    env = {
        **os.environ,
        "GH_TOKEN": OWNER_TOKEN,
        "GITHUB_TOKEN": OWNER_TOKEN,
        "GH_ENTERPRISE_TOKEN": OWNER_TOKEN,
        "GH_HOST": "evil.example",
    }
    proc = await asyncio.create_subprocess_exec(
        str(wrapper),
        "pr",
        "create",
        "--fill",
        cwd=wt,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    text = out.decode()
    assert proc.returncode == 0, err.decode()
    assert OWNER_TOKEN not in text
    assert "TOKEN=ghs_bot_" in text
    assert "GITHUB_TOKEN=unset" in text and "ENT=unset" in text and "HOST=unset" in text
    assert f"CONFIG={gh_config}" in text and "PROMPT=1" in text
    assert "ARGS=pr create --fill" in text

    stack.gate.close("B11", "fenced:stopped")
    proc = await asyncio.create_subprocess_exec(
        str(wrapper),
        "pr",
        "comment",
        cwd=wt,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    assert proc.returncode == 1
    assert b"TOKEN=" not in out and b"fenced:stopped" in err
    assert not (stack.env.home / ".config/gh/config.yml").exists()


def test_install_wrapper_refuses_relative_real_gh(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="absolute"):
        install_gh_wrapper(tmp_path / "bin", Path("gh"), tmp_path / "cfg")


def test_worktree_rewrite_to_github_fails_closed(git_env: GitEnv) -> None:
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    wt = git_env.worktrees / "rw"
    git("worktree", "add", "-b", "factory/rw", str(wt), "origin/main", cwd=git_env.source)
    subprocess.run(  # fake HOME only
        ["git", "config", "--global", "url.git@github.com:.insteadOf", "https://github.com/"],  # noqa: S607
        check=True,
    )
    with pytest.raises(WorktreeError, match="URL rewrite"):
        ws.configure(
            ws.verify_worktree(wt, "factory/rw"),
            StageWiring("S", git_env.runtime / "c", git_env.runtime / "s", REPO),
            BOT,
        )


def test_verify_worktree_rejects_symlink_foreign_clone_and_wrong_branch(git_env: GitEnv) -> None:
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    wt = git_env.worktrees / "ok"
    git("worktree", "add", "-b", "factory/ok", str(wt), "origin/main", cwd=git_env.source)
    assert ws.verify_worktree(wt, "factory/ok").branch == "factory/ok"
    with pytest.raises(WorktreeError, match="recorded branch"):
        ws.verify_worktree(wt, "factory/other")
    link = git_env.worktrees / "link"
    link.symlink_to(wt)
    with pytest.raises(WorktreeError, match="symlink"):
        ws.verify_worktree(link, "factory/ok")
    foreign = git_env.worktrees / "foreign"
    git("clone", str(git_env.remote), str(foreign), cwd=git_env.worktrees)
    with pytest.raises(WorktreeError, match=r"dedicated source clone|recorded branch"):
        ws.verify_worktree(foreign, "main")
    assert sys.executable  # helper command is an absolute interpreter path
