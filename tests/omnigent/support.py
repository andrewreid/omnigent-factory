"""Wiring for Omnigent adapter tests: fake server + real local Git + real broker."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from omnigent_factory.core.effects import (
    EffectIntent,
    EffectKind,
    ExecutionContext,
    JsonValue,
    Preconditions,
)
from omnigent_factory.core.types import SessionKind
from omnigent_factory.credentials.broker import LocalCredentialBroker
from omnigent_factory.credentials.capabilities import CapabilityRegistry
from omnigent_factory.credentials.server import BrokerServer, StageProvisioner
from omnigent_factory.omnigent.adapter import OmnigentConfig, OmnigentExecutionAdapter
from omnigent_factory.omnigent.directory import FormValue, MemoryOwnItemLedger, StageSpec
from omnigent_factory.omnigent.rest import OmnigentRest
from omnigent_factory.testing.fakes import FakeClock
from tests.credentials.fakes import FakeGate, FakeMinter
from tests.credentials.repos import BOT, REPO, GitEnv
from tests.omnigent.fake_server import FakeOmnigentServer

AGENT = "ag_molly"
HOST = "host_coder"
PROJECT = "proj_factory"
CTX = ExecutionContext(boot_id="boot", lease_epoch=1, parcel_version=1, attempt=1)


@dataclass
class Directory:
    specs: dict[str, StageSpec] = field(default_factory=dict)
    texts: dict[str, str] = field(default_factory=dict)
    answers: dict[str, Mapping[str, FormValue]] = field(default_factory=dict)

    async def stage_spec(self, session_id: str) -> StageSpec | None:
        return self.specs.get(session_id)

    async def message_text(self, effect: EffectIntent) -> str | None:
        return self.texts.get(effect.effect_id)

    async def elicitation_content(self, effect: EffectIntent) -> Mapping[str, FormValue] | None:
        return self.answers.get(effect.effect_id)

    def set_root(self, session_id: str, root: str) -> None:
        self.specs[session_id] = replace(self.specs[session_id], root_id=root)


@dataclass
class Rig:
    env: GitEnv
    server: FakeOmnigentServer
    directory: Directory
    ledger: MemoryOwnItemLedger
    clock: FakeClock
    broker: LocalCredentialBroker
    adapter: OmnigentExecutionAdapter
    rest: OmnigentRest


def make_rig(env: GitEnv, *, page_limit: int = 2, **config: Any) -> Rig:
    server = FakeOmnigentServer(source_clone=env.source, worktree_root=env.worktrees)
    clock = FakeClock()
    socket = env.runtime / "broker.sock"
    broker = LocalCredentialBroker(
        gate=FakeGate(),
        minter=FakeMinter(clock),
        clock=clock,
        capabilities=CapabilityRegistry(env.runtime / "caps", socket, REPO, volatile_ok=True),
        repository=REPO,
    )
    ws = env.workspaces()
    bserver = BrokerServer(broker, socket, workspaces=ws, identity=BOT)
    rest = OmnigentRest("http://omnigent.test", transport=server.transport(), page_limit=page_limit)
    directory = Directory()
    ledger = MemoryOwnItemLedger(volatile_ok=True)
    adapter = OmnigentExecutionAdapter(
        rest=rest,
        config=OmnigentConfig(
            agent_id=AGENT, host_id=HOST, repository=REPO, project_id=PROJECT, **config
        ),
        directory=directory,
        ledger=ledger,
        workspaces=ws,
        provisioner=StageProvisioner(broker, bserver),
        identity=BOT,
        clock=clock,
        broker_socket=socket,
    )
    return Rig(env, server, directory, ledger, clock, broker, adapter, rest)


def spec(
    sid: str = "S1",
    *,
    kind: SessionKind = SessionKind.BUILD,
    branch: str = "factory/issue-42-g1",
    nonce: str = "nonce-abc",
    **kw: Any,
) -> StageSpec:
    return StageSpec(
        session_id=sid,
        parcel_id="I_kwIssue42",
        kind=kind,
        attempt=1,
        nonce=nonce,
        branch=branch,
        title="Factory timesheets#42 — build",
        grant_id="gr_1",
        granted_us=4 * 3_600_000_000,
        **kw,
    )


def intent(
    kind: EffectKind,
    sid: str = "S1",
    *,
    id: str | None = None,
    **args: JsonValue,
) -> EffectIntent:
    return EffectIntent(
        effect_id=id or f"ef_{kind.value}_{sid}",
        kind=kind,
        parcel_id="I_kwIssue42",
        target=sid,
        preconditions=Preconditions(parcel_version=1, eligibility_epoch=0, session_id=sid),
        args=args,
    )
