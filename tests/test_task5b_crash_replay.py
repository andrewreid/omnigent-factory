"""Crash after each external write, restart on the same SQLite file, prove no duplicate.

Every case runs the production ``FactoryService`` and ``EffectExecutor`` with the real
``GitHubAPIAdapter``, ``OmnigentExecutionAdapter`` (behind ``RecordingOmnigentAdapter``,
with the store-backed own-send ledger and dispatch directory) and the real credential
broker minting through ``InstallationTokenService``. Only HTTP is replaced: the
source-shaped fake Omnigent server and a small GitHub fake, both behind
:class:`CrashAfterCommit`, which counts *every* external write and kills the process right
after the targeted write commits. A second service is then built on the same state
directory and database file; each case asserts exactly one external write and the
recovered outcome the design requires (adoption, or an explicit unknown/held state).
"""

from __future__ import annotations

import asyncio
import json
import shutil
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import CredentialProfile, EffectIntent, EffectKind
from omnigent_factory.core.types import DecisionImpact, Lifecycle, Parcel, Size, Via
from omnigent_factory.credentials.broker import LocalCredentialBroker
from omnigent_factory.credentials.capabilities import CapabilityRegistry, read_capability_file
from omnigent_factory.credentials.server import BrokerServer, StageProvisioner
from omnigent_factory.github.adapter import BoardSchema, GitHubAPIAdapter, ParcelBinding
from omnigent_factory.github.auth import AppAuthenticator, InstallationTokenService
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.omnigent.adapter import OmnigentConfig, OmnigentExecutionAdapter
from omnigent_factory.omnigent.rest import OmnigentRest
from omnigent_factory.ports.credentials import TokenGrant, TokenRefusal
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.credentials import AppInstallationTokenMinter, StoreExecutionGate
from omnigent_factory.service.db import StoreWorker
from omnigent_factory.service.directory import (
    PublicationRenderer,
    RecordingOmnigentAdapter,
    ServiceDispatchDirectory,
)
from omnigent_factory.service.durable import StoreCapabilityStore, StoreOwnItemLedger
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import (
    OWNER_ID,
    REPO_ID,
    T0,
    contract_text,
    result_candidate,
)
from omnigent_factory.testing.crash_replay import CrashAfterCommit, InjectedCrash
from omnigent_factory.testing.fakes import FakeClock
from tests.credentials.repos import BOT, REPO, GitEnv, make_git_env
from tests.omnigent.fake_server import FakeOmnigentServer, FakeSession, elicitation
from tests.omnigent.support import AGENT, HOST, PROJECT
from tests.store_driver import ISSUE, StoreDriver

ROOT = "root-1"
BRANCH = "factory/issue-1"
ITEM = "PVTI_1"


# ------------------------------------------------------------------ external fakes


class FakeGitHub:
    """Issue comments, one Project item's single-select fields and installation tokens."""

    def __init__(self, config: ServiceConfig) -> None:
        self.config = config
        self.comments: list[dict[str, Any]] = []
        self.fields: dict[str, str] = {}
        self.tokens = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else {}
        if path.endswith("/access_tokens") and request.method == "POST":
            self.tokens += 1
            return httpx.Response(
                201,
                json={
                    "token": f"ghs_{self.tokens:036d}",
                    "expires_at": "2030-01-01T00:00:00Z",
                    "permissions": body["permissions"],
                    "repositories": [{"full_name": self.config.repository}],
                },
            )
        if path == f"/repos/{self.config.repository}/issues/1/comments":
            if request.method == "POST":
                comment = {
                    "id": 5000 + len(self.comments),
                    "body": body["body"],
                    "user": {"id": self.config.github_bot_user_id},
                    "created_at": "2027-01-15T08:00:00Z",
                }
                self.comments.append(comment)
                return httpx.Response(201, json=comment)
            return httpx.Response(200, json=self.comments)
        if path == "/graphql":
            return self._graphql(body)
        # Issue snapshots are unavailable: reconcile reads retry and never add facts.
        return httpx.Response(503)

    def _graphql(self, body: dict[str, Any]) -> httpx.Response:
        query, variables = str(body.get("query", "")), body.get("variables", {})
        if "updateProjectV2ItemFieldValue" in query:
            update = variables["input"]
            self.fields[update["fieldId"]] = update["value"]["singleSelectOptionId"]
            return httpx.Response(
                200, json={"data": {"updateProjectV2ItemFieldValue": {"projectV2Item": {}}}}
            )
        if "fieldValueByName" in query and variables.get("id") == ITEM:
            name = variables.get("fieldName")
            field_id = (
                self.config.status_field_node_id
                if name == "Status"
                else self.config.bot_field_node_id
            )
            option = self.fields.get(field_id)
            value = None if option is None else {"optionId": option, "field": {"id": field_id}}
            return httpx.Response(200, json={"data": {"node": {"fieldValueByName": value}}})
        return httpx.Response(503)


def _root(server: FakeOmnigentServer, **fields: Any) -> FakeSession:
    return server.add(
        FakeSession(
            id=ROOT,
            agent_id=AGENT,
            host_id=HOST,
            project_id=PROJECT,
            git_branch=BRANCH,
            **fields,
        )
    )


# ------------------------------------------------------------------ rig


@dataclass(frozen=True)
class Boundary:
    """Real reducer transitions that leave ``target`` as the one effect to execute."""

    seed: Callable[[StoreDriver], None]
    target: EffectKind


class BrokerBoot:
    """Production boot order: the broker restores capabilities before dispatch starts."""

    def __init__(self, broker: LocalCredentialBroker) -> None:
        self.broker = broker

    async def start(self) -> None:
        await self.broker.restore()

    async def close(self) -> None:
        return None

    def healthy(self) -> bool:
        return True


class CrashRig:
    def __init__(self, tmp_path: Path, git_env: GitEnv) -> None:
        self.git_env = git_env
        secrets = tmp_path / "secrets"
        secrets.mkdir(mode=0o700)
        self.config = ServiceConfig(
            state_dir=tmp_path / "svc",
            runtime_dir=git_env.runtime / "svc",
            secrets_dir=secrets,
            repo_id=REPO_ID,
            owners=frozenset({OWNER_ID}),
            repository=REPO,
            source_clone=git_env.source,
            worktree_root=git_env.worktrees,
            omnigent_host_id=HOST,
            omnigent_agent_id=AGENT,
            omnigent_project_id=PROJECT,
            effect_poll_seconds=0.001,
            clock_interval_seconds=0.05,
            reconcile_interval_seconds=3_600,
        )
        self.config.prepare_private_directories()
        self.clock = FakeClock(start_us=T0)
        self.omnigent = FakeOmnigentServer(
            source_clone=git_env.source, worktree_root=git_env.worktrees
        )
        self.github = FakeGitHub(self.config)
        self.omnigent_wire = CrashAfterCommit(self.omnigent.handle)
        self.github_wire = CrashAfterCommit(self.github)
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.app_key = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        self.effect: EffectIntent | None = None
        self._clients: list[httpx.AsyncClient] = []

    # ----------------------------------------------------------- seeding

    def seed(self, boundary: Boundary) -> EffectIntent:
        """Apply real reducer transitions to the database file, keep only the target."""
        store = SqliteStore.open(self.config.database_path, self.clock)
        try:
            store.ensure_repository(self.config.trusted)
            driver = StoreDriver(store, self.config.trusted, ISSUE, start_us=T0)
            driver.unpause()
            boundary.seed(driver)
            [effect] = driver.of(boundary.target)
            # Every other outbox row stands for work the seed already acknowledged.
            store._conn.execute(
                "UPDATE effects SET state = 'done' WHERE effect_id != ?", (effect.effect_id,)
            )
            self.clock.advance(driver.f.now - self.clock.now_utc_us() + 1)
        finally:
            store.close()
        self.effect = effect
        return effect

    # ----------------------------------------------------------- one daemon lifetime

    def build(self) -> tuple[FactoryService, LocalCredentialBroker]:
        config = self.config
        service = FactoryService(config, clock=self.clock, fatal_exit=lambda _: None)
        directory = ServiceDispatchDirectory(service.db, config)
        github_http = httpx.AsyncClient(transport=self.github_wire.transport())
        self._clients.append(github_http)
        github = GitHubAPIAdapter(
            GitHubClient(github_http, "token", api_url=config.github_api_url),
            repository=config.repository,
            repository_node_id=config.repo_id,
            project_node_id=config.project_node_id,
            status_field_node_id=config.status_field_node_id,
            bot_user_id=config.github_bot_user_id,
            required_checks=frozenset(),
            now_us=self.clock.now_utc_us,
            board_schema=BoardSchema(
                config.status_field_node_id,
                config.status_options,
                config.bot_field_node_id,
                config.bot_options,
            ),
            publication_renderer=PublicationRenderer(directory, config),
            # Production does not yet resolve issue numbers for comment effects
            # (reported FOLLOW_UP); the binding is the adapter's own resolution input.
            parcel_bindings={ISSUE: ParcelBinding(issue_number=1, project_item_id=ITEM)},
            owner_ids=config.owners,
        )
        workspaces = self.git_env.workspaces()
        broker = LocalCredentialBroker(
            gate=StoreExecutionGate(service.db, self.clock),
            minter=AppInstallationTokenMinter(
                InstallationTokenService(
                    github_http,
                    AppAuthenticator(config.github_app_id, self.app_key),
                    config.github_installation_id,
                    config.repository,
                    config.github_api_url,
                )
            ),
            clock=self.clock,
            capabilities=CapabilityRegistry(
                config.capability_dir,
                config.broker_socket,
                config.repository,
                store=StoreCapabilityStore(service.db),
            ),
            repository=config.repository,
        )
        broker_server = BrokerServer(
            broker, config.broker_socket, workspaces=workspaces, identity=BOT
        )
        omnigent = OmnigentExecutionAdapter(
            rest=OmnigentRest(
                "http://omnigent.test", transport=self.omnigent_wire.transport(), page_limit=50
            ),
            config=OmnigentConfig(
                agent_id=AGENT, host_id=HOST, repository=REPO, project_id=PROJECT
            ),
            directory=directory,
            ledger=StoreOwnItemLedger(service.db),
            workspaces=workspaces,
            provisioner=StageProvisioner(broker, broker_server),
            identity=BOT,
            clock=self.clock,
            broker_socket=config.broker_socket,
        )
        service.bind_integrations(
            adapters=(github, RecordingOmnigentAdapter(omnigent, directory), broker),
            managed=(BrokerBoot(broker),),
        )
        return service, broker

    async def provision(self, session_id: str) -> str:
        """The capability the adapter's PREPARE step created before the crash window."""
        worker = StoreWorker(self.config.database_path, self.clock)
        await worker.start()
        try:
            registry = CapabilityRegistry(
                self.config.capability_dir,
                self.config.broker_socket,
                self.config.repository,
                store=StoreCapabilityStore(worker),
            )
            record = await registry.provision(session_id)
        finally:
            await worker.close()
        return read_capability_file(record.path).secret

    async def crash(self, service: FactoryService) -> None:
        """Run until the armed write committed and the process 'died' before its ack."""
        effect = self.effect
        assert effect is not None
        await service.start()
        try:

            async def died() -> bool:
                row = await service.db.call(lambda db: db.get_effect(effect.effect_id))
                wires = self.github_wire.crashed + self.omnigent_wire.crashed
                return bool(wires) and row is not None and row.state == "claimed"

            await until(died)
        finally:
            await service.stop()

    async def close(self) -> None:
        for client in self._clients:
            await client.aclose()


async def until(predicate: Callable[[], Awaitable[bool]], seconds: float = 20.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while loop.time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached")


async def parcel(service: FactoryService) -> Parcel:
    loaded = await service.db.call(lambda db: db.load_parcel(ISSUE))
    assert loaded is not None
    return loaded


async def bodies(service: FactoryService, kind: type[ev.EventBody]) -> list[Any]:
    events = await service.db.call(lambda db: db.events_for(ISSUE))
    return [event.body for event in events if isinstance(event.body, kind)]


async def effect_state(service: FactoryService, effect_id: str) -> str | None:
    row = await service.db.call(lambda db: db.get_effect(effect_id))
    return None if row is None else row.state


@pytest.fixture
def rig(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[CrashRig]:
    env = make_git_env(tmp_path / "git", monkeypatch)
    try:
        yield CrashRig(tmp_path, env)
    finally:
        shutil.rmtree(env.runtime, ignore_errors=True)


def _path(method: str, suffix: str) -> Callable[[httpx.Request], bool]:
    return lambda request: request.method == method and request.url.path.endswith(suffix)


def _message_post(request: httpx.Request) -> bool:
    if request.method != "POST" or request.url.path != f"/v1/sessions/{ROOT}/events":
        return False
    return json.loads(request.content).get("type") == "message"


def _status_write(request: httpx.Request) -> bool:
    """A Project mutation of the Status field (the Bot field is a separate effect)."""
    if request.url.path != "/graphql" or b"updateProjectV2ItemFieldValue" not in request.content:
        return False
    update = json.loads(request.content)["variables"]["input"]
    return bool(update["fieldId"] == ServiceConfig.model_fields["status_field_node_id"].default)


# ------------------------------------------------------------------ seeds


def _triage_requested(driver: StoreDriver) -> None:
    driver.eligible()
    driver.send(ev.RequestTriage(via=Via.DRAG))


def _triage_active(driver: StoreDriver) -> None:
    _triage_requested(driver)
    driver.create_ok(ROOT)


def _elicitation_answered(driver: StoreDriver) -> None:
    _triage_active(driver)
    session = driver.session()
    driver.send(
        ev.ElicitationOpened(
            session_id=session.session_id,
            elicitation_id="el_1",
            impact=DecisionImpact.WITHIN_CONTRACT,
        )
    )
    [decision] = driver.store.load_parcel(ISSUE).decisions  # type: ignore[union-attr]
    driver.send(ev.Decide(decision_id=decision.decision_id, answer="yes", within_contract=True))


def _continued_after_checkpoint(driver: StoreDriver) -> None:
    _triage_active(driver)
    session = driver.session()
    driver.send(
        ev.ActiveLimitReached(session_id=session.session_id, grant_id=session.grant.grant_id)
    )
    driver.send(ev.Continue(duration_us=3_600_000_000))


def _triage_commanded(driver: StoreDriver) -> None:
    driver.eligible()
    driver.send(ev.RequestTriage(via=Via.COMMAND), ack_moves=False)


def _plan_published(driver: StoreDriver) -> None:
    driver.eligible()
    driver.send(ev.RequestPlan(via=Via.DRAG))
    session = driver.create_ok(ROOT)
    driver.send(
        result_candidate(
            session.session_id,
            ROOT,
            session.revision,
            ev.ResultKind.PLAN,
            publication_kind=ev.PublicationKind.CONTRACT,
            contract_canonical=contract_text("M", "Bump @types/node safely"),
            size=Size.M,
            open_decision_ids=(),
        )
    )


# ------------------------------------------------------------------ the seven boundaries


@pytest.mark.asyncio
async def test_session_create_crash_is_adopted_by_nonce_without_a_second_create(
    rig: CrashRig,
) -> None:
    effect = rig.seed(Boundary(_triage_requested, EffectKind.CREATE_SESSION))
    rig.omnigent_wire.arm(_path("POST", "/v1/sessions"))
    first, _ = rig.build()
    await rig.crash(first)
    [created] = rig.omnigent.sessions.values()

    second, _ = rig.build()
    await second.start()
    try:

        async def adopted() -> bool:
            session = (await parcel(second)).current_session
            return session is not None and session.root_id == created.id

        await until(adopted)
        assert rig.omnigent_wire.count(_path("POST", "/v1/sessions")) == 1
        # Adoption settles the ambiguous create: closed as done, never re-sent (#822).
        assert await effect_state(second, effect.effect_id) == "done"
        [adoption] = await bodies(second, ev.AdoptionResult)
        assert (adoption.matches, adoption.root_id) == (1, created.id)
    finally:
        await second.stop()
        await rig.close()


@pytest.mark.asyncio
async def test_message_send_crash_is_adopted_from_the_ledger_without_a_resend(
    rig: CrashRig,
) -> None:
    _root(rig.omnigent)
    effect = rig.seed(Boundary(_triage_active, EffectKind.SEND_MESSAGE))
    rig.omnigent_wire.arm(_message_post)
    first, _ = rig.build()
    await rig.crash(first)
    [item] = rig.omnigent.sessions[ROOT].items

    second, _ = rig.build()
    await second.start()
    try:

        async def reconciled() -> bool:
            session = (await parcel(second)).current_session
            return session is not None and item["id"] in session.own_items

        await until(reconciled)
        current = await parcel(second)
        assert current.current_session is not None
        assert not current.current_session.message_unknown
        assert current.unknown_effect(effect.effect_id) is None
        assert rig.omnigent_wire.count(_message_post) == 1
        [outcome] = await bodies(second, ev.EffectReconciled)
        assert (outcome.effect_id, outcome.delivered, outcome.item_id) == (
            effect.effect_id,
            True,
            item["id"],
        )
    finally:
        await second.stop()
        await rig.close()


@pytest.mark.asyncio
async def test_elicitation_resolve_crash_stays_unknown_and_is_never_resent(
    rig: CrashRig,
) -> None:
    _root(rig.omnigent, pending_elicitations=[elicitation("el_1")])
    resolve = _path("POST", "/elicitations/el_1/resolve")
    effect = rig.seed(Boundary(_elicitation_answered, EffectKind.RESOLVE_ELICITATION))
    rig.omnigent_wire.arm(resolve)
    first, _ = rig.build()
    await rig.crash(first)
    assert len(rig.omnigent.resolved) == 1

    second, _ = rig.build()
    await second.start()
    try:

        async def reconcile_ran() -> bool:
            rows = await second.db.call(lambda db: db.effects_in_state("done"))
            return any(
                row.effect.kind == EffectKind.RECONCILE_SESSION
                and row.effect.args.get("effect_id") == effect.effect_id
                for row in rows
            )

        await until(reconcile_ran)
        await asyncio.sleep(0.2)  # let any (wrong) follow-up dispatch surface
        current = await parcel(second)
        # "gone" is not proof either way (§3.4): the ambiguity stays and gates work.
        assert current.unknown_effect(effect.effect_id) is not None
        assert current.current_session is not None and current.current_session.message_unknown
        assert rig.omnigent_wire.count(resolve) == 1
        assert await bodies(second, ev.EffectReconciled) == []
    finally:
        await second.stop()
        await rig.close()


@pytest.mark.asyncio
async def test_policy_write_crash_is_held_unknown_and_never_rewritten(rig: CrashRig) -> None:
    _root(rig.omnigent)
    policy_post = _path("POST", f"/v1/sessions/{ROOT}/policies")
    effect = rig.seed(Boundary(_continued_after_checkpoint, EffectKind.REPLACE_COST_POLICY))
    rig.omnigent_wire.arm(policy_post)
    first, _ = rig.build()
    await rig.crash(first)

    second, _ = rig.build()
    await second.start()
    try:

        async def recorded_unknown() -> bool:
            current = await parcel(second)
            return current.unknown_effect(effect.effect_id) is not None

        await until(recorded_unknown)
        await asyncio.sleep(0.2)
        current = await parcel(second)
        assert current.current_session is not None
        assert not current.current_session.grant.ready  # no PolicyReady without proof
        assert await effect_state(second, effect.effect_id) == "unknown"
        assert rig.omnigent_wire.count(policy_post) == 1
        assert await bodies(second, ev.PolicyReady) == []
        assert len(await bodies(second, ev.EffectUnknown)) == 1
    finally:
        await second.stop()
        await rig.close()


@pytest.mark.asyncio
async def test_comment_post_crash_is_held_unknown_and_never_reposted(rig: CrashRig) -> None:
    comment = _path("POST", "/issues/1/comments")
    effect = rig.seed(Boundary(_plan_published, EffectKind.PUBLISH_CONTRACT))
    rig.github_wire.arm(comment)
    first, _ = rig.build()
    await rig.crash(first)
    [posted] = rig.github.comments
    assert f"effect={effect.effect_id}" in posted["body"]

    second, _ = rig.build()
    await second.start()
    try:

        async def recorded_unknown() -> bool:
            return (await parcel(second)).unknown_effect(effect.effect_id) is not None

        await until(recorded_unknown)
        await asyncio.sleep(0.2)
        current = await parcel(second)
        [contract] = current.contracts
        assert contract.comment_id is None  # publication unverified: approval stays closed
        assert await effect_state(second, effect.effect_id) == "unknown"
        assert rig.github_wire.count(comment) == 1
        assert await bodies(second, ev.ContractPublished) == []
    finally:
        await second.stop()
        await rig.close()


@pytest.mark.asyncio
async def test_board_write_crash_keeps_the_move_pending_and_never_rewrites(
    rig: CrashRig,
) -> None:
    effect = rig.seed(Boundary(_triage_commanded, EffectKind.MOVE_CARD))
    rig.github_wire.arm(_status_write)
    first, _ = rig.build()
    await rig.crash(first)
    assert (
        rig.github.fields[rig.config.status_field_node_id] == rig.config.status_options["Triaged"]
    )

    second, _ = rig.build()
    await second.start()
    try:

        async def recorded_unknown() -> bool:
            return (await parcel(second)).unknown_effect(effect.effect_id) is not None

        await until(recorded_unknown)
        await asyncio.sleep(0.2)
        current = await parcel(second)
        assert [move.effect_id for move in current.pending_moves] == [effect.effect_id]
        # No session is created while the move is unproven.
        assert rig.omnigent_wire.count(_path("POST", "/v1/sessions")) == 0
        assert await effect_state(second, effect.effect_id) == "unknown"
        assert rig.github_wire.count(_status_write) == 1
        assert await bodies(second, ev.ColumnObserved) == []
    finally:
        await second.stop()
        await rig.close()


@pytest.mark.asyncio
async def test_token_issue_crash_leaves_issuance_denied_until_recheck(rig: CrashRig) -> None:
    mint = _path("POST", "/access_tokens")
    effect = rig.seed(Boundary(_triage_active, EffectKind.ENABLE_ISSUANCE))
    session_id = effect.preconditions.session_id
    assert session_id is not None
    secret = await rig.provision(session_id)
    first, broker = rig.build()
    await first.start()
    try:

        async def enabled() -> bool:
            return broker.issuance_enabled(session_id)

        await until(enabled)
        rig.github_wire.arm(mint)
        with pytest.raises(InjectedCrash):
            await broker.request_token(session_id, secret, REPO)
    finally:
        await first.stop()
    assert rig.github.tokens == 1

    second, broker = rig.build()
    await second.start()
    try:
        assert broker.capabilities.verify(session_id, secret) is not None
        refused = await broker.request_token(session_id, secret, REPO)
        assert isinstance(refused, TokenRefusal) and refused.reason == "issuance-disabled"
        assert await effect_state(second, effect.effect_id) == "done"  # not replayed
        assert rig.github_wire.count(mint) == 1
        # Only the boot recheck against the persisted gate may re-enable issuance.
        assert await broker.reenable_after_recheck(
            session_id, CredentialProfile(str(effect.args["profile"]))
        )
        granted = await broker.request_token(session_id, secret, REPO)
        assert isinstance(granted, TokenGrant)
        assert rig.github_wire.count(mint) == 2
        assert (await parcel(second)).current_session.lifecycle == Lifecycle.ACTIVE  # type: ignore[union-attr]
    finally:
        await second.stop()
        await rig.close()
