"""Production composition root for the single-process daemon."""

from __future__ import annotations

import asyncio
import logging
import os
import stat
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import httpx

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import IssueSnapshot, Lifecycle, Parcel
from omnigent_factory.credentials.broker import LocalCredentialBroker
from omnigent_factory.credentials.capabilities import CapabilityRegistry
from omnigent_factory.credentials.gh_wrapper import install_gh_wrapper
from omnigent_factory.credentials.git_helper import install_git_helper
from omnigent_factory.credentials.push_guard import install_push_guard
from omnigent_factory.credentials.server import BrokerServer, StageProvisioner
from omnigent_factory.credentials.worktree import BotIdentity, Workspaces
from omnigent_factory.github.adapter import BoardSchema, GitHubAPIAdapter
from omnigent_factory.github.auth import AppAuthenticator, InstallationTokenService
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.github.webhook import DeliveryIdentity, DeliveryNormalizer
from omnigent_factory.omnigent.adapter import OmnigentConfig, OmnigentExecutionAdapter
from omnigent_factory.omnigent.policies import PolicyError
from omnigent_factory.omnigent.rest import OmnigentReadError, OmnigentRest
from omnigent_factory.ports.clock import SystemClock
from omnigent_factory.ports.github import IssueRef
from omnigent_factory.service.cleanup import CleanupAdapter, WorkspaceCleaner
from omnigent_factory.service.config import ConfigError, ServiceConfig
from omnigent_factory.service.credentials import AppInstallationTokenMinter, StoreExecutionGate
from omnigent_factory.service.directory import (
    PublicationRenderer,
    RecordingOmnigentAdapter,
    ServiceDispatchDirectory,
    ServiceParcelResolver,
)
from omnigent_factory.service.durable import (
    StoreCapabilityStore,
    StoreOwnItemLedger,
    StoreWorkerGrantStore,
    reenable_issuance_after_boot,
)
from omnigent_factory.service.github_delivery import (
    GitHubDeliveryProcessor,
    GitHubWebhookVerifier,
)
from omnigent_factory.service.locking import ProcessLock
from omnigent_factory.service.mcp import McpEndpoint, build_endpoint
from omnigent_factory.service.observer import OmnigentObserver
from omnigent_factory.service.omnigent_auth import log_expiry, omnigent_auth
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.service.tokens import DaemonTokenProvider

LOG = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProductionComposition:
    """Fully wired daemon plus the verifier consumed by the HTTP application."""

    service: FactoryService
    verifier: GitHubWebhookVerifier
    mcp: McpEndpoint


class ProductionRuntime:
    """Own external resources and establish deny-by-default boot ordering."""

    def __init__(
        self,
        *,
        service: FactoryService,
        broker: LocalCredentialBroker,
        broker_server: BrokerServer,
        observer: OmnigentObserver,
        github: GitHubAPIAdapter,
        workspaces: Workspaces,
        config: ServiceConfig,
        github_http: httpx.AsyncClient,
        omnigent: OmnigentRest,
        omnigent_adapter: OmnigentExecutionAdapter,
    ) -> None:
        self.service = service
        self.broker = broker
        self.broker_server = broker_server
        self.observer = observer
        self.github = github
        self.workspaces = workspaces
        self.config = config
        self.github_http = github_http
        self.omnigent = omnigent
        self.omnigent_adapter = omnigent_adapter
        self._started = False

    async def start(self) -> None:
        helper = install_git_helper(self.config.wrapper_bin_dir)
        install_push_guard(push_guard_dir(self.config))
        install_gh_wrapper(
            self.config.wrapper_bin_dir, self.config.real_gh_path, self.config.gh_config_dir
        )
        await _in_thread(self.workspaces.ensure_source_clone)
        # Molly/Rosie scratch state never shows up as untracked work (never .gitignore).
        await asyncio.to_thread(self.workspaces.ensure_excluded, "/.molly/")
        await self.broker.restore()
        await self.broker_server.restore()
        github_observed = await self._github_reconcile()
        omnigent_observed = await self.observer.observe_once()
        parcels = [
            parcel
            for parcel in await _load_parcels(self.service)
            if parcel.parcel_id in github_observed
            and parcel.current_session_id in omnigent_observed
        ]
        for parcel in parcels:
            session = parcel.current_session
            if session is not None and not self.broker.capabilities.usable(session.session_id):
                await self.broker.provision(session.session_id)
        held = await self._upgrade_live_policies(parcels)
        # A run whose policy guard failed or was just repaired stays closed: it reopens
        # (once) only after reconciliation, the propagation barrier and verification.
        await reenable_issuance_after_boot(
            self.broker, [p for p in parcels if p.current_session_id not in held]
        )
        await self.broker_server.start()
        await self.observer.start()
        # Keep the installed helper reachable in diagnostics and make accidental changes
        # to the configured command visible at boot.
        if self.broker_server.helper_command != f"!{helper}":
            raise RuntimeError("credential helper installation path changed during boot")
        self._started = True

    async def _upgrade_live_policies(self, parcels: list[Parcel]) -> frozenset[str]:
        """Live runs get the current factory policies (caller guard included), in place.

        Returns the runs held closed: any whose reconciliation failed or changed something
        (a change needs the propagation barrier before it is effective). Each gets a
        ``PolicyGuardFailed`` event, which closes its work gate and schedules the
        reconcile -> barrier -> verify sequence that alone reopens it.
        """
        held: set[str] = set()
        for parcel in parcels:
            session = parcel.current_session
            if (
                session is None
                or session.root_id is None
                or not session.prepared
                or session.lifecycle in (Lifecycle.RETIRED, Lifecycle.FENCED)
            ):
                continue
            try:
                changed = await self.omnigent_adapter.upgrade_static_policies(session.session_id)
            except (PolicyError, OmnigentReadError) as exc:
                LOG.warning("policy upgrade failed session=%s reason=%s", session.session_id, exc)
                changed = True
            if not changed:
                continue
            LOG.info("policy guard held closed session=%s", session.session_id)
            held.add(session.session_id)
            await self.service.apply_event(
                Event(
                    event_id=f"boot-policy-hold:{session.session_id}:{self.service.clock.now_utc_us()}",
                    repo_id=self.config.repo_id,
                    parcel_id=parcel.parcel_id,
                    source_time_us=self.service.clock.now_utc_us(),
                    provenance=Provenance.ADAPTER,
                    body=ev.PolicyGuardFailed(session_id=session.session_id),
                )
            )
        return frozenset(held)

    async def _github_reconcile(self) -> frozenset[str]:
        observed: set[str] = set()
        for parcel in await _load_parcels(self.service):
            if parcel.issue_number is None or parcel.current_session is None:
                continue
            snapshot = await self.github.issue_snapshot(
                IssueRef(self.config.repo_id, parcel.issue_number, parcel.parcel_id)
            )
            if not isinstance(snapshot, IssueSnapshot):
                continue
            await self.service.apply_event(
                Event(
                    event_id=f"startup-github:{parcel.parcel_id}:{snapshot.read_at_us}",
                    repo_id=self.config.repo_id,
                    parcel_id=parcel.parcel_id,
                    source_time_us=snapshot.read_at_us,
                    provenance=Provenance.ADAPTER,
                    body=ev.GitHubSnapshot(),
                    evidence=snapshot,
                )
            )
            observed.add(parcel.parcel_id)
        return frozenset(observed)

    async def close(self) -> None:
        await self.observer.close()
        await self.broker_server.close()
        await self.omnigent.aclose()
        await self.github_http.aclose()
        self._started = False

    def healthy(self) -> bool:
        return not self._started or self.observer.healthy()


async def build_production(
    config: ServiceConfig,
    *,
    fatal_exit: Callable[[int], object] | None = None,
    github_transport: httpx.AsyncBaseTransport | None = None,
    omnigent_transport: httpx.AsyncBaseTransport | None = None,
) -> ProductionComposition:
    """Build every real adapter used by ``serve``; no network I/O happens here."""
    _require_production_config(config)
    log_expiry(config)
    config.prepare_private_directories()
    process_lock = ProcessLock(config.state_dir)
    process_lock.acquire()
    clock = SystemClock()
    github_http = httpx.AsyncClient(transport=github_transport, timeout=15.0)
    authenticator = AppAuthenticator(
        config.github_app_id, _private_file(config.resolved_app_private_key_file)
    )
    token_service = InstallationTokenService(
        github_http,
        authenticator,
        config.github_installation_id,
        config.repository,
        config.github_api_url,
    )
    daemon_tokens = DaemonTokenProvider(token_service, clock)
    github_client = GitHubClient(github_http, daemon_tokens.token, api_url=config.github_api_url)

    service = FactoryService(
        config,
        clock=clock,
        fatal_exit=fatal_exit,
        process_lock=process_lock,
    )
    directory = ServiceDispatchDirectory(service.db, config)
    publications = PublicationRenderer(directory, config)
    github = GitHubAPIAdapter(
        github_client,
        repository=config.repository,
        repository_node_id=config.repo_id,
        project_node_id=config.project_node_id,
        status_field_node_id=config.status_field_node_id,
        bot_user_id=config.github_bot_user_id,
        required_checks=frozenset(config.required_checks),
        now_us=clock.now_utc_us,
        board_schema=BoardSchema(
            config.status_field_node_id,
            config.status_options,
            config.bot_field_node_id,
            config.bot_options,
            config.note_field_node_id,
        ),
        publication_renderer=publications,
        independent_reviewer_ids=config.independent_reviewer_ids,
        owner_ids=config.owners,
        parcel_resolver=ServiceParcelResolver(service.db),
        triage_fields=publications.triage_fields,
        cross_vendor_review=publications.cross_vendor_review,
        review_bot_login=config.review_bot_login,
        review_bot_mention=config.review_bot_mention,
    )
    identity = DeliveryIdentity(
        app_id=config.github_app_id,
        installation_id=config.github_installation_id,
        organization_id=_required(config.organization_id, "organization_id"),
        project_node_id=config.project_node_id,
        status_field_node_id=config.status_field_node_id,
        repository_id=_required(config.repository_database_id, "repository_database_id"),
        repository_node_id=config.repo_id,
        repository_full_name=config.repository,
        owner_ids=config.owners,
        bot_user_id=config.github_bot_user_id,
        status_option_ids=github.status_options,
    )
    normalizer = DeliveryNormalizer(identity)

    workspaces = Workspaces(
        config.source_clone,
        owned_worktree_roots(config),
        config.repository,
        push_guard_dir=push_guard_dir(config),
        harness_settings=True,
    )
    capability_store = StoreCapabilityStore(service.db)
    capabilities = CapabilityRegistry(
        config.capability_dir,
        config.broker_socket,
        config.repository,
        store=capability_store,
    )
    broker = LocalCredentialBroker(
        gate=StoreExecutionGate(service.db, clock),
        minter=AppInstallationTokenMinter(token_service),
        clock=clock,
        capabilities=capabilities,
        repository=config.repository,
    )
    helper_command = f"!{config.wrapper_bin_dir / 'git-credential-omnigent-factory'}"
    bot_slug = config.github_bot_login.removesuffix("[bot]")
    bot_identity = BotIdentity(
        config.github_bot_login,
        f"{config.github_bot_user_id}+{bot_slug}[bot]@users.noreply.github.com",
    )
    broker_server = BrokerServer(
        broker,
        config.broker_socket,
        workspaces=workspaces,
        identity=bot_identity,
        helper_command=helper_command,
        grants=StoreWorkerGrantStore(service.db),
    )
    omnigent = OmnigentRest(
        config.omnigent_base_url,
        auth=omnigent_auth(config),
        transport=omnigent_transport,
    )
    omnigent_adapter = OmnigentExecutionAdapter(
        rest=omnigent,
        config=OmnigentConfig(
            agent_id=_required_text(config.omnigent_agent_id, "omnigent_agent_id"),
            host_id=_required_text(config.omnigent_host_id, "omnigent_host_id"),
            repository=config.repository,
            project_id=_required_text(config.omnigent_project_id, "omnigent_project_id"),
            default_branch=config.default_branch,
        ),
        directory=directory,
        ledger=StoreOwnItemLedger(service.db),
        workspaces=workspaces,
        provisioner=StageProvisioner(broker, broker_server),
        identity=bot_identity,
        clock=clock,
        broker_socket=config.broker_socket,
        helper_command=helper_command,
    )
    recording_omnigent = RecordingOmnigentAdapter(omnigent_adapter, directory)
    observer = OmnigentObserver(
        service,
        omnigent_adapter,
        directory,
        clock,
        interval_seconds=config.observation_interval_seconds,
        settled_interval_seconds=config.settled_observation_interval_seconds,
    )
    runtime = ProductionRuntime(
        service=service,
        broker=broker,
        broker_server=broker_server,
        observer=observer,
        github=github,
        workspaces=workspaces,
        config=config,
        github_http=github_http,
        omnigent=omnigent,
        omnigent_adapter=omnigent_adapter,
    )
    service.comment_rerenderer = github.rerender_comment
    service.busy_nodes = omnigent_adapter.busy_nodes

    def adopt_reloaded(new: ServiceConfig) -> None:
        # Hot-reloadable keys only (the service rejects any other change).
        directory.config = new
        publications.config = new
        github.independent_reviewer_ids = new.independent_reviewer_ids
        github.review_bot_login = new.review_bot_login
        github.review_bot_mention = new.review_bot_mention

    service.config_listeners.append(adopt_reloaded)
    cleaner = WorkspaceCleaner(directory, workspaces, config.worktree_root)
    service.workspace_cleaner = cleaner
    service.bind_integrations(
        adapters=(github, recording_omnigent, broker, CleanupAdapter(cleaner)),
        delivery_processor=GitHubDeliveryProcessor(service, normalizer, github, clock),
        managed=(runtime,),
    )
    return ProductionComposition(
        service,
        GitHubWebhookVerifier(config.resolved_webhook_secret_file, normalizer, clock),
        build_endpoint(service, directory, config),
    )


def push_guard_dir(config: ServiceConfig) -> Path:
    """Daemon-owned ``core.hooksPath`` for factory worktrees (parcel-branch push guard)."""
    return config.wrapper_bin_dir.parent / "hooks"


def owned_worktree_roots(config: ServiceConfig) -> tuple[Path, ...]:
    """Roots a stage worktree may live in.

    Omnigent's JSON create (``git.branch_name`` + ``base_branch``) makes the worktree
    beside the clone as ``<clone>-worktrees/<branch-slug>``; the configured
    ``worktree_root`` stays accepted for bound/existing worktrees.
    """
    sibling = config.source_clone.with_name(f"{config.source_clone.name}-worktrees")
    return (config.worktree_root, sibling)


def _require_production_config(config: ServiceConfig) -> None:
    required = [config.resolved_app_private_key_file, config.resolved_webhook_secret_file]
    if config.omnigent_cli_store is None:
        required.append(config.resolved_omnigent_token_file)
    for path in required:
        _private_file(path)
    _required(config.repository_database_id, "repository_database_id")
    _required(config.organization_id, "organization_id")
    _required_text(config.omnigent_host_id, "omnigent_host_id")
    _required_text(config.omnigent_agent_id, "omnigent_agent_id")
    _required_text(config.omnigent_project_id, "omnigent_project_id")


def _private_file(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ConfigError(f"required secret is not a regular file: {path}")
    metadata = path.stat(follow_symlinks=False)
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise ConfigError(f"required secret must be owned by the daemon and mode 0600: {path}")
    return path.read_bytes()


def _required(value: int | None, name: str) -> int:
    if value is None or value <= 0:
        raise ConfigError(f"{name} must be resolved before serve")
    return value


def _required_text(value: str | None, name: str) -> str:
    if value is None or not value:
        raise ConfigError(f"{name} must be resolved before serve")
    return value


async def _load_parcels(service: FactoryService) -> list[Parcel]:
    rows = await service.db.call(lambda store: store.query("SELECT aggregate_json FROM parcels"))
    return [parcel_from_json(str(row[0])) for row in rows]


async def _in_thread(function: object) -> None:
    await asyncio.to_thread(cast(Callable[[], None], function))
