"""Production composition root for the single-process daemon."""

from __future__ import annotations

import asyncio
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
from omnigent_factory.core.types import IssueSnapshot, Parcel
from omnigent_factory.credentials.broker import LocalCredentialBroker
from omnigent_factory.credentials.capabilities import CapabilityRegistry
from omnigent_factory.credentials.gh_wrapper import install_gh_wrapper
from omnigent_factory.credentials.git_helper import install_git_helper
from omnigent_factory.credentials.server import BrokerServer, StageProvisioner
from omnigent_factory.credentials.worktree import BotIdentity, Workspaces
from omnigent_factory.github.adapter import BoardSchema, GitHubAPIAdapter
from omnigent_factory.github.auth import AppAuthenticator, InstallationTokenService
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.github.config import FactoryConfig
from omnigent_factory.github.webhook import DeliveryIdentity, DeliveryNormalizer
from omnigent_factory.omnigent.adapter import OmnigentConfig, OmnigentExecutionAdapter
from omnigent_factory.omnigent.rest import FileTokenAuth, OmnigentRest
from omnigent_factory.ports.clock import SystemClock
from omnigent_factory.ports.github import IssueRef
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
from omnigent_factory.service.observer import OmnigentObserver
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.service.tokens import DaemonTokenProvider


@dataclass(frozen=True, slots=True)
class ProductionComposition:
    """Fully wired daemon plus the verifier consumed by the HTTP application."""

    service: FactoryService
    verifier: GitHubWebhookVerifier


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
        self._started = False

    async def start(self) -> None:
        helper = install_git_helper(self.config.wrapper_bin_dir)
        install_gh_wrapper(
            self.config.wrapper_bin_dir, self.config.real_gh_path, self.config.gh_config_dir
        )
        await _in_thread(self.workspaces.ensure_source_clone)
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
        await reenable_issuance_after_boot(self.broker, parcels)
        await self.broker_server.start()
        await self.observer.start()
        # Keep the installed helper reachable in diagnostics and make accidental changes
        # to the configured command visible at boot.
        if self.broker_server.helper_command != f"!{helper}":
            raise RuntimeError("credential helper installation path changed during boot")
        self._started = True

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
    """Build every real adapter used by ``serve``; network I/O is config preflight only."""
    _require_production_config(config)
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
    try:
        repository_config = await github_client.default_branch_config(config.repository)
        config = _apply_repository_config(config, repository_config)
    except BaseException:
        await github_http.aclose()
        process_lock.close()
        raise
    preflight_token = daemon_tokens.cached()
    await github_http.aclose()
    github_http = httpx.AsyncClient(transport=github_transport, timeout=15.0)
    token_service = InstallationTokenService(
        github_http,
        authenticator,
        config.github_installation_id,
        config.repository,
        config.github_api_url,
    )
    daemon_tokens = DaemonTokenProvider(token_service, clock, initial=preflight_token)
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
        ),
        publication_renderer=publications,
        independent_reviewer_ids=config.independent_reviewer_ids,
        owner_ids=config.owners,
        parcel_resolver=ServiceParcelResolver(service.db),
        triage_fields=publications.triage_fields,
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

    workspaces = Workspaces(config.source_clone, owned_worktree_roots(config), config.repository)
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
        auth=FileTokenAuth(config.resolved_omnigent_token_file),
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
    )
    service.comment_rerenderer = github.rerender_comment
    service.bind_integrations(
        adapters=(github, recording_omnigent, broker),
        delivery_processor=GitHubDeliveryProcessor(service, normalizer, github, clock),
        managed=(runtime,),
    )
    return ProductionComposition(
        service,
        GitHubWebhookVerifier(config.resolved_webhook_secret_file, normalizer, clock),
    )


def owned_worktree_roots(config: ServiceConfig) -> tuple[Path, ...]:
    """Roots a stage worktree may live in.

    Omnigent's JSON create (``git.branch_name`` + ``base_branch``) makes the worktree
    beside the clone as ``<clone>-worktrees/<branch-slug>``; the configured
    ``worktree_root`` stays accepted for bound/existing worktrees.
    """
    sibling = config.source_clone.with_name(f"{config.source_clone.name}-worktrees")
    return (config.worktree_root, sibling)


def _apply_repository_config(config: ServiceConfig, repo: FactoryConfig) -> ServiceConfig:
    if repo.review.bot_login != config.github_bot_login:
        raise ConfigError("factory.yml bot login differs from the configured App bot")
    if frozenset(repo.review.approver_ids) != config.owners:
        raise ConfigError("factory.yml approvers differ from the host trust root")
    reviewers = frozenset(repo.review.independent_reviewer_ids)
    values = config.model_dump()
    values.update(
        max_building=min(config.max_building, repo.concurrency.max_building),
        max_open_bot_prs=min(config.max_open_bot_prs, repo.concurrency.max_open_bot_prs),
        checkpoint_block_hours=repo.checkpoints.block_hours.model_dump(),
        checkpoint_grace_minutes=repo.checkpoints.grace_minutes,
        cost_backstop_usd_per_hour=repo.checkpoints.cost_backstop_usd_per_hour,
        independent_reviewer_ids=reviewers,
        triage_guidance=repo.guidance.triage,
        engineering_guidance=repo.guidance.engineering,
    )
    return ServiceConfig.model_validate(values)


def _require_production_config(config: ServiceConfig) -> None:
    for path in (
        config.resolved_app_private_key_file,
        config.resolved_webhook_secret_file,
        config.resolved_omnigent_token_file,
    ):
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
