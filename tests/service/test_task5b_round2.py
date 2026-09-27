"""Round-2 remediation: board deliveries are never dropped; plans publish byte-exact."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.canonical import canonical_contract
from omnigent_factory.core.effects import (
    Ack,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    ExecutionContext,
)
from omnigent_factory.core.protocol import (
    MAX_CONTRACT_CHARS,
    Correlation,
    ResultError,
    parse_factory_result,
)
from omnigent_factory.core.types import InboxHoldReason, Size, Via
from omnigent_factory.github.adapter import GitHubAPIAdapter, ParcelBinding
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.github.webhook import DeliveryIdentity, DeliveryNormalizer
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import PublicationRenderer, ServiceDispatchDirectory
from omnigent_factory.service.github_delivery import GitHubDeliveryProcessor
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import DeliveryRecord, SqliteStore
from omnigent_factory.testing.builders import contract, result_candidate
from omnigent_factory.testing.fakes import FakeClock
from tests.store_driver import ISSUE, StoreDriver

REPO_NODE = "R_kgDOTC12Fg"
REPO_DB_ID = 1278047766


# ------------------------------------------------------------------ shared helpers


def _identity(config: ServiceConfig) -> DeliveryIdentity:
    return DeliveryIdentity(
        app_id=900,
        installation_id=901,
        organization_id=296340858,
        project_node_id=config.project_node_id,
        status_field_node_id=config.status_field_node_id,
        repository_id=REPO_DB_ID,
        repository_node_id=REPO_NODE,
        repository_full_name=config.repository,
        owner_ids=config.owners,
        bot_user_id=config.github_bot_user_id,
    )


def _adapter(http: httpx.AsyncClient, config: ServiceConfig, **kwargs: Any) -> GitHubAPIAdapter:
    return GitHubAPIAdapter(
        GitHubClient(http, "token"),
        repository=config.repository,
        repository_node_id=config.repo_id,
        project_node_id=config.project_node_id,
        status_field_node_id=config.status_field_node_id,
        bot_user_id=config.github_bot_user_id,
        required_checks=frozenset(),
        **kwargs,
    )


# ------------------------------------------------------------------ R1: board deliveries


def _board_delivery(guid: str, content_id: str = ISSUE) -> DeliveryRecord:
    body = json.dumps(
        {
            "action": "edited",
            "installation": {"id": 901, "app_id": 900},
            "organization": {"id": 296340858},
            "sender": {"id": 114979},
            "projects_v2_item": {
                "node_id": "PVTI_1",
                "project_node_id": "PVT_kwDOEanNes4BkJhb",
                "content_node_id": content_id,
            },
            "changes": {
                "field_value": {
                    "field_node_id": "PVTSSF_lADOEanNes4BkJhbzhi7I9w",
                    "from": {"id": "f75ad846"},
                    "to": {"id": "47fc9ee4"},
                }
            },
        },
        separators=(",", ":"),
    ).encode()
    return DeliveryRecord(
        guid,
        "projects_v2_item",
        body,
        {},
        app_id=900,
        installation_id=901,
        source_time_us=1_800_000_000_000_000,
    )


def _issue_node(config: ServiceConfig, **overrides: Any) -> dict[str, Any]:
    node: dict[str, Any] = {
        "__typename": "Issue",
        "id": ISSUE,
        "number": 1,
        "title": "issue",
        "body": "body",
        "state": "OPEN",
        "repository": {
            "id": REPO_NODE,
            "databaseId": REPO_DB_ID,
            "nameWithOwner": config.repository,
        },
        "assignees": {"nodes": []},
        "projectItems": {
            "nodes": [
                {
                    "id": "PVTI_1",
                    "project": {"id": config.project_node_id},
                    "fieldValueByName": None,  # Status temporarily unreadable
                }
            ],
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        },
    }
    node.update(overrides)
    return node


class BoardRig:
    def __init__(
        self, config: ServiceConfig, answer: Callable[[httpx.Request], httpx.Response]
    ) -> None:
        self.config = config.model_copy(
            update={
                "delivery_resolution_backoff_seconds": 900.0,
                "delivery_resolution_max_attempts": 3,
            }
        )
        self.clock = FakeClock()
        self.calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            self.calls += 1
            return answer(request)

        self.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        self.service = FactoryService(self.config, clock=self.clock)
        self.processor = GitHubDeliveryProcessor(
            self.service,
            DeliveryNormalizer(_identity(self.config)),
            _adapter(self.http, self.config),
            self.clock,
        )

    async def __aenter__(self) -> BoardRig:
        await self.service.db.start()
        await self.service.db.call(lambda store: store.ensure_repository(self.config.trusted))
        await self.service.parked.load()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.service.db.close()
        await self.http.aclose()

    async def known_parcel(self) -> None:
        def seed(store: SqliteStore) -> None:
            StoreDriver(store, self.config.trusted, start_us=self.clock.now_utc_us()).eligible()

        await self.service.db.call(seed)

    async def deliver(self, record: DeliveryRecord) -> None:
        await self.service.db.call(lambda store: store.append_delivery(record))

    async def run(self, seconds: int) -> None:
        for _ in range(seconds):
            for delivery in await self.service.db.call(lambda s: s.pending_deliveries()):
                await self.processor.process(delivery)
            self.clock.advance(1_000_000)

    async def status(self, guid: str) -> tuple[str, int]:
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT status, resolution_attempts FROM deliveries WHERE delivery_guid = ?",
                (guid,),
            )
        )
        return str(rows[0][0]), int(rows[0][1])

    async def holds(self) -> tuple[tuple[str, InboxHoldReason], ...]:
        parcel = await self.service.db.call(lambda store: store.load_parcel(ISSUE))
        assert parcel is not None
        return tuple((h.delivery_guid, h.reason) for h in parcel.inbox_holds)


def _graphql(node: object) -> Callable[[httpx.Request], httpx.Response]:
    return lambda _: httpx.Response(200, json={"data": {"node": node}})


@pytest.mark.asyncio
async def test_exhausted_lookup_on_known_parcel_parks_and_keeps_the_hold(
    service_config: ServiceConfig,
) -> None:
    async with BoardRig(service_config, _graphql(_issue_node(service_config))) as rig:
        await rig.known_parcel()
        await rig.deliver(_board_delivery("drag"))
        await rig.run(1)
        assert await rig.status("drag") == ("unresolved", 1)
        assert await rig.holds() == (("drag", InboxHoldReason.UNRESOLVED),)
        await rig.run(3 * 3_600)
        assert rig.calls == 3  # the durable backoff still bounds GitHub reads
        assert await rig.status("drag") == ("rejected", 3)
        assert rig.service.parked.records() == (("drag", ISSUE),)
        assert rig.service.parked.blocks(ISSUE)
        # The fence is upgraded to an operator-only hold, never silently lifted.
        assert await rig.service.release_resolved_inbox_holds() == 0
        assert await rig.holds() == (("drag", InboxHoldReason.PARKED),)

        # Operator release re-queues the delivery with a fresh bounded budget.
        await rig.service.operator_command("release-delivery", {"delivery": "drag"})
        assert await rig.status("drag") == ("pending", 0)
        assert await rig.holds() == ()


@pytest.mark.parametrize(
    "node",
    [
        pytest.param(None, id="null-node"),
        pytest.param({"__typename": "Issue", "id": "I_other"}, id="different-node"),
        pytest.param({"id": ISSUE}, id="typename-missing"),
        pytest.param("items-empty", id="not-in-project"),
        pytest.param("repo-partial", id="repository-inconsistent"),
        pytest.param("repo-missing", id="repository-missing"),
    ],
)
@pytest.mark.asyncio
async def test_inconclusive_lookup_never_retires_on_first_attempt(
    service_config: ServiceConfig, node: object
) -> None:
    if node == "items-empty":
        node = _issue_node(
            service_config,
            projectItems={"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
        )
    elif node == "repo-partial":
        node = _issue_node(
            service_config,
            repository={"id": REPO_NODE, "databaseId": 1, "nameWithOwner": "x/y"},
        )
    elif node == "repo-missing":
        node = _issue_node(service_config, repository=None)
    async with BoardRig(service_config, _graphql(node)) as rig:
        await rig.known_parcel()
        await rig.deliver(_board_delivery("drag"))
        await rig.run(1)
        assert await rig.status("drag") == ("unresolved", 1)
        assert await rig.holds() == (("drag", InboxHoldReason.UNRESOLVED),)
        assert await rig.service.release_resolved_inbox_holds() == 0


@pytest.mark.asyncio
async def test_failed_lookups_are_set_aside_with_backoff_then_parked_repository_wide(
    service_config: ServiceConfig,
) -> None:
    async with BoardRig(service_config, lambda _: httpx.Response(502)) as rig:
        await rig.deliver(_board_delivery("drag", "I_unknown"))
        await rig.run(1)  # no exception escapes: the failure is recorded durably
        assert await rig.status("drag") == ("unresolved", 1)
        await rig.run(3 * 3_600)
        assert rig.calls == 3  # bounded by the durable backoff
        assert await rig.status("drag") == ("rejected", 3)
        # The card cannot be tied to a parcel, so every new dispatch is gated.
        assert rig.service.parked.records() == (("drag", None),)
        assert rig.service.parked.blocks("anything")


@pytest.mark.parametrize(
    "node",
    [
        pytest.param({"__typename": "DraftIssue", "id": ISSUE}, id="draft"),
        pytest.param({"__typename": "PullRequest", "id": ISSUE}, id="pull-request"),
        pytest.param("foreign-repo", id="other-repository"),
    ],
)
@pytest.mark.asyncio
async def test_proven_foreign_cards_are_retired_once(
    service_config: ServiceConfig, node: object
) -> None:
    if node == "foreign-repo":
        node = _issue_node(
            service_config,
            repository={"id": "R_other", "databaseId": 5, "nameWithOwner": "other/repo"},
        )
    async with BoardRig(service_config, _graphql(node)) as rig:
        await rig.deliver(_board_delivery("card"))
        await rig.run(3_600)
        assert rig.calls == 1
        assert await rig.status("card") == ("processed", 0)
        assert rig.service.parked.records() == ()


@pytest.mark.asyncio
async def test_foreign_looking_card_that_is_a_known_parcel_is_set_aside(
    service_config: ServiceConfig,
) -> None:
    node = _issue_node(
        service_config,
        repository={"id": "R_other", "databaseId": 5, "nameWithOwner": "other/repo"},
    )
    async with BoardRig(service_config, _graphql(node)) as rig:
        await rig.known_parcel()
        await rig.deliver(_board_delivery("moved"))
        await rig.run(1)
        assert await rig.status("moved") == ("unresolved", 1)
        assert await rig.holds() == (("moved", InboxHoldReason.UNRESOLVED),)


# ------------------------------------------------------------------ R2: plan publication


class CommentServer:
    """GitHub issue comments: counts every POST and serves what was stored."""

    def __init__(self, bot_id: int) -> None:
        self.bot_id = bot_id
        self.comments: list[dict[str, Any]] = []
        self.posts = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if not request.url.path.endswith("/comments"):
            return httpx.Response(404)
        if request.method == "POST":
            self.posts += 1
            comment = {
                "id": 1000 + len(self.comments),
                "body": json.loads(request.content)["body"],
                "user": {"id": self.bot_id},
                "created_at": "2027-01-15T08:00:00Z",
            }
            self.comments.append(comment)
            return httpx.Response(201, json=comment)
        return httpx.Response(200, json=self.comments)


def _big_contract(chars: int) -> dict[str, object]:
    base = contract("M", "Ship it")
    per = 15_000
    base["acceptance_criteria"] = [
        {"id": f"AC{n}", "criterion": "c" * per, "verification": "unit test"}
        for n in range(chars // per + 1)
    ]
    return base


async def _publish(
    config: ServiceConfig, canonical: str
) -> tuple[object, CommentServer, EffectIntent]:
    """Plan -> PUBLISH_CONTRACT in a real store, executed by the real adapter + renderer."""
    service = FactoryService(config, clock=FakeClock())
    await service.db.start()
    server = CommentServer(config.github_bot_user_id)
    try:

        def seed(store: SqliteStore) -> EffectIntent:
            store.ensure_repository(config.trusted)
            driver = StoreDriver(store, config.trusted, start_us=1_800_000_000_000_000)
            driver.unpause()
            driver.eligible()
            driver.send(ev.RequestPlan(via=Via.DRAG))
            session = driver.create_ok("root-1")
            driver.send(
                result_candidate(
                    session.session_id,
                    "root-1",
                    session.revision,
                    ev.ResultKind.PLAN,
                    publication_kind=ev.PublicationKind.CONTRACT,
                    contract_canonical=canonical,
                    size=Size.M,
                    open_decision_ids=(),
                )
            )
            [publish] = driver.of(EffectKind.PUBLISH_CONTRACT)
            return publish

        effect = await service.db.call(seed)
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            directory = ServiceDispatchDirectory(service.db, config)
            adapter = _adapter(
                http,
                config,
                publication_renderer=PublicationRenderer(directory, config),
                now_us=lambda: 1,
                # Production does not resolve the issue number for comment effects yet
                # (reported FOLLOW_UP); bind it so this test isolates publication bytes.
                parcel_bindings={ISSUE: ParcelBinding(issue_number=1)},
            )
            outcome = await adapter.execute(effect, ExecutionContext("boot", 1, 1, 1))
    finally:
        await service.db.close()
    return outcome, server, effect


@pytest.mark.parametrize(
    "goal",
    [
        pytest.param("Bump @types/node and fix @Injectable usage for @team", id="mentions"),
        pytest.param("g" * 9_000, id="over-free-form-cap"),
    ],
)
@pytest.mark.asyncio
async def test_contract_is_published_byte_exact_and_verifies(
    service_config: ServiceConfig, goal: str
) -> None:
    canonical = canonical_contract(contract("M", goal)).decode()
    outcome, server, effect = await _publish(service_config, canonical)
    assert isinstance(outcome, Ack) and outcome.detail["verified"] is True
    assert server.posts == 1
    posted = server.comments[0]["body"]
    assert f"```parcel-contract\n{canonical}\n```" in posted
    assert "\u200b" not in posted and "[factory output truncated]" not in posted
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    assert digest == effect.args["full_hash"]


@pytest.mark.asyncio
async def test_unpublishable_contract_is_refused_before_any_post(
    service_config: ServiceConfig,
) -> None:
    canonical = canonical_contract(_big_contract(70_000)).decode()
    assert len(canonical) > 65_536
    outcome, server, _ = await _publish(service_config, canonical)
    assert isinstance(outcome, DefinitiveFailure)
    assert server.posts == 0


def _plan_result(contract_body: dict[str, object]) -> str:
    envelope = {
        "version": 1,
        "parcel_id": "P",
        "stage_session_id": "S",
        "dispatch_nonce": "N",
        "revision": 0,
        "result": {
            "kind": "plan",
            "publication_kind": "contract",
            "approach": "a",
            "risks": [],
            "contract": contract_body,
            "open_decision_ids": [],
        },
    }
    return f"FACTORY_RESULT_V1\n```factory-result\n{json.dumps(envelope)}\n```"


def test_plan_result_whose_contract_cannot_be_published_whole_is_invalid() -> None:
    expected = Correlation("P", "S", "N", 0, "plan")
    fits = _big_contract(MAX_CONTRACT_CHARS - 20_000)
    assert parse_factory_result(_plan_result(fits), expected).contract_canonical is not None
    with pytest.raises(ResultError, match="publishable comment size"):
        parse_factory_result(_plan_result(_big_contract(MAX_CONTRACT_CHARS)), expected)
