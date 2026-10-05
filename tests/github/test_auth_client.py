from __future__ import annotations

import base64
import json

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from omnigent_factory.core.effects import CredentialProfile
from omnigent_factory.github.auth import AppAuthenticator, InstallationTokenService
from omnigent_factory.github.client import GitHubClient, RateLimited
from omnigent_factory.ports.credentials import TokenGrant, TokenRefusal


def private_key() -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def decode_segment(value: str):
    padding = "=" * (-len(value) % 4)
    return json.loads(base64.urlsafe_b64decode(value + padding))


def test_app_jwt_has_short_lifetime_and_numeric_app_issuer():
    token = AppAuthenticator(1234, private_key(), now=lambda: 2_000_000_000).jwt()
    header, payload, signature = token.split(".")
    assert decode_segment(header) == {"alg": "RS256", "typ": "JWT"}
    assert decode_segment(payload) == {"iat": 1_999_999_940, "exp": 2_000_000_540, "iss": "1234"}
    assert signature


@pytest.mark.asyncio
async def test_installation_token_is_repo_scoped_and_profile_limited():
    recorded = []

    def handler(request: httpx.Request) -> httpx.Response:
        recorded.append(request)
        requested = json.loads(request.content)
        return httpx.Response(
            201,
            json={
                "token": "opaque",
                "expires_at": "2030-01-01T00:00:00Z",
                "permissions": requested["permissions"],
                "repositories": [{"full_name": "SA-Ambulance/timesheets"}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = InstallationTokenService(
            http,
            AppAuthenticator(1, private_key(), now=lambda: 2_000_000_000),
            99,
            "SA-Ambulance/timesheets",
        )
        grant = await service.mint("SA-Ambulance/timesheets", CredentialProfile.READ_ONLY)
        refused = await service.mint("SA-Ambulance/other", CredentialProfile.BUILD)
    assert isinstance(grant, TokenGrant)
    assert isinstance(refused, TokenRefusal)
    body = json.loads(recorded[0].content)
    assert body["repositories"] == ["timesheets"]
    assert body["permissions"]["contents"] == "read"
    assert set(body["permissions"].values()) == {"read"}
    assert recorded[0].headers["authorization"].startswith("Bearer ")


@pytest.mark.asyncio
async def test_build_token_has_only_approved_repository_permissions():
    body = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal body
        body = json.loads(request.content)
        return httpx.Response(
            201,
            json={
                "token": "opaque",
                "expires_at": "2030-01-01T00:00:00Z",
                "permissions": body["permissions"],
                "repositories": [{"full_name": "SA-Ambulance/timesheets"}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = InstallationTokenService(
            http,
            AppAuthenticator(1, private_key()),
            99,
            "SA-Ambulance/timesheets",
        )
        await service.mint("SA-Ambulance/timesheets", CredentialProfile.BUILD)
    assert body is not None
    assert body["permissions"] == {
        "actions": "write",
        "checks": "read",
        "contents": "write",
        "issues": "write",
        "metadata": "read",
        "pull_requests": "write",
        "statuses": "read",
        "workflows": "write",
    }


@pytest.mark.asyncio
async def test_workflows_write_is_build_only_and_must_be_granted_exactly():
    """Owner decision 2026-10-01 (#694): BUILD requests workflows:write; nothing else does."""
    bodies: list[dict] = []
    drop_workflows = False

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        bodies.append(body)
        granted = dict(body["permissions"])
        if drop_workflows:
            granted.pop("workflows", None)
        return httpx.Response(
            201,
            json={
                "token": "opaque",
                "expires_at": "2030-01-01T00:00:00Z",
                "permissions": granted,
                "repositories": [{"full_name": "SA-Ambulance/timesheets"}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = InstallationTokenService(
            http, AppAuthenticator(1, private_key()), 99, "SA-Ambulance/timesheets"
        )
        build = await service.mint("SA-Ambulance/timesheets", CredentialProfile.BUILD)
        read_only = await service.mint("SA-Ambulance/timesheets", CredentialProfile.READ_ONLY)
        daemon = await service.mint_daemon()
        drop_workflows = True
        short = await service.mint("SA-Ambulance/timesheets", CredentialProfile.BUILD)
    assert isinstance(build, TokenGrant) and isinstance(read_only, TokenGrant)
    assert bodies[0]["permissions"]["workflows"] == "write"
    assert "workflows" not in bodies[1]["permissions"]
    assert "workflows" not in bodies[2]["permissions"]
    assert not isinstance(daemon, TokenRefusal) and "workflows" not in daemon.permissions
    # An installation that has not accepted workflows:write yields a narrower token; the
    # exact-scope check refuses it rather than handing out a token that cannot push.
    assert isinstance(short, TokenRefusal)
    assert "differ from the requested" in short.reason


@pytest.mark.asyncio
async def test_actions_write_is_build_only_and_must_be_granted_exactly():
    """Owner decision 2026-10-06: BUILD requests actions:write (re-run / cancel CI runs);
    read-only stage and daemon tokens keep actions:read."""
    bodies: list[dict] = []
    downgrade_actions = False

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        bodies.append(body)
        granted = dict(body["permissions"])
        if downgrade_actions:
            granted["actions"] = "read"
        return httpx.Response(
            201,
            json={
                "token": "opaque",
                "expires_at": "2030-01-01T00:00:00Z",
                "permissions": granted,
                "repositories": [{"full_name": "SA-Ambulance/timesheets"}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = InstallationTokenService(
            http, AppAuthenticator(1, private_key()), 99, "SA-Ambulance/timesheets"
        )
        build = await service.mint("SA-Ambulance/timesheets", CredentialProfile.BUILD)
        read_only = await service.mint("SA-Ambulance/timesheets", CredentialProfile.READ_ONLY)
        daemon = await service.mint_daemon()
        downgrade_actions = True
        short = await service.mint("SA-Ambulance/timesheets", CredentialProfile.BUILD)
    assert isinstance(build, TokenGrant) and isinstance(read_only, TokenGrant)
    assert bodies[0]["permissions"]["actions"] == "write"
    assert bodies[1]["permissions"]["actions"] == "read"
    assert bodies[2]["permissions"]["actions"] == "read"
    assert not isinstance(daemon, TokenRefusal) and daemon.permissions["actions"] == "read"
    # An installation that has not accepted actions:write is refused, not silently narrowed.
    assert isinstance(short, TokenRefusal)
    assert "differ from the requested" in short.reason


@pytest.mark.asyncio
async def test_installation_token_scope_omissions_fail_closed():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                201, json={"token": "opaque", "expires_at": "2030-01-01T00:00:00Z"}
            )
        )
    ) as http:
        result = await InstallationTokenService(
            http, AppAuthenticator(1, private_key()), 99, "SA-Ambulance/timesheets"
        ).mint("SA-Ambulance/timesheets", CredentialProfile.READ_ONLY)
    assert isinstance(result, TokenRefusal)
    assert "omitted permission scope" in result.reason


@pytest.mark.asyncio
async def test_pagination_follows_every_link_and_authenticates():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.params.get("page") == "2":
            return httpx.Response(200, json=[{"id": 2}])
        return httpx.Response(
            200,
            json=[{"id": 1}],
            headers={"Link": '<https://api.github.com/items?page=2>; rel="next"'},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await GitHubClient(http, "installation-token").paginate("/items")
    assert result == [{"id": 1}, {"id": 2}]
    assert all(
        request.headers["authorization"] == "Bearer installation-token" for request in requests
    )


@pytest.mark.asyncio
async def test_rate_limit_exposes_retry_delay_without_retrying():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            403,
            headers={"x-ratelimit-remaining": "0", "retry-after": "17"},
            json={"message": "rate limited"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(RateLimited) as raised:
            await GitHubClient(http, "token").get_json("/items")
    assert raised.value.retry_after_us == 17_000_000
    assert calls == 1


@pytest.mark.asyncio
async def test_authenticated_delivery_recovery_gets_all_pages_and_original_payload():
    auth = []

    def handler(request: httpx.Request) -> httpx.Response:
        auth.append(request.headers["authorization"])
        if request.url.path == "/app/hook/deliveries/10":
            return httpx.Response(
                200,
                json={
                    "guid": "delivery-guid",
                    "event": "issues",
                    "delivered_at": "2026-01-01T00:00:00Z",
                    "request": {
                        "headers": {"X-GitHub-Event": "issues"},
                        "payload": {"action": "created"},
                    },
                },
            )
        if request.url.params.get("page") == "2":
            return httpx.Response(200, json=[{"id": 11}])
        return httpx.Response(
            200,
            json=[{"id": 10}],
            headers={"Link": '<https://api.github.com/app/hook/deliveries?page=2>; rel="next"'},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = GitHubClient(http, "app-jwt")
        deliveries = await client.recover_deliveries()
        recovered = await client.recover_delivery(10)
    assert deliveries == [{"id": 10}, {"id": 11}]
    assert json.loads(recovered.raw_body) == {"action": "created"}
    assert recovered.guid == "delivery-guid"
    assert recovered.event == "issues"
    assert recovered.delivered_at_us == 1_767_225_600_000_000
    assert recovered.raw_body_is_original is False
    assert set(auth) == {"Bearer app-jwt"}
