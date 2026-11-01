from __future__ import annotations

import httpx
import pytest

from omnigent_factory.github.client import GitHubClient, GitHubRejected
from omnigent_factory.github.links import ensure_label

REPOSITORY = "SA-Ambulance/timesheets"
LABEL = "bundle:proposed"


async def _ensure(responses: list[httpx.Response]) -> tuple[bool, list[httpx.Request]]:
    seen: list[httpx.Request] = []
    remaining = iter(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return next(remaining)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await ensure_label(
            GitHubClient(http, "token"), REPOSITORY, LABEL, "Bundle proposed"
        )
    return result, seen


@pytest.mark.asyncio
async def test_existing_label_is_unchanged() -> None:
    created, seen = await _ensure([httpx.Response(200, json={"name": LABEL})])

    assert created is False
    assert [request.method for request in seen] == ["GET"]


@pytest.mark.asyncio
async def test_absent_label_is_created() -> None:
    created, seen = await _ensure([httpx.Response(404), httpx.Response(201, json={})])

    assert created is True
    assert [request.method for request in seen] == ["GET", "POST"]


@pytest.mark.asyncio
async def test_concurrent_create_already_exists_is_success() -> None:
    created, seen = await _ensure(
        [
            httpx.Response(404),
            httpx.Response(
                422,
                json={
                    "message": "Validation Failed",
                    "errors": [{"resource": "Label", "code": "already_exists", "field": "name"}],
                },
            ),
        ]
    )

    assert created is False
    assert [request.method for request in seen] == ["GET", "POST"]


@pytest.mark.asyncio
async def test_other_create_validation_error_still_raises() -> None:
    with pytest.raises(GitHubRejected, match=r"POST .* returned HTTP 422"):
        await _ensure(
            [
                httpx.Response(404),
                httpx.Response(
                    422,
                    json={
                        "message": "Validation Failed",
                        "errors": [{"code": "invalid", "field": "color"}],
                    },
                ),
            ]
        )
