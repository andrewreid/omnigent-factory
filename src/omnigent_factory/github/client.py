"""Small typed GitHub HTTP client with complete pagination and rate-limit handling."""

from __future__ import annotations

import base64
import email.utils
import inspect
import json
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from omnigent_factory.github.config import FactoryConfig, parse_factory_config

TokenSource = str | Callable[[], str] | Callable[[], Awaitable[str]]


class GitHubAPIError(RuntimeError):
    pass


class AmbiguousRequest(GitHubAPIError):
    """A mutating request lost its response and may have taken effect."""


@dataclass(frozen=True, slots=True)
class GitHubRejected(GitHubAPIError):
    status_code: int
    method: str
    path: str

    def __str__(self) -> str:
        return f"GitHub {self.method} {self.path} returned HTTP {self.status_code}"


@dataclass(frozen=True, slots=True)
class RateLimited(GitHubAPIError):
    retry_after_us: int

    def __str__(self) -> str:
        return f"GitHub rate limit reached; retry in {self.retry_after_us}us"


@dataclass(frozen=True, slots=True)
class RecoveredDelivery:
    """Recovered webhook metadata.

    ``raw_body_is_original`` is true only when GitHub returns the payload as a string.
    GitHub commonly returns a decoded object; its re-encoding is suitable for trusted
    recovery normalization but must not be compared with the live delivery byte digest.
    """

    delivery_id: int
    guid: str
    event: str
    delivered_at_us: int
    headers: Mapping[str, str]
    raw_body: bytes
    raw_body_is_original: bool


class GitHubClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        token: TokenSource,
        *,
        api_url: str = "https://api.github.com",
        now: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._token = token
        self._api_url = api_url.rstrip("/")
        self._api_origin = httpx.URL(self._api_url)
        self._now = now

    async def _headers(self) -> dict[str, str]:
        token = self._token() if callable(self._token) else self._token
        if inspect.isawaitable(token):
            token = await token
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    async def request(
        self,
        method: str,
        path: str,
        *,
        json_body: Mapping[str, object] | None = None,
        expected: frozenset[int] = frozenset({200}),
    ) -> httpx.Response:
        url = self._request_url(path)
        try:
            response = await self._client.request(
                method,
                url,
                headers=await self._headers(),
                json=json_body,
            )
        except httpx.HTTPError as exc:
            error = f"GitHub request failed: {type(exc).__name__}"
            if method.upper() not in {"GET", "HEAD"}:
                raise AmbiguousRequest(error) from exc
            raise GitHubAPIError(error) from exc
        if response.status_code in {403, 429} and (
            "retry-after" in response.headers
            or response.headers.get("x-ratelimit-remaining") == "0"
            or response.status_code == 429
        ):
            raise RateLimited(self._retry_after_us(response.headers))
        if response.status_code not in expected:
            raise GitHubRejected(response.status_code, method, path)
        return response

    def _request_url(self, path: str) -> str:
        if not path.startswith(("http://", "https://")):
            return f"{self._api_url}{path}"
        target = httpx.URL(path)
        if (
            target.scheme != self._api_origin.scheme
            or target.host != self._api_origin.host
            or target.port != self._api_origin.port
        ):
            raise GitHubAPIError("refusing to send GitHub credential to an unconfigured host")
        return str(target)

    def _retry_after_us(self, headers: httpx.Headers) -> int:
        retry_after = headers.get("retry-after")
        if retry_after is not None:
            try:
                seconds = float(retry_after)
            except ValueError:
                try:
                    parsed = email.utils.parsedate_to_datetime(retry_after)
                    seconds = parsed.timestamp() - self._now()
                except (TypeError, ValueError, OverflowError):
                    seconds = 60
            return max(1_000_000, int(seconds * 1_000_000))
        reset = headers.get("x-ratelimit-reset")
        if reset is not None:
            try:
                seconds = float(reset) - self._now()
            except ValueError:
                seconds = 60
            return max(1_000_000, int(seconds * 1_000_000))
        return 60_000_000

    async def get_json(self, path: str) -> object:
        return (await self.request("GET", path)).json()

    async def paginate(self, path: str) -> list[Any]:
        """Follow GitHub Link headers until the collection is complete."""
        items: list[Any] = []
        next_url: str | None = path
        while next_url is not None:
            response = await self.request("GET", next_url)
            page: Any = response.json()
            if not isinstance(page, list):
                raise GitHubAPIError("paginated GitHub response was not a list")
            items.extend(page)
            next_url = response.links.get("next", {}).get("url")
        return items

    async def graphql(self, query: str, variables: Mapping[str, object]) -> dict[str, Any]:
        response = await self.request(
            "POST",
            "/graphql",
            json_body={"query": query, "variables": dict(variables)},
        )
        data: Any = response.json()
        if not isinstance(data, dict) or data.get("errors"):
            raise GitHubAPIError("GitHub GraphQL request returned errors")
        result = data.get("data")
        if not isinstance(result, dict):
            raise GitHubAPIError("GitHub GraphQL response omitted data")
        return result

    async def default_branch_config(self, repository: str) -> FactoryConfig:
        """Read .github/factory.yml explicitly from the current default branch."""
        metadata: Any = await self.get_json(f"/repos/{repository}")
        default_branch = metadata.get("default_branch") if isinstance(metadata, dict) else None
        if not isinstance(default_branch, str) or not default_branch:
            raise GitHubAPIError("repository metadata omitted default_branch")
        encoded_ref = httpx.QueryParams({"ref": default_branch})
        content: Any = await self.get_json(
            f"/repos/{repository}/contents/.github/factory.yml?{encoded_ref}"
        )
        if not isinstance(content, dict) or content.get("encoding") != "base64":
            raise GitHubAPIError("factory config response was not base64 file content")
        encoded = content.get("content")
        if not isinstance(encoded, str):
            raise GitHubAPIError("factory config response omitted content")
        try:
            raw = base64.b64decode("".join(encoded.split()), validate=True)
        except ValueError as exc:
            raise GitHubAPIError("factory config content was invalid base64") from exc
        return parse_factory_config(raw)

    async def recover_deliveries(self, *, per_page: int = 100) -> list[dict[str, Any]]:
        deliveries = await self.paginate(f"/app/hook/deliveries?per_page={per_page}")
        return [item for item in deliveries if isinstance(item, dict)]

    async def recover_delivery(self, delivery_id: int) -> RecoveredDelivery:
        item: Any = await self.get_json(f"/app/hook/deliveries/{delivery_id}")
        request = item.get("request") if isinstance(item, dict) else None
        payload = request.get("payload") if isinstance(request, dict) else None
        if payload is None:
            raise GitHubAPIError("delivery recovery omitted original request payload")
        guid = item.get("guid") if isinstance(item, dict) else None
        event = item.get("event") if isinstance(item, dict) else None
        delivered_at = item.get("delivered_at") if isinstance(item, dict) else None
        headers = request.get("headers") if isinstance(request, dict) else None
        if not isinstance(guid, str) or not isinstance(event, str) or not isinstance(headers, dict):
            raise GitHubAPIError("delivery recovery omitted original identity or headers")
        if isinstance(payload, str):
            raw_body_is_original = True
            raw_body = payload.encode()
        else:
            raw_body_is_original = False
            raw_body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
        if not isinstance(delivered_at, str):
            raise GitHubAPIError("delivery recovery omitted original delivery time")
        try:
            delivered_at_us = int(
                datetime.fromisoformat(delivered_at.replace("Z", "+00:00")).timestamp() * 1e6
            )
        except ValueError as exc:
            raise GitHubAPIError("delivery recovery time was malformed") from exc
        string_headers = {
            str(key): str(value) for key, value in headers.items() if isinstance(key, str)
        }
        return RecoveredDelivery(
            delivery_id,
            guid,
            event,
            delivered_at_us,
            string_headers,
            raw_body,
            raw_body_is_original,
        )
