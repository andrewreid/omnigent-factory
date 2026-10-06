"""Thin explicit REST transport for the Omnigent server (architecture §4, §5).

The adapter speaks JSON REST directly with its own ``httpx`` client rather than through
SDK private attributes: the SDK's create helper omits ``host_id``/``project_id``/``git``
and its child-list helper cannot page. Authentication is the owner's Omnigent CLI token,
supplied as an ``httpx.Auth`` (e.g. :class:`FileTokenAuth`, re-read per request so
rotation is honoured and the token is never logged).

Reads raise :class:`OmnigentReadError`; writes never raise and return a
:class:`WriteResponse` that the caller classifies (:func:`classify_write`). Pagination
walks ``has_more``/``last_id`` to exhaustion; a ``stale_cursor`` 400 restarts from the
first page (bounded), never skips.
"""

from __future__ import annotations

import enum
import json
from collections.abc import AsyncIterator, Callable, Generator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

#: 4xx statuses the server returns before any handler side effect (auth, missing
#: resource, request-model validation, rate limiting). Everything else is ambiguous.
PRE_SIDE_EFFECT_STATUSES = frozenset({401, 403, 404, 422, 429})


def as_map(value: object) -> Mapping[str, Any]:
    """``value`` if it is a JSON object, else an empty mapping."""
    return value if isinstance(value, dict) else {}


def as_list(value: object) -> list[Any]:
    """``value`` if it is a JSON array, else an empty list."""
    return value if isinstance(value, list) else []


class OmnigentReadError(RuntimeError):
    """A read could not be completed; never evidence of absence or idleness."""

    def __init__(self, reason: str, status: int | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status = status


class FileTokenAuth(httpx.Auth):
    """Bearer auth from a private token file, re-read per request."""

    def __init__(self, path: Path, scheme: str = "Bearer") -> None:
        self._path = path
        self._scheme = scheme

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response]:
        token = self._path.read_text(encoding="utf-8").strip()
        request.headers["Authorization"] = f"{self._scheme} {token}"
        yield request


class WriteClass(enum.StrEnum):
    OK = "ok"
    DEFINITIVE = "definitive"  # server-proven: no side effect
    AMBIGUOUS = "ambiguous"  # may or may not have happened


@dataclass(frozen=True, slots=True)
class WriteResponse:
    status: int | None
    body: Mapping[str, Any] | None
    error: str | None = None

    @property
    def error_code(self) -> str | None:
        if not self.body:
            return None
        err = self.body.get("error")
        if isinstance(err, Mapping):
            code = err.get("code")
            return code if isinstance(code, str) else None
        return None


def classify_write(resp: WriteResponse) -> WriteClass:
    if resp.status is None:
        return WriteClass.AMBIGUOUS  # transport error / timeout: request may have landed
    if 200 <= resp.status < 300:
        return WriteClass.OK if resp.body is not None else WriteClass.AMBIGUOUS
    if resp.status in PRE_SIDE_EFFECT_STATUSES:
        return WriteClass.DEFINITIVE
    return WriteClass.AMBIGUOUS


class OmnigentRest:
    def __init__(
        self,
        base_url: str,
        *,
        auth: httpx.Auth | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 15.0,
        max_pages: int = 200,
        page_limit: int = 100,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            auth=auth,
            transport=transport,
            timeout=httpx.Timeout(timeout_s),
            follow_redirects=False,
        )
        self.max_pages = max_pages
        self.page_limit = page_limit

    async def aclose(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------ reads

    async def get_json(
        self, path: str, params: Mapping[str, str | int] | None = None
    ) -> dict[str, Any]:
        try:
            resp = await self._client.get(path, params=dict(params or {}))
        except httpx.HTTPError as exc:
            raise OmnigentReadError(f"GET {path}: {type(exc).__name__}") from exc
        if resp.status_code != 200:
            raise OmnigentReadError(f"GET {path}: HTTP {resp.status_code}", resp.status_code)
        try:
            body = resp.json()
        except ValueError as exc:
            raise OmnigentReadError(f"GET {path}: non-JSON body") from exc
        if not isinstance(body, dict):
            raise OmnigentReadError(f"GET {path}: non-object body")
        return body

    async def paginate(
        self,
        path: str,
        params: Mapping[str, str | int] | None = None,
        *,
        restarts: int = 2,
        until: Callable[[Mapping[str, Any]], bool] | None = None,
    ) -> list[dict[str, Any]]:
        """All rows of a cursor-paginated list, or :class:`OmnigentReadError`.

        ``until``: stop after the first page holding a row it accepts (that whole page is
        returned). Without it, or when no row matches, every page is read.
        """
        for _attempt in range(restarts + 1):
            rows: list[dict[str, Any]] = []
            after: str | None = None
            try:
                for _page in range(self.max_pages):
                    query: dict[str, str | int] = {"limit": self.page_limit, **(params or {})}
                    if after is not None:
                        query["after"] = after
                    body = await self.get_json(path, query)
                    data = body.get("data")
                    if not isinstance(data, list):
                        raise OmnigentReadError(f"GET {path}: missing data list")
                    page = [d for d in data if isinstance(d, dict)]
                    rows.extend(page)
                    if not body.get("has_more") or (until is not None and any(map(until, page))):
                        return rows
                    last = body.get("last_id")
                    if not isinstance(last, str) or not last or last == after:
                        raise OmnigentReadError(f"GET {path}: has_more without a new cursor")
                    after = last
                raise OmnigentReadError(f"GET {path}: page ceiling reached")
            except OmnigentReadError as exc:
                if exc.status == 400 and after is not None:
                    continue  # stale cursor: restart from the first page
                raise
        raise OmnigentReadError(f"GET {path}: cursor kept going stale")

    # ------------------------------------------------------------ writes

    async def _write(self, method: str, path: str, body: Mapping[str, Any] | None) -> WriteResponse:
        try:
            resp = await self._client.request(method, path, json=body)
        except httpx.HTTPError as exc:
            return WriteResponse(None, None, type(exc).__name__)
        parsed: Mapping[str, Any] | None
        try:
            raw = resp.json()
            parsed = raw if isinstance(raw, dict) else None
        except ValueError:
            parsed = None
        return WriteResponse(resp.status_code, parsed)

    async def post_json(self, path: str, body: Mapping[str, Any]) -> WriteResponse:
        return await self._write("POST", path, body)

    async def delete(self, path: str) -> WriteResponse:
        return await self._write("DELETE", path, None)

    async def patch_json(self, path: str, body: Mapping[str, Any]) -> WriteResponse:
        return await self._write("PATCH", path, body)

    # ------------------------------------------------------------ stream

    async def stream(self, session_id: str) -> AsyncIterator[dict[str, Any]]:
        """Live-tail ``/v1/sessions/{id}/stream``. No replay; errors propagate."""
        async with self._client.stream(
            "GET", f"/v1/sessions/{session_id}/stream", timeout=httpx.Timeout(600.0)
        ) as resp:
            if resp.status_code != 200:
                raise OmnigentReadError(f"stream {session_id}: HTTP {resp.status_code}")
            async for envelope in parse_sse(resp.aiter_lines()):
                yield envelope


async def parse_sse(lines: AsyncIterator[str]) -> AsyncIterator[dict[str, Any]]:
    """Parse the server's ``event:``/``data:`` framing; ``[DONE]`` ends the stream."""
    current: str | None = None
    async for raw in lines:
        line = raw.rstrip("\r\n")
        if line.startswith("event: "):
            current = line[7:]
        elif line.startswith("data: ") and current is not None:
            data = line[6:]
            if data.strip() == "[DONE]":
                return
            try:
                parsed = json.loads(data)
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                yield parsed
            current = None
        elif line == "":
            current = None
