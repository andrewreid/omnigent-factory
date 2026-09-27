"""Process-loss injection for crash/replay tests of the production adapters.

:class:`CrashAfterCommit` wraps the handler behind an ``httpx.MockTransport``. Every
mutating request is recorded (duplicates included), the backend commits it, and - once
armed for a matching request - :class:`InjectedCrash` is raised before any response
reaches the adapter. That models the daemon dying after an external write and before its
outcome is recorded; the test then rebuilds the service on the same SQLite file.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field

import httpx

Handler = Callable[[httpx.Request], httpx.Response]
Match = Callable[[httpx.Request], bool]

_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


class InjectedCrash(BaseException):
    """Model process loss, bypassing the executor's ordinary exception handling."""


def is_graphql_mutation(request: httpx.Request) -> bool:
    if request.url.path != "/graphql" or not request.content:
        return False
    query = json.loads(request.content).get("query", "")
    return isinstance(query, str) and query.lstrip().startswith("mutation")


def is_mutation(request: httpx.Request) -> bool:
    """A request that changes external state (GraphQL queries are reads)."""
    if request.method not in _WRITE_METHODS:
        return False
    return request.url.path != "/graphql" or is_graphql_mutation(request)


@dataclass
class CrashAfterCommit:
    """Count every external write and crash once right after a matching one commits."""

    backend: Handler
    writes: list[httpx.Request] = field(default_factory=list)
    crashed: list[httpx.Request] = field(default_factory=list)
    _armed: Match | None = None

    def arm(self, match: Match) -> None:
        self._armed = match

    def count(self, match: Match) -> int:
        return sum(1 for request in self.writes if match(request))

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        response = self.backend(request)
        if not is_mutation(request):
            return response
        self.writes.append(request)
        if self._armed is not None and self._armed(request):
            self._armed = None
            self.crashed.append(request)
            raise InjectedCrash(f"{request.method} {request.url.path}")
        return response
