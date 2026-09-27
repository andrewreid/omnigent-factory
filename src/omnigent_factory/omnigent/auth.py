"""Omnigent API credentials for the daemon.

Preferred: the owner's Omnigent CLI credential store (``~/.omnigent/auth_tokens.json``),
read at call time. Its entry for the server holds the CLI login's access token, its
``expires_at`` and a refresh token. First-party login grants do not rotate their refresh
token, so the daemon can refresh (``POST /oauth/token``, ``grant_type=refresh_token``,
1-hour access tokens) without disturbing the CLI. The grant itself ends at the server's
grant lifetime (30 days from login unless ``OMNIGENT_GRANT_MAX_LIFETIME_DAYS`` is set on
the server); after that the owner runs ``omnigent login`` again and the daemon picks the
new entry up without a restart.

Fallback: a private token file (a copied bearer token, which cannot refresh).
"""

from __future__ import annotations

import base64
import json
import logging
import threading
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

LOG = logging.getLogger(__name__)

#: Refresh (or switch source) this long before an access token expires.
REFRESH_MARGIN_SECONDS = 300
#: After a refused refresh, wait this long before trying again.
REFRESH_BACKOFF_SECONDS = 300


@dataclass(frozen=True, slots=True)
class LoginExpiry:
    """When the credential the daemon depends on stops working, as far as it can tell."""

    source: str  # "cli-store" | "token-file"
    expires_at: float | None
    refreshable: bool


def _store_entry(store: Path, base_url: str) -> dict[str, Any] | None:
    try:
        data = json.loads(store.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    entry = data.get(base_url.rstrip("/")) or data.get(base_url)
    return entry if isinstance(entry, dict) else None


def jwt_expiry(token: str) -> float | None:
    """The unverified ``exp`` claim of a JWT bearer token (display/diagnostics only)."""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    except (ValueError, TypeError):
        return None
    exp = claims.get("exp") if isinstance(claims, dict) else None
    return float(exp) if isinstance(exp, int | float) else None


def login_expiry(store: Path | None, base_url: str, token_file: Path | None) -> LoginExpiry | None:
    """The login's end: the CLI store entry's ``expires_at`` (the login token lives as long
    as its grant), else the token file's JWT ``exp``."""
    if store is not None:
        entry = _store_entry(store, base_url)
        if entry is not None:
            expires = entry.get("expires_at")
            return LoginExpiry(
                "cli-store",
                float(expires) if isinstance(expires, int | float) else None,
                isinstance(entry.get("refresh_token"), str) and bool(entry["refresh_token"]),
            )
    if token_file is not None:
        try:
            token = token_file.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        return LoginExpiry("token-file", jwt_expiry(token), False)
    return None


class CliStoreAuth(httpx.Auth):
    """Bearer auth from the CLI credential store, refreshed near expiry (see module doc)."""

    requires_response_body = True

    def __init__(
        self,
        store: Path,
        base_url: str,
        *,
        fallback_file: Path | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._store = store
        self._base_url = base_url.rstrip("/")
        self._fallback = fallback_file
        self._now = now
        self._lock = threading.Lock()
        self._cached: tuple[str, float] | None = None
        self._refused_until = 0.0

    def _usable(self, expires_at: object) -> bool:
        return (
            isinstance(expires_at, int | float)
            and expires_at > self._now() + REFRESH_MARGIN_SECONDS
        )

    def _current(self) -> tuple[str | None, str | None]:
        """(usable token or None, refresh token when a refresh is needed)."""
        with self._lock:
            if self._cached is not None and self._usable(self._cached[1]):
                return self._cached[0], None
        entry = _store_entry(self._store, self._base_url)
        if entry is not None:
            token = entry.get("token")
            if isinstance(token, str) and token and self._usable(entry.get("expires_at")):
                return token, None  # e.g. after a fresh `omnigent login`
            refresh = entry.get("refresh_token")
            if isinstance(refresh, str) and refresh and self._now() >= self._refused_until:
                return None, refresh
            if isinstance(token, str) and token:
                return token, None  # expired: the server's 401 makes the failure explicit
        if self._fallback is not None:
            try:
                return self._fallback.read_text(encoding="utf-8").strip(), None
            except OSError:
                return None, None
        return None, None

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response]:
        token, refresh = self._current()
        if refresh is not None:
            response = yield httpx.Request(
                "POST",
                f"{self._base_url}/oauth/token",
                data={"grant_type": "refresh_token", "refresh_token": refresh},
            )
            token = self._accept(response)
            if token is None:
                token, _ = self._fallback_token()
        if token:
            request.headers["Authorization"] = f"Bearer {token}"
        yield request

    def _accept(self, response: httpx.Response) -> str | None:
        body: Any = None
        if response.status_code == 200:
            try:
                body = response.json()
            except ValueError:
                body = None
        token = body.get("access_token") if isinstance(body, dict) else None
        if not isinstance(token, str) or not token:
            with self._lock:
                self._refused_until = self._now() + REFRESH_BACKOFF_SECONDS
            LOG.error(
                "Omnigent login refresh refused (HTTP %s): the login grant has expired or was "
                "revoked; run `omnigent login` on this host",
                response.status_code,
            )
            return None
        expires_in = body.get("expires_in")
        ttl = float(expires_in) if isinstance(expires_in, int | float) else 3600.0
        with self._lock:
            self._cached = (token, self._now() + ttl)
        LOG.info("Omnigent access token refreshed from the CLI login (ttl=%ss)", int(ttl))
        return token

    def _fallback_token(self) -> tuple[str | None, None]:
        entry = _store_entry(self._store, self._base_url)
        token = entry.get("token") if entry is not None else None
        if isinstance(token, str) and token:
            return token, None
        if self._fallback is not None:
            try:
                return self._fallback.read_text(encoding="utf-8").strip(), None
            except OSError:
                return None, None
        return None, None
