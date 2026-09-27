"""Daemon Omnigent credentials from configuration, with expiry diagnostics."""

from __future__ import annotations

import logging
import time

import httpx

from omnigent_factory.omnigent.auth import CliStoreAuth, LoginExpiry, login_expiry
from omnigent_factory.omnigent.rest import FileTokenAuth
from omnigent_factory.service.config import ServiceConfig

LOG = logging.getLogger(__name__)

#: Warn this far ahead of the Omnigent login ending.
EXPIRY_WARNING_SECONDS = 7 * 24 * 3600
RENEW_HINT = (
    "run `omnigent login` on this host (the daemon reads the CLI login when "
    "omnigent_cli_store is configured); for a longer grant set "
    "OMNIGENT_GRANT_MAX_LIFETIME_DAYS on the Omnigent server"
)


def omnigent_auth(config: ServiceConfig) -> httpx.Auth:
    if config.omnigent_cli_store is not None:
        return CliStoreAuth(
            config.omnigent_cli_store,
            config.omnigent_base_url,
            fallback_file=config.resolved_omnigent_token_file,
        )
    return FileTokenAuth(config.resolved_omnigent_token_file)


def expiry(config: ServiceConfig) -> LoginExpiry | None:
    return login_expiry(
        config.omnigent_cli_store, config.omnigent_base_url, config.resolved_omnigent_token_file
    )


def expiry_message(config: ServiceConfig, now: float | None = None) -> tuple[str, str] | None:
    """(level, message) when the Omnigent login is expired or ends within 7 days."""
    info = expiry(config)
    if info is None or info.expires_at is None:
        return None
    remaining = info.expires_at - (time.time() if now is None else now)
    when = time.strftime("%Y-%m-%d %H:%M", time.localtime(info.expires_at))
    if remaining <= 0:
        return "error", f"Omnigent login ({info.source}) expired at {when}; {RENEW_HINT}"
    if remaining <= EXPIRY_WARNING_SECONDS:
        days = remaining / 86400
        return (
            "warning",
            f"Omnigent login ({info.source}) ends {when} ({days:.1f} days); {RENEW_HINT}",
        )
    return None


def log_expiry(config: ServiceConfig) -> None:
    found = expiry_message(config)
    if found is None:
        return
    level, message = found
    (LOG.error if level == "error" else LOG.warning)("%s", message)
