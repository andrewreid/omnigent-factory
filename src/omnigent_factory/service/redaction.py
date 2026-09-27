"""Logging helpers that never expose request bodies, tokens, or secret values."""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping

_SENSITIVE = ("authorization", "cookie", "secret", "signature", "token", "private_key")
_VALUE_PATTERN = re.compile(
    r"(?i)\b(authorization|cookie|secret|signature|token|private_key)\s*[=:]\s*([^\s,;]+)"
)
_BEARER_PATTERN = re.compile(r"(?i)\bBearer\s+[^\s,;]+")


def redact_text(value: str) -> str:
    value = _VALUE_PATTERN.sub(lambda match: f"{match.group(1)}=[REDACTED]", value)
    return _BEARER_PATTERN.sub("Bearer [REDACTED]", value)


def redact_fields(values: Mapping[str, object]) -> dict[str, object]:
    return {
        key: "[REDACTED]" if any(part in key.lower() for part in _SENSITIVE) else value
        for key, value in values.items()
    }


class SecretRedactionFilter(logging.Filter):
    """Redact structured and formatted secret values before handlers see a record."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.args, dict):
            record.args = redact_fields(record.args)
        record.msg = redact_text(record.getMessage())
        record.args = ()
        return True


_FILTER = SecretRedactionFilter()


def install_redaction_filter() -> SecretRedactionFilter:
    """Idempotently install redaction on package/uvicorn loggers and handlers."""
    installed = _FILTER
    root = logging.getLogger()
    for handler in root.handlers:
        if installed not in handler.filters:
            handler.addFilter(installed)
    for name, candidate in logging.Logger.manager.loggerDict.items():
        if isinstance(candidate, logging.Logger) and (
            name.startswith("omnigent_factory") or name.startswith("uvicorn")
        ):
            if installed not in candidate.filters:
                candidate.addFilter(installed)
            for handler in candidate.handlers:
                if installed not in handler.filters:
                    handler.addFilter(installed)
    return installed


def configure_logging(level: int = logging.INFO) -> None:
    """Daemon logging to stderr (journald) with secret redaction on every handler."""
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        root.addHandler(handler)
    root.setLevel(level)
    # Per-request HTTP client lines add noise and carry full URLs; keep them quiet.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    install_redaction_filter()
