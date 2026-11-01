"""GitHub timestamp parsing bounded by Factory's observation clock."""

from __future__ import annotations

import logging
from datetime import datetime

FUTURE_SKEW_WARNING_US = 5_000_000


def observed_github_time_us(
    value: object,
    observed_at_us: int,
    *,
    fallback_us: int = 0,
    logger: logging.Logger,
    context: str,
) -> int:
    """Parse a GitHub timestamp without letting it postdate its observation.

    Invalid or absent values retain the caller's existing fallback semantics. Every
    positive skew is clamped; only skew beyond the small tolerance is operationally
    noisy.
    """
    if not isinstance(value, str):
        return fallback_us
    try:
        parsed_us = int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1e6)
    except ValueError:
        return fallback_us
    skew_us = parsed_us - observed_at_us
    if skew_us > FUTURE_SKEW_WARNING_US:
        logger.warning("future GitHub timestamp clamped %s skew_us=%s", context, skew_us)
    return min(parsed_us, observed_at_us)
