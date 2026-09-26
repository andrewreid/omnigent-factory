"""Time port. The core never reads a clock; adapters and the service do, via this."""

from __future__ import annotations

import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Source of time. Both values are integer microseconds.

    ``now_utc_us`` correlates persisted samples and event times across restarts.
    ``monotonic_us`` measures elapsed active time within one process boot only.
    """

    def now_utc_us(self) -> int: ...

    def monotonic_us(self) -> int: ...


class SystemClock:
    """Real clock for production wiring."""

    def now_utc_us(self) -> int:
        return time.time_ns() // 1000

    def monotonic_us(self) -> int:
        return time.monotonic_ns() // 1000
