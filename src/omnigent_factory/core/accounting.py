"""Active-time arithmetic (architecture §5.3, §2.8 invariant 11).

Active time is the *union* of intervals in which any node of a stage tree runs
productive or cleanup work. Parallel children count union time, never summed time; a
root parked on an owner elicitation contributes nothing by itself. Open intervals are
closed at ``now_us`` (a monotonic sample within one boot).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ActivityInterval:
    node_id: str
    start_us: int
    end_us: int | None = None
    running: bool = True  # False for owner waits / parked prompts


def union_active_us(intervals: Iterable[ActivityInterval], now_us: int) -> int:
    spans = sorted(
        (i.start_us, min(i.end_us if i.end_us is not None else now_us, now_us))
        for i in intervals
        if i.running
    )
    total = 0
    cur_start: int | None = None
    cur_end = 0
    for start, end in spans:
        if end <= start:
            continue
        if cur_start is None or start > cur_end:
            if cur_start is not None:
                total += cur_end - cur_start
            cur_start, cur_end = start, end
        else:
            cur_end = max(cur_end, end)
    if cur_start is not None:
        total += cur_end - cur_start
    return total
