"""Active-time estimation for a stage grant (architecture §5.3, §2.8 invariant 11).

Active time is the *union* of intervals in which any node of the tree does productive or
cleanup work, measured with the process's monotonic clock. Parallel children count once;
a root parked on an owner prompt contributes nothing by itself while a running child
still counts.

Two bounds are kept:

* ``lower_us`` - measured productive intervals only;
* ``upper_us`` - additionally provisioning/async-wait intervals, stream gaps and an
  unobserved restart outage while the tree was (or may have been) active.

Samples report the *upper* bound so a grant is checkpointed conservatively rather than
silently extended. A new tracker after a restart starts from the persisted consumption.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from omnigent_factory.core.accounting import ActivityInterval, union_active_us
from omnigent_factory.core.events import ActiveTimeSample
from omnigent_factory.omnigent.tree import NodeState, TreeObservation
from omnigent_factory.ports.clock import Clock


@dataclass(frozen=True, slots=True)
class ActiveEstimate:
    lower_us: int
    upper_us: int

    @property
    def uncertain_us(self) -> int:
        return self.upper_us - self.lower_us


@dataclass
class ActivityTracker:
    clock: Clock
    session_id: str
    grant_id: str
    baseline_us: int = 0
    _measured: list[ActivityInterval] = field(default_factory=list)
    _inferred: list[ActivityInterval] = field(default_factory=list)
    _open_measured: dict[str, int] = field(default_factory=dict)
    _open_inferred: dict[str, int] = field(default_factory=dict)
    _gap_start: int | None = None

    # ------------------------------------------------------------ observations

    def _set(
        self, table: dict[str, int], sink: list[ActivityInterval], node: str, on: bool
    ) -> None:
        now = self.clock.monotonic_us()
        if on and node not in table:
            table[node] = now
        elif not on and node in table:
            sink.append(ActivityInterval(node, table.pop(node), now))

    def observe_node(self, node: NodeState) -> None:
        self._set(self._open_measured, self._measured, node.node_id, node.productive)
        self._set(self._open_inferred, self._inferred, node.node_id, node.maybe_productive)

    def observe_status(self, node_id: str, status: str, *, parked: bool = False) -> None:
        """A ``session.status`` stream edge for one node."""
        productive = status == "running" and not parked
        maybe = productive or (status in ("launching", "waiting") and not parked)
        self._set(self._open_measured, self._measured, node_id, productive)
        self._set(self._open_inferred, self._inferred, node_id, maybe)

    def observe_tree(self, obs: TreeObservation) -> None:
        for node in obs.nodes.values():
            self.observe_node(node)
        for gone in set(self._open_measured) - set(obs.nodes):
            if obs.complete:
                self._set(self._open_measured, self._measured, gone, False)
        for gone in set(self._open_inferred) - set(obs.nodes):
            if obs.complete:
                self._set(self._open_inferred, self._inferred, gone, False)

    def gap_started(self) -> None:
        if self._gap_start is None:
            self._gap_start = self.clock.monotonic_us()

    def gap_ended(self) -> None:
        if self._gap_start is not None:
            self._inferred.append(
                ActivityInterval("<gap>", self._gap_start, self.clock.monotonic_us())
            )
            self._gap_start = None

    def restart_outage(self, outage_us: int, *, tree_possibly_active: bool) -> None:
        """Account an unobserved daemon outage (UTC-estimated) to the upper bound only."""
        if tree_possibly_active and outage_us > 0:
            start = self.clock.monotonic_us() - outage_us
            self._inferred.append(ActivityInterval("<outage>", start, self.clock.monotonic_us()))

    # ------------------------------------------------------------ estimates

    def estimate(self) -> ActiveEstimate:
        now = self.clock.monotonic_us()
        measured = [
            *self._measured,
            *(ActivityInterval(n, s) for n, s in self._open_measured.items()),
        ]
        inferred = [
            *self._inferred,
            *(ActivityInterval(n, s) for n, s in self._open_inferred.items()),
        ]
        if self._gap_start is not None:
            inferred.append(ActivityInterval("<gap>", self._gap_start))
        lower = union_active_us(measured, now)
        upper = union_active_us([*measured, *inferred], now)
        return ActiveEstimate(self.baseline_us + lower, self.baseline_us + upper)

    def sample(self) -> ActiveTimeSample:
        return ActiveTimeSample(
            session_id=self.session_id,
            grant_id=self.grant_id,
            consumed_us=self.estimate().upper_us,
        )
