"""Recursive tree scans, interruption, stream/snapshot observation and active time."""

from __future__ import annotations

import json

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.types import DecisionImpact
from omnigent_factory.omnigent.activity import ActivityTracker
from omnigent_factory.omnigent.observe import StreamNormalizer, drain_stream
from omnigent_factory.omnigent.outcomes import observations
from omnigent_factory.omnigent.rest import OmnigentRest
from omnigent_factory.omnigent.tree import NodeState, TreeObservation, scan_tree
from omnigent_factory.testing.fakes import FakeClock
from tests.omnigent.fake_server import FakeOmnigentServer, FakeSession, elicitation
from tests.omnigent.support import AGENT, CTX, intent

pytestmark = pytest.mark.asyncio

ROOT = "conv_root"


def _server() -> tuple[FakeOmnigentServer, OmnigentRest]:
    server = FakeOmnigentServer()
    server.add(FakeSession(id=ROOT, agent_id=AGENT))
    return server, OmnigentRest("http://o.test", transport=server.transport(), page_limit=2)


def _chain(server: FakeOmnigentServer, parent: str, depth: int, prefix: str) -> str:
    for i in range(depth):
        child = f"{prefix}{i}"
        server.add(FakeSession(id=child, agent_id=AGENT, parent_session_id=parent))
        parent = child
    return parent


# ------------------------------------------------------------------ tree scans


async def test_all_pages_deep_tree_includes_root() -> None:
    server, rest = _server()
    for i in range(7):  # wide: several child pages at page_limit=2
        server.add(FakeSession(id=f"conv_w{i}", agent_id=AGENT, parent_session_id=ROOT))
    leaf = _chain(server, "conv_w6", 6, "conv_d")  # deep
    obs = await scan_tree(rest, ROOT)
    assert obs.complete
    assert ROOT in obs.nodes and leaf in obs.nodes
    assert len(obs.nodes) == 1 + 7 + 6
    assert not obs.busy


async def test_archived_intermediate_parent_and_its_running_child_are_found() -> None:
    server, rest = _server()
    server.add(FakeSession(id="conv_arch", agent_id=AGENT, parent_session_id=ROOT, archived=True))
    server.add(
        FakeSession(
            id="conv_hidden", agent_id=AGENT, parent_session_id="conv_arch", status="running"
        )
    )
    # The child-list route hides archived children...
    listed = await rest.paginate(f"/v1/sessions/{ROOT}/child_sessions")
    assert listed == []
    # ...so the archive-inclusive inventory must close the tree.
    obs = await scan_tree(rest, ROOT)
    assert {"conv_arch", "conv_hidden"} <= set(obs.nodes)
    assert obs.nodes["conv_arch"].archived and obs.busy and not obs.quiescent


async def test_idle_root_with_running_child_is_busy() -> None:
    server, rest = _server()
    server.add(FakeSession(id="conv_c", agent_id=AGENT, parent_session_id=ROOT, status="running"))
    obs = await scan_tree(rest, ROOT)
    assert obs.nodes[ROOT].status == "idle" and obs.busy
    assert obs.to_scan().busy and obs.to_scan().complete


@pytest.mark.parametrize(
    "setup",
    [
        {"status": "launching"},
        {"status": "waiting"},
        # Background tasks count while a turn is live (a parked waiter alone is not busy).
        {
            "status": "waiting",
            "pending_elicitations": [elicitation("elicit_1")],
            "background_tasks": [{"id": "t1", "status": "running"}],
        },
        {"pending_inputs": [{"pending_id": "p", "content": []}]},
        {"current_task_status": "in_progress"},
    ],
)
async def test_nonterminal_work_or_relaunchable_input_is_busy(setup: dict[str, object]) -> None:
    server, rest = _server()
    child = FakeSession(id="conv_c", agent_id=AGENT, parent_session_id=ROOT)
    for k, v in setup.items():
        setattr(child, k, v)
    server.add(child)
    assert (await scan_tree(rest, ROOT)).busy


async def test_parked_prompt_is_a_waiter_not_busy_but_running_sibling_counts() -> None:
    server, rest = _server()
    root = server.sessions[ROOT]
    root.status = "running"
    root.pending_elicitations.append(elicitation("elicit_1"))
    obs = await scan_tree(rest, ROOT)
    assert obs.complete and obs.pending_waiter and not obs.busy
    server.add(FakeSession(id="conv_c", agent_id=AGENT, parent_session_id=ROOT, status="running"))
    obs2 = await scan_tree(rest, ROOT)
    assert obs2.pending_waiter and obs2.busy


async def test_any_failed_read_makes_the_scan_incomplete() -> None:
    server, rest = _server()
    server.add(FakeSession(id="conv_c", agent_id=AGENT, parent_session_id=ROOT))
    for failing in ("/v1/sessions/conv_c", f"/v1/sessions/{ROOT}/child_sessions", "/v1/sessions"):
        server.fail_paths = {failing}
        obs = await scan_tree(rest, ROOT)
        assert not obs.complete and not obs.to_scan().complete, failing
        assert obs.errors


async def test_node_ceiling_is_unknown_not_idle() -> None:
    server, rest = _server()
    _chain(server, ROOT, 12, "conv_n")
    obs = await scan_tree(rest, ROOT, max_nodes=5)
    assert not obs.complete and "node-ceiling" in obs.errors


async def test_known_node_is_retained_when_pages_omit_it() -> None:
    server, rest = _server()
    server.add(FakeSession(id="conv_orphan", agent_id=AGENT, status="running"))
    obs = await scan_tree(rest, ROOT, known_ids=["conv_orphan"])
    assert "conv_orphan" in obs.nodes and obs.busy


async def test_stale_cursor_restarts_from_first_page() -> None:
    server, rest = _server()
    for i in range(5):
        server.add(FakeSession(id=f"conv_w{i}", agent_id=AGENT, parent_session_id=ROOT))
    calls = {"n": 0}
    original = server.handle

    def flaky(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("child_sessions") and request.url.params.get("after"):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(400, json={"error": {"code": "stale_cursor", "message": "x"}})
        return original(request)

    rest2 = OmnigentRest("http://o.test", transport=httpx.MockTransport(flaky), page_limit=2)
    rows = await rest2.paginate(f"/v1/sessions/{ROOT}/child_sessions")
    assert sorted(r["id"] for r in rows) == [f"conv_w{i}" for i in range(5)]
    assert rest  # silence


# ------------------------------------------------------------------ interrupt / scan effects


async def test_interrupt_hits_root_and_busy_descendants_and_is_not_stop_evidence(git_env) -> None:
    from tests.omnigent.support import make_rig

    rig = make_rig(git_env)
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT))
    rig.server.add(
        FakeSession(id="conv_a", agent_id=AGENT, parent_session_id=ROOT, status="running")
    )
    rig.server.add(FakeSession(id="conv_b", agent_id=AGENT, parent_session_id=ROOT))
    rig.server.add(
        FakeSession(
            id="conv_p",
            agent_id=AGENT,
            parent_session_id="conv_b",
            pending_elicitations=[elicitation("e9")],
            status="waiting",
        )
    )
    out = await rig.adapter.execute(intent(EffectKind.INTERRUPT_TREE, root_id=ROOT), CTX)
    assert isinstance(out, Ack) and out.detail["stop_evidence"] is False
    assert sorted(rig.server.interrupts) == sorted([ROOT, "conv_a", "conv_p"])
    assert observations(intent(EffectKind.INTERRUPT_TREE, root_id=ROOT), out) == ()
    scan = intent(EffectKind.SCAN_TREE, root_id=ROOT)
    scanned = await rig.adapter.execute(scan, CTX)
    assert isinstance(scanned, Ack)
    (tq,) = observations(scan, scanned)
    assert tq == ev.TreeQuiescent(session_id="S1", complete=True, busy=False, pending_waiter=True)
    port_scan = await rig.adapter.scan_tree(ROOT)
    assert port_scan.complete and "conv_p" in port_scan.node_ids


async def test_failed_interrupt_is_reported_not_hidden(git_env) -> None:
    from tests.omnigent.support import make_rig

    rig = make_rig(git_env)
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT, status="running"))
    rig.server.faults[("POST", f"/v1/sessions/{ROOT}/events")].append("timeout-before")
    out = await rig.adapter.execute(intent(EffectKind.INTERRUPT_TREE, root_id=ROOT), CTX)
    assert isinstance(out, Ack) and out.detail["failed"] == [ROOT]


# ------------------------------------------------------------------ stream observation


def _norm() -> StreamNormalizer:
    return StreamNormalizer(session_id="S1", root_id=ROOT)


async def test_ask_distinction_cost_vs_policy_vs_agent_question() -> None:
    n = _norm()
    cost = n.on_event(ROOT, elicitation("e_cost", policy_name="factory-cost-grant-0002"))
    gh = n.on_event("conv_c", elicitation("e_gh", policy_name="factory-github"))
    ask = n.on_event("conv_c", elicitation("e_q"))
    unknown = DecisionImpact.UNKNOWN
    assert cost == [
        ev.ElicitationOpened(
            "S1", "e_cost", unknown, True, "factory-cost-grant-0002: Question?", ROOT
        )
    ]
    assert gh == [
        ev.ElicitationOpened("S1", "e_gh", unknown, False, "factory-github: Question?", "conv_c")
    ]
    assert ask == [ev.ElicitationOpened("S1", "e_q", unknown, False, "Question?", "conv_c")]
    # "waiting" status alone is async work, not a question.
    assert n.on_event(ROOT, {"type": "session.status", "status": "waiting"}) == [
        ev.RuntimeActivity(session_id="S1", busy=False)
    ]


async def test_no_replay_after_reconnect_snapshot() -> None:
    n = _norm()
    n.seen_elicitations.add("e_persisted")  # seeded from persisted decisions after restart
    assert n.on_event(ROOT, elicitation("e_persisted")) == []
    first = n.on_event(ROOT, elicitation("e_live"))
    assert len(first) == 1
    node = NodeState(
        ROOT, None, "waiting", elicitations=(elicitation("e_live"), elicitation("e_new"))
    )
    snap = TreeObservation(ROOT, True, {ROOT: node})
    out = n.on_snapshot(snap, open_elicitations={"e_live"})
    assert out == [
        ev.ElicitationOpened("S1", "e_new", DecisionImpact.UNKNOWN, False, "Question?", ROOT)
    ]
    assert n.on_snapshot(snap, open_elicitations={"e_live", "e_new"}) == []


async def test_missing_prompt_only_from_complete_snapshot() -> None:
    n = _norm()
    empty_incomplete = TreeObservation(ROOT, False, {ROOT: NodeState(ROOT, None, "idle")})
    assert n.on_snapshot(empty_incomplete, open_elicitations={"e_lost"}) == []
    empty = TreeObservation(ROOT, True, {ROOT: NodeState(ROOT, None, "idle")})
    assert n.on_snapshot(empty, open_elicitations={"e_lost"}) == [
        ev.ElicitationGone(session_id="S1", elicitation_id="e_lost")
    ]


async def test_resolution_correlation_and_owner_direct_messages() -> None:
    n = _norm()
    n.own_resolved.add("e_ours")
    n.own_item_ids.add("msg_ours")
    n.own_effect_ids.add("ef_7")
    assert n.on_event(
        ROOT,
        {"type": "response.elicitation_resolved", "elicitation_id": "e_ours", "action": "accept"},
    ) == [ev.ElicitationResolved("S1", "e_ours", True)]
    assert n.on_event(
        ROOT,
        {"type": "response.elicitation_resolved", "elicitation_id": "e_ui", "action": "accept"},
    ) == [ev.ElicitationResolved("S1", "e_ui", False)]

    def consumed(item_id: str, text: str) -> dict[str, object]:
        return {
            "type": "session.input.consumed",
            "data": {
                "item_id": item_id,
                "type": "message",
                "data": {"role": "user", "content": [{"type": "input_text", "text": text}]},
            },
        }

    assert n.on_event(ROOT, consumed("msg_ours", "x")) == []
    assert n.on_event(ROOT, consumed("msg_native", "brief\n\n[factory-effect ef_7]")) == []
    assert n.on_event(ROOT, consumed("msg_forged", "go [factory-effect ef_unknown]")) == [
        ev.OwnerDirectOmnigentMessage(session_id="S1", item_id="msg_forged")
    ]
    assert n.on_event(ROOT, consumed("msg_owner", "hey molly")) == [
        ev.OwnerDirectOmnigentMessage(session_id="S1", item_id="msg_owner")
    ]


async def test_drain_stream_parses_sse_and_reports_gap() -> None:
    events = [
        {"type": "session.status", "conversation_id": ROOT, "status": "running"},
        elicitation("e_s"),
        {
            "type": "session.child_session.updated",
            "conversation_id": ROOT,
            "child_session_id": "conv_new",
            "child": {},
        },
        {"type": "response.output_text.delta", "delta": "hi"},
    ]
    body = "".join(f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/v1/sessions/{ROOT}/stream"
        return httpx.Response(
            200,
            text=body + "event: done\ndata: garbage{\n\n",
            headers={"content-type": "text/event-stream"},
        )

    rest = OmnigentRest("http://o.test", transport=httpx.MockTransport(handler))
    n = _norm()
    result = await drain_stream(rest, ROOT, n)
    assert result.gap  # EOF: reconnect + snapshot before trusting state
    assert result.events == (
        ev.RuntimeActivity(session_id="S1", busy=True),
        ev.ElicitationOpened("S1", "e_s", DecisionImpact.UNKNOWN, False, "Question?", ROOT),
    )
    assert n.new_nodes == {"conv_new"}

    def broken(request: httpx.Request) -> httpx.Response:
        raise httpx.RemoteProtocolError("reset", request=request)

    gap = await drain_stream(
        OmnigentRest("http://o.test", transport=httpx.MockTransport(broken)), ROOT, _norm()
    )
    assert gap.gap and gap.events == ()


# ------------------------------------------------------------------ active time

MIN = 60_000_000


async def test_union_time_for_parallel_children_and_parked_root() -> None:
    clock = FakeClock()
    t = ActivityTracker(clock, "S1", "gr_1")
    t.observe_status("root", "running", parked=True)  # parked on owner prompt: no time
    t.observe_status("a", "running")
    t.observe_status("b", "running")
    clock.advance(10 * MIN)
    t.observe_status("a", "idle")
    clock.advance(5 * MIN)
    t.observe_status("b", "idle")
    clock.advance(30 * MIN)  # everyone idle / owner wait: nothing accrues
    est = t.estimate()
    assert est.lower_us == est.upper_us == 15 * MIN  # union, not 25 min summed
    assert t.sample() == ev.ActiveTimeSample(session_id="S1", grant_id="gr_1", consumed_us=15 * MIN)


async def test_idle_root_running_child_counts_and_launching_is_upper_only() -> None:
    clock = FakeClock()
    t = ActivityTracker(clock, "S1", "gr_1")
    obs = TreeObservation(
        ROOT,
        True,
        {
            ROOT: NodeState(ROOT, None, "idle"),
            "c": NodeState("c", ROOT, "running"),
            "l": NodeState("l", ROOT, "launching"),
        },
    )
    t.observe_tree(obs)
    clock.advance(4 * MIN)
    t.observe_tree(
        TreeObservation(
            ROOT,
            True,
            {
                ROOT: NodeState(ROOT, None, "idle"),
                "c": NodeState("c", ROOT, "idle"),
                "l": NodeState("l", ROOT, "running"),
            },
        )
    )
    clock.advance(2 * MIN)
    est = t.estimate()
    assert est.lower_us == 6 * MIN - 0 and est.upper_us == 6 * MIN
    t2 = ActivityTracker(clock, "S1", "gr_1")
    t2.observe_tree(TreeObservation(ROOT, True, {"l": NodeState("l", ROOT, "launching")}))
    clock.advance(MIN)
    assert t2.estimate().lower_us == 0 and t2.estimate().upper_us == MIN


async def test_stream_gap_and_restart_widen_only_the_upper_bound() -> None:
    clock = FakeClock()
    t = ActivityTracker(clock, "S1", "gr_1", baseline_us=20 * MIN)  # persisted consumption
    t.gap_started()
    clock.advance(3 * MIN)
    t.gap_ended()
    t.restart_outage(2 * MIN, tree_possibly_active=True)
    t.restart_outage(9 * MIN, tree_possibly_active=False)
    est = t.estimate()
    assert est.lower_us == 20 * MIN
    assert est.upper_us == 20 * MIN + 3 * MIN  # outage overlaps the gap window: union
    assert t.sample().consumed_us == est.upper_us  # conservative sample


async def test_elicitation_summary_is_redacted_and_bounded() -> None:
    from omnigent_factory.omnigent.observe import elicitation_summary

    text = elicitation_summary(
        {
            "policy_name": "claude_sdk_permission",
            "message": "Claude wants to use **Bash**",
            "content_preview": 'Bash({"command": "curl -H \'Authorization: Bearer abcdefghijkl\' '
            + "x" * 500
            + ' ghp_abcdefghijklmnopqrstuv"})',
        }
    )
    assert text.startswith("claude_sdk_permission: Claude wants to use **Bash** (Bash(")
    assert "abcdefghijkl" not in text and "ghp_" not in text and "[redacted]" in text
    assert len(text) <= 300
