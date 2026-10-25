"""Harness permission prompts: readable, debounced, and seen within seconds (#1394).

A worker's harness asking to run a tool (a Claude/Codex/... native permission card) used
to be relayed as an owner question whose comment dumped the raw prompt summary
(``claude_native_permission: Claude wants to call **Bash** (Bash({...json...``), posted
even for prompts that cleared within seconds, and was noticed only when a full observer
pass finished (8 minutes late on #1394). Now the comment is one readable line plus the
command or path, posted only once the prompt has been open for
``permission_prompt_grace_seconds``; a root-snapshot probe finds new prompts between
full passes.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from omnigent_factory.core import codec
from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Event
from omnigent_factory.core.types import BotState, DecisionImpact, DecisionStatus
from omnigent_factory.omnigent.observe import (
    StreamNormalizer,
    is_permission_prompt,
    permission_ask,
)
from omnigent_factory.omnigent.rest import OmnigentRest
from omnigent_factory.omnigent.tree import NodeState, TreeObservation, root_prompts, scan_tree
from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig
from omnigent_factory.service.directory import _decision_text, safe_publication
from omnigent_factory.service.observer import PROMPT_PROBE_INTERVAL_SECONDS, OmnigentObserver
from omnigent_factory.service.status_comment import Question, StatusComments
from omnigent_factory.testing.builders import REPO_ID
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness
from tests.omnigent.fake_server import FakeOmnigentServer, FakeSession, elicitation

P = "I_parcel_1"
SECOND = 1_000_000
#: The #1394 command: a heredoc whose first line says nothing about what it edits.
COMMAND = (
    "python3 - <<'EOF'\n"
    "p='src/scripts/reference-seed.ts'\n"
    "s=open(p).read()\n"
    'old_start=s.index("// Insert-only, in one transaction (API-FR-IN-006).")\n'
    "EOF"
)


def _claude_bash(eid: str, command: str = COMMAND, description: str = "") -> dict[str, Any]:
    """The shape Omnigent's Claude PermissionRequest hook publishes (routes_hooks.py)."""
    tool_input: dict[str, Any] = {"command": command}
    if description:
        tool_input["description"] = description
    return {
        "type": "response.elicitation_request",
        "elicitation_id": eid,
        "method": "elicitation/create",
        "params": {
            "mode": "form",
            "message": "Claude wants to call **Bash**",
            "requestedSchema": None,
            "phase": "pre_tool_use",
            "policy_name": "claude_native_permission",
            # Capped at 1024 characters upstream: real previews are often cut mid-JSON.
            "content_preview": f"Bash({json.dumps(tool_input)})"[:180],
            "tool_name": "Bash",
            "cwd": "/w",
            "permission_mode": "default",
        },
    }


# ------------------------------------------------------------------ rendering


def test_a_claude_bash_prompt_reads_as_one_line_plus_the_command() -> None:
    ask = permission_ask(_claude_bash("e1")["params"])
    assert ask is not None
    assert ask.summary == (
        "A worker is asking permission to run a Bash command that touches "
        "`src/scripts/reference-seed.ts`."
    )
    assert ask.detail == "Command: `python3 - <<'EOF'` (first line of several)"
    text = _decision_text(
        {"permission": True, "summary": ask.summary, "detail": ask.detail, "root_id": "r1"},
        "https://omnigent.example",
    )
    assert text == (
        "A worker is asking permission to run a Bash command that touches "
        "`src/scripts/reference-seed.ts`. Approve or deny it in "
        "[Omnigent](https://omnigent.example/c/r1).\n\n"
        "Command: `python3 - <<'EOF'` (first line of several)"
    )
    for raw in ("claude_native_permission", "{", '"command"', "**Bash**"):
        assert raw not in text


def test_the_normalizer_opens_a_permission_prompt_with_the_readable_summary() -> None:
    [body] = StreamNormalizer("ss_1", "root").on_event("root", _claude_bash("e1"))
    assert isinstance(body, ev.ElicitationOpened)
    assert body.permission and not body.cost_ask
    assert body.summary.startswith("A worker is asking permission to run a Bash command")
    assert "claude_native_permission" not in body.summary


def test_credentials_are_redacted_and_mentions_neutralised(
    service_config: ServiceConfig,
) -> None:
    ask = permission_ask(
        _claude_bash(
            "e1",
            command="curl -H 'Authorization: Bearer abcdefghijklmnop' https://x.test",
            description="ping @octocat with token=hunter2hunter2",
        )["params"]
    )
    assert ask is not None
    text = safe_publication(
        _decision_text({"permission": True, "summary": ask.summary, "detail": ask.detail}),
        service_config,
    )
    assert "abcdefghijklmnop" not in text and "hunter2" not in text
    assert "[redacted]" in text
    assert "@octocat" not in text and "@\u200boctocat" in text
    assert text.startswith("A worker is asking permission to run a Bash command.")


@pytest.mark.parametrize(
    ("params", "summary"),
    [
        (
            {
                "policy_name": "claude_native_permission",
                "message": "Claude wants to call **Edit**",
                "tool_name": "Edit",
                "content_preview": 'Edit({"file_path": "/w/src/app.ts", "old_string": "a"',
            },
            "A worker is asking permission to use Edit on `/w/src/app.ts`.",
        ),
        (
            {
                "policy_name": "codex_native_command_approval",
                "message": "Codex wants to run **rm -rf build**",
                "command": "rm -rf build",
                "reason": "clean the build",
                "content_preview": '{"command": ["rm", "-rf", "build"]}',
            },
            "A worker is asking permission to run a command.",
        ),
        (
            {
                "policy_name": "codex_native_apply_patch_approval",
                "message": "Codex wants to apply a patch",
                "files": ["src/a.py", "src/b.py"],
            },
            "A worker is asking permission to apply a patch to `src/a.py`, `src/b.py`.",
        ),
        (
            {
                "policy_name": "claude_sdk_permission",
                "message": "Claude wants to use **WebFetch**",
                "content_preview": 'WebFetch({"url": "https://example.com/x?token=abc"})',
            },
            "A worker is asking permission to use WebFetch on `example.com`.",
        ),
        (
            # Identified by the hook phase and tool, whatever the policy is named.
            {"phase": "pre_tool_use", "tool_name": "Write", "message": "Kiro wants **Write**"},
            "A worker is asking permission to use Write.",
        ),
    ],
)
def test_other_harness_permission_prompts_are_recognised(
    params: dict[str, Any], summary: str
) -> None:
    ask = permission_ask(params)
    assert ask is not None and ask.summary == summary


@pytest.mark.parametrize(
    "params",
    [
        # A real question for the owner, even when it rides a permission card.
        {"policy_name": "claude_native_permission", "tool_name": "AskUserQuestion"},
        {"policy_name": "codex_native_request_user_input", "message": "Which one?"},
        {"policy_name": "codex_native_mcp_elicitation", "message": "Pick a repo"},
        {"policy_name": "agy_native_ask_question", "message": "Which?"},
        # A policy-engine ASK and an agent question.
        {"policy_name": "github_write_ask", "phase": "tool_call", "message": "Push?"},
        {"message": "Which export format?"},
        elicitation("e1", policy_name="factory-cost-grant-abc")["params"],
    ],
)
def test_questions_and_policy_asks_are_not_permission_prompts(params: dict[str, Any]) -> None:
    assert not is_permission_prompt(params)
    assert permission_ask(params) is None


def test_owner_questions_keep_their_rendering() -> None:
    assert _decision_text({"summary": "Which export format?"}) == "Which export format?"
    assert _decision_text({}) == "I have a question before I can go on."


# ------------------------------------------------------------------ reducer


def _opened(h: Harness, *, permission: bool, eid: str = "e1") -> Any:
    return h.send(
        P,
        ev.ElicitationOpened(
            session_id=h.cur().session_id,
            elicitation_id=eid,
            impact=DecisionImpact.UNKNOWN,
            summary="A worker is asking permission to use Edit on `a.ts`.",
            permission=permission,
            detail="Why: tidy",
        ),
    )


def _decision_comments(result: Any) -> list[Any]:
    return [
        e for e in Harness.of(result, EffectKind.POST_COMMENT) if e.args["template"] == "decision"
    ]


def test_a_permission_prompt_is_needs_you_at_once_but_commented_only_when_due() -> None:
    h = Harness()
    h.to_building()
    opened = _opened(h, permission=True)
    assert opened.audit.accepted and not _decision_comments(opened)
    p = h.p()
    assert p.bot == BotState.NEEDS_YOU
    [d] = p.open_decisions
    assert d.comment_deferred and d.opened_at_us > 0
    due = h.send(P, ev.PermissionPromptDue(h.cur().session_id, "e1"))
    [comment] = _decision_comments(due)
    assert comment.args["permission"] is True
    assert comment.args["summary"] == "A worker is asking permission to use Edit on `a.ts`."
    assert comment.args["detail"] == "Why: tidy"
    assert not h.p().decisions[0].comment_deferred
    again = h.send(P, ev.PermissionPromptDue(h.cur().session_id, "e1"))
    assert not again.audit.accepted and not _decision_comments(again)


def test_a_permission_prompt_cleared_within_the_grace_is_never_commented() -> None:
    h = Harness()
    h.to_building()
    _opened(h, permission=True)
    h.send(P, ev.ElicitationGone(h.cur().session_id, "e1"))
    assert h.p().decisions[0].status == DecisionStatus.RESOLVED_IN_OMNIGENT
    assert h.p().bot != BotState.NEEDS_YOU
    late = h.send(P, ev.PermissionPromptDue(h.cur().session_id, "e1"))
    assert not late.audit.accepted and not _decision_comments(late)


def test_other_prompts_are_still_commented_at_once() -> None:
    h = Harness()
    h.to_building()
    [comment] = _decision_comments(_opened(h, permission=False))
    assert "permission" not in comment.args
    assert not h.p().decisions[0].comment_deferred


def test_a_deferred_decision_survives_the_store_codec() -> None:
    h = Harness()
    h.to_building()
    _opened(h, permission=True)
    [d] = codec.parcel_from_json(codec.parcel_to_json(h.p())).decisions
    assert d == h.p().decisions[0] and d.comment_deferred and d.summary and d.detail


# ------------------------------------------------------------------ observer


class _Service:
    """The real reducer behind the observer; event IDs are durable no-ops when repeated,
    as in the store."""

    def __init__(self, h: Harness, grace_seconds: float = 60.0) -> None:
        self.h = h
        self.config = SimpleNamespace(
            repo_id=REPO_ID,
            permission_prompt_grace_seconds=grace_seconds,
            background_failure_limit=10,
            background_error_backoff_seconds=0.01,
        )
        self.events: list[Event] = []
        self.seen: set[str] = set()
        self.db = self

    async def call(self, operation: Any) -> Any:
        return self.h.p(P)

    async def open_run_parcel_ids(self) -> list[str]:
        return [P]

    async def apply_event(self, event: Event, **_: object) -> Any:
        if event.event_id in self.seen:
            return None
        self.seen.add(event.event_id)
        self.events.append(event)
        return self.h.apply(event)

    def comments(self) -> list[Any]:
        """Decision comments the reducer emitted, from every accepted event."""
        return [
            e
            for event, result in self.h.log
            if result is not None
            for e in Harness.of(result, EffectKind.POST_COMMENT)
            if e.args["template"] == "decision"
        ]


class _Omnigent:
    """One build tree: the root holding the prompt and a busy worker (as on #1394)."""

    def __init__(self, root: str) -> None:
        self.root = root
        self.prompts: dict[str, dict[str, Any]] = {}
        self.scans = 0
        self.probes = 0
        self.stuck: asyncio.Event | None = None

    async def observe_trees(self, roots: list[str]) -> list[TreeObservation]:
        self.scans += 1
        if self.stuck is not None:
            await self.stuck.wait()  # a pass stuck on slow reads elsewhere
        root = NodeState(self.root, None, "running", elicitations=tuple(self.prompts.values()))
        worker = NodeState("worker", self.root, "running")
        return [TreeObservation(self.root, True, {self.root: root, "worker": worker})]

    async def pending_prompts(self, root_id: str) -> dict[str, Any]:
        self.probes += 1
        return root_prompts(root_id, {"pending_elicitations": list(self.prompts.values())})


def _rig(grace_seconds: float = 60.0) -> tuple[Harness, _Service, _Omnigent, FakeClock]:
    h = Harness()
    h.to_building()
    root = h.cur().root_id
    assert root is not None
    clock = FakeClock(start_us=h.f(P).now + SECOND)
    return h, _Service(h, grace_seconds), _Omnigent(root), clock


def _observer(service: _Service, omnigent: _Omnigent, clock: FakeClock) -> OmnigentObserver:
    return OmnigentObserver(
        service,  # type: ignore[arg-type]
        omnigent,  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        clock,
        interval_seconds=5,
        prompt_probe_interval_seconds=0.01,
    )


@pytest.mark.asyncio
async def test_the_comment_waits_for_the_grace_then_is_posted_once() -> None:
    h, service, omnigent, clock = _rig()
    observer = _observer(service, omnigent, clock)
    omnigent.prompts["e1"] = _claude_bash("e1")
    await observer.observe_once()
    assert h.p().bot == BotState.NEEDS_YOU and h.p().open_decisions
    assert service.comments() == []
    clock.advance(59 * SECOND)
    await observer.observe_once()
    await observer.probe_prompts_once()
    assert service.comments() == []
    clock.advance(1 * SECOND)
    await observer.probe_prompts_once()
    [comment] = service.comments()
    assert comment.args["summary"].startswith("A worker is asking permission to run a Bash")
    for _ in range(3):
        clock.advance(30 * SECOND)
        await observer.observe_once()
        await observer.probe_prompts_once()
    assert len(service.comments()) == 1


@pytest.mark.asyncio
async def test_a_prompt_cleared_within_the_grace_posts_nothing() -> None:
    h, service, omnigent, clock = _rig()
    observer = _observer(service, omnigent, clock)
    omnigent.prompts["e1"] = _claude_bash("e1")
    await observer.observe_once()
    clock.advance(20 * SECOND)
    omnigent.prompts.clear()  # approved in Omnigent after 20 s
    await observer.probe_prompts_once()  # a probe never decides a prompt is gone
    assert h.p().open_decisions
    await observer.observe_once()
    assert not h.p().open_decisions and h.p().bot == BotState.WORKING
    clock.advance(120 * SECOND)
    await observer.observe_once()
    await observer.probe_prompts_once()
    assert service.comments() == []


@pytest.mark.asyncio
async def test_a_restart_neither_duplicates_nor_loses_the_comment() -> None:
    h, service, omnigent, clock = _rig()
    omnigent.prompts["e1"] = _claude_bash("e1")
    await _observer(service, omnigent, clock).observe_once()
    clock.advance(30 * SECOND)
    # Restart inside the grace: the new process has no memory of the prompt.
    clock.reboot()
    await _observer(service, omnigent, clock).observe_once()
    assert service.comments() == [] and len(h.p().decisions) == 1
    clock.advance(31 * SECOND)
    # Restart again past the grace with the prompt still open: posted, once.
    clock.reboot()
    await _observer(service, omnigent, clock).probe_prompts_once()
    assert len(service.comments()) == 1
    clock.reboot()
    restarted = _observer(service, omnigent, clock)
    await restarted.observe_once()
    await restarted.probe_prompts_once()
    assert len(service.comments()) == 1


@pytest.mark.asyncio
async def test_grace_zero_comments_on_the_pass_that_sees_the_prompt_and_reload_applies() -> None:
    _h, service, omnigent, clock = _rig(grace_seconds=0)
    observer = _observer(service, omnigent, clock)
    omnigent.prompts["e1"] = _claude_bash("e1")
    await observer.observe_once()
    assert len(service.comments()) == 1
    service.config.permission_prompt_grace_seconds = 300  # `reload` swaps the config
    omnigent.prompts["e2"] = _claude_bash("e2")
    clock.advance(120 * SECOND)
    await observer.observe_once()
    clock.advance(120 * SECOND)
    await observer.probe_prompts_once()
    assert len(service.comments()) == 1  # e2 is inside the reloaded grace


@pytest.mark.asyncio
async def test_a_prompt_is_seen_while_a_full_pass_is_stuck() -> None:
    """#1394: the full pass scans every due tree, node by node (15 s per timed-out read),
    before it applies anything; the probe reads the root snapshot only."""
    h, service, omnigent, clock = _rig(grace_seconds=0)
    await _observer(service, omnigent, clock).observe_once()  # tree seen busy, no prompt
    omnigent.stuck = asyncio.Event()
    observer = _observer(service, omnigent, clock)
    await observer.start()
    try:
        await asyncio.sleep(0.05)  # the pass is stuck reading
        omnigent.prompts["e1"] = _claude_bash("e1")
        async with asyncio.timeout(2):
            while not service.comments():
                await asyncio.sleep(0.01)
        assert h.p().bot == BotState.NEEDS_YOU
        assert omnigent.scans == 2  # the stuck pass has not finished
    finally:
        omnigent.stuck.set()
        await observer.close()


@pytest.mark.asyncio
async def test_a_pass_read_before_the_probe_saw_the_prompt_does_not_close_it() -> None:
    """The pass read the tree before the prompt opened; the probe opened it meanwhile."""
    h, service, omnigent, clock = _rig()
    observer = _observer(service, omnigent, clock)
    await observer.observe_once()
    omnigent.stuck = asyncio.Event()
    clock.advance(5 * SECOND)
    stale_pass = asyncio.create_task(observer.observe_once())
    await asyncio.sleep(0.01)  # its tree read (no prompt yet) is in flight
    omnigent.prompts["e1"] = _claude_bash("e1")
    clock.advance(1 * SECOND)
    await observer.probe_prompts_once()
    assert h.p().open_decisions
    omnigent.prompts.clear()  # the stale read the pass returns has no prompt...
    omnigent.stuck.set()
    await stale_pass
    assert h.p().open_decisions  # ...and it proves nothing about a newer prompt
    clock.advance(5 * SECOND)
    await observer.observe_once()  # a scan started after it opened does
    assert not h.p().open_decisions


@pytest.mark.asyncio
async def test_the_probe_skips_parked_and_settled_runs() -> None:
    h, service, omnigent, clock = _rig()
    observer = _observer(service, omnigent, clock)
    await observer.observe_once()
    assert await observer.probe_prompts_once() == {h.cur().session_id}
    parcel = h.p()
    session = parcel.current_session
    assert session is not None
    observer._last_tree[session.session_id] = (True, False, False)  # seen idle
    h.parcels[P] = replace(parcel, slot_parked=True)
    assert await observer.probe_prompts_once() == frozenset()


@pytest.mark.asyncio
async def test_the_probe_costs_one_request_per_active_tree() -> None:
    """Simulated request delta on a #1394-sized tree (root plus 40 workers, a prompt on
    one of them): the probe is one snapshot read, a full scan reads every node."""
    server = FakeOmnigentServer()
    server.add(FakeSession(id="root", agent_id="ag"))
    for i in range(40):
        server.add(FakeSession(id=f"w{i:02d}", agent_id="ag", parent_session_id="root"))
    server.sessions["w07"].pending_elicitations.append(_claude_bash("e1"))
    rest = OmnigentRest("http://o.test", transport=server.transport(), page_limit=100)
    before = len(server.requests)
    prompts = root_prompts("root", await rest.get_json("/v1/sessions/root"))
    probe = len(server.requests) - before
    assert probe == 1 and prompts["e1"][0] == "w07"
    before = len(server.requests)
    obs = await scan_tree(rest, "root")
    full = len(server.requests) - before
    assert "e1" in obs.owned_elicitations()
    assert full >= 80  # child lists and a snapshot per node, plus the inventory
    # Every 15 s, against a full pass every 5 s plus its duration: +1 request per
    # 3 full-scan passes of ~80 requests (~0.4%).
    assert PROMPT_PROBE_INTERVAL_SECONDS == 15.0


def test_the_grace_is_hot_reloadable_with_a_one_minute_default() -> None:
    field = ServiceConfig.model_fields["permission_prompt_grace_seconds"]
    assert field.default == 60.0
    assert "permission_prompt_grace_seconds" in HOT_RELOAD_KEYS


@pytest.mark.asyncio
async def test_the_status_comment_shows_the_ask_while_its_comment_waits() -> None:
    """Needs you on the card, and the status comment (edited, not a notification) says
    what is asked, before the grace has passed."""
    h = Harness()
    h.to_building()
    _opened(h, permission=True)

    class _NoEffects:
        async def call(self, operation: Any) -> list[Any]:
            return []  # no decision comment posted yet

    status = StatusComments(
        SimpleNamespace(db=_NoEffects(), config=SimpleNamespace(repository="o/r")),  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        str,
    )
    assert await status._question(h.p()) == Question(
        "A worker is asking permission to use Edit on `a.ts`."
    )
