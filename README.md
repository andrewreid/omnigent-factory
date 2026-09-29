# omnigent-factory

A GitHub-issue and GitHub Projects driven "agentic software factory" built on
[Omnigent](https://github.com/omnigent-ai/omnigent).

Issues move left to right across a project board (Inbox → Triaged → Scoped →
Building → Ready). A human drag authorises each stage; a deterministic daemon
receives the GitHub App's webhooks and starts Omnigent agent sessions that triage,
plan and build. A human always merges and closes.

Status: under construction.

## Development

Requires [uv](https://docs.astral.sh/uv/) and Python 3.13 (pinned in `.python-version`).

```sh
uv sync --locked              # create .venv from uv.lock
uv run pytest -q              # full test suite (no network)
uv run ruff check             # lint
uv run ruff format --check    # formatting
uv run mypy                   # strict type check of src/omnigent_factory
```

Hypothesis runs the `default` profile (200 examples, 40 stateful steps, derandomized).
For a longer randomized search: `HYPOTHESIS_PROFILE=thorough uv run pytest -q tests/test_invariants_model.py`.

### Layout

| Path | Contents |
| --- | --- |
| `src/omnigent_factory/core/` | Pure state kernel, no I/O: domain types (`types.py`), event catalog (`events.py`), effect intents and adapter outcomes (`effects.py`), the reducer `transition(State, Event)` (`reducer.py`), authorization predicates (`predicates.py`), effect precondition re-check (`preconditions.py`), board projection and admission (`projection.py`), contract/waiver canonicalization and hashing (`canonical.py`), the stage-result schema v1 and its validation (`protocol.py`, `result_schema_v1.json`), active-time union (`accounting.py`) and the JSON codec (`codec.py`). |
| `src/omnigent_factory/store/` | SQLite store: checksummed migrations (`migrations.py`) and `SqliteStore` (`sqlite.py`) with the durable delivery inbox, one-transaction event application, outbox of effect intents with dispatch nonces, parcel leases, WIP reservations and audit. |
| `src/omnigent_factory/ports/` | Adapter protocols only (GitHub, Omnigent, credential broker, scheduler, clock). |
| `src/omnigent_factory/testing/` | In-memory fakes and a fake clock, fixture builders, and a multi-parcel reducer harness with scripted flows. |
| `src/omnigent_factory/cli.py` | `omnigent-factory` entry point (currently `version` and `db init`). |
| `tests/` | Canonicalization goldens, protocol parsing, every transition-table row and rejecting default, Hypothesis stateful invariants, named traces, and store migration/crash/lease/cap tests. |

## Operations

The daemon runs as the systemd user unit `omnigent-factory`. Every operator command
talks to it over its local socket and takes `--config <path>` (omitted below). The socket
opens once startup reconcile finishes (tens of seconds with live sessions); the CLI
waits up to 90 s for it instead of failing.

| Command | What it does |
|---|---|
| `status` | Readiness, pause state, building count/cap, queue, pending/unknown effects, parked deliveries. |
| `doctor` | Checks config, secrets, GitHub/Omnigent reachability, Omnigent login expiry (fails when expired, warns within 7 days) and server/client version drift (warning only). `doctor --live` (opt-in) also creates a throwaway session for the configured agent in the configured project and archives it, which catches server-side create failures such as unresolved agent env vars. |
| `explain <parcel>` | The parcel's persisted state: stage, bot, sessions, holds, effects. |
| `recovery` | Failed/unknown effects and parked webhook deliveries. |
| `retry-effect <effect_id>` | Requeue a failed/unknown `publish_triage`/`publish_report`/`post_comment`; it adopts an existing comment by its marker, so it never duplicates. |
| `rerender-comment <effect_id>` | Re-render a published comment in place (found by its marker). |
| `resume <parcel> --message-file note.md` | Re-open the parcel's existing stage session after a stale block and relay one operator note. No authority, approval or time is added. |
| `cleanup <parcel> [--merged]` | Remove a finished parcel's factory worktree(s) and local `factory/` branch from the factory clone. |
| `release-delivery <guid>` | Release one parked webhook delivery for processing. |
| `pause` / `unpause` | Stop / resume admitting new work repository-wide; in-flight parcels and safety events carry on. |
| `reload` | Re-read the config file into the running daemon (also `SIGHUP`). Applies only `max_building`, `max_open_bot_prs`, checkpoint settings, cost backstop, `review_bot_grace_minutes`, guidance and `independent_reviewer_ids`; any other change is refused with `restart required: <keys>` and an invalid file changes nothing. Lowering a cap never stops running builds. |

The host config file (`~/.config/omnigent-factory/config.toml`) is the single source
of factory configuration; the target repository carries no factory config file.

A triage/report/status publication that failed definitively leaves the card at
`Bot: Blocked`. Fix the cause, then run `recovery` and `retry-effect <effect_id>`.

### Card status vs. comments

Bot comments notify the owner, so the factory comments only when he must read or act:
triage, plan, Ready report (again after a rework), agent questions, checkpoint
(`/continue`) and blocks that need owner action (agent blocked, restart exhausted, create
refused, stop unverified, ambiguous adoption, PR not Ready with no fix attempt left). Everything else (queued, stopped,
approval acknowledged/invalidated, Ready withdrawn, rework, PR closed unmerged, refused
commands, invalid result, ...) is the card's `Factory note` text field: one line with the latest
reason, written only when it changes and cleared when the card moves on. Blocked,
Checkpoint, Needs you and Queued cards without a specific reason get a derived note.
`Bot: Queued` marks an approved build waiting for capacity; its note (`Queued: 2nd in line`)
follows the queue as builds ahead are admitted (refreshed by the periodic reconcile). Owner command comments get a
reaction: 👍 accepted, 😕 refused (the reason is in the note). Config keys:
`note_field_node_id` and `bot_options.Queued`; `doctor` checks both exist.

### Issue sessions and the factory MCP endpoint

Each issue gets one Omnigent session (the configured `omnigent_agent_id`, default
`rosie`), titled `#<n> · <issue title>`. Triage, plan and build are successive stage runs
in that session; each run has its own credential capability, work gate and policies, and
a stop or revoke ends the run, never the session. A dead, archived, foreign-agent or
context-full session is replaced before the next run (the new one gets a short stored
summary in its start message). When the parcel is terminal (merged or closed) and its
tree is quiescent, the session is archived; a session replaced by a fresh one is archived
too. When the issue title changes, the live session is renamed.

Stage messages are short pointers; the agent works through six MCP tools served by the
daemon at `http://127.0.0.1:<mcp_port>/mcp/` (loopback only, bearer token):
`factory_get_issue`, `factory_get_plan`, `factory_get_feedback`, `factory_get_status`,
`factory_ask_owner` and `factory_submit_result`. Each takes the caller's own Omnigent
`session_id`; a per-session CEL policy (`factory-caller@…`) denies factory tool calls
carrying any other id, and the daemon resolves issue, run and stage from its own store.
Create the token once with `omnigent-factory setup mcp-token` (written to
`<secrets_dir>/mcp-token`, mode 0600, never printed) and give it to the agent host as
`FACTORY_MCP_TOKEN`; `doctor` checks the token file, the loopback listener, the agent
and a dry compile of the identity policy. The factory clone's `info/exclude` ignores
`/.molly/`.

### Steering by comment

Every plain (non-command) comment from an owner is recorded, and so is, on the parcel's
PR, every owner conversation comment and every owner review that says something (request
changes, comment, or an approval with text; inline comments are read from GitHub when the
review arrives). `factory_get_feedback` serves all of them to every later run, oldest
first, flagged `new` since the session's last result for that stage. Commands work on the
issue only; reactions and other people's comments never count. In Triaged a comment
re-runs triage in the issue session (a revised triage comment follows); in Scoped it
revises the plan; in Building it is relayed to a build waiting on checks without touching
the approval. Elsewhere it only waits for later stages. A run whose turn ended without a result (its tree is idle) gets the comment
as one message, even if it reported blocked; a comment that arrives mid-turn is relayed when
that turn ends without a result (periodic tree scans can be incomplete, so this is the
fallback). Further comments wait until the run is idle again.
A run that is mid-turn gets no extra message: its result is refused until
it has read every comment, so a burst of comments folds into the run in progress.

**Rework.** Owner feedback on a Ready card (an issue comment, a PR comment or review, or
dragging the card back to Building) moves it to Building with the note `Rework: owner
feedback` and starts a new build run under the same approval: it is admitted like any
build (`Bot: Queued` while no build slot is free), runs in the same issue session (or a
fresh one with a summary), changes the same branch and PR, and gets a fresh time block and
a fresh fix budget. Feedback beyond the approved plan is asked back with
`factory_ask_owner`; to replan, drag the card to Scoped. A Building card left at Needs you
after its build finished (for example "no fix attempt left") is reworked the same way by
an owner comment. Ready is then re-evaluated as usual and a new Ready report is posted.
Not after `/stop` or once the PR is merged. A burst of feedback is one rework: later
comments fold into the queued or running rework run.

### Review bots

Every review-bot finding needs an outcome; a clean bot verdict is not required. The build
fixes in-scope findings; for a valid out-of-scope finding it opens a follow-up issue (as
the bot, so it lands in the board Inbox), replies on the thread with the disposition and
link, and resolves the thread; an advisory finding gets a reply and is resolved. It
resolves only threads it replied to: readiness counts a bot thread as handled when the
factory bot replied or a person resolved it, and a thread the bot resolved without a reply
stays open. The agent's shell policy allows `gh issue create`, thread replies and
`resolveReviewThread`, and denies closing, deleting, transferring or locking issues and PRs
(CLI, REST and GraphQL), merging and administration. A PR head is Ready only from a green
read taken `review_bot_grace_minutes` (default 10) after the build reported it, so late bot
comments are handled by the build's one readiness wake instead of pulling a Ready card back.

### Finished parcels

When a parcel's PR is merged or its issue is closed, the daemon removes its worktree and
local branch once every stage session has retired. Only paths under `worktree_root` are
touched; a worktree with uncommitted changes is kept unless the PR is merged; the local
branch is deleted only when the PR is merged or its tip is the verified Ready head.
Skipped items are logged. `cleanup` runs the same step by hand.

### Logs

One line per webhook, parcel transition, effect start/outcome and created session, with
secret redaction:

```sh
journalctl --user -u omnigent-factory -f
journalctl --user -u omnigent-factory --since "1 hour ago" | grep -E "WARNING|ERROR"
```

### Omnigent login

Set `omnigent_cli_store = "~/.omnigent/auth_tokens.json"` under `[service]`. The daemon
then uses the owner's `omnigent login` from the CLI credential store and refreshes its
1-hour access tokens itself. The login grant still ends at the server's grant lifetime
(30 days from login by default). The daemon logs a WARNING at startup within 7 days of
that and an ERROR once it has expired; `doctor` reports the same. To renew, either:

- run `omnigent login` on the host (no daemon restart needed), or
- set `OMNIGENT_GRANT_MAX_LIFETIME_DAYS` (for example `365`) in the Omnigent server's
  environment and restart the server, which extends existing grants too.

Without `omnigent_cli_store`, the daemon reads a copied bearer token from
`omnigent_token_file`; it cannot refresh and must be replaced before its `exp`.

### Deploy / upgrade

Install from a pushed commit into the service venv, restart, and check:

```sh
git archive <commit> | tar -x -C "$SRC"
cd "$SRC" && UV_PROJECT_ENVIRONMENT="$HOME/.local/share/omnigent-factory/venv" \
  uv sync --locked --no-dev --no-editable
systemctl --user restart omnigent-factory
omnigent-factory status
omnigent-factory doctor
```

State (SQLite) migrates on start. Applied results are not re-read after a restart.
