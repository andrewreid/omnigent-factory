# omnigent-factory

A GitHub-issue and GitHub Projects driven "agentic software factory" built on
[Omnigent](https://github.com/omnigent-ai/omnigent).

Issues move left to right across a project board (Inbox → Triage → Planning →
Building → Ready). The board is read and written by Status option ID and the displayed
names come from `[service.status_names]` in the host config; the persisted stage
identities of Triage and Planning stay `Triaged` and `Scoped`. A human drag authorises each stage; a deterministic daemon
receives the GitHub App's webhooks and starts Omnigent agent sessions that triage,
plan and build. A human always merges and closes.

Status: under construction.

## Using the factory on GitHub

GitHub is the control plane: the factory acts only on card drags, `factory:` labels,
slash commands and plain comments on GitHub, and only from an owner (a numeric GitHub user
ID in `owners`; logins are never matched). Anyone else's comments, labels and reviews are
ignored, the factory bot's own actions never count, and a control older than the latest
stop or safety barrier is refused. Columns below use the board's display names (Inbox,
Triage, Planning, Building, Ready, from `[service.status_names]`).

Typing in the parcel's Omnigent session is not a control: it grants no approval, time or
stage and does not answer the agent's questions. A native Omnigent prompt answered there
only closes that prompt; no approval or time is inferred. Activity in a stopped run's
session holds the next run until the session is idle (see [Card status vs. comments](#card-status-vs-comments)).

### Slash commands

Write the command as the whole comment, on the issue (a command on a PR is ignored).
Accepted commands get a 👍 reaction, refused ones 😕; most refusals also put
`Command refused: <reason>` in the card's `Factory note`. `<N>h` is a whole number of
hours, 1–12. Without one, a grant is the time block for the parcel's size:
`checkpoint_block_hours` S/M/L = 2/4/6 h by default (size from triage, else the plan;
otherwise S for triage and M for the rest).

| Command | Where | What it does |
|---|---|---|
| `/triage` | Inbox; any column but Ready after a stop | Moves the card to Triage and starts a triage run. In Triage, re-run triage with a plain comment instead; a repeat while the same triage is still starting or running is ignored (😕, no note). |
| `/plan`, `/replan` | Inbox, Triage, Planning, Building; not Ready | Starts a plan run; in Planning, `/plan` while the same plan is still unpublished is ignored (😕, no note). In Building, or with a build approved, it revokes the build and replans. |
| `/approve [hash] [for <N>h]` | Planning or Building | Approves the latest published plan and queues the build. `hash` is a prefix (≥ 12 lowercase hex) that must identify the latest plan; without it, the plan must have been posted before the command. Refused while a question is open, a revision is pending or another build holds the parcel. Re-approving a running approval only notes `Approved: build starts when capacity allows`. |
| `/continue [for <N>h]` | A card at `Bot: Checkpoint` | Grants another time block to the paused run. Refused when the run is not at a checkpoint, is stopped or revoked, or a question is still open. |
| `/stop` | Any column | Stops all work: drains every run, drops any queued build and pending start. Note `Stopped: /stop`. Resume with a new control (`/triage`, `/plan`, a drag, or a plain comment in Triage or Planning); a comment never starts a rework on a stopped card. |
| `/decide <id> <answer>` | A card with an open question | Answers the open question `<id>` with `<answer>`. On a build, `/decide` revokes the build and replans (a plain reply is taken within the approval). The id is not shown in the question comment, so a plain reply is the normal answer. |

### Labels

Adding a label is a control like the matching command. Only the owner's labelling counts,
including labels applied when the issue is opened (GitHub sends a `labeled` event for each).

| Label | Acts like |
|---|---|
| `factory:triage` | `/triage` |
| `factory:plan` | `/plan` |
| `factory:build` | Build without a plan: approves the issue's current title and body as written (default block M), from any column. Refused while a question is open, a revision is pending or a build is live. Editing the title or body later voids that approval. |

### Card drags

A rightward owner drag is a control; any leftward drag (by anyone, including a move to
Inbox or off the board) is a stop first. A refused drag into Building moves the card back
with the reason in the note.

| Drag | Effect |
|---|---|
| Inbox → Triage | Starts triage. |
| Inbox or Triage → Planning | Starts a plan. |
| Planning → Building | Approves the latest plan (as `/approve`, default block). |
| Inbox or Triage → Building | Builds without a plan (as `factory:build`). |
| Building → Ready | Accepted only if the run is closed and the PR is Ready; otherwise moved back (`Kept in Building: ...`). |
| Building → Planning | Stops and revokes the build and voids the approval; an owner drag also starts a replan. |
| Ready → Building | Stops, then reworks the build under the same approval (see [Rework](#steering-by-comment)); if rework is refused the card stays in Building, stopped, with `Rework refused: <reason>`. |
| Any other leftward move | Stop only; nothing starts. |

Assigning the issue to a person (not a bot) takes the parcel out of the factory: work
stops and controls are refused until the person is unassigned and a new control is
given. Closing, deleting or transferring the issue also stops it.

### Plain comments

A plain (non-`/`) owner comment is recorded for every later run. While the agent has an
open question, the next plain comment on the issue or its PR is the answer. Otherwise,
by column:

| Column | A plain comment |
|---|---|
| Inbox, Done | Recorded only. |
| Triage | Re-runs triage (or is read by the triage run in progress). |
| Planning | Revises the plan; voids any approval of the previous version. |
| Building | Guidance to the build within its approval: relayed to an idle run or one waiting on checks, read by a busy run before it can submit. On a finished build, or at `Needs you` with no fix attempt left, it starts a rework. |
| Ready | Starts a rework (back to Building, `Rework: owner feedback`). |
| Any, at `Bot: Checkpoint` | Recorded only; the run needs `/continue` before it reads it. |

On the parcel's PR, an owner conversation comment, or a review that requests changes,
comments, or approves with text, counts as the same plain comment (so on a Ready card it
starts a rework). Not after `/stop` or once merged. See
[Steering by comment](#steering-by-comment) for details.

### Bot and Factory note

| `Bot` | Meaning |
|---|---|
| `Working` | A run is doing work. |
| `Queued` | Approved build waiting for a slot; note `Queued: 2nd in line`. |
| `Needs you` | An open question, or a hold only the owner can clear. |
| `Checkpoint` | The run used its time block; comment `/continue`. |
| `Blocked` | Something failed, or a required check is red on Ready; the note says what. |
| `Idle` | Nothing to do; the next move is the owner's. |

`Factory note` is one line with the latest reason (refusals, stops, queue position,
rework), cleared when the card moves on.

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
| `prune [--dry-run]` | Apply history retention now (see [State database size](#state-database-size)) and print what was (or would be) removed. Runs through the daemon when it is up, else directly on the file. |
| `vacuum` | Compact the state database and switch it to incremental auto_vacuum. Refuses while the daemon runs (it holds the write lock for the whole rebuild). |
| `reload` | Re-read the config file into the running daemon (also `SIGHUP`). Applies only `max_building`, `max_open_bot_prs`, checkpoint settings, `drain_timeout_minutes`, cost backstop, `review_bot_grace_minutes`, `review_bot_login`, `review_bot_mention`, guidance, `independent_reviewer_ids`, `status_names`, the reconcile intervals and the retention windows; any other change is refused with `restart required: <keys>` and an invalid file changes nothing. Lowering a cap never stops running builds. |

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

Comments read like a fellow developer's: plain GitHub Markdown in the agent's own words
(its summaries, reasons and questions are published as written, with only mentions and
HTML comments neutralised, credentials blocked and an 8000-character cap), no Omnigent
links, internal ids or labels, and no how-to lines. Hidden markers stay.

`Bot: Needs you` means the current stage cannot continue without the owner: an open
question, or a hold that needs his decision (e.g. PR not Ready with no fix attempt left).
A stage that finished with the owner's move next is `Idle`: triage posted, plan posted
awaiting approval, Ready. Activity in a stopped run's issue session from outside the
factory (someone typing in Omnigent) holds the next run until the session is idle again;
that asks nothing of the owner, so the note reads `Waiting: the Omnigent session is busy
outside the factory; ...` and `Bot` stays as derived.

**Ready means the bot's work is done**: the PR is open and closes the issue, the
cross-vendor review is accepted for the head (or carried to an owner/base sync of it),
every review-bot finding has an outcome and the bot has nothing left to do. Required
checks then only set `Bot`: green `Idle`, pending `Idle` with the note `Checks running on
<sha>`, red `Blocked` with the note `Required check red: <check>; <check>` (no comment, no
wake). Ready and `Working`/`Queued`/`Checkpoint` never go together: any work for a Ready
card (rework, an operator note) moves it to Building with that work, and an owner drag to
Ready while the bot is still working is moved back (`Kept in Building: ...`). A red check sends a Building
card back to its run with the one check wake only while that wake is unused and the
head has the bot's own commits; once it is spent (the agent re-submits when the red check
is outside the change, or ends that woken turn without a new result), or the head only
syncs the base branch, the card goes to Ready, `Blocked`. A run woken by an owner comment
that ends its turn without re-submitting is `Needs you`, never left `Working`. Review-bot
findings without an outcome get a wake of their own (once per build or rework, separate
from the check wake, so findings that arrive after the check wake still reach the run);
findings still without an outcome after it are `Needs you`. A blocked report for a defect in the change stays with the owner. An owner drag
from Building to Ready is accepted on the same terms (Bot by the checks, the run is
closed); otherwise the card returns to Building with the reason in the note
(`Kept in Building: ...`).

**Questions.** While an agent question is open, the owner's next plain (non-`/`) comment
on the issue or its PR is the answer: the question is resolved with that comment, the
answer is sent to the run that asked, and the card returns to Working. On a Building card
at Needs you with no fix attempt left the reply instead starts a rework (below), which
reads the answer with the comment. `/decide <id> <answer>` also works, but the id is not
shown on GitHub; replying in the Omnigent session does not answer the question.

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
issue only; reactions and other people's comments never count. In Triage a comment
re-runs triage in the issue session (a revised triage comment follows); in Planning it
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
`factory_ask_owner`; to replan, drag the card to Planning. A Building card left at Needs you
with no fix attempt left (its run closed, or idle and waiting, which is then retired) is
reworked the same way by an owner comment. Ready is then re-evaluated as usual and a new Ready report is posted.
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
(CLI, REST and GraphQL), merging and administration. While the review bot
(`review_bot_login`, default `chatgpt-codex-connector[bot]`) can still respond to the PR
head, the head is Ready only from a green read taken `review_bot_grace_minutes` (default
10) after the later of the build report and the trigger (the head push or PR opening, or a
`review_bot_mention` re-ping after its last answer), so late bot comments are handled by
the build's findings wake instead of pulling a Ready card back. The bot can still
respond until it has answered that head (a review of the commit, a verdict naming it, or a
+1 on the PR after the push); once it has and nothing re-pinged it, readiness is judged on
current evidence at once. When the bot's state cannot be read, the grace applies.

The Ready report is the agent's summary (what changed, decisions, follow-ups; no CI or
review-bot status) followed by the factory's lines from the read that decided Ready: PR,
CI and `Review bot` (e.g. `Codex: 👍 on <sha>`, `Codex: reviewed <sha>, 2 findings, all
with outcomes`, or `Codex: no response within the grace window`). While the card stays in
Ready on that head, a later read that changes those lines (a late verdict, the check
summary, a red check) edits the report in place (found by its marker; edits do not
notify), never posting a second one. GitHub sends no webhook for a +1 reaction, so a
Ready card whose report still shows no bot response is re-read on each reconcile, for up
to 24 hours after the grace. A new head, withdrawal or rework never edits it.

### Finished parcels

When a parcel's PR is merged or its issue is closed, the daemon removes its worktree and
local branch once every stage session has retired. Only paths under `worktree_root` are
touched; a worktree with uncommitted changes is kept unless the PR is merged; the local
branch is deleted only when the PR is merged or its tip is the verified Ready head.
Skipped items are logged. `cleanup` runs the same step by hand.

### State database size

The parcel aggregate (`parcels.aggregate_json`) is the state; events are history the
reducer never replays. Retention runs every 15 minutes in the daemon, in short batches:

| `[service]` key | Default | Removes |
|---|---|---|
| `observation_retention_hours` | `2` | Periodic observation events (`ReconcileDue`, `GitHubSnapshot`, delivery-keyed `ChecksChanged`, `ReadinessEvidence`, `TreeQuiescent`, cost/runtime samples, `CapacityAvailable`), their audit rows and the settled read/board-drift effects they spawned. |
| `delivery_body_retention_days` | `1` | Body and headers of processed deliveries that no event, or only check/PR/review observations, references. The row and its GUID stay for duplicate detection. |
| `completed_reconcile_interval_seconds` | `3600` | Not a deletion: a completed parcel with no live session or open decision is reconciled hourly instead of every `reconcile_interval_seconds`. |

Never removed: each parcel's newest event of each kind (read back as current issue
evidence and readiness), events referenced by authorizations, fences, approvals or
decisions, acks of effects still pending/claimed/unknown, events whose effects are
unsettled, carry a semantic dedupe key, are not reads/board-drift corrections or have a
dispatch intent, own item or own send, and every control, comment, safety and
session-lifecycle event. Deleted events have IDs that never recur (clock-stamped,
one-shot effect acks, or webhook delivery GUIDs the inbox still dedupes), so a restart
cannot re-apply one.

Freed pages return to the filesystem only once the file uses incremental auto_vacuum
(new files do). For an existing file, run once with the daemon stopped:

```sh
systemctl --user stop omnigent-factory
omnigent-factory prune --dry-run && omnigent-factory prune && omnigent-factory vacuum
systemctl --user start omnigent-factory
```

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
