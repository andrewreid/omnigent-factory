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

`factory:skip` is not a control: it only keeps the issue out of
[idle-time auto-triage](#idle-time-auto-triage) (anyone may add it; the factory never
starts anything because of it).

### Card drags

A rightward owner drag is a control; any leftward drag (by anyone, including a move to
Inbox or off the board) is a stop first. A refused drag into Building moves the card back
with the reason in the note. A drag is judged by its own from and to columns: a periodic
read that sees the new column before the drag's webhook arrives changes nothing about
it. A card moved right with no owner drag behind it (a non-owner, or a drag whose webhook
never came) gets no authority and the note `Moved to <column> without an owner command
seen: drag it again or add a factory: label`.

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

At most `triage_concurrency` triage runs (default 1) run at once, whatever started them
(drag, label, command, comment or auto-triage). A further triage request is not dropped:
the card moves to Triage with the note `Queued: triage starts when a triage slot is free`
and its run starts once a slot frees (within a reconcile interval). A run waiting on an
owner answer or at a checkpoint does not hold a slot.

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
rework), cleared when the card moves on. When nothing else is to be said, it shows the
issues whose triage named this one, e.g. `Related: #12 (overlap), #9 (conflict)` (also
cleared when the card moves on); any status reason or Blocked, Needs you, Checkpoint or
Queued note takes precedence.

### Related issues

Triage checks the other open issues on the board (`factory_list_issues`) for duplicates,
overlaps and contradictions. The triage comment then ends with a short **Related** list
(`- Overlaps #12: <how the scope is split>`); a duplicate gets a keep/close recommendation
(you close it), an overlap a proposed scope split, a genuine contradiction a question to
you (`Needs you`), and a clash with Building or Ready work is flagged. Each related issue
open on the board gets the `Related: ...` note above, without a comment. Plan and build
runs read the list and its agreed outcome, so later stages keep to this issue's side of a
split.

### Idle-time auto-triage

Off by default. When on, and the factory is otherwise idle (no plan, build or rework run
working or draining, no queued build that could start now, no stage request waiting, a
triage slot free, no triage ranking running, not paused), the factory triages the oldest eligible Inbox issue, one at
a time, exactly as if you had dragged it to Triage: the bot moves the card, posts the
normal triage comment and leaves it `Idle`. It never goes past Triage. A card in Building
or Ready whose `Bot` is `Blocked`, `Idle`, `Needs you` or `Checkpoint` (once the run's
checkpoint grace is over), or whose run waits on checks, does not keep the factory busy,
even while it holds a build slot. A running
triage finishes even if a build arrives; no new one starts until the factory is idle
again.

Eligible: an open issue (not a pull request or draft) of the configured repository in the
Inbox column, assigned to nobody, without the `factory:skip` label, created more than
`auto_triage_min_age_hours` ago, that the factory has never worked on (no run, request or
hold, no parked delivery). It is checked about every `reconcile_interval_seconds`, at most
`auto_triage_daily_limit` per local day (`auto-triage grant <n>` adds more for today). The
audit records each start as an `AutoTriage` event from the trusted clock (the operator's
standing authorisation), never as an owner control.

### Triage ranking

Off by default (`ranking = true` or `ranking on`). When the factory is idle by the
auto-triage rule and no triage is running, and either `ranking_min_new_triages` (5)
triage results are new or changed since the last ranking or 24 hours have passed with a
change in the Triage column, one read-only Omnigent session (`Factory ranking · <date>`)
orders the Triage column. It weighs priority, size, dependencies and blockers from each
triage's related list, overlaps, clashes with Building or Ready work, stale or likely-done
findings and age; it reads with `factory_list_issues` and `factory_get_issue` and submits
once with `factory_submit_ranking`. It never moves cards, starts stages or closes issues,
and a ranking and auto-triage never run at the same time. `ranking now` skips the idle rule: it
starts as soon as no triage and no other ranking is running, even while builds, plans or
reworks run.

What it writes: the project's `Rank` number field (1 = next), only where the value
changes; a `Priority` change only when the run found the triage priority wrong, each with
one short comment on the issue (reason, old -> new); and one short project status update
(top 5 and any priority changes; `ranking_status_update = false` turns it off; without
access the run posts none and logs it).

Your choices win. Set a card's Rank yourself and it is pinned there: later rankings order
the other cards around it and never overwrite it, until you clear the field. Set a
Priority yourself and the factory never changes that issue's Priority again (the value
triage filled in is not yours). The session is archived when the run completes, fails or
is abandoned; a failed run backs off (1 h, doubling, at most 24 h).
Only your own board edit (its `projects_v2_item` webhook) marks a Rank or Priority as yours; any other value the factory did not write is a baseline ranking may change, and owner choices recorded without such a webhook (before this rule) are cleared at startup and logged.

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
| `doctor` | Checks config, secrets, GitHub/Omnigent reachability, Omnigent login expiry (fails when expired, warns within 7 days) and server/client version drift (warning only). `doctor --live` (opt-in) also creates a throwaway session for the configured agent in the configured project and archives it, which catches server-side create failures such as unresolved agent env vars. It also reads the configured agent's bundle (through its newest session, or the `--live` probe) and fails naming any factory MCP tool the agent's `tools:` allowlist for the factory server leaves out (a warning when the bundle cannot be read). It warns when the App installation is not subscribed to `push` (tick Push in the App settings), when the subscribed events can be read. |
| `explain <parcel>` | The parcel's persisted state: stage, bot, sessions, holds, effects. |
| `recovery` | Failed/unknown effects and parked webhook deliveries. |
| `retry-effect <effect_id>` | Requeue a failed/unknown `publish_triage`/`publish_report`/`post_comment`; it adopts an existing comment by its marker, so it never duplicates. |
| `rerender-comment <effect_id>` | Re-render a published comment in place (found by its marker). |
| `resume <parcel> --message-file note.md` | Re-open the parcel's existing stage session after a stale block and relay one operator note. No authority, approval or time is added. |
| `cleanup <parcel> [--merged]` | Remove a finished parcel's factory worktree(s) and local `factory/` branch from the factory clone. |
| `release-delivery <guid>` | Release one parked webhook delivery for processing. |
| `pause` / `unpause` | Stop / resume admitting new work repository-wide; in-flight parcels and safety events carry on. |
| `auto-triage status` | Idle-time auto-triage: enabled (and whether config or the CLI decides), today's used/limit/granted, idle or what keeps it busy, triage slots, the next candidate. |
| `auto-triage on` / `off` | Turn auto-triage on/off at runtime. Stored in the state database, so it survives restarts, until `auto_triage` in the config file changes: then the config value applies again (the newer intent wins). |
| `auto-triage grant <n>` | Add `<n>` (1-1000) auto-triages to today's budget (local day; it does not carry over). |
| `ranking status` | Triage ranking: enabled (and whether config or the CLI decides), idle or what keeps it busy, new triage results since the last ranking, failures/backoff, recent runs. |
| `ranking on` / `off` | Turn triage ranking on/off at runtime; stored like `auto-triage on`/`off` (the newer intent wins). |
| `ranking now` | Run one ranking as soon as no triage or other ranking is running (it does not wait for builds, plans, reworks, queued builds or stage requests), whatever changed (also when ranking is off, or backing off; not while paused). `ranking status` then shows only what it still waits on. |
| `sessions prune [--dry-run]` | Delete (or list) the factory sessions session retention would remove now (through the daemon). |
| `prune [--dry-run]` | Apply history retention now (see [State database size](#state-database-size)) and print what was (or would be) removed. Runs through the daemon when it is up, else directly on the file. |
| `vacuum` | Compact the state database and switch it to incremental auto_vacuum. Refuses while the daemon runs (it holds the write lock for the whole rebuild). |
| `reload` | Re-read the config file into the running daemon (also `SIGHUP`). Applies only `max_building`, `max_open_bot_prs`, checkpoint settings, `drain_timeout_minutes`, cost backstop, `review_bot_grace_minutes`, `review_bot_ack_minutes`, `review_bot_max_wait_minutes`, `review_bot_login`, `review_bot_mention`, guidance, `independent_reviewer_ids`, `status_names`, the reconcile intervals, the retention windows, `auto_triage`, `auto_triage_daily_limit`, `auto_triage_min_age_hours`, `triage_concurrency`, `ranking`, `ranking_min_new_triages`, `ranking_status_update`, `rank_field_node_id` and `session_retention_days`; any other change is refused with `restart required: <keys>` and an invalid file changes nothing. Lowering a cap never stops running builds. |

The host config file (`~/.config/omnigent-factory/config.toml`) is the single source
of factory configuration; the target repository carries no factory config file.
Auto-triage keys (under `[service]`, all hot-reloadable):

```toml
auto_triage = false              # idle-time auto-triage of Inbox issues
auto_triage_daily_limit = 20     # auto-started triages per local day
auto_triage_min_age_hours = 24   # grace before a new issue is taken
triage_concurrency = 1           # triage runs at once, however started
ranking = false                  # idle-time ranking of the Triage column
ranking_min_new_triages = 5      # new/changed triage results that start a ranking
ranking_status_update = true     # short project status update per ranking
rank_field_node_id = ""          # the project's "Rank" NUMBER field (doctor checks it)
session_retention_days = 30      # delete factory sessions archived this long; 0 = never
```

Create the `Rank` field with the board migration (`setup render` lists it) and record its
node ID as `rank_field_node_id`; ranking does not start without it.

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
findings still without an outcome after it are `Needs you`. Findings that arrive on the
head after the card reached Ready (the bot answered after its wait ended) still get that
wake when it is unused: the card goes back to Building, its closed build run is re-opened
once (same authorization and approval, a free build slot needed) and woken; with the
wake spent, no slot free or on an owner sync head, they are `Needs you` as before (a
withdrawn card is re-checked on each read, so a slot freed later still gets the wake). Whenever readiness puts the
card at `Needs you`, one comment says why and lists each open review-bot thread (path,
`P0`-`P3` badge, title, link), noting a further review round after a fix commit; it is
not posted again while that state stands. A blocked report for a defect in the change stays with the owner. An owner drag
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
Triage, plan and ranking runs (and their sub-agents) may not run Git commands that move
HEAD or change the working tree (`checkout`, `switch`, `reset`, `stash`, `commit`, …); they
read other refs with `git show <ref>:<path>`, `git diff <ref>` or `git log <ref>`.
Preparing a run switches a worktree found off its recorded branch back to it when the tree
is clean and nothing is in progress; otherwise the card's note says why, e.g.
`Blocked: worktree off branch factory/issue-761 (detached at 469e2af, uncommitted changes)`.

Stage messages are short pointers; the agent works through eight MCP tools served by the
daemon at `http://127.0.0.1:<mcp_port>/mcp/` (loopback only, bearer token):
`factory_get_issue`, `factory_get_plan`, `factory_get_feedback`, `factory_get_status`,
`factory_list_issues` (a compact index of the other open board issues, read from GitHub
through a one-minute cache), `factory_ask_owner`, `factory_submit_result` and
`factory_submit_ranking` (triage ranking sessions only, which may also read any issue with
`factory_get_issue`). Each takes the caller's own Omnigent
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
head, the card is not Ready, so late bot comments are handled by the build's findings
wake instead of pulling a Ready card back. The bot can still respond until it has
answered that head (a review of the commit, a verdict naming it, or a +1 on the PR after
the push); once it has and nothing re-pinged it, readiness is judged on current evidence
at once. Its trigger is the head push or PR opening, or a `review_bot_mention` re-ping
after its last answer. Codex shows it is reviewing with a 👀 reaction (on the PR, or on
the re-ping comment), which sends no webhook, so each read (every reconcile, about every
2 minutes, while it is waited for) looks again:

| Bot state on its latest trigger | Wait |
|---|---|
| Answered (review, `Reviewed commit` comment, +1 after the push) | none: judged at once (a review webhook triggers the read) |
| 👀 taken at or after the trigger | until it answers, at most `review_bot_max_wait_minutes` (default 45) after the trigger; kept even if the 👀 is later removed |
| No 👀 | `review_bot_ack_minutes` (default 5) after the trigger |
| Unreadable | as for 👀: at most `review_bot_max_wait_minutes` after the trigger (or, with none known, after the first unreadable read) |
| No bot configured (`review_bot_login = ""`) | `review_bot_grace_minutes` (default 10) after the later of the build report and the trigger |

All three are hot-reloadable. For example:

```toml
review_bot_login = "chatgpt-codex-connector[bot]"
review_bot_mention = "@codex"
review_bot_ack_minutes = 5        # no 👀 by then: stop waiting
review_bot_max_wait_minutes = 45  # 👀 or unreadable: wait at most this long
review_bot_grace_minutes = 10     # used when the bot's 👀 cannot be told (no login set)
```

The Ready report is the agent's summary (what changed, decisions, follow-ups; no CI or
review-bot status) followed by the factory's lines from the read that decided Ready: PR,
CI and `Review bot` (e.g. `Codex: 👍 on <sha>`, `Codex: reviewed <sha>, 2 findings, all
with outcomes`, or `Codex: no response within the grace window`). While the card stays in
Ready on that head, a later read that changes those lines (a late verdict, the check
summary, a red check) edits the report in place (found by its marker; edits do not
notify), never posting a second one. GitHub sends no webhook for a +1 reaction, so a
Ready card whose report still shows no bot response is re-read on each reconcile, for up
to 24 hours after the wait. A new head, withdrawal or rework never edits it.

### Merge conflicts with main

A push to the default branch (the App's `push` event) re-reads the PR of every Building
or Ready card that has one; pushes to other branches are ignored. Each PR read (also the
periodic catch-up reads, the backstop for a lost webhook) asks GitHub whether the PR
merges into its base. GitHub computes that lazily: a read that gets no answer re-reads it
a few times over about 7 seconds, and still no answer is unknown, asked again at the next
catch-up read (never taken as a conflict, nor as clean). A PR that is only behind but
merges cleanly needs nothing: the factory never updates the branch.

On a conflict, once per (PR head, main head):

| Card | What happens |
|---|---|
| Ready, or Building with its build run closed | Back to Building with the note `Merge conflict with main` and a conflict rework under the same approval (admitted like any build, `Queued` while no slot is free): its first message tells the agent to merge `origin/main` into the issue branch, resolve within the approved plan, run the checks, push and resubmit. |
| Building, the build run waiting on checks or idle | One conflict wake to that run with the same instruction. |
| Building, the build run mid-turn | Recorded only; the run is not interrupted. The next read after its turn ends decides. |

If the run ends without resolving it, the card is `Needs you` with one comment. A
cross-vendor review given before a conflict was seen is never carried to a later head as
a base sync: the merge that resolves the conflict needs a fresh review before Ready.
Clean syncs (an owner "Update branch" with no conflict seen) keep the review as before.

### Finished parcels

When a parcel's PR is merged or its issue is closed, the daemon removes its worktree and
local branch once every stage session has retired. Only paths under `worktree_root` are
touched; a worktree with uncommitted changes is kept unless the PR is merged; the local
branch is deleted only when the PR is merged or its tip is the verified Ready head.
Skipped items are logged. `cleanup` runs the same step by hand.

### Reconciliation and observation load

Only parcels with live work get a per-issue GitHub read every `reconcile_interval_seconds`
(spread over the interval, never one burst): an open (or stopping, fenced, checkpointed)
run, an unsettled tree, an ambiguous or unsettled write, an open owner decision, stage
authority waiting to start, an in-flight board write, a queued or admitted build, or a PR
whose readiness is unverified (review bot pending, merge unknown). Every other parcel is
covered by a board-wide diff every `board_diff_interval_minutes` (default `5`,
hot-reloadable): one Projects read compares each card's column, Bot, open/closed,
assignees, labels, title and `updatedAt` with the values stored at the last diff, and only
a changed card (or one never compared) gets a per-issue read. A missed webhook (close,
reopen, move, edit, label, assignment) is thus applied within one diff interval. Boot reads
only live parcels.

The Omnigent observer scans only open runs; a WAITING run parked on the owner (open
decision, plan approval, Needs you) is read on the settled cadence. All trees due in one
pass share one archive-inclusive inventory read (taken after every tree's child walk); its
full resync runs every `session_inventory_resync_hours` (default `6`, hot-reloadable). A
failed read still makes every scan of the pass incomplete, never idle.

### State database size

The parcel aggregate (`parcels.aggregate_json`) is the state; events are history the
reducer never replays. Retention runs every 15 minutes in the daemon, in short batches:

| `[service]` key | Default | Removes |
|---|---|---|
| `observation_retention_hours` | `2` | Periodic observation events (`ReconcileDue`, `GitHubSnapshot`, delivery-keyed `ChecksChanged`, `ReadinessEvidence`, `TreeQuiescent`, cost/runtime samples, `CapacityAvailable`), their audit rows and the settled read/board-drift effects they spawned. |
| `delivery_body_retention_days` | `1` | Body and headers of processed deliveries that no event, or only check/PR/review observations, references. The row and its GUID stay for duplicate detection. |
| `delivery_row_retention_days` | `14` | Rows (and delivery attempts) of processed deliveries whose body was pruned, unless an event, a parked delivery, a parcel hold or stored review comments reference them; quarantined attempts are kept. A delivery that produced no event (ignored, no transition) loses its body and headers as soon as it is processed (GUID and sha256 stay). |
| `completed_reconcile_interval_seconds` | `3600` | Not a deletion: only when no board diff is wired, a parcel with no live work is reconciled at this cadence instead of every `reconcile_interval_seconds`. |

An issue read (`GitHubSnapshot`) whose values equal the parcel's newest stored read and
changes nothing is not stored at all (nor its read effect or audit row); when a newer read
is stored, the previous one keeps a sha256 of the issue body instead of the text (only the
newest read's text is read back, as the issue the agent sees).

Never removed: each parcel's newest event of each kind (read back as current issue
evidence and readiness), events referenced by authorizations, fences, approvals or
decisions, acks of effects still pending/claimed/unknown, events whose effects are
unsettled, carry a semantic dedupe key, are not reads/board-drift corrections or have a
dispatch intent, own item or own send, and every control, comment, safety and
session-lifecycle event. Deleted events have IDs that never recur (clock-stamped,
one-shot effect acks, or webhook delivery GUIDs the inbox still dedupes), so a restart
cannot re-apply one.

**Omnigent sessions.** Factory-created sessions archived longer than
`session_retention_days` (default 30, hot-reloadable, `0` = never) are deleted, a few per
sweep: only sessions the factory recorded (issue sessions and replaced ones of issues that
are merged, closed or Done with every run settled, and finished ranking sessions), still
carrying the factory's `factory.dispatch` label, archived, and with nothing running or
waiting anywhere in their tree. Deleting a session removes its child and worker sessions
too (Omnigent's `DELETE /v1/sessions/{id}`); worktrees and branches are left alone. Each
deletion is recorded and never retried. `omnigent-factory sessions prune --dry-run` lists
what would go now. Triage ranking keeps its newest 10 finished runs; older ones are
removed once their session is deleted (or archived, with retention off).

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
