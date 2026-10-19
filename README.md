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

The board's `Auto-build` field is a control too: `Queued` set by you approves the posted
plan for [auto-build](#auto-build).

`factory:skip` is not a control: it only keeps the issue out of
[idle-time auto-triage](#idle-time-auto-triage) (anyone may add it; the factory never
starts anything because of it).

`follow-up` marks an issue the factory filed from another issue's run (one per PR, see
[Follow-ups](#follow-ups)); epic autopilot holds it.
Removing it yourself, or adding `factory:build-follow-ups` to the epic, releases it (see
[Follow-ups](#follow-ups)).

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

**Build slots count running builds only.** A build (or rework) holds a `max_building`
slot, and an auto-build also an `auto_build_concurrency` slot, only while its run works,
winds down or is about to start. A parked build releases them: `Blocked` or `Needs you`
with its tree seen idle, a checkpoint once its wrap-up has drained, or idle in Building
waiting on checks or the review bot. A tree that could not be read completely, or is seen
busy again, counts as running (even over the cap). When a parked build must work again
(your comment or answer, `/continue`, a checks, findings or conflict wake, an operator
note), the message takes a free slot at once; with none free it is held, the card shows
`Queued: waiting for a build slot to resume`, and it is delivered exactly once when the
build is admitted. Resuming builds are admitted before new ones (then manual builds, then
auto-builds). Open PRs still count towards `max_open_bot_prs` while parked. `status`
lists running and parked builds separately.

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
| Building | Guidance to the build within its approval: relayed at once, to an idle run, one waiting on checks, or a busy one (its turn or its workers still running), at most one message per 2 minutes; the run must also read it before it can submit. On a finished build, or at `Needs you` with no fix attempt left, it starts a rework. |
| Ready | Starts a rework (back to Building, `Rework: owner feedback`). |
| Any, at `Bot: Checkpoint` | Recorded only; the run needs `/continue` before it reads it. |

On the parcel's PR, an owner conversation comment, or a review that requests changes,
comments, or approves with text, counts as the same plain comment (so on a Ready card it
starts a rework). Not after `/stop` or once merged. An accepted issue or PR conversation
comment gets 👀 when taken and 👍 once a run has read it (`factory_get_feedback`); a
review has no reaction API, so it only gets the note. While a run has it, the card's
`Factory note` says `Feedback received: relayed to Rosie` (or `... Rosie reads it before
submitting` when it was folded or gated). See
[Steering by comment](#steering-by-comment) for details.

### Bot and Factory note

| `Bot` | Meaning |
|---|---|
| `Working` | A run is doing work. |
| `Queued` | Approved build waiting for a slot; note `Queued: 2nd in line (1/1 running: #673)`, `Queued: waiting for a build slot to resume` or `Waiting for an auto-build slot (1/1 in use: #673)`. |
| `Needs you` | An open question, or a hold only the owner can clear. |
| `Checkpoint` | The run used its time block; comment `/continue`. |
| `Blocked` | Something failed, or a required check is red on Ready; the note says what. |
| `Idle` | Nothing to do; the next move is the owner's. |

`Factory note` is one line with the latest reason (refusals, stops, queue position,
rework), cleared when the card moves on. When nothing else is to be said, it shows the
issues whose triage named this one, e.g. `Related: #12 (overlap), #9 (conflict)` (also
cleared when the card moves on); any status reason or Blocked, Needs you, Checkpoint or
Queued note takes precedence.

Both fields are display only. The periodic board diff reads them with the rest of the
card (no extra request) and rewrites a value that differs from the one the factory last
wrote (a lost, cancelled or hand-edited write), logging `board drift corrected`; a card
with a Bot/note write in flight, of unknown outcome or landed within the last two minutes
is left for a later diff. A required check read red on a PR head first seen less than
`red_check_confirm_seconds` (default 120, hot-reloadable, 0 = off) ago shows as checks
running until a second read (scheduled for the window's end) confirms it or the window
ends: a just-pushed head's check rollup can read red for a moment.

### Related issues

Triage checks the other open issues on the board (`factory_list_issues`) for duplicates,
overlaps and contradictions. The triage comment then ends with a short **Related** list
(`- Overlaps #12: <how the scope is split>`); a duplicate gets a keep/close recommendation
(you close it), an overlap a proposed scope split, a genuine contradiction a question to
you (`Needs you`), and a clash with Building or Ready work is flagged. Each related issue
open on the board gets the `Related: ...` note above, without a comment. Plan and build
runs read the list and its agreed outcome, so later stages keep to this issue's side of a
split.

### Native issue links and epics

The factory reads GitHub's own issue links (parent, sub-issues, blocked by, blocking) with
the reads it already makes: the per-issue read stores them on the parcel and the board
diff compares them (a blocker closing changes the card it blocks, though GitHub does not
move that card's `updatedAt`). The App's `sub_issues` and `issue_dependencies` webhooks
(Issues read; `doctor` warns when the installation is not subscribed) and a closed or
reopened blocker re-read the issues they name at once; without them a change is seen
within one board diff interval. Rosie sees the links: `factory_get_issue` lists them with
state and title, `factory_list_issues` adds e.g. `parent #821; blocked by #823 (open);
blocks #825` to each line.

| Rule | What happens |
|---|---|
| Auto-build and blockers | A queued card with an open blocker does not start: note `Waiting on #823`, `auto-build status` shows `waiting on #823`. It starts by itself once a read shows every blocker closed. A start whose fresh read cannot see every blocker is refused (fails closed; manual builds are not affected). |
| Manual builds and blockers | A drag, `/approve` or `factory:build` still builds; the note reads `Started despite open blocker #823` (no comment). |
| Ranking | After Rosie's ranking, a blocker always ranks above what it blocks, and sub-issues of one parent stay together where that rule allows. An owner-pinned Rank is kept even when it forces a violation; that is logged. |
| Dependency links | Every result Rosie submits (triage, epic triage, plan, build, blocked) may list `related` issues; each `depends_on` or `blocks` entry becomes a native blocked-by link (`addBlockedBy`) once the result is accepted, created by the factory (Rosie needs no write access, and never asks you to add one): only between issues of this repository (an epic may be the blocker), skipped when it exists, never removing one, each logged. Parent/sub-issue links are not created (that would turn an issue into an epic, which changes what may be built), except that a [follow-up](#follow-ups) joins its source issue's epic. |

An issue with at least one sub-issue is an **epic**. It is never built: a drag, `/approve`
or `factory:build` on it is refused with the note `Epic: build its sub-issues` (a drag is
moved back), an `Auto-build` mark on it is cleared, and auto-build never starts one. Its
card note shows progress, e.g. `Epic · 1/8 done · next: #823` (next: the best-ranked open
sub-issue on the board with no open blocker), written after a board diff only when it
changes. Triage of an epic (a drag to Triage, `/triage`, or auto-triage, once like any
issue) is the **epic triage**: Rosie gets the epic body and each sub-issue with its state,
column, links and triage summary, and submits `epic_triage` (coverage gaps and overlaps
against the epic body, a build order, suggested blocked-by links between sub-issues; no
priority or size). The factory creates those links (same rules as above) and posts one
short comment with the order and the gaps; the card ends `Idle` in Triage.

When the repository has an `Epic` issue type, every epic without a type gets it; a type the
owner set is never changed (logged). Without one (`doctor` warns, naming the types that
exist) this is a no-op. `setup render` also lists an `Epics` table view
(`createProjectV2View`, then `updateProjectV2View` for its filter
`is:issue is:open has:sub-issues-progress`); it is not applied by the factory.

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

### Auto-build

Off by default (`auto_build = true` or `auto-build on`). The project's `Auto-build`
single-select field lets you approve a posted plan now and have the factory start the
build when it has room, without dragging the card yourself.

Setting a card's `Auto-build` to `Queued` is your approval of the plan posted on it at
that moment, exactly like `/approve <hash>`: only your own board edit counts (its
`projects_v2_item` webhook, from an owner's numeric ID, like a card drag). Anyone else's
`Queued`, and the factory's own writes, approve nothing. It is accepted only on a card in
Planning with a posted plan; anywhere else (no plan yet, Inbox, Triage, a plan being
revised, a build already approved) the factory clears the field and the `Factory note`
says why (`Auto-build cleared: ...`). An accepted mark notes `Auto-build queued: ...`.

| Situation | What happens |
|---|---|
| A question on the plan is open | The mark waits (`eligible: false` in `auto-build status`); once you answer, it can start if the plan is unchanged (a plan the run re-posts after your answer must have the same hash). |
| The plan is revised after the mark (plain comment, `/plan`, a replan, a changed plan re-posted) | The mark lapses: the field is cleared, note `Plan revised: re-queue to approve`. A build never runs a plan you have not seen. |
| A stop, a leftward drag or the card leaving Planning | The mark lapses (field cleared, with the reason). |
| You clear the field before the build starts | It leaves the queue. |
| You clear the field after the build started | Nothing: stop a build with `/stop` or a leftward drag. |
| A board read shows `Queued` but no owner webhook was seen | Not acted on: note `Auto-build mark not confirmed: re-select it` (once). Clear and select it again. |
| You select `Started` | Not an approval (it is the factory's own option): the field is cleared. |

Whenever a build slot is free (this does not wait for the factory to be idle, unlike
auto-triage), the factory starts the first startable mark by `Rank` (lowest first, unranked
last), then the oldest issue. Manually approved builds (drag, `/approve`,
`factory:build`) always go first: no auto-build starts while one waits for a slot, and an
approved manual build is admitted before a queued auto-build. Auto-builds count towards
`max_building`; at most `auto_build_concurrency` (1) of them hold a build slot at once.
A queued auto-build that waits only for that cap says so, naming the running ones:
`Waiting for an auto-build slot (1/1 in use: #673)` (also `waiting_on` in `auto-build
status`, which lists running and parked auto-builds separately).
Optionally `auto_build_daily_limit` caps starts per local day (0, the default, is
unlimited; `auto-build grant <n>` adds more for today). The audit records each start as
an `AutoBuild` event from the trusted clock carrying your mark's approval (owner, event,
plan hash).

Once started, the factory sets the field to `Started` and it is a normal build: the card
moves to Building, with the usual checkpoints, fix budget and Ready rules. When the card
is Done or the issue closes (or its PR merges), the factory clears the field. Marks,
approvals and starts are stored with the parcel, so a restart never starts a mark twice or
repeats a note.

### Epic autopilot

Off by default (`epic_autopilot = true` or `epic-autopilot on`). It moves an epic's
sub-issues through plan and build one after another, in dependency order, with you
approving the epic plan once and merging each PR.

Set the epic card's `Autopilot` field (only your own board edit counts, as for
`Auto-build`; a read that shows a level without your webhook notes `Autopilot mark not
confirmed: re-select it`): the epic moves to Triage and the epic triage writes the
**epic plan**: build order, each sub-issue's part, coordination between them, gaps,
steps only you can take (tenant, Azure, Entra, secrets, DNS ...) and blockers outside the
epic, posted as one comment headed by its hash. Nothing advances until you `/approve` it
on the epic (hash-bound like a plan). A comment on the epic revises the plan: running
builds go on, nothing new starts until you approve the revision. On approval each human
step becomes a sub-issue of the epic assigned to you, with blocked-by links from the
sub-issues it comes before (never created twice); closing it unblocks them.

| Level | After a sub-issue's plan is posted |
|---|---|
| `Full` | Its build is queued at once. |
| `Delayed` | Its build starts `epic_autopilot_delay_minutes` (60) after the plan was posted, unless you object first: a comment (a plan revision), a leftward drag, `/stop`, clearing its `Auto-build` field, assigning a person, or clearing the epic's `Autopilot` field. |
| `Plan only` | Nothing: approve it as usual (drag, `/approve`, `Auto-build` = Queued). |

Next sub-issue: open, assigned to nobody, not `factory:skip`, not a held
[follow-up](#follow-ups), not an epic itself (a nested epic only blocks), every blocker closed (a merged PR closes it; blockers outside
the epic are waited on, never started), by the plan's build order, then `Rank`, then
age. It goes straight to planning (no triage), its worktree synced with the base. Its
plan must say whether it stays within its part of the epic; one that does not is never
built. At most `epic_autopilot_concurrency` (1) sub-issues per epic are in progress (the
epic's `Parallel` number overrides it). Builds are ordinary auto-builds: they count
against `auto_build_concurrency`, `max_building`, the daily limit and the parked-slot
rules, and your own builds go first (`auto_build` itself may stay off).

Autopilot pauses (starts nothing new) while a sub-issue it drives is `Needs you` or
`Blocked` or its plan goes beyond its part, and stops to ask once (one comment) when
neither the plan nor blocked-by links give an order. Its state is the epic card's note,
e.g. `Epic · 3/9 done · autopilot: planning #882, next #883` or `Autopilot paused: #881
needs you`; no status comments. Clearing the epic's field turns it off and drops queued
starts. Delayed starts and every step are stored with the issues, so a restart never
repeats one.

### Follow-ups

Rosie no longer opens issues (her shell policy denies `gh issue create` and the REST and
GraphQL equivalents, chained commands included; the refusal points to `split_out` and
`deferred`). Every review finding gets exactly one
outcome: **fix in this PR** (a real defect in the plan's area with a small, local fix; the
default for in-scope nits), **follow-up** (a real defect with real-world impact outside the
plan, or one that would materially grow the PR) or **advisory** (no realistic trigger in
this repository, a style preference, hardening against threats not in play: a one-line
reply, nothing filed). She replies on the thread, resolves it and lists follow-ups and
advisories in her build_ready or blocked result's `deferred` (severity, path, title, thread
URL, reason, optional `blocks`).

The factory files the follow-ups of one PR as **one** issue, `Follow-ups from #<PR>`: a
checklist, one line per item, labelled `follow-up`, a sub-issue of the source issue's epic,
with a native blocked-by link for every `blocks` issue. Items a later result adds go into
the same issue. A hidden marker makes the create restart-safe: an unclear create is looked
up by its marker before any retry, so the issue is never opened twice. When a rework fixes
an item, the factory ticks its line (`→ fixed in #<PR> (<sha>)`); it never closes the
issue, you do. Each link step is done once, logged and retried after a failure; nothing is
ever removed, and a label or parent you change afterwards is left as you set it. The older
`follow_ups` result field (issues a run opened itself) is still accepted: those issues are
labelled and linked as before and listed in the Ready report. As a backstop, an issue the
factory bot opens while another issue's run is open, naming that issue in its body, is
treated as its follow-up (label and epic only, never blocked-by links); epic autopilot's
own human-step issues and anything else the factory writes itself (a hidden `factory-`
marker) are excluded.

**Owner-directed splits.** When you tell a run "do these bits, file the rest as an issue"
(in triage, planning or a build), Rosie lists each separate piece in her result's
`split_out` (title, body, reason, `relation` to this issue: `none`, `blocks` or
`blocked_by`, optional `blocks`; at most 10 per result). The factory opens one issue per
piece (never consolidated), in the Inbox like any new issue, with the line "Split out of
#N at the owner's direction". It gets no `follow-up` label and is not follow-up lineage
(its own build may defer follow-ups); it joins the source issue's epic and gets native
blocked-by links per `relation` and `blocks`. It is filed once (keyed by source issue and
title; an unclear create is found by its marker before any retry), posts no comment of its
own: the stage's normal comment (triage, plan, blocked or Ready report) lists it under
**Split out**. Splits are not Ready report checkboxes and not subject to the depth limit.

**Depth limit.** A follow-up never spawns follow-ups: when the issue being built is itself
a follow-up (the factory recorded it as one of another issue, a consolidated issue
included), `factory_submit_result` refuses `deferred` follow-ups (and `follow_ups`) with
"fix each of these findings in this PR, or give it outcome advisory". `follow_up_max_depth`
(default 1, hot-reloadable) sets how many generations are allowed.

**Review rounds.** The board's `Review rounds` NUMBER field is how many review-bot reviews
an issue's PR gets; empty, `review_rounds_default` (2, hot-reloadable) applies, and an
epic's value applies to its sub-issues without their own. Only your own board edit counts
(as for `Auto-build`): a value without your webhook is not trusted. One round is one review
of a head the bot pushed, counted per issue across runs; comments and reworks do not reset
it. From round `max(2, budget)` on, only P1 findings with real impact must be fixed; P2/P3
take the outcome rule. Once the rounds are used up the run wraps up (fixes remaining
in-scope P1s, defers the rest, stops asking the bot), the factory neither waits for another
review nor wakes the run for findings on heads beyond the budget, and the card goes to
Ready when otherwise ready, with the note `Codex round 2/2`. `factory_get_status` shows
`review_rounds` (used, budget, strict_from_round).

**Ready report checkboxes.** The Ready report ends with a **Deferred** list: one line per
deferred item and advisory (``- [ ] Fix now: P2 `path` title (in #<issue>)`` or
`(advisory: reason)`), plus `- [ ] One more Codex review round` once the rounds are used up. Tick lines
to have them done: only your ticks count (the comment's `edited` webhook from an owner,
diffed against the previous body; a hidden marker per line, never its text). Ticks within
2 minutes of the first become one rework (back to Building through the normal queue, same
branch and PR, the plan approval stands) whose brief is exactly the ticked items; "One more
round" raises that issue's budget by one and has Rosie ask the bot again. Each line then
shows what happened (`→ fixing (rework started)`, `→ fixed in <sha>`, `→ not started:
<reason>`); the factory re-reads the comment before writing and keeps ticks it has not yet
processed. Unticking does nothing; a tick is acted on once, a restart included. A lost
webhook is recovered when the board diff next reads the Ready card: a ticked line with no
recorded tick counts only if the comment's edit history shows you as the editor who ticked
it; otherwise it is ignored and logged.

Epic autopilot does not start a sub-issue labelled `follow-up` unless it blocks another
open sub-issue of the same epic; the epic note counts the rest (`2 follow-ups held`).
Two owner overrides (only your own label changes count; a lost label webhook is
recovered from the issue's timeline at the next board diff):

| Override | Effect |
|---|---|
| Remove `follow-up` from the issue | Autopilot treats it as a normal sub-issue. Removal by anyone else, the bot included, changes nothing. |
| Add `factory:build-follow-ups` to the epic | Autopilot builds every follow-up of that epic. Removing it restores the default. |

Manual starts (a drag, `factory:build`, `/approve`) are not affected.

### Triage ranking

Off by default (`ranking = true` or `ranking on`). When the factory is idle by the
auto-triage rule and no triage is running, and either `ranking_min_new_triages` (5)
triage results are new or changed since the last ranking or 24 hours have passed with a
change in the Triage or Planning column, one read-only Omnigent session (`Factory ranking ·
<date>`) ranks the Triage and Planning columns together on one scale: a single `Rank`
1..N across both columns (no two cards share a rank), so planned cards and triaged ones
are weighed against each other and auto-build starts queued plans in that order. It weighs
priority, size, dependencies and blockers (native links and each triage's related list), overlaps,
clashes with Building or Ready work, stale or likely-done findings, how ready an issue is
and age; it reads with `factory_list_issues` and `factory_get_issue` and submits
once with `factory_submit_ranking`. It never moves cards, starts stages or closes issues,
and a ranking and auto-triage never run at the same time. `ranking now` skips the idle rule: it
starts as soon as no triage and no other ranking is running, even while builds, plans or
reworks run.

What it writes: the project's `Rank` number field (1 = next), only where the value
changes; a `Priority` change only when the run found the triage priority wrong, each with
one short comment on the issue (reason, old -> new); and one short project status update
(top 5 of the combined Triage and Planning ranking and any priority changes; `ranking_status_update = false` turns it off; without
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
| `status` | Readiness, pause state, building count/cap with the running and parked builds, queue, pending/unknown effects, parked deliveries, and the [health sweep](#health-sweep-and-stalled-runs): Omnigent state, breaker, nudges last sweep and each open run's state and stall episode. |
| `doctor` | Checks config, secrets, GitHub/Omnigent reachability, Omnigent login expiry (fails when expired, warns within 7 days) and server/client version drift (warning only). `doctor --live` (opt-in) also creates a throwaway session for the configured agent in the configured project and archives it, which catches server-side create failures such as unresolved agent env vars. It also reads the configured agent's bundle (through its newest session, or the `--live` probe) and fails naming any factory MCP tool the agent's `tools:` allowlist for the factory server leaves out (a warning when the bundle cannot be read). It warns when the App installation is not subscribed to `push` (tick Push in the App settings) or to `sub_issues`/`issue_dependencies` (Sub issues, Issue dependencies), when the subscribed events can be read, and when the repository has no `Epic` issue type (epics are then not typed). It fails when Omnigent's own `/health` is not ok and warns when the configured host is offline. |
| `explain <parcel>` | The parcel's persisted state: stage, bot, sessions, holds, effects, the current run's stall episode (`stall`) and what the last health sweep made of it. |
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
| `auto-build status` | Auto-build: enabled (and whether config or the CLI decides), the concurrency cap, auto-builds running and waiting for a slot, today's used/limit/granted, what the next start waits on (`waiting_on`), and the queue in start order with each card's rank, `eligible` and `blocker`. |
| `auto-build on` / `off` | Turn auto-build on/off at runtime; stored like `auto-triage on`/`off` (the newer intent wins). Marks stay queued while it is off. |
| `auto-build grant <n>` | Add `<n>` (1-1000) auto-builds to today's limit (only matters with `auto_build_daily_limit` > 0). |
| `epic-autopilot status` | Epic autopilot: enabled (and whether config or the CLI decides), delay, concurrency, and each epic on autopilot with its level, plan state, pause, note and human gates. |
| `epic-autopilot on` / `off` | Turn epic autopilot on/off at runtime; stored like `auto-build on`/`off` (the newer intent wins). Queued autopilot starts wait while it is off. |
| `ranking status` | Triage ranking: enabled (and whether config or the CLI decides), idle or what keeps it busy, new triage results since the last ranking, failures/backoff, recent runs. |
| `ranking on` / `off` | Turn triage ranking on/off at runtime; stored like `auto-triage on`/`off` (the newer intent wins). |
| `ranking now` | Run one ranking as soon as no triage or other ranking is running (it does not wait for builds, plans, reworks, queued builds or stage requests), whatever changed (also when ranking is off, or backing off; not while paused). `ranking status` then shows only what it still waits on. |
| `sessions prune [--dry-run]` | Delete (or list) the factory sessions session retention would remove now (through the daemon). |
| `prune [--dry-run]` | Apply history retention now (see [State database size](#state-database-size)) and print what was (or would be) removed. Runs through the daemon when it is up, else directly on the file. |
| `vacuum` | Compact the state database and switch it to incremental auto_vacuum. Refuses while the daemon runs (it holds the write lock for the whole rebuild). |
| `reload` | Re-read the config file into the running daemon (also `SIGHUP`). Applies only `max_building`, `max_open_bot_prs`, checkpoint settings, `drain_timeout_minutes`, cost backstop, `review_bot_grace_minutes`, `review_bot_ack_minutes`, `review_bot_max_wait_minutes`, `red_check_confirm_seconds`, `review_bot_login`, `review_bot_mention`, guidance, `independent_reviewer_ids`, `status_names`, `review_rounds_default`, `follow_up_max_depth`, the reconcile intervals, the retention windows, `auto_triage`, `auto_triage_daily_limit`, `auto_triage_min_age_hours`, `triage_concurrency`, `auto_build`, `auto_build_concurrency`, `auto_build_daily_limit`, `epic_autopilot`, `epic_autopilot_delay_minutes`, `epic_autopilot_concurrency`, `ranking`, `ranking_min_new_triages`, `ranking_status_update`, `rank_field_node_id`, `session_retention_days` and the health sweep settings; any other change is refused with `restart required: <keys>` and an invalid file changes nothing. Lowering a cap never stops running builds. |

The host config file (`~/.config/omnigent-factory/config.toml`) is the single source
of factory configuration; the target repository carries no factory config file.
Auto-triage, auto-build and ranking keys (under `[service]`; hot-reloadable unless marked
"restart"):

```toml
auto_triage = false              # idle-time auto-triage of Inbox issues
auto_triage_daily_limit = 20     # auto-started triages per local day
auto_triage_min_age_hours = 24   # grace before a new issue is taken
triage_concurrency = 1           # triage runs at once, however started
auto_build = false               # start builds you queued with the Auto-build field
auto_build_concurrency = 1       # auto-builds holding a build slot at once
auto_build_daily_limit = 0       # auto-builds started per local day (0 = unlimited)
auto_build_field_node_id = ""    # the project's "Auto-build" SINGLE_SELECT field (restart)
auto_build_options = { Queued = "", Started = "" }  # its option IDs (restart)
epic_autopilot = false           # advance epics whose Autopilot field you set
epic_autopilot_delay_minutes = 60  # Delayed: build starts this long after the plan
epic_autopilot_concurrency = 1   # sub-issues per epic in progress (Parallel overrides)
autopilot_field_node_id = ""     # the epic "Autopilot" SINGLE_SELECT field (restart)
autopilot_options = { Full = "", Delayed = "", "Plan only" = "" }  # its options (restart)
parallel_field_node_id = ""      # the epic "Parallel" NUMBER field (restart)
review_rounds_field_node_id = "" # the "Review rounds" NUMBER field (restart)
review_rounds_default = 2        # review-bot rounds of an issue whose field is empty
follow_up_max_depth = 1          # follow-up generations that may defer follow-ups
ranking = false                  # idle-time ranking of the Triage and Planning columns
ranking_min_new_triages = 5      # new/changed triage results that start a ranking
ranking_status_update = true     # short project status update per ranking
rank_field_node_id = ""          # the project's "Rank" NUMBER field (doctor checks it)
session_retention_days = 30      # delete factory sessions archived this long; 0 = never
```

Create the `Rank` field with the board migration (`setup render` lists it) and record its
node ID as `rank_field_node_id`; ranking does not start without it.

Create the `Auto-build` field the same way: `setup render` lists it under `create_fields`
(a SINGLE_SELECT with options `Queued` and `Started`). Apply that one `createProjectV2Field`
input, then record the returned field node ID as `auto_build_field_node_id` and its two
option IDs as `auto_build_options = { Queued = "...", Started = "..." }` and restart the
daemon (these two keys are not hot-reloadable). While `auto_build` is on or the field
is configured, `doctor` fails when the field is missing (naming its ID and options when
the board already has one) or its IDs differ.

Create the `Review rounds` NUMBER field the same way and record its node ID as
`review_rounds_field_node_id` (restart); unset, every issue gets `review_rounds_default`;
`doctor` fails when a configured ID does not match.

Create the `Autopilot` (SINGLE_SELECT: `Full`, `Delayed`, `Plan only`) and `Parallel`
(NUMBER) fields the same way (`setup render` lists both), record their IDs as
`autopilot_field_node_id`, `autopilot_options` and `parallel_field_node_id`, and restart.
While `epic_autopilot` is on or either field is configured, `doctor` fails when the
Autopilot field is missing or its IDs differ (naming the board's IDs when it has one).

A triage/report/status publication that failed definitively leaves the card at
`Bot: Blocked`. Fix the cause, then run `recovery` and `retry-effect <effect_id>`.

### Card status vs. comments

Bot comments notify the owner, so the factory comments only when he must read or act:
triage, plan, Ready report (again after a rework), agent questions, checkpoint
(`/continue`) and blocks that need owner action (agent blocked, restart exhausted, create
refused, stop unverified, ambiguous adoption, PR not Ready with no fix attempt left, a
stalled run). Everything else (queued, stopped,
approval acknowledged/invalidated, Ready withdrawn, rework, PR closed unmerged, refused
commands, invalid result, ...) is the card's `Factory note` text field: one line with the latest
reason, written only when it changes and cleared when the card moves on. Blocked,
Checkpoint, Needs you and Queued cards without a specific reason get a derived note.
`Bot: Queued` marks an approved build waiting for capacity; its note (`Queued: 2nd in line`)
follows the queue as builds ahead are admitted (refreshed by the periodic reconcile). Owner command comments get a
reaction: 👍 accepted, 😕 refused (the reason is in the note). Plain owner comments get 👀
when taken and 👍 once a run has read them (the 👀 stays; GitHub has no webhook for
reactions either way). Reactions are display only: a failed one blocks nothing. Config keys:
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
that ends its turn without re-submitting is `Needs you`, never left `Working`. A run that
had submitted build_ready and is reopened by `/continue` may re-submit (the same head
included); if it ends the continued turn without a new result, its build_ready stands for
the head it attested and the card goes to Ready on the usual terms. Review-bot
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
daemon at `http://<bind_host>:<bind_port>/mcp/` and `http://127.0.0.1:<mcp_port>/mcp/`
(bearer token; any other listener address gets 404):
`factory_get_issue`, `factory_get_plan`, `factory_get_feedback`, `factory_get_status`,
`factory_list_issues` (a compact index of the other open board issues, read from GitHub
through a one-minute cache), `factory_ask_owner`, `factory_submit_result` and
`factory_submit_ranking` (triage ranking sessions only, which may also read any issue with
`factory_get_issue`). Each takes the caller's own Omnigent
`session_id`; a per-session CEL policy (`factory-caller@…`) denies factory tool calls
carrying any other id, and the daemon resolves issue, run and stage from its own store.
Create the token once with `omnigent-factory setup mcp-token` (written to
`<secrets_dir>/mcp-token`, mode 0600, never printed) and give it to the agent host as
`FACTORY_MCP_TOKEN`; `doctor` checks the token file, the `bind_host` and loopback
listeners (one tokenless probe each), the agent and a dry compile of the identity policy.
The endpoint is plaintext HTTP protected only by the bearer token: ingress must never
forward `/mcp`, and serving it on a LAN `bind_host` is the operator's choice (point
`FACTORY_MCP_URL` at either address). The factory clone's `info/exclude` ignores
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
as one message, even if it reported blocked.

A build that is busy (its turn running, or its turn ended while the workers it dispatched
still run) gets the comment at once as a mid-build message: read it with
`factory_get_feedback`, redirect in-flight workers, stay within the approved plan, ask
with `factory_ask_owner` if it asks for more (#884: a turn and its workers can run for
hours). Omnigent delivers it straight away to a root whose turn has ended (it starts a
turn); to a root mid-turn it is queued and delivered as the next turn when that turn ends
(the claude-sdk harness cannot inject into a running turn). Comments within 2 minutes of
the last relay are folded into one message, sent on the first periodic reconcile after
the window (or when the turn ends without a result, whichever is first); a comment is
never relayed twice, a restart included (the reducer state and the event dedupe are
durable). A gated run (stopped, open question, approval invalid) only records it.
A busy triage or plan run gets no extra message. Every run's result is refused until it
has read every comment, so a burst still folds into the run in progress.

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

Every review-bot finding needs an outcome (fix in this PR, follow-up or advisory, see
[Follow-ups](#follow-ups)); a clean bot verdict is not required. The build replies on the
thread with the outcome and resolves it; it resolves only threads it replied to:
readiness counts a bot thread as handled when the factory bot replied or a person resolved
it, and a thread the bot resolved without a reply stays open. The agent's shell policy
allows thread replies and `resolveReviewThread`, and denies opening issues, closing,
deleting, transferring or locking issues and PRs (CLI, REST and GraphQL), merging and
administration. While the review bot
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

The Ready report is the agent's summary (what changed, decisions; no CI or review-bot
status) followed by the factory's lines from the read that decided Ready: PR, CI and
`Review bot` (e.g. `Codex: 👍 on <sha>`, `Codex: reviewed <sha>, 2 findings, all
with outcomes`, or `Codex: no response within the grace window`). While the card stays in
Ready on that head, a later read that changes those lines (a late verdict, the check
summary, a red check) edits the report in place (found by its marker; edits do not
notify), never posting a second one. GitHub sends no webhook for a +1 reaction, so a
Ready card whose report still shows no bot response is re-read on each reconcile, for up
to 24 hours after the wait. A new head, withdrawal or rework never edits it. The report
ends with the [Deferred checkboxes](#follow-ups); an in-place edit keeps your ticks.

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

The outbox, admission and clock loops do not poll: each sleeps until a change is
committed to the state database (a webhook delivery, an effect enqueued or finished, a
parcel transition, an operator command), a config reload, or its own next deadline (a
deferred effect retry, a checkpoint grace or drain timeout), never re-running sooner than
`effect_poll_seconds` / `clock_interval_seconds` (the busy cadence) and never sleeping
longer than `idle_fallback_seconds` (default `30`, hot-reloadable). An idle factory makes
almost no database calls. GitHub calls share one pool of at most 4 connections, kept
alive for 120 s.

The Omnigent observer scans only open runs. A parked run, or one waiting on the owner, CI
or the review bot with its tree last seen complete and idle (Needs you, Blocked, a settled
checkpoint, plan approval, idle on checks, triage done), is read every
`parked_observation_interval_seconds` (default `300`, hot-reloadable); starting, running,
grace and draining runs keep `observation_interval_seconds` (`5`). The factory relaying
an owner's move changes the run's state, which puts it back on the active cadence; a
parked tree a scan sees busy counts as running again. All trees due in one
pass share one archive-inclusive inventory read (taken after every tree's child walk); its
full resync runs every `session_inventory_resync_hours` (default `6`, hot-reloadable). A
failed read still makes every scan of the pass incomplete, never idle.

### Health sweep and stalled runs

A run can go quiet without ever submitting a result: #891's build ended its turn waiting
on a worker and Omnigent never woke it for the worker's second turn. Idle runs accrue no
active time, so no grant ever ran out and the card sat `Working` for hours. The health
sweep catches that. Every `health_sweep_interval_minutes` it makes one `GET /health`, one
read of the configured host (`GET /v1/hosts/<id>`) and one batched
`GET /health?session_ids=...` for every open run's runner/host liveness; trees come from
the observer's latest scans (no extra reads) and the branch head from the factory clone.

**Global gate and breaker.** If `/health` fails or the host is offline, nothing is done to
any run that sweep and `/healthz` answers `degraded` with `reason` `omnigent_unhealthy` or
`host_offline`. No card note is written for an outage: it would touch every card at once
and need clearing afterwards; the card keeps its own reason. Three failed checks in a row,
a nudge whose write failed or is unconfirmed (a 5xx, a timeout) or a nudge Omnigent
answered with `runner_failed_to_start` open a breaker for `omnigent_breaker_minutes`
(`/healthz` reason `omnigent_breaker_open`): the sweep then only observes. The breaker is in
memory, so a restart closes it (three more failed checks reopen it). Nothing is ever sent
to a session whose host is not online (Omnigent would consume the message as a failed
turn).

**What it acts on.** Only the current run of a parcel, of any kind (triage, plan, build),
while it is ACTIVE: not WAITING (on the owner, checks or plan approval), DRAINING (the drain
timeout handles it), at a checkpoint, stopped or fenced; and only with no open question, no
owner or blocking hold (Needs you, Blocked, awaiting the owner, waiting on blockers),
no unconfirmed write, no owner comment queued for it, no result submitted, no queued
rework and its build slot not parked. A nudge also needs the ordinary work gate and, for
a build, a valid approval: the sweep adds no authority, approval or time.

| Case | When | What happens |
|---|---|---|
| A. Quiet without a result | Turn ended, no result, nothing new anywhere in the tree for `stall_nudge_minutes` | One nudge for this stall episode ("your turn ended without submitting a result; check your workers' latest reports and session history, then continue or report blocked"). No real progress within `stall_escalate_minutes` after it: `Needs you`, one comment and the note `Stalled: nudged <time>, no progress since`. `max_stall_nudges_per_run` nudges with no new commit or result between them: `Needs you` (`Stalled: 3 nudges without a new commit or result`). |
| B. Runner gone | `runner_online` false, host online | Quiet: as A, but the nudge (which relaunches the runner) does not count toward the run's nudge limit. Still `running` with no activity for 15 min: left to Omnigent to reconcile to idle; a `running` session is never poked. |
| C. Busy but frozen | Busy, nothing new anywhere in the tree for `stall_frozen_minutes`, no live background task | `Needs you`, one comment, note `Stalled: no activity since <time>`. No message, no interrupt. |
| D. Root failed | Root status `failed` | Unchanged: `SessionCrashed` (one restart, then Blocked). |

**Real progress** (needed for a new episode, and it clears a stalled card by itself): a
worker (any session in the tree but the root) appeared or its `updated_at`, status or
task status changed (it started, worked or reported); the run's branch head moved (a
commit, pushed or not); a result was submitted; a question was asked. A reply from the
root alone (e.g. to the nudge) is not progress. A frozen card also clears on any new
activity in its tree. An owner comment, `/continue` or a new stage resets the nudge
count; the comment is relayed to the run as usual.

Episodes live in the run's persisted state (`explain` shows them as `stall`); nudges and
escalation comments carry dedupe keys from never-reset counters
(`stall:<run>:<n>`, `stall:<run>:escalation:<n>`), so a restart or a repeated sweep never
duplicates one. At most `max_nudges_per_sweep` nudges go out per sweep.

```toml
health_sweep = true                 # the sweep (off: no checks, no nudges)
health_sweep_interval_minutes = 5
stall_nudge_minutes = 30            # quiet this long: one nudge
stall_escalate_minutes = 30         # no progress this long after a nudge: Needs you
stall_frozen_minutes = 120          # busy with nothing moving this long: Needs you
max_stall_nudges_per_run = 3        # nudges without a new commit or result: Needs you
max_nudges_per_sweep = 3
omnigent_breaker_minutes = 30       # observe-only time after Omnigent looked unhealthy
```

All are hot-reloadable (`reload`).

### State database size

The parcel aggregate (`parcels.aggregate_json`) is the state; events are history the
reducer never replays. Retention runs every 15 minutes in the daemon, in short batches:

| `[service]` key | Default | Removes |
|---|---|---|
| `observation_retention_hours` | `2` | Periodic observation events (`ReconcileDue`, `GitHubSnapshot`, delivery-keyed `ChecksChanged`, `ReadinessEvidence`, `TreeQuiescent`, cost/runtime samples, `CapacityAvailable`), their audit rows and the settled read/board-drift effects they spawned. |
| `delivery_body_retention_days` | `0.083` (2 h) | Body and headers of processed deliveries that no event, or only check/PR/review observations, references. The row and its GUID stay for duplicate detection. |
| `delivery_row_retention_days` | `14` | Rows (and delivery attempts) of processed deliveries whose body was pruned, unless an event, a parked delivery, a parcel hold or stored review comments reference them; quarantined attempts are kept. A delivery that produced no event (ignored, no transition) loses its body and headers as soon as it is processed (GUID and sha256 stay). |
| `completed_reconcile_interval_seconds` | `3600` | Not a deletion: only when no board diff is wired, a parcel with no live work is reconciled at this cadence instead of every `reconcile_interval_seconds`. |

Active time is stored rounded up to 5-minute steps (`ActiveTimeSample`, one event per
step instead of one per 10 s observer pass; 92% fewer rows on the live database), never
past the grant's end until the estimate reaches it, so a checkpoint comes on the same
pass as before and a crash can only over-count. A changed cost total (`CostSample`,
audit only; the cost backstop is the Omnigent policy) is stored at most every 5 minutes
per run.

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

A pruned delivery row is rewritten rather than emptied in place, so its page space is
reused (an emptied row left its page nearly empty until the row itself was deleted); the
WAL file is truncated back to 4 MiB after each checkpoint.

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
