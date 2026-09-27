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
| `src/omnigent_factory/core/` | Pure state kernel, no I/O: domain types (`types.py`), event catalog (`events.py`), effect intents and adapter outcomes (`effects.py`), the reducer `transition(State, Event)` (`reducer.py`), authorization predicates (`predicates.py`), effect precondition re-check (`preconditions.py`), board projection and admission (`projection.py`), contract/waiver canonicalization and hashing (`canonical.py`), the frozen stage-result protocol v1 (`protocol.py`, `result_schema_v1.json`), active-time union (`accounting.py`) and the JSON codec (`codec.py`). |
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
| `doctor` | Checks config, secrets, GitHub/Omnigent reachability, Omnigent login expiry (fails when expired, warns within 7 days) and server/client version drift (warning only). |
| `explain <parcel>` | The parcel's persisted state: stage, bot, sessions, holds, effects. |
| `recovery` | Failed/unknown effects and parked webhook deliveries. |
| `retry-effect <effect_id>` | Requeue a failed/unknown `publish_triage`/`publish_report`/`post_comment`; it adopts an existing comment by its marker, so it never duplicates. |
| `rerender-comment <effect_id>` | Re-render a published comment in place (found by its marker). |
| `resume <parcel> --message-file note.md` | Re-open the parcel's existing stage session after a stale block and relay one operator note. No authority, approval or time is added. |
| `cleanup <parcel> [--merged]` | Remove a finished parcel's factory worktree(s) and local `factory/` branch from the factory clone. |
| `release-delivery <guid>` | Release one parked webhook delivery for processing. |
| `pause` / `unpause` | Stop / resume admitting new work repository-wide; in-flight parcels and safety events carry on. |

A triage/report/status publication that failed definitively leaves the card at
`Bot: Blocked`. Fix the cause, then run `recovery` and `retry-effect <effect_id>`.

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
