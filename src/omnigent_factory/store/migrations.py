"""Explicit, checksummed SQLite migrations (architecture §3.5).

Each migration runs in its own ``BEGIN IMMEDIATE`` transaction together with its
``schema_migrations`` row, so a failure rolls back completely. On open, every applied
migration's checksum is compared with the code's; a mismatch or a database newer than the
code refuses to open (fail closed) rather than guessing.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


BOOTSTRAP_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at_us INTEGER NOT NULL,
    checksum TEXT NOT NULL
);
"""

V1_SQL = """
CREATE TABLE deliveries (
    delivery_guid TEXT PRIMARY KEY,
    app_id INTEGER,
    installation_id INTEGER,
    event_name TEXT NOT NULL,
    action TEXT,
    headers_json TEXT NOT NULL,
    body BLOB NOT NULL,
    body_sha256 TEXT NOT NULL,
    source_time_us INTEGER,
    received_at_us INTEGER NOT NULL,
    provenance TEXT NOT NULL CHECK (provenance IN ('webhook', 'recovery')),
    status TEXT NOT NULL
        CHECK (status IN ('pending', 'processed', 'unresolved', 'rejected')),
    processed_at_us INTEGER
);

CREATE TABLE delivery_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_guid TEXT NOT NULL,
    recovery_delivery_id TEXT,
    body BLOB,
    body_sha256 TEXT NOT NULL,
    received_at_us INTEGER NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('inserted', 'duplicate', 'quarantined'))
);
CREATE INDEX ix_delivery_attempts_guid ON delivery_attempts (delivery_guid);

CREATE TABLE repositories (
    repo_id TEXT PRIMARY KEY,
    full_name TEXT,
    org_id TEXT,
    installation_id TEXT,
    project_id TEXT,
    config_digest TEXT,
    paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
    next_sequence INTEGER NOT NULL,
    open_bot_prs_json TEXT NOT NULL,
    max_building INTEGER NOT NULL,
    max_open_bot_prs INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE parcels (
    parcel_id TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL REFERENCES repositories (repo_id),
    issue_number INTEGER,
    project_item_id TEXT,
    stage TEXT,
    version INTEGER NOT NULL,
    eligibility_epoch INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    revision_pending INTEGER NOT NULL,
    current_session_id TEXT,
    current_contract_id TEXT,
    current_approval_id TEXT,
    pending_authorization_id TEXT,
    holds_json TEXT NOT NULL,
    bot TEXT NOT NULL,
    aggregate_json TEXT NOT NULL,
    updated_at_us INTEGER NOT NULL,
    UNIQUE (repo_id, issue_number),
    UNIQUE (repo_id, project_item_id)
);

CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    logical_key TEXT NOT NULL UNIQUE,
    sequence INTEGER NOT NULL UNIQUE,
    repo_id TEXT NOT NULL,
    parcel_id TEXT REFERENCES parcels (parcel_id),
    delivery_guid TEXT REFERENCES deliveries (delivery_guid),
    kind TEXT NOT NULL,
    class TEXT NOT NULL,
    actor_id INTEGER,
    provenance TEXT NOT NULL,
    source_time_us INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    accepted INTEGER NOT NULL CHECK (accepted IN (0, 1)),
    reason TEXT NOT NULL,
    applied_at_us INTEGER NOT NULL
);
CREATE INDEX ix_events_parcel ON events (parcel_id, sequence);

CREATE TABLE stage_authorizations (
    authorization_id TEXT PRIMARY KEY,
    parcel_id TEXT NOT NULL REFERENCES parcels (parcel_id),
    kind TEXT NOT NULL CHECK (kind IN ('triage', 'plan', 'build')),
    generation INTEGER NOT NULL,
    source_event_id TEXT NOT NULL REFERENCES events (event_id),
    revision INTEGER NOT NULL,
    eligibility_epoch INTEGER NOT NULL,
    approval_id TEXT,
    grant_duration_us INTEGER NOT NULL,
    cancelled INTEGER NOT NULL CHECK (cancelled IN (0, 1)),
    UNIQUE (parcel_id, generation),
    UNIQUE (source_event_id, kind)
);

CREATE TABLE stage_sessions (
    session_id TEXT PRIMARY KEY,
    parcel_id TEXT NOT NULL REFERENCES parcels (parcel_id),
    kind TEXT NOT NULL CHECK (kind IN ('triage', 'plan', 'build')),
    generation INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    authorization_id TEXT NOT NULL REFERENCES stage_authorizations (authorization_id),
    omnigent_root_id TEXT UNIQUE,
    nonce TEXT NOT NULL UNIQUE,
    lifecycle TEXT NOT NULL,
    execution_closed INTEGER NOT NULL CHECK (execution_closed IN (0, 1)),
    fence_mask INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    worktree TEXT,
    branch TEXT,
    restart_count INTEGER NOT NULL,
    correction_count INTEGER NOT NULL,
    UNIQUE (parcel_id, kind, generation, attempt)
);
-- At most one live, unfenced, open stage session per parcel (§3.5).
CREATE UNIQUE INDEX ux_stage_sessions_open_gate ON stage_sessions (parcel_id)
    WHERE execution_closed = 0 AND fence_mask = 0;

CREATE TABLE fences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES stage_sessions (session_id),
    kind TEXT NOT NULL CHECK (kind IN ('safety', 'stopped', 'revoked', 'checkpoint')),
    cause_event_id TEXT NOT NULL REFERENCES events (event_id),
    set_at_us INTEGER NOT NULL,
    cleared_at_us INTEGER,
    cleared_by_event_id TEXT REFERENCES events (event_id)
);
CREATE UNIQUE INDEX ux_fences_active ON fences (session_id, kind) WHERE cleared_at_us IS NULL;

CREATE TABLE contracts (
    contract_id TEXT PRIMARY KEY,
    parcel_id TEXT NOT NULL REFERENCES parcels (parcel_id),
    revision INTEGER NOT NULL,
    canonical TEXT NOT NULL,
    full_hash TEXT NOT NULL CHECK (length(full_hash) = 64),
    prefix TEXT NOT NULL,
    comment_id TEXT UNIQUE,
    published INTEGER NOT NULL CHECK (published IN (0, 1)),
    posted_at_us INTEGER,
    intact INTEGER NOT NULL CHECK (intact IN (0, 1)),
    superseded INTEGER NOT NULL CHECK (superseded IN (0, 1)),
    source_session_id TEXT NOT NULL REFERENCES stage_sessions (session_id),
    UNIQUE (parcel_id, revision, full_hash)
);
CREATE UNIQUE INDEX ux_contracts_current_publication ON contracts (parcel_id, revision)
    WHERE published = 1 AND superseded = 0;

CREATE TABLE approvals (
    approval_id TEXT PRIMARY KEY,
    parcel_id TEXT NOT NULL REFERENCES parcels (parcel_id),
    kind TEXT NOT NULL CHECK (kind IN ('plan', 'skip')),
    full_hash TEXT NOT NULL CHECK (length(full_hash) = 64),
    contract_id TEXT REFERENCES contracts (contract_id),
    snapshot_canonical TEXT,
    owner_id INTEGER NOT NULL,
    source_event_id TEXT NOT NULL UNIQUE REFERENCES events (event_id),
    source_time_us INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    invalidated_reason TEXT,
    invalidated_at_us INTEGER,
    CHECK (
        (kind = 'plan' AND contract_id IS NOT NULL AND snapshot_canonical IS NULL)
        OR (kind = 'skip' AND contract_id IS NULL AND snapshot_canonical IS NOT NULL)
    )
);

CREATE TABLE decisions (
    decision_id TEXT PRIMARY KEY,
    parcel_id TEXT NOT NULL REFERENCES parcels (parcel_id),
    session_id TEXT NOT NULL REFERENCES stage_sessions (session_id),
    elicitation_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    impact TEXT NOT NULL,
    status TEXT NOT NULL,
    checkpoint_prompt INTEGER NOT NULL,
    answer TEXT,
    answer_event_id TEXT REFERENCES events (event_id),
    UNIQUE (session_id, elicitation_id)
);

CREATE TABLE grants (
    grant_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES stage_sessions (session_id),
    source_event_id TEXT NOT NULL,
    duration_us INTEGER NOT NULL CHECK (duration_us > 0),
    consumed_us INTEGER NOT NULL,
    ready INTEGER NOT NULL,
    grace_deadline_us INTEGER,
    policy_generation INTEGER NOT NULL,
    is_current INTEGER NOT NULL CHECK (is_current IN (0, 1)),
    UNIQUE (source_event_id, session_id)
);
CREATE UNIQUE INDEX ux_grants_current ON grants (session_id) WHERE is_current = 1;

CREATE TABLE session_nodes (
    omnigent_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES stage_sessions (session_id),
    parent_id TEXT,
    status TEXT,
    task_state TEXT,
    prompt_state TEXT,
    last_snapshot_json TEXT,
    cursor TEXT,
    retired_at_us INTEGER
);

CREATE TABLE activity_intervals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES stage_sessions (session_id),
    node_id TEXT,
    start_us INTEGER NOT NULL,
    end_us INTEGER,
    measurement TEXT NOT NULL CHECK (measurement IN ('measured', 'inferred', 'unknown'))
);

CREATE TABLE effects (
    effect_id TEXT PRIMARY KEY,
    parcel_id TEXT REFERENCES parcels (parcel_id),
    event_id TEXT NOT NULL REFERENCES events (event_id),
    parcel_version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    target TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    retry_class TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL
        CHECK (state IN ('pending', 'claimed', 'done', 'cancelled', 'unknown', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_at_us INTEGER,
    lease_epoch INTEGER,
    claimed_by_boot TEXT,
    remote_id TEXT,
    outcome_reason TEXT,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);
CREATE INDEX ix_effects_state ON effects (state, next_at_us);

CREATE TABLE dispatch_intents (
    effect_id TEXT PRIMARY KEY REFERENCES effects (effect_id),
    session_id TEXT NOT NULL UNIQUE REFERENCES stage_sessions (session_id),
    nonce TEXT NOT NULL UNIQUE,
    request_digest TEXT NOT NULL,
    request_json TEXT NOT NULL,
    create_state TEXT NOT NULL,
    adopted_root_id TEXT UNIQUE
);

CREATE TABLE own_items (
    provider TEXT NOT NULL,
    remote_id TEXT NOT NULL,
    effect_id TEXT UNIQUE REFERENCES effects (effect_id),
    session_id TEXT,
    comment_id TEXT,
    text_digest TEXT,
    PRIMARY KEY (provider, remote_id)
);

CREATE TABLE queue (
    parcel_id TEXT PRIMARY KEY REFERENCES parcels (parcel_id),
    repo_id TEXT NOT NULL REFERENCES repositories (repo_id),
    approval_id TEXT NOT NULL UNIQUE,
    approval_sequence INTEGER NOT NULL,
    status TEXT NOT NULL
        CHECK (status IN ('QUEUED', 'RESERVED', 'HELD', 'RELEASED', 'CANCELLED')),
    reason TEXT
);

CREATE TABLE reservations (
    reservation_id TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL REFERENCES repositories (repo_id),
    parcel_id TEXT NOT NULL REFERENCES parcels (parcel_id),
    kind TEXT NOT NULL CHECK (kind IN ('building', 'open_pr')),
    episode_id TEXT NOT NULL,
    pr_number INTEGER,
    live INTEGER NOT NULL CHECK (live IN (0, 1))
);
CREATE UNIQUE INDEX ux_reservations_live ON reservations (parcel_id, kind) WHERE live = 1;

CREATE TABLE pull_requests (
    repo_id TEXT NOT NULL,
    number INTEGER NOT NULL,
    parcel_id TEXT REFERENCES parcels (parcel_id),
    branch TEXT,
    author_id INTEGER,
    open INTEGER NOT NULL,
    head_sha TEXT,
    evidence_digest TEXT,
    remediation_batches INTEGER NOT NULL DEFAULT 0,
    targeted_rechecks INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo_id, number)
);

CREATE TABLE leases (
    parcel_id TEXT PRIMARY KEY,
    boot_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    heartbeat_at_us INTEGER NOT NULL
);

CREATE TABLE timers (
    timer_id TEXT PRIMARY KEY,
    parcel_id TEXT REFERENCES parcels (parcel_id),
    session_id TEXT,
    grant_id TEXT,
    kind TEXT NOT NULL,
    deadline_us INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    fired_event_id TEXT UNIQUE
);

CREATE TABLE capabilities (
    capability_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES stage_sessions (session_id),
    secret_hash TEXT NOT NULL,
    generation INTEGER NOT NULL,
    profile TEXT NOT NULL CHECK (profile IN ('read_only', 'build')),
    enabled INTEGER NOT NULL,
    expires_at_us INTEGER,
    rotated_at_us INTEGER
);

CREATE TABLE audit (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    parcel_id TEXT,
    event_id TEXT,
    effect_id TEXT,
    before_digest TEXT,
    after_digest TEXT,
    accepted INTEGER,
    reason TEXT NOT NULL,
    detail_json TEXT,
    created_at_us INTEGER NOT NULL
);
CREATE INDEX ix_audit_parcel ON audit (parcel_id, sequence);
"""

# Task 5a durable adapter state. V1 is frozen (its checksum is verified on open).
#
# * own_sends: the Omnigent own-send/resolve ledger, written BEFORE each POST. V1's
#   own_items cannot hold a pre-acknowledgement send (remote_id is part of its primary
#   key and it has no node/kind/elicitation columns), so the ledger is keyed by effect_id
#   with a nullable, unique item_id filled in on acknowledgement or adoption. No FK to
#   effects: the adapter records only claimed effects, and the ledger must accept the
#   record even if effect rows are later archived.
# * capability_records: stage and worker capability hashes/generations (never secrets).
#   Rows are kept after revocation so generations stay monotonic across rotations. V1's
#   capabilities table is superseded (its FK to stage_sessions cannot hold a worker key
#   and its profile is not known at provisioning); it was never written and is left as
#   is. Issuance enablement is deliberately NOT stored: it is default-deny on boot.
# * worker_grants: daemon-recorded exact (worker_id, path, branch, profile) tuples.
# * parked_deliveries: operator-releasable parked delivery scope (NULL = repository-wide).
#   Replaces the Task-4 side JSON registry; parking/release commit with the delivery
#   status in one transaction. No FK so an imported legacy entry whose delivery row is
#   missing (e.g. restored database) still fails closed.
V2_SQL = """
CREATE TABLE own_sends (
    effect_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('message', 'resolve')),
    text_sha256 TEXT NOT NULL,
    elicitation_id TEXT NOT NULL,
    item_id TEXT,
    recorded_at_us INTEGER NOT NULL,
    acknowledged_at_us INTEGER
);
CREATE INDEX ix_own_sends_session ON own_sends (session_id);
CREATE UNIQUE INDEX ux_own_sends_item ON own_sends (item_id) WHERE item_id IS NOT NULL;

CREATE TABLE capability_records (
    session_key TEXT PRIMARY KEY,
    stage_session_id TEXT NOT NULL,
    worker_id TEXT,
    worker_profile TEXT CHECK (worker_profile IN ('read_only', 'build')),
    capability_id TEXT NOT NULL UNIQUE,
    secret_sha256 TEXT NOT NULL CHECK (length(secret_sha256) = 64),
    generation INTEGER NOT NULL CHECK (generation >= 1),
    path TEXT NOT NULL,
    revoked INTEGER NOT NULL CHECK (revoked IN (0, 1)),
    updated_at_us INTEGER NOT NULL,
    CHECK ((worker_id IS NULL) = (worker_profile IS NULL))
);

CREATE TABLE worker_grants (
    stage_session_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    path TEXT NOT NULL,
    branch TEXT NOT NULL,
    profile TEXT NOT NULL CHECK (profile IN ('read_only', 'build')),
    recorded_at_us INTEGER NOT NULL,
    PRIMARY KEY (stage_session_id, worker_id)
);

CREATE TABLE parked_deliveries (
    delivery_guid TEXT PRIMARY KEY,
    parcel_id TEXT,
    parked_at_us INTEGER NOT NULL
);
"""

# Task 5b resolution throttling. Retry state is durable so a daemon restart cannot
# turn a repository-less Project delivery into another GitHub API burst.
V3_SQL = """
ALTER TABLE deliveries ADD COLUMN resolution_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deliveries ADD COLUMN resolution_retry_at_us INTEGER;
"""

# Pilot: a parked delivery records why it was parked (daemon-authored text, no body).
V4_SQL = """
ALTER TABLE parked_deliveries ADD COLUMN reason TEXT;
"""

# Issue sessions and the factory MCP endpoint.
#
# * Successive stage runs share one Omnigent root, so V1's UNIQUE
#   stage_sessions.omnigent_root_id can no longer hold every run's root. It is left for
#   rows written before this migration; runs record their root in issue_root_id.
# * mcp_receipts: one row per accepted factory tool mutation, written in the same
#   transaction as its reducer event. ``receipt_key`` is the idempotency key (run, tool,
#   slot); ``request_sha256`` detects a conflicting retry.
# * mcp_plan_reads: the approved plan digest a run fetched with factory_get_plan, which a
#   build result must echo.
V5_SQL = """
ALTER TABLE stage_sessions ADD COLUMN issue_root_id TEXT;
CREATE INDEX ix_stage_sessions_issue_root ON stage_sessions (issue_root_id);

CREATE TABLE mcp_receipts (
    receipt_key TEXT PRIMARY KEY,
    parcel_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    tool TEXT NOT NULL,
    event_id TEXT NOT NULL,
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    receipt_json TEXT NOT NULL,
    created_at_us INTEGER NOT NULL
);
CREATE INDEX ix_mcp_receipts_run ON mcp_receipts (run_id);

CREATE TABLE mcp_plan_reads (
    run_id TEXT NOT NULL,
    plan_hash TEXT NOT NULL,
    read_at_us INTEGER NOT NULL,
    PRIMARY KEY (run_id, plan_hash)
);
"""

# Owner feedback reads: the newest owner-comment event (by sequence) a run has fetched
# with factory_get_feedback. A result is refused while newer owner comments are unread.
V6_SQL = """
CREATE TABLE mcp_feedback_reads (
    run_id TEXT PRIMARY KEY,
    sequence INTEGER NOT NULL,
    read_at_us INTEGER NOT NULL
);
"""

MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "initial-schema", V1_SQL),
    Migration(2, "durable-adapter-state", V2_SQL),
    Migration(3, "delivery-resolution-backoff", V3_SQL),
    Migration(4, "parked-delivery-reason", V4_SQL),
    Migration(5, "issue-sessions-and-mcp", V5_SQL),
    Migration(6, "mcp-feedback-reads", V6_SQL),
)
