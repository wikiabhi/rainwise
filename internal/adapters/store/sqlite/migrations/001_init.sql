CREATE TABLE settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE bookmark_state (
    id                 TEXT PRIMARY KEY,
    known_collection   TEXT NOT NULL,
    known_tags         TEXT NOT NULL,
    pin_source         TEXT NOT NULL,
    pinned_at          TEXT,
    input_hash         TEXT NOT NULL,
    taxonomy_version   INTEGER NOT NULL,
    model              TEXT NOT NULL,
    status             TEXT NOT NULL,
    pending_collection TEXT,
    pending_tags       TEXT NOT NULL,
    fetch_attempts     INTEGER NOT NULL,
    processed_at       TEXT,
    restored_at        TEXT
);

CREATE TABLE events (
    id           TEXT PRIMARY KEY,
    dedup_key    TEXT NOT NULL UNIQUE,
    payload      TEXT NOT NULL,
    status       TEXT NOT NULL,
    attempts     INTEGER NOT NULL,
    next_attempt TEXT NOT NULL,
    received_at  TEXT NOT NULL,
    claimed      INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX events_due ON events (status, next_attempt);

CREATE TABLE jobs (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    status          TEXT NOT NULL,
    provider        TEXT NOT NULL,
    taxonomy_id     TEXT NOT NULL,
    dry_run         INTEGER NOT NULL,
    params          TEXT NOT NULL,
    scope           TEXT NOT NULL,
    from_dry_run    TEXT NOT NULL,
    snapshot_id     TEXT NOT NULL,
    idempotency_key TEXT,
    error           TEXT NOT NULL,
    progress        TEXT NOT NULL,
    checkpoint      TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT
);
CREATE UNIQUE INDEX jobs_idempotency ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_status ON jobs (status, provider);

CREATE TABLE job_decisions (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id   TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    bookmark TEXT NOT NULL,
    body     TEXT NOT NULL,
    UNIQUE (job_id, bookmark)
);

CREATE TABLE snapshots (
    id                  TEXT PRIMARY KEY,
    provider            TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    collections         TEXT NOT NULL,
    created_collections TEXT NOT NULL,
    scope_all           INTEGER NOT NULL
);

CREATE TABLE snapshot_items (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id TEXT NOT NULL REFERENCES snapshots (id) ON DELETE CASCADE,
    bookmark    TEXT NOT NULL,
    collection  TEXT NOT NULL,
    tags        TEXT NOT NULL
);
CREATE INDEX snapshot_items_snap ON snapshot_items (snapshot_id, seq);

CREATE TABLE taxonomies (
    id      TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    status  TEXT NOT NULL,
    body    TEXT NOT NULL,
    proposals TEXT NOT NULL
);
