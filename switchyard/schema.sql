PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    spec TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pools (
    name TEXT PRIMARY KEY,
    capacity INTEGER NOT NULL CHECK (capacity >= 1)
);
CREATE TABLE IF NOT EXISTS jobs (
    run_id TEXT NOT NULL REFERENCES runs(id),
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    pool TEXT NOT NULL REFERENCES pools(name),
    max_attempts INTEGER NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'waiting',
    ready_at REAL NOT NULL DEFAULT 0,
    owner TEXT,
    token TEXT,
    expires REAL,
    result TEXT,
    error TEXT,
    PRIMARY KEY(run_id, name)
);
CREATE TABLE IF NOT EXISTS edges (
    run_id TEXT NOT NULL,
    parent TEXT NOT NULL,
    child TEXT NOT NULL,
    PRIMARY KEY(run_id, parent, child),
    FOREIGN KEY(run_id, parent) REFERENCES jobs(run_id, name),
    FOREIGN KEY(run_id, child) REFERENCES jobs(run_id, name)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    job TEXT,
    kind TEXT NOT NULL,
    at REAL NOT NULL,
    detail TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS runnable ON jobs(state, ready_at);
CREATE INDEX IF NOT EXISTS inflight ON jobs(pool, state, expires);
CREATE INDEX IF NOT EXISTS upstream ON edges(run_id, child);
CREATE INDEX IF NOT EXISTS history ON events(run_id, id);
