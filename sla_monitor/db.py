"""SQLite schema and connection helper (autocommit mode; transactions are explicit)."""
from __future__ import annotations

import os
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS dataset_registry (
    dataset_id            TEXT PRIMARY KEY,
    dataset_name          TEXT NOT NULL,
    expected_arrival_time TEXT NOT NULL,              -- HH:MM, UTC, daily
    sla_completion_time   TEXT NOT NULL,              -- HH:MM, UTC, same calendar day
    grace_minutes         INTEGER NOT NULL CHECK (grace_minutes >= 0),
    max_staleness_minutes INTEGER NOT NULL CHECK (max_staleness_minutes > 0),
    owner                 TEXT NOT NULL,
    active_from           TEXT NOT NULL,              -- YYYY-MM-DD
    is_active             INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1))
);

-- Append-only event history. One row per distinct (dataset, expected_date, arrived_at, completed_at).
CREATE TABLE IF NOT EXISTS arrival_events (
    event_id           TEXT PRIMARY KEY,              -- sha1(dataset_id|expected_date|arrived_at|completed_at)
    dataset_id         TEXT NOT NULL REFERENCES dataset_registry(dataset_id),
    expected_date      TEXT NOT NULL,
    arrived_at         TEXT NOT NULL,
    completed_at       TEXT,
    row_count          INTEGER NOT NULL CHECK (row_count >= 0),
    source_file        TEXT NOT NULL,
    source_row_number  INTEGER NOT NULL,
    run_id             TEXT NOT NULL,
    ingested_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_key ON arrival_events(dataset_id, expected_date);

CREATE TABLE IF NOT EXISTS rejects (
    reject_id          TEXT PRIMARY KEY,              -- sha1(source_file|source_row_number)
    run_id             TEXT NOT NULL,
    source_file        TEXT NOT NULL,
    source_row_number  INTEGER NOT NULL,
    reject_reason      TEXT NOT NULL,
    reject_detail      TEXT,
    raw_record         TEXT NOT NULL,                 -- JSON array of the original fields
    rejected_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              TEXT PRIMARY KEY,
    source_file         TEXT,
    as_of               TEXT NOT NULL,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    status              TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_in             INTEGER,
    rows_loaded         INTEGER,
    rows_already_loaded INTEGER,
    rows_rejected       INTEGER,
    status_rows_changed INTEGER,
    alerts_opened       INTEGER,
    alerts_resolved     INTEGER,
    failure_reason      TEXT,
    warnings            TEXT,
    duration_seconds    REAL
);

-- Derived, current view of each (dataset, expected_date). Recomputed from arrival_events every run.
CREATE TABLE IF NOT EXISTS arrival_status (
    dataset_id           TEXT NOT NULL REFERENCES dataset_registry(dataset_id),
    expected_date        TEXT NOT NULL,
    expected_arrival_at  TEXT NOT NULL,
    arrival_deadline_at  TEXT NOT NULL,
    sla_deadline_at      TEXT NOT NULL,
    status               TEXT NOT NULL CHECK (status IN
                         ('pending','in_progress','on_time','late','sla_breached','missing')),
    first_arrived_at     TEXT,
    completed_at         TEXT,
    row_count            INTEGER,
    event_count          INTEGER NOT NULL,
    delay_minutes        INTEGER,                     -- arrival delay; for missing/pending: minutes overdue
    sla_overrun_minutes  INTEGER,
    evaluated_as_of      TEXT NOT NULL,
    run_id               TEXT NOT NULL,               -- run that last wrote this row
    PRIMARY KEY (dataset_id, expected_date)
);

-- Every status transition is kept (including the first evaluation).
CREATE TABLE IF NOT EXISTS arrival_status_history (
    dataset_id       TEXT NOT NULL,
    expected_date    TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    previous_status  TEXT,
    new_status       TEXT NOT NULL,
    changed_as_of    TEXT NOT NULL,
    PRIMARY KEY (dataset_id, expected_date, run_id)
);

CREATE TABLE IF NOT EXISTS dataset_freshness (
    dataset_id             TEXT PRIMARY KEY REFERENCES dataset_registry(dataset_id),
    last_completed_at      TEXT,
    freshness_minutes      INTEGER,
    max_staleness_minutes  INTEGER NOT NULL,
    freshness_status       TEXT NOT NULL CHECK (freshness_status IN ('fresh','stale','no_data')),
    evaluated_as_of        TEXT NOT NULL,
    run_id                 TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id         TEXT PRIMARY KEY,                -- sha1(dataset_id|alert_type|alert_key)
    dataset_id       TEXT NOT NULL,
    alert_type       TEXT NOT NULL CHECK (alert_type IN ('missing','late','sla_breached','stale')),
    alert_key        TEXT NOT NULL,                   -- expected_date, or last_completed_at for 'stale'
    severity         TEXT NOT NULL CHECK (severity IN ('critical','high','medium')),
    message          TEXT NOT NULL,
    opened_run_id    TEXT NOT NULL,
    opened_as_of     TEXT NOT NULL,
    resolved_run_id  TEXT,
    resolved_as_of   TEXT,
    resolution_note  TEXT
);
"""


def connect(path: str) -> sqlite3.Connection:
    if path != ":memory:":
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.isolation_level = None  # autocommit; we issue BEGIN/COMMIT/ROLLBACK ourselves
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
