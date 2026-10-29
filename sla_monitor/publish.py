"""Writes to the target tables. All functions run inside the caller's transaction."""
from __future__ import annotations

import sqlite3
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Tuple

from .common import fmt_ts, sha1_id
from .transform import FreshnessRow, StatusRow
from .validate import Event, Reject


def insert_events(conn: sqlite3.Connection, events: List[Event], source_file: str,
                  run_id: str, ingested_at: str) -> Tuple[int, int]:
    """Bulk, idempotent insert. Returns (newly_loaded, already_loaded).

    INSERT OR IGNORE on the deterministic event_id means re-running the same file inserts nothing.
    (One executemany; no per-row existence query. See benchmark.py for the measured difference.)
    """
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO arrival_events (event_id, dataset_id, expected_date, arrived_at, completed_at,"
        " row_count, source_file, source_row_number, run_id, ingested_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [(e.event_id, e.dataset_id, e.expected_date, e.arrived_at, e.completed_at, e.row_count,
          source_file, e.source_row_number, run_id, ingested_at) for e in events],
    )
    loaded = conn.total_changes - before
    return loaded, len(events) - loaded


def insert_rejects(conn: sqlite3.Connection, rejects: List[Reject], source_file: str,
                   run_id: str, rejected_at: str) -> None:
    """reject_id = sha1(source_file|row_number): a rerun of the same file does not duplicate rejects."""
    conn.executemany(
        "INSERT OR IGNORE INTO rejects (reject_id, run_id, source_file, source_row_number, reject_reason,"
        " reject_detail, raw_record, rejected_at) VALUES (?,?,?,?,?,?,?,?)",
        [(sha1_id(source_file, r.source_row_number), run_id, source_file, r.source_row_number,
          r.reason, r.detail, r.raw_record, rejected_at) for r in rejects],
    )


def load_event_tuples(conn: sqlite3.Connection) -> Iterable[Tuple[str, str, str, Optional[str], int]]:
    return conn.execute("SELECT dataset_id, expected_date, arrived_at, completed_at, row_count FROM arrival_events")


def load_last_completed(conn: sqlite3.Connection) -> Dict[str, str]:
    return {r[0]: r[1] for r in conn.execute(
        "SELECT dataset_id, MAX(completed_at) FROM arrival_events WHERE completed_at IS NOT NULL GROUP BY dataset_id")}


def min_expected_date(conn: sqlite3.Connection) -> Optional[date]:
    r = conn.execute("SELECT MIN(expected_date) FROM arrival_events").fetchone()[0]
    return date.fromisoformat(r) if r else None


def upsert_statuses(conn: sqlite3.Connection, statuses: List[StatusRow], run_id: str, as_of: datetime) -> int:
    """Write only rows that are new or whose derived values changed; keep a history row per transition.

    Returns the number of status rows written.
    """
    as_of_s = fmt_ts(as_of)
    existing = {(r[0], r[1]): r[2:] for r in conn.execute(
        "SELECT dataset_id, expected_date, expected_arrival_at, arrival_deadline_at, sla_deadline_at, status,"
        " first_arrived_at, completed_at, row_count, event_count, delay_minutes, sla_overrun_minutes"
        " FROM arrival_status")}
    prev_status = {k: v[3] for k, v in existing.items()}   # index of 'status' within r[2:]
    changed = [s for s in statuses if existing.get((s.dataset_id, s.expected_date)) != s.comparable()[2:]]
    conn.executemany(
        "INSERT INTO arrival_status (dataset_id, expected_date, expected_arrival_at, arrival_deadline_at,"
        " sla_deadline_at, status, first_arrived_at, completed_at, row_count, event_count, delay_minutes,"
        " sla_overrun_minutes, evaluated_as_of, run_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(dataset_id, expected_date) DO UPDATE SET"
        " expected_arrival_at=excluded.expected_arrival_at, arrival_deadline_at=excluded.arrival_deadline_at,"
        " sla_deadline_at=excluded.sla_deadline_at, status=excluded.status, first_arrived_at=excluded.first_arrived_at,"
        " completed_at=excluded.completed_at, row_count=excluded.row_count, event_count=excluded.event_count,"
        " delay_minutes=excluded.delay_minutes, sla_overrun_minutes=excluded.sla_overrun_minutes,"
        " evaluated_as_of=excluded.evaluated_as_of, run_id=excluded.run_id",
        [s.comparable()[:12] + (as_of_s, run_id) for s in changed],
    )
    conn.executemany(
        "INSERT OR IGNORE INTO arrival_status_history (dataset_id, expected_date, run_id, previous_status,"
        " new_status, changed_as_of) VALUES (?,?,?,?,?,?)",
        [(s.dataset_id, s.expected_date, run_id, prev_status.get((s.dataset_id, s.expected_date)), s.status, as_of_s)
         for s in changed if prev_status.get((s.dataset_id, s.expected_date)) != s.status],
    )
    return len(changed)


def replace_freshness(conn: sqlite3.Connection, rows: List[FreshnessRow], run_id: str, as_of: datetime) -> None:
    conn.execute("DELETE FROM dataset_freshness")  # fully derived snapshot, safe to rebuild
    conn.executemany(
        "INSERT INTO dataset_freshness (dataset_id, last_completed_at, freshness_minutes, max_staleness_minutes,"
        " freshness_status, evaluated_as_of, run_id) VALUES (?,?,?,?,?,?,?)",
        [(r.dataset_id, r.last_completed_at, r.freshness_minutes, r.max_staleness_minutes,
          r.freshness_status, fmt_ts(as_of), run_id) for r in rows],
    )
