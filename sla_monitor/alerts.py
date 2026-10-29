"""Alert generation and lifecycle.

alert_id = sha1(dataset_id | alert_type | alert_key) so re-running never duplicates an alert.
  * Opened  : status in (missing, late, sla_breached) for expected_date within the lookback window,
              or a dataset whose freshness is 'stale'.
  * Resolved: the condition no longer holds on a later run (e.g. a missing load arrived late).
              Resolution is recorded, the alert row is never deleted.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Iterable, Tuple

from .common import fmt_ts, sha1_id
from .registry import DatasetRule
from .transform import ALERT_STATUSES, FreshnessRow, StatusRow

SEVERITY = {"missing": "critical", "sla_breached": "high", "stale": "high", "late": "medium"}


def _fmt_minutes(m: int | None) -> str:
    if m is None:
        return "n/a"
    return f"{m // 60}h{m % 60:02d}m" if m >= 60 else f"{m}m"


def _status_message(row: StatusRow, rule: DatasetRule, as_of: datetime) -> str:
    base = f"{row.dataset_id} ({rule.dataset_name}) for {row.expected_date}"
    if row.status == "missing":
        return (f"[CRITICAL] {base}: no arrival; expected by {row.arrival_deadline_at}, overdue "
                f"{_fmt_minutes(row.delay_minutes)} as of {fmt_ts(as_of)}. Owner: {rule.owner}. "
                f"Action: check the upstream producer/job and the drop location; re-run the pipeline once the file lands.")
    if row.status == "sla_breached":
        done = f"completed {row.completed_at}" if row.completed_at else "not completed"
        return (f"[HIGH] {base}: SLA breached by {_fmt_minutes(row.sla_overrun_minutes)} "
                f"(deadline {row.sla_deadline_at}, {done}). Owner: {rule.owner}. "
                f"Action: check the load job for slowness or failure; notify downstream consumers.")
    return (f"[MEDIUM] {base}: arrived {_fmt_minutes(row.delay_minutes)} after expected time "
            f"(first arrival {row.first_arrived_at}, grace deadline {row.arrival_deadline_at}). Owner: {rule.owner}. "
            f"Action: review producer schedule if this repeats.")


def _stale_message(f: FreshnessRow, rule: DatasetRule) -> str:
    return (f"[HIGH] {f.dataset_id} ({rule.dataset_name}): data is stale, last completed load "
            f"{f.last_completed_at}, {_fmt_minutes(f.freshness_minutes)} old (limit "
            f"{_fmt_minutes(f.max_staleness_minutes)}). Owner: {rule.owner}. "
            f"Action: confirm the producer is running; consumers are reading old data.")


def sync_alerts(conn: sqlite3.Connection, statuses: Iterable[StatusRow], freshness: Iterable[FreshnessRow],
                rules: Dict[str, DatasetRule], run_id: str, as_of: datetime, lookback_days: int) -> Tuple[int, int]:
    """Open new alerts, resolve cleared ones. Returns (opened, resolved)."""
    window_start = (as_of - timedelta(days=lookback_days)).date().isoformat()
    as_of_s = fmt_ts(as_of)

    condition_now: Dict[str, tuple] = {}   # alert_id -> row for ALL currently true conditions
    for s in statuses:
        if s.status in ALERT_STATUSES:
            condition_now[sha1_id(s.dataset_id, s.status, s.expected_date)] = ("status", s)
    for f in freshness:
        if f.freshness_status == "stale":
            condition_now[sha1_id(f.dataset_id, "stale", f.last_completed_at)] = ("stale", f)

    existing = {r[0]: r[1] for r in conn.execute("SELECT alert_id, resolved_run_id FROM alerts")}
    opened = 0
    to_insert, to_reopen = [], []
    for aid, (kind, row) in condition_now.items():
        if kind == "status":
            if row.expected_date < window_start:
                continue  # only raise new alerts for recent dates (old ones are history, not news)
            rule = rules[row.dataset_id]
            rec = (aid, row.dataset_id, row.status, row.expected_date, SEVERITY[row.status],
                   _status_message(row, rule, as_of), run_id, as_of_s)
        else:
            rule = rules[row.dataset_id]
            rec = (aid, row.dataset_id, "stale", row.last_completed_at, SEVERITY["stale"],
                   _stale_message(row, rule), run_id, as_of_s)
        if aid not in existing:
            to_insert.append(rec)
        elif existing[aid] is not None:       # was resolved, condition is back
            to_reopen.append((rec[5], run_id, as_of_s, aid))
    conn.executemany(
        "INSERT INTO alerts (alert_id, dataset_id, alert_type, alert_key, severity, message, opened_run_id, opened_as_of)"
        " VALUES (?,?,?,?,?,?,?,?)", to_insert)
    conn.executemany(
        "UPDATE alerts SET message=?, opened_run_id=?, opened_as_of=?, resolved_run_id=NULL, resolved_as_of=NULL,"
        " resolution_note=NULL WHERE alert_id=?", to_reopen)
    opened = len(to_insert) + len(to_reopen)

    # Resolve open alerts whose condition no longer holds.
    open_ids = [r[0] for r in conn.execute("SELECT alert_id FROM alerts WHERE resolved_run_id IS NULL")]
    to_resolve = [(run_id, as_of_s, "condition no longer true as of this run", aid)
                  for aid in open_ids if aid not in condition_now]
    conn.executemany(
        "UPDATE alerts SET resolved_run_id=?, resolved_as_of=?, resolution_note=? WHERE alert_id=?", to_resolve)
    return opened, len(to_resolve)
