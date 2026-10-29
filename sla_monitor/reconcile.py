"""Reconciliation: internal invariants + (optionally) comparison with the generator's answer key.

Exit code 0 = every check passed, 1 = at least one failed.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import List, Optional

from . import db


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


def _one(conn, sql, *args):
    return conn.execute(sql, args).fetchone()[0]


def internal_checks(conn: sqlite3.Connection) -> List[Check]:
    out: List[Check] = []

    bad = conn.execute(
        "SELECT run_id, rows_in, rows_loaded, rows_already_loaded, rows_rejected FROM pipeline_runs "
        "WHERE status='success' AND rows_in != rows_loaded + rows_already_loaded + rows_rejected").fetchall()
    n_runs = _one(conn, "SELECT COUNT(*) FROM pipeline_runs WHERE status='success'")
    out.append(Check("run_counts_balance (rows_in = loaded + already_loaded + rejected, per run)", not bad,
                     f"{n_runs} successful runs, {len(bad)} unbalanced {bad if bad else ''}".strip()))

    ev, ld = _one(conn, "SELECT COUNT(*) FROM arrival_events"), _one(
        conn, "SELECT COALESCE(SUM(rows_loaded),0) FROM pipeline_runs WHERE status='success'")
    out.append(Check("events_table_equals_sum_of_loaded", ev == ld, f"arrival_events={ev}, sum(rows_loaded)={ld}"))

    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    out.append(Check("referential_integrity (foreign_key_check)", not fk, f"{len(fk)} violations"))

    untraceable = _one(conn, "SELECT COUNT(*) FROM rejects WHERE source_file='' OR source_row_number<1 "
                             "OR reject_reason='' OR raw_record=''")
    out.append(Check("rejects_traceable (file, line, reason, raw record present)", untraceable == 0,
                     f"{_one(conn, 'SELECT COUNT(*) FROM rejects')} rejects, {untraceable} untraceable"))

    no_status = _one(conn, "SELECT COUNT(*) FROM (SELECT DISTINCT dataset_id, expected_date FROM arrival_events) e "
                           "LEFT JOIN arrival_status s USING (dataset_id, expected_date) WHERE s.status IS NULL")
    out.append(Check("every_event_date_has_a_status_row", no_status == 0, f"{no_status} without status"))

    mism = _one(conn, "SELECT COUNT(*) FROM arrival_status s WHERE s.event_count != "
                      "(SELECT COUNT(*) FROM arrival_events e WHERE e.dataset_id=s.dataset_id AND e.expected_date=s.expected_date)")
    out.append(Check("status_event_count_matches_events", mism == 0, f"{mism} mismatches"))

    orphan = _one(conn, "SELECT COUNT(*) FROM alerts a WHERE a.resolved_run_id IS NULL AND a.alert_type != 'stale' AND NOT EXISTS "
                        "(SELECT 1 FROM arrival_status s WHERE s.dataset_id=a.dataset_id AND s.expected_date=a.alert_key "
                        "AND s.status=a.alert_type)")
    out.append(Check("open_alerts_match_current_status", orphan == 0, f"{orphan} open alerts without matching status"))

    fresh = _one(conn, "SELECT COUNT(*) FROM dataset_freshness f WHERE COALESCE(f.last_completed_at,'') != COALESCE("
                       "(SELECT MAX(completed_at) FROM arrival_events e WHERE e.dataset_id=f.dataset_id),'')")
    out.append(Check("freshness_matches_latest_completed_load", fresh == 0, f"{fresh} mismatches"))
    return out


def answer_key_checks(conn: sqlite3.Connection, expected_dir: str, source_file: str) -> List[Check]:
    out: List[Check] = []
    with open(os.path.join(expected_dir, "expected_summary.json")) as fh:
        summ = json.load(fh)

    exp_status = {}
    with open(os.path.join(expected_dir, "expected_status.csv")) as fh:
        for r in csv.DictReader(fh):
            exp_status[(r["dataset_id"], r["expected_date"])] = r["expected_status"]
    act_status = {(r[0], r[1]): r[2] for r in conn.execute("SELECT dataset_id, expected_date, status FROM arrival_status")}
    diffs = [(k, exp_status.get(k), act_status.get(k)) for k in sorted(set(exp_status) | set(act_status))
             if exp_status.get(k) != act_status.get(k)]
    out.append(Check("status_matches_answer_key", not diffs,
                     f"{len(exp_status)} expected, {len(act_status)} produced, {len(diffs)} differences {diffs[:3] if diffs else ''}".strip()))

    exp_rej = {}
    with open(os.path.join(expected_dir, "expected_rejects.csv")) as fh:
        for r in csv.DictReader(fh):
            exp_rej[int(r["source_row_number"])] = r["expected_reject_reason"]
    act_rej = {r[0]: r[1] for r in conn.execute(
        "SELECT source_row_number, reject_reason FROM rejects WHERE source_file=?", (source_file,))}
    rdiff = [(k, exp_rej.get(k), act_rej.get(k)) for k in sorted(set(exp_rej) | set(act_rej)) if exp_rej.get(k) != act_rej.get(k)]
    out.append(Check("rejects_match_answer_key (line number and reason)", not rdiff,
                     f"{len(exp_rej)} expected, {len(act_rej)} produced, {len(rdiff)} differences {rdiff[:3] if rdiff else ''}".strip()))

    loaded = _one(conn, "SELECT COUNT(*) FROM arrival_events WHERE source_file=?", source_file)
    out.append(Check("rows_loaded_matches_answer_key", loaded == summ["rows_loaded"],
                     f"loaded={loaded}, expected={summ['rows_loaded']}"))
    sm = _one(conn, "SELECT COALESCE(SUM(row_count),0) FROM arrival_events WHERE source_file=?", source_file)
    out.append(Check("sum_row_count_matches_answer_key (key aggregate)", sm == summ["sum_row_count_loaded"],
                     f"sum={sm}, expected={summ['sum_row_count_loaded']}"))
    return out


def render(checks: List[Check]) -> str:
    lines = ["RECONCILIATION REPORT", "=" * 21]
    for c in checks:
        lines.append(f"[{'PASS' if c.passed else 'FAIL'}] {c.name}\n       {c.detail}")
    ok = sum(c.passed for c in checks)
    lines.append(f"\n{ok}/{len(checks)} checks passed")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Reconcile pipeline output.")
    p.add_argument("--db", default="output/sla_monitor.db")
    p.add_argument("--expected", help="answer-key directory (data/sample/expected)")
    p.add_argument("--source-file", default="events.csv", help="file name the answer key belongs to")
    p.add_argument("--out", help="also write the report to this file")
    a = p.parse_args(argv)
    conn = db.connect(a.db)
    checks = internal_checks(conn)
    if a.expected:
        checks += answer_key_checks(conn, a.expected, a.source_file)
    text = render(checks)
    print(text)
    if a.out:
        with open(a.out, "w") as fh:
            fh.write(text + "\n")
    return 0 if all(c.passed for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
