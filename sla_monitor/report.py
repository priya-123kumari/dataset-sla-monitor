"""Export result tables as CSV and print/write a human-readable SLA summary."""
from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import List, Optional

from . import db

TABLES = {
    "arrival_status.csv": "SELECT * FROM arrival_status ORDER BY dataset_id, expected_date",
    "dataset_freshness.csv": "SELECT * FROM dataset_freshness ORDER BY dataset_id",
    "alerts.csv": "SELECT * FROM alerts ORDER BY opened_as_of, severity, dataset_id, alert_key",
    "rejects.csv": "SELECT * FROM rejects ORDER BY source_file, source_row_number",
    "pipeline_runs.csv": "SELECT * FROM pipeline_runs ORDER BY started_at",
    "arrival_status_history.csv": "SELECT * FROM arrival_status_history ORDER BY dataset_id, expected_date, changed_as_of",
}


def export_csvs(conn, out_dir: str) -> List[str]:
    os.makedirs(out_dir, exist_ok=True)
    paths = []
    for name, sql in TABLES.items():
        cur = conn.execute(sql)
        path = os.path.join(out_dir, name)
        with open(path, "w", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow([d[0] for d in cur.description])
            w.writerows(cur)
        paths.append(path)
    return paths


def summary_text(conn) -> str:
    L = []
    run = conn.execute("SELECT run_id, source_file, as_of, status, rows_in, rows_loaded, rows_already_loaded, "
                       "rows_rejected, alerts_opened FROM pipeline_runs ORDER BY started_at DESC LIMIT 1").fetchone()
    if run:
        L += ["LATEST RUN", f"  run_id={run[0]} file={run[1]} as_of={run[2]} status={run[3]}",
              f"  rows_in={run[4]} loaded={run[5]} already_loaded={run[6]} rejected={run[7]} alerts_opened={run[8]}", ""]
    L.append("SLA COMPLIANCE BY DATASET (expected dates evaluated)")
    L.append(f"  {'dataset':<8}{'name':<20}{'days':>5}{'on_time':>9}{'late':>6}{'breach':>8}{'missing':>9}{'on_time_%':>11}")
    for r in conn.execute(
            "SELECT s.dataset_id, r.dataset_name, COUNT(*), SUM(status='on_time'), SUM(status='late'),"
            " SUM(status='sla_breached'), SUM(status='missing') FROM arrival_status s "
            "JOIN dataset_registry r USING(dataset_id) GROUP BY s.dataset_id ORDER BY s.dataset_id"):
        L.append(f"  {r[0]:<8}{r[1]:<20}{r[2]:>5}{r[3]:>9}{r[4]:>6}{r[5]:>8}{r[6]:>9}{100.0 * r[3] / r[2]:>10.1f}%")
    L += ["", "DATASET FRESHNESS"]
    for r in conn.execute("SELECT dataset_id, last_completed_at, freshness_minutes, max_staleness_minutes, freshness_status "
                          "FROM dataset_freshness ORDER BY dataset_id"):
        L.append(f"  {r[0]}  last_completed={r[1]}  age={r[2]}min (limit {r[3]})  {r[4]}")
    L += ["", "OPEN ALERTS"]
    rows = conn.execute("SELECT severity, message FROM alerts WHERE resolved_run_id IS NULL ORDER BY "
                        "CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END, dataset_id, alert_key").fetchall()
    L += [f"  {m}" for _, m in rows] or ["  (none)"]
    L += ["", "REJECTS BY REASON"]
    for r in conn.execute("SELECT reject_reason, COUNT(*) FROM rejects GROUP BY 1 ORDER BY 2 DESC, 1"):
        L.append(f"  {r[0]:<28}{r[1]:>5}")
    return "\n".join(L)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Print SLA summary and export result CSVs.")
    p.add_argument("--db", default="output/sla_monitor.db")
    p.add_argument("--out-dir", help="export CSVs + sla_summary.txt here")
    a = p.parse_args(argv)
    conn = db.connect(a.db)
    text = summary_text(conn)
    print(text)
    if a.out_dir:
        export_csvs(conn, a.out_dir)
        with open(os.path.join(a.out_dir, "sla_summary.txt"), "w") as fh:
            fh.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
