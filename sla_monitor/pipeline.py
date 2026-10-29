"""Pipeline orchestration + CLI.

One invocation = one run = one input file = one database transaction.
If any step fails the transaction is rolled back (no partial loads), the failure is recorded in
pipeline_runs, and the process exits non-zero. A retry is simply running the same command again.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional

from . import alerts, db, ingest, publish, registry, transform, validate
from .common import fmt_ts, make_run_id, parse_date, parse_timestamp, utc_now

log = logging.getLogger("sla_monitor")


@dataclass
class RunResult:
    run_id: str
    status: str
    source_file: str
    as_of: str
    rows_in: int = 0
    rows_loaded: int = 0
    rows_already_loaded: int = 0
    rows_rejected: int = 0
    status_rows_changed: int = 0
    alerts_opened: int = 0
    alerts_resolved: int = 0
    failure_reason: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    stage_seconds: Dict[str, float] = field(default_factory=dict)
    duration_seconds: float = 0.0


def run_pipeline(db_path: str, input_path: str, as_of: datetime, registry_path: Optional[str] = None,
                 run_id: Optional[str] = None, start_date: Optional[date] = None,
                 alert_lookback_days: int = 3) -> RunResult:
    t_run = time.perf_counter()
    started = utc_now()
    run_id = run_id or make_run_id(started)
    source_file = os.path.basename(input_path)
    conn = db.connect(db_path)
    db.init_schema(conn)
    res = RunResult(run_id=run_id, status="running", source_file=source_file, as_of=fmt_ts(as_of))
    # Recorded outside the data transaction so a crash still leaves evidence of the attempt.
    conn.execute("INSERT INTO pipeline_runs (run_id, source_file, as_of, started_at, status) VALUES (?,?,?,?, 'running')",
                 (run_id, source_file, res.as_of, fmt_ts(started)))
    log.info("run_id=%s stage=start source_file=%s as_of=%s", run_id, source_file, res.as_of)

    def stage(name: str, t0: float) -> None:
        res.stage_seconds[name] = round(time.perf_counter() - t0, 4)

    try:
        conn.execute("BEGIN")
        t = time.perf_counter()
        if registry_path:
            n = registry.load_registry(conn, registry_path)
            log.info("run_id=%s stage=registry rows=%d", run_id, n)
        rules = registry.read_rules(conn)
        if not rules:
            raise registry.RegistryError("registry is empty: pass --registry on the first run")
        stage("registry", t)

        t = time.perf_counter()
        parsed = ingest.read_events_file(input_path)
        res.warnings.extend(parsed.warnings)
        res.rows_in = len(parsed.rows)
        stage("read", t)

        t = time.perf_counter()
        vr = validate.validate_rows(parsed, rules, as_of)
        res.rows_rejected = len(vr.rejects)
        stage("validate", t)

        t = time.perf_counter()
        now_s = fmt_ts(utc_now())
        res.rows_loaded, res.rows_already_loaded = publish.insert_events(conn, vr.valid, source_file, run_id, now_s)
        publish.insert_rejects(conn, vr.rejects, source_file, run_id, now_s)
        stage("load", t)

        t = time.perf_counter()
        arrivals = transform.canonical_arrivals(publish.load_event_tuples(conn))
        start = start_date or publish.min_expected_date(conn) or as_of.date()
        statuses = transform.build_statuses(rules, arrivals, as_of, start)
        freshness = transform.build_freshness(rules, publish.load_last_completed(conn), as_of)
        stage("transform", t)

        t = time.perf_counter()
        res.status_rows_changed = publish.upsert_statuses(conn, statuses, run_id, as_of)
        publish.replace_freshness(conn, freshness, run_id, as_of)
        stage("publish", t)

        t = time.perf_counter()
        res.alerts_opened, res.alerts_resolved = alerts.sync_alerts(
            conn, statuses, freshness, rules, run_id, as_of, alert_lookback_days)
        stage("alerts", t)

        res.status = "success"
        res.duration_seconds = round(time.perf_counter() - t_run, 4)
        _finish(conn, res)
        conn.execute("COMMIT")
        log.info("run_id=%s stage=done status=success rows_in=%d loaded=%d already_loaded=%d rejected=%d "
                 "status_rows_changed=%d alerts_opened=%d alerts_resolved=%d duration_s=%.3f", run_id, res.rows_in,
                 res.rows_loaded, res.rows_already_loaded, res.rows_rejected, res.status_rows_changed,
                 res.alerts_opened, res.alerts_resolved, res.duration_seconds)
    except Exception as exc:  # noqa: BLE001 - any failure must roll back and be recorded
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        res.status = "failed"
        res.failure_reason = f"{type(exc).__name__}: {exc}"
        # Nothing was loaded; zero the counters so the run row is not misleading.
        res.rows_loaded = res.rows_already_loaded = res.status_rows_changed = 0
        res.alerts_opened = res.alerts_resolved = 0
        res.duration_seconds = round(time.perf_counter() - t_run, 4)
        _finish(conn, res)
        log.error("run_id=%s stage=failed reason=%s", run_id, res.failure_reason)
    finally:
        conn.close()
    return res


def _finish(conn, res: RunResult) -> None:
    conn.execute(
        "UPDATE pipeline_runs SET finished_at=?, status=?, rows_in=?, rows_loaded=?, rows_already_loaded=?,"
        " rows_rejected=?, status_rows_changed=?, alerts_opened=?, alerts_resolved=?, failure_reason=?,"
        " warnings=?, duration_seconds=? WHERE run_id=?",
        (fmt_ts(utc_now()), res.status, res.rows_in, res.rows_loaded, res.rows_already_loaded, res.rows_rejected,
         res.status_rows_changed, res.alerts_opened, res.alerts_resolved, res.failure_reason,
         "; ".join(res.warnings) or None, res.duration_seconds, res.run_id))


def _load_config(path: Optional[str]) -> dict:
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def main(argv: Optional[List[str]] = None) -> int:
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", default="config.json")
    known, _ = pre.parse_known_args(argv)
    cfg = _load_config(known.config)

    p = argparse.ArgumentParser(description="Load a dataset-arrival events file and refresh SLA status/alerts.",
                                parents=[pre])
    p.add_argument("--input", required=True, help="events CSV to ingest")
    p.add_argument("--registry", help="dataset registry CSV (upserted each run; optional after the first run)")
    p.add_argument("--db", default=cfg.get("db_path", "output/sla_monitor.db"))
    p.add_argument("--as-of", help="evaluation time, ISO-8601 (default: now, UTC)")
    p.add_argument("--start-date", help="first expected_date to evaluate (default: earliest event date)")
    p.add_argument("--alert-lookback-days", type=int, default=cfg.get("alert_lookback_days", 3))
    p.add_argument("--run-id", help="override the generated run_id")
    p.add_argument("--quiet", action="store_true", help="suppress log lines, print only the JSON summary")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.WARNING if args.quiet else logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(message)s")
    as_of = parse_timestamp(args.as_of) if args.as_of else utc_now()
    if as_of is None:
        p.error(f"--as-of is not a valid ISO-8601 timestamp: {args.as_of!r}")
    start = parse_date(args.start_date) if args.start_date else None
    if args.start_date and start is None:
        p.error("--start-date must be YYYY-MM-DD")

    res = run_pipeline(args.db, args.input, as_of, args.registry, args.run_id, start, args.alert_lookback_days)
    print(json.dumps(asdict(res), indent=2))
    return 0 if res.status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
