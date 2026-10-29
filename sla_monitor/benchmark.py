"""Simple performance measurement + one optimisation, with before/after numbers.

  python -m sla_monitor.benchmark --rows 100000 --datasets 50

Measures (1) the full pipeline per stage on a generated dataset, (2) a rerun of the same file,
and (3) the event-load step implemented two ways on identical input:
    naive      : per-row "SELECT 1 ... WHERE event_id=?" followed by INSERT
    optimised  : one executemany of INSERT OR IGNORE (what the pipeline uses)
"""
from __future__ import annotations

import argparse
import os
import tempfile
import time
from datetime import date, datetime, timedelta, timezone

from . import db, ingest, publish, registry, validate
from .common import fmt_ts, parse_timestamp
from .generate_data import generate
from .pipeline import run_pipeline


def _fresh_db(path, reg_path):
    conn = db.connect(path)
    db.init_schema(conn)
    registry.load_registry(conn, reg_path)
    return conn


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=100_000)
    p.add_argument("--datasets", type=int, default=50)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args(argv)

    # --- timestamp formatting: strftime (before) vs isoformat (after, used by fmt_ts) ---
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    dts = [base + timedelta(minutes=i) for i in range(300_000)]
    t = time.perf_counter()
    old = [d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") for d in dts]
    t_old = time.perf_counter() - t
    t = time.perf_counter()
    new_ = [fmt_ts(d) for d in dts]
    t_new = time.perf_counter() - t
    assert old == new_, "fmt_ts output changed"
    print(f"OPTIMISATION 1 (found by profiling): format {len(dts):,} timestamps, identical output")
    print(f"  strftime : {t_old:.3f}s\n  isoformat: {t_new:.3f}s   -> {t_old / t_new:.1f}x faster\n")

    with tempfile.TemporaryDirectory() as tmp:
        t = time.perf_counter()
        s = generate(tmp, seed=a.seed, datasets=a.datasets, rows=a.rows, start=date(2020, 1, 1))
        gen_s = time.perf_counter() - t
        events, reg = os.path.join(tmp, "events.csv"), os.path.join(tmp, "registry.csv")
        as_of = parse_timestamp(s["as_of"])
        print(f"generated {s['rows_in']:,} rows ({a.datasets} datasets x {s['days']} days) in {gen_s:.2f}s, seed={a.seed}")

        dbp = os.path.join(tmp, "bench.db")
        r1 = run_pipeline(dbp, events, as_of, reg)
        r2 = run_pipeline(dbp, events, as_of, reg)
        print(f"\nFULL PIPELINE (status={r1.status}; loaded={r1.rows_loaded:,} rejected={r1.rows_rejected:,})")
        print(f"  {'stage':<12}{'first run (s)':>15}{'rerun (s)':>12}")
        for k in r1.stage_seconds:
            print(f"  {k:<12}{r1.stage_seconds[k]:>15.3f}{r2.stage_seconds[k]:>12.3f}")
        print(f"  {'TOTAL':<12}{r1.duration_seconds:>15.3f}{r2.duration_seconds:>12.3f}")
        print(f"  throughput (first run): {r1.rows_in / r1.duration_seconds:,.0f} rows/s; "
              f"rerun wrote {r2.status_rows_changed} status rows (unchanged rows are skipped)")

        # --- event load: naive vs optimised, identical input ---
        c0 = _fresh_db(os.path.join(tmp, "x.db"), reg)
        rules = registry.read_rules(c0)
        c0.close()
        valid = validate.validate_rows(ingest.read_events_file(events), rules, as_of).valid

        c = _fresh_db(os.path.join(tmp, "naive.db"), reg)
        t = time.perf_counter()
        c.execute("BEGIN")
        loaded = 0
        for e in valid:
            if c.execute("SELECT 1 FROM arrival_events WHERE event_id=?", (e.event_id,)).fetchone() is None:
                c.execute("INSERT INTO arrival_events VALUES (?,?,?,?,?,?,?,?,?,?)",
                          (e.event_id, e.dataset_id, e.expected_date, e.arrived_at, e.completed_at, e.row_count,
                           "events.csv", e.source_row_number, "r", "t"))
                loaded += 1
        c.execute("COMMIT")
        naive = time.perf_counter() - t
        c.close()

        c = _fresh_db(os.path.join(tmp, "opt.db"), reg)
        t = time.perf_counter()
        c.execute("BEGIN")
        new, _ = publish.insert_events(c, valid, "events.csv", "r", "t")
        c.execute("COMMIT")
        opt = time.perf_counter() - t
        c.close()
        assert new == loaded, (new, loaded)
        print(f"\nOPTIMISATION 2: event load of {len(valid):,} rows (identical result: {new:,} rows inserted)")
        print(f"  naive      (SELECT + INSERT per row) : {naive:.3f}s")
        print(f"  optimised  (executemany INSERT OR IGNORE): {opt:.3f}s   -> {naive / opt:.1f}x faster")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
