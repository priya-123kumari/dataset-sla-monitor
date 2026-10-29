"""Seeded synthetic data generator.

Writes
  <out>/registry.csv                  dataset registry
  <out>/events.csv                    source events (shuffled, so out-of-order), with injected problems
  <out>/expected/expected_status.csv  answer key: status per (dataset, expected_date)
  <out>/expected/expected_rejects.csv answer key: line number -> reject reason
  <out>/expected/expected_summary.json  answer key: counts, sums and the as_of to use

The answer key is derived BY CONSTRUCTION: each (dataset, day) is assigned a scenario that decides
the timestamps, so the expected status is known without calling any pipeline code. The key lives in
a separate folder and is never read by the pipeline.

Same --seed => byte-identical output.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

HEADER = ["dataset_id", "expected_date", "arrived_at", "completed_at", "row_count"]
ARRIVAL_TIMES = ["02:00", "04:30", "06:00", "08:00"]
SLA_AFTER_EXPECTED_MIN = 120
NAMES = ["orders", "customers", "payments", "inventory", "shipments", "returns", "web_sessions", "ad_spend",
         "products", "suppliers", "invoices", "tickets", "campaigns", "refunds", "pricing", "reviews"]

# scenario -> weight.  (expected status by construction)
SCENARIOS = {
    "on_time": 0.60, "late": 0.10, "sla_breached": 0.07, "never_completed": 0.03, "missing": 0.06,
    "boundary_on_time": 0.04, "boundary_late": 0.04, "sla_boundary_ok": 0.03, "cross_midnight": 0.03,
}
EXPECTED = {"on_time": "on_time", "late": "late", "sla_breached": "sla_breached", "never_completed": "sla_breached",
            "missing": "missing", "boundary_on_time": "on_time", "boundary_late": "late",
            "sla_boundary_ok": "on_time", "cross_midnight": "sla_breached"}


def _z(dt: datetime, rng: random.Random) -> str:
    """Mostly UTC 'Z'; sometimes +05:30 local time (same instant) to exercise offset normalisation."""
    if rng.random() < 0.05:
        return dt.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%dT%H:%M:%S+05:30")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _pick_scenario(rng: random.Random) -> str:
    return rng.choices(list(SCENARIOS), weights=list(SCENARIOS.values()))[0]


def generate(out_dir: str, seed: int = 42, datasets: int = 5, days: int = 8, start: date = date(2026, 10, 1),
             rows: Optional[int] = None) -> Dict:
    rng = random.Random(seed)
    if rows:
        days = max(1, math.ceil(rows / (datasets * 1.02)))
    os.makedirs(os.path.join(out_dir, "expected"), exist_ok=True)
    as_of = datetime(start.year, start.month, start.day, 23, 0, tzinfo=timezone.utc) + timedelta(days=days - 1)

    # ---- registry ----
    reg = []
    for i in range(datasets):
        h, m = map(int, ARRIVAL_TIMES[i % len(ARRIVAL_TIMES)].split(":"))
        sla = h * 60 + m + SLA_AFTER_EXPECTED_MIN
        reg.append({
            "dataset_id": f"ds_{i + 1:03d}",
            "dataset_name": f"daily_{NAMES[i % len(NAMES)]}" + ("" if i < len(NAMES) else f"_{i // len(NAMES)}"),
            "expected_arrival_time": f"{h:02d}:{m:02d}",
            "sla_completion_time": f"{sla // 60:02d}:{sla % 60:02d}",
            "grace_minutes": rng.choice([15, 30]),
            "max_staleness_minutes": 1560,
            "owner": f"team_{'abcd'[i % 4]}@example.com",
            "active_from": start.isoformat(),
            "is_active": 1,
        })
    with open(os.path.join(out_dir, "registry.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(reg[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(reg)

    # ---- scenario per (dataset, day) ----
    cells = [(r, start + timedelta(days=d)) for r in reg for d in range(days)]
    last_day = start + timedelta(days=days - 1)
    assign = {(r["dataset_id"], d): _pick_scenario(rng) for r, d in cells}
    # cross_midnight needs a following day inside as_of
    for (ds, d), sc in list(assign.items()):
        if sc == "cross_midnight" and d == last_day:
            assign[(ds, d)] = "on_time"
    keys = list(assign)
    for sc in SCENARIOS:  # guarantee every scenario appears at least once (small samples)
        if sc not in assign.values():
            cands = [k for k in keys if assign[k] == "on_time" and not (sc == "cross_midnight" and k[1] == last_day)]
            if cands:
                assign[rng.choice(cands)] = sc

    # ---- valid rows ----
    out_rows: List[Tuple[List[str], dict]] = []   # (fields, meta)
    expected_status = []
    total_valid_sum = 0
    for r, d in cells:
        sc = assign[(r["dataset_id"], d)]
        expected_status.append((r["dataset_id"], d.isoformat(), EXPECTED[sc]))
        if sc == "missing":
            continue
        eh, em = map(int, r["expected_arrival_time"].split(":"))
        E = datetime(d.year, d.month, d.day, eh, em, tzinfo=timezone.utc)
        g = r["grace_minutes"]
        S = E + timedelta(minutes=SLA_AFTER_EXPECTED_MIN)
        m = timedelta(minutes=1)
        if sc == "on_time":
            a = E + rng.randint(-20, g) * m; c = a + rng.randint(5, 40) * m
        elif sc == "boundary_on_time":
            a = E + g * m; c = a + 10 * m
        elif sc == "boundary_late":
            a = E + (g + 1) * m; c = a + 10 * m
        elif sc == "late":
            a = E + (g + rng.randint(2, 50)) * m; c = a + rng.randint(5, 20) * m
        elif sc == "sla_boundary_ok":
            a = E; c = S
        elif sc == "sla_breached":
            a = E + rng.randint(0, g) * m; c = S + rng.randint(1, 180) * m
        elif sc == "never_completed":
            a = E + rng.randint(0, g) * m; c = None
        else:  # cross_midnight: arrives after midnight, for yesterday's expectation
            nxt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) + timedelta(days=1)
            a = nxt + rng.randint(30, 180) * m; c = a + rng.randint(10, 60) * m
        rc = rng.randint(1000, 50000)
        if rng.random() < 0.01:
            rc = 0  # boundary: empty load is valid
        base = [r["dataset_id"], d.isoformat(), _z(a, rng), _z(c, rng) if c else "", str(rc)]
        key = (r["dataset_id"], d, a, c)
        variants = [(base, key, rc, True)]
        if c is not None and rng.random() < 0.04 and sc != "cross_midnight":
            # "in progress" event followed by "completed" event for the SAME arrival -> one load
            variants = [([base[0], base[1], base[2], "", base[4]], (r["dataset_id"], d, a, None), rc, True),
                        (base, key, rc, True)]
        if rng.random() < 0.03:
            # re-load later the same day: kept as history, must not change the status
            a2 = a + rng.randint(180, 240) * m
            c2 = a2 + rng.randint(5, 40) * m
            variants.append(([r["dataset_id"], d.isoformat(), _z(a2, rng), _z(c2, rng), str(rng.randint(1000, 50000))],
                             (r["dataset_id"], d, a2, c2), 0, True))
        for fields, k, _rc, _v in variants:
            out_rows.append((fields, {"kind": "valid", "key": k, "rc": int(fields[4])}))

    # ---- exact duplicates ----
    valid_rows = [x for x in out_rows if x[1]["kind"] == "valid"]
    for _ in range(max(1, round(0.015 * len(valid_rows)))):
        src = rng.choice(valid_rows)
        out_rows.append((list(src[0]), {"kind": "valid", "key": src[1]["key"], "rc": src[1]["rc"]}))

    # ---- invalid rows (each a single, labelled problem; never affects expected status) ----
    def fresh_base() -> List[str]:
        r = rng.choice(reg)
        d = start + timedelta(days=rng.randrange(days))
        eh, em = map(int, r["expected_arrival_time"].split(":"))
        a = datetime(d.year, d.month, d.day, eh, em, tzinfo=timezone.utc) + rng.randint(0, 10) * timedelta(minutes=1)
        return [r["dataset_id"], d.isoformat(), a.strftime("%Y-%m-%dT%H:%M:%SZ"),
                (a + timedelta(minutes=20)).strftime("%Y-%m-%dT%H:%M:%SZ"), str(rng.randint(1000, 5000))]

    future = (as_of + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    mutators = {
        "missing_required_field": [lambda b: ["", *b[1:]], lambda b: [b[0], b[1], "", b[3], b[4]],
                                   lambda b: [*b[:4], ""], lambda b: [b[0], "", *b[2:]]],
        "invalid_date": [lambda b: [b[0], "2026-13-45", *b[2:]], lambda b: [b[0], "01/10/2026", *b[2:]]],
        "invalid_timestamp": [lambda b: [b[0], b[1], "not-a-time", b[3], b[4]],
                              lambda b: [b[0], b[1], b[1], b[3], b[4]],       # date only, no time part
                              lambda b: [b[0], b[1], b[2], "25:99", b[4]]],
        "invalid_row_count": [lambda b: [*b[:4], "abc"], lambda b: [*b[:4], "-5"], lambda b: [*b[:4], "12.5"]],
        "completed_before_arrived": [lambda b: [b[0], b[1], b[3], b[2], b[4]]],
        "timestamp_in_future": [lambda b: [b[0], b[1], future, "", b[4]]],
        "unknown_dataset": [lambda b: ["ds_999", *b[1:]], lambda b: ["DS_001", *b[1:]]],
        "malformed_row": [lambda b: b[:3], lambda b: [*b, "extra", "fields"]],
    }
    n_inv = max(1, round(0.004 * len(valid_rows)))
    for reason, muts in mutators.items():
        for i in range(n_inv):
            out_rows.append((muts[i % len(muts)](fresh_base()), {"kind": "invalid", "reason": reason}))

    rng.shuffle(out_rows)  # out-of-order delivery

    # ---- write events + derive row-level labels from the FINAL order ----
    ev_path = os.path.join(out_dir, "events.csv")
    expected_rejects, seen, loaded_sum, loaded_n = [], set(), 0, 0
    with open(ev_path, "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(HEADER)
        for i, (fields, meta) in enumerate(out_rows):
            w.writerow(fields)
            line_no = i + 2
            if meta["kind"] == "invalid":
                expected_rejects.append((line_no, meta["reason"]))
            elif meta["key"] in seen:
                expected_rejects.append((line_no, "duplicate_event_in_file"))
            else:
                seen.add(meta["key"])
                loaded_n += 1
                loaded_sum += meta["rc"]
    with open(os.path.join(out_dir, "expected", "expected_status.csv"), "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["dataset_id", "expected_date", "expected_status"])
        w.writerows(sorted(expected_status))
    with open(os.path.join(out_dir, "expected", "expected_rejects.csv"), "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["source_row_number", "expected_reject_reason"])
        w.writerows(sorted(expected_rejects))
    counts: Dict[str, int] = {}
    for _, _, s in expected_status:
        counts[s] = counts.get(s, 0) + 1
    summary = {
        "seed": seed, "datasets": datasets, "days": days, "start_date": start.isoformat(),
        "as_of": as_of.strftime("%Y-%m-%dT%H:%M:%SZ"), "rows_in": len(out_rows), "rows_loaded": loaded_n,
        "rows_rejected": len(expected_rejects), "sum_row_count_loaded": loaded_sum,
        "status_counts": dict(sorted(counts.items())),
        "reject_counts": dict(sorted({r: sum(1 for _, x in expected_rejects if x == r)
                                      for r in {x for _, x in expected_rejects}}.items())),
    }
    with open(os.path.join(out_dir, "expected", "expected_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
        fh.write("\n")
    return summary


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Generate synthetic dataset-arrival data + answer key.")
    p.add_argument("--out", default="data/sample")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--datasets", type=int, default=5)
    p.add_argument("--days", type=int, default=8)
    p.add_argument("--rows", type=int, help="approximate number of event rows (overrides --days)")
    p.add_argument("--start-date", default="2026-10-01")
    a = p.parse_args(argv)
    s = generate(a.out, a.seed, a.datasets, a.days, date.fromisoformat(a.start_date), a.rows)
    print(json.dumps(s, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
