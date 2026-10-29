"""Dataset registry: the reference table that defines expectations and valid dataset_ids."""
from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from datetime import date, time
from typing import Dict, List

from .common import parse_date, parse_hhmm

REGISTRY_COLUMNS = [
    "dataset_id", "dataset_name", "expected_arrival_time", "sla_completion_time",
    "grace_minutes", "max_staleness_minutes", "owner", "active_from", "is_active",
]


class RegistryError(Exception):
    """Registry file is invalid. Reference data errors fail the run (fail fast)."""


@dataclass(frozen=True)
class DatasetRule:
    dataset_id: str
    dataset_name: str
    expected_arrival_time: time
    sla_completion_time: time
    grace_minutes: int
    max_staleness_minutes: int
    owner: str
    active_from: date
    is_active: bool


def read_registry_file(path: str) -> List[dict]:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        missing = [c for c in REGISTRY_COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise RegistryError(f"registry missing columns: {missing}")
        rows = []
        seen = set()
        for line_no, row in enumerate(reader, start=2):
            try:
                rec = {
                    "dataset_id": (row["dataset_id"] or "").strip(),
                    "dataset_name": (row["dataset_name"] or "").strip(),
                    "expected_arrival_time": row["expected_arrival_time"].strip(),
                    "sla_completion_time": row["sla_completion_time"].strip(),
                    "grace_minutes": int(row["grace_minutes"]),
                    "max_staleness_minutes": int(row["max_staleness_minutes"]),
                    "owner": (row["owner"] or "").strip(),
                    "active_from": row["active_from"].strip(),
                    "is_active": int(row["is_active"]),
                }
                if not rec["dataset_id"] or not rec["dataset_name"] or not rec["owner"]:
                    raise ValueError("dataset_id, dataset_name and owner are required")
                if rec["grace_minutes"] < 0 or rec["max_staleness_minutes"] <= 0:
                    raise ValueError("grace_minutes must be >= 0 and max_staleness_minutes > 0")
                if rec["is_active"] not in (0, 1):
                    raise ValueError("is_active must be 0 or 1")
                if parse_hhmm(rec["sla_completion_time"]) < parse_hhmm(rec["expected_arrival_time"]):
                    raise ValueError("sla_completion_time must not be before expected_arrival_time")
                if parse_date(rec["active_from"]) is None:
                    raise ValueError("active_from must be YYYY-MM-DD")
                if rec["dataset_id"] in seen:
                    raise ValueError("duplicate dataset_id")
                seen.add(rec["dataset_id"])
            except (ValueError, TypeError, KeyError) as exc:
                raise RegistryError(f"{path}:{line_no}: {exc}") from exc
            rows.append(rec)
        return rows


def load_registry(conn: sqlite3.Connection, path: str) -> int:
    """Upsert registry rows (idempotent). Returns number of rows in the file."""
    rows = read_registry_file(path)
    conn.executemany(
        """INSERT INTO dataset_registry
             (dataset_id, dataset_name, expected_arrival_time, sla_completion_time,
              grace_minutes, max_staleness_minutes, owner, active_from, is_active)
           VALUES (:dataset_id, :dataset_name, :expected_arrival_time, :sla_completion_time,
                   :grace_minutes, :max_staleness_minutes, :owner, :active_from, :is_active)
           ON CONFLICT(dataset_id) DO UPDATE SET
             dataset_name = excluded.dataset_name,
             expected_arrival_time = excluded.expected_arrival_time,
             sla_completion_time = excluded.sla_completion_time,
             grace_minutes = excluded.grace_minutes,
             max_staleness_minutes = excluded.max_staleness_minutes,
             owner = excluded.owner,
             active_from = excluded.active_from,
             is_active = excluded.is_active""",
        rows,
    )
    return len(rows)


def read_rules(conn: sqlite3.Connection) -> Dict[str, DatasetRule]:
    out: Dict[str, DatasetRule] = {}
    cur = conn.execute(
        "SELECT dataset_id, dataset_name, expected_arrival_time, sla_completion_time, grace_minutes,"
        " max_staleness_minutes, owner, active_from, is_active FROM dataset_registry ORDER BY dataset_id"
    )
    for r in cur:
        out[r[0]] = DatasetRule(
            dataset_id=r[0], dataset_name=r[1],
            expected_arrival_time=parse_hhmm(r[2]), sla_completion_time=parse_hhmm(r[3]),
            grace_minutes=r[4], max_staleness_minutes=r[5], owner=r[6],
            active_from=date.fromisoformat(r[7]), is_active=bool(r[8]),
        )
    return out
