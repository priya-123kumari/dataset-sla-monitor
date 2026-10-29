"""Row-level validation, normalisation and in-file duplicate handling.

Checks run in a fixed order and a row is rejected for the FIRST failing check, so
reject_reason is deterministic:

  1 malformed_row             wrong number of fields for the header
  2 missing_required_field    dataset_id / expected_date / arrived_at / row_count blank
  3 invalid_date              expected_date not a real YYYY-MM-DD date
  4 invalid_timestamp         arrived_at / completed_at not ISO-8601 with a time part
  5 invalid_row_count         row_count not a non-negative integer
  6 completed_before_arrived  completed_at < arrived_at
  7 timestamp_in_future       arrived_at / completed_at later than the run's as_of
  8 unknown_dataset           dataset_id not in dataset_registry (referential integrity)
  9 duplicate_event_in_file   same event_id already seen earlier in this file
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Set

from .common import (fmt_ts, parse_date, parse_nonneg_int, parse_timestamp, sha1_id)
from .ingest import ParsedFile, RawRow
from .registry import DatasetRule

REJECT_REASONS = [
    "malformed_row", "missing_required_field", "invalid_date", "invalid_timestamp",
    "invalid_row_count", "completed_before_arrived", "timestamp_in_future",
    "unknown_dataset", "duplicate_event_in_file",
]


@dataclass(frozen=True)
class Event:
    event_id: str
    dataset_id: str
    expected_date: str
    arrived_at: str
    completed_at: str | None
    row_count: int
    source_row_number: int


@dataclass(frozen=True)
class Reject:
    source_row_number: int
    reason: str
    detail: str
    raw_record: str  # JSON array


@dataclass
class ValidationResult:
    valid: List[Event]
    rejects: List[Reject]


def make_event_id(dataset_id: str, expected_date: str, arrived_at: str, completed_at: str | None) -> str:
    return sha1_id(dataset_id, expected_date, arrived_at, completed_at)


def validate_rows(parsed: ParsedFile, rules: Dict[str, DatasetRule], as_of: datetime) -> ValidationResult:
    idx = {name: i for i, name in enumerate(parsed.columns)}
    valid: List[Event] = []
    rejects: List[Reject] = []
    seen: Set[str] = set()

    def reject(row: RawRow, reason: str, detail: str = "") -> None:
        rejects.append(Reject(row.line_no, reason, detail, json.dumps(row.fields, ensure_ascii=False)))

    for row in parsed.rows:
        f = row.fields
        if len(f) != len(parsed.columns):
            reject(row, "malformed_row", f"expected {len(parsed.columns)} fields, got {len(f)}")
            continue
        get = lambda name: f[idx[name]].strip()  # noqa: E731
        dataset_id, expected_date_s = get("dataset_id"), get("expected_date")
        arrived_s, completed_s, row_count_s = get("arrived_at"), get("completed_at"), get("row_count")

        blanks = [n for n, v in (("dataset_id", dataset_id), ("expected_date", expected_date_s),
                                 ("arrived_at", arrived_s), ("row_count", row_count_s)) if v == ""]
        if blanks:
            reject(row, "missing_required_field", ",".join(blanks))
            continue
        expected_date = parse_date(expected_date_s)
        if expected_date is None:
            reject(row, "invalid_date", expected_date_s)
            continue
        arrived = parse_timestamp(arrived_s)
        completed = parse_timestamp(completed_s) if completed_s else None
        if arrived is None or (completed_s and completed is None):
            reject(row, "invalid_timestamp", arrived_s if arrived is None else completed_s)
            continue
        row_count = parse_nonneg_int(row_count_s)
        if row_count is None:
            reject(row, "invalid_row_count", row_count_s)
            continue
        if completed is not None and completed < arrived:
            reject(row, "completed_before_arrived", f"{fmt_ts(completed)} < {fmt_ts(arrived)}")
            continue
        if arrived > as_of or (completed is not None and completed > as_of):
            reject(row, "timestamp_in_future", f"as_of={fmt_ts(as_of)}")
            continue
        if dataset_id not in rules:
            reject(row, "unknown_dataset", dataset_id)
            continue

        arrived_iso, completed_iso = fmt_ts(arrived), fmt_ts(completed)
        eid = make_event_id(dataset_id, expected_date.isoformat(), arrived_iso, completed_iso)
        if eid in seen:
            reject(row, "duplicate_event_in_file", eid)
            continue
        seen.add(eid)
        valid.append(Event(eid, dataset_id, expected_date.isoformat(), arrived_iso,
                           completed_iso, row_count, row.line_no))
    return ValidationResult(valid=valid, rejects=rejects)
