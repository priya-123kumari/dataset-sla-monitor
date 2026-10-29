"""Read the source events CSV and check its schema.

Schema policy
  * Missing required column, duplicate column, or no header at all -> SchemaError (run fails,
    nothing is loaded).
  * Extra, unknown columns -> tolerated with a warning (forward compatible); they are ignored.
  * Header-only file -> a valid *empty batch*.
Row-level problems are NOT handled here; they go to the reject path in validate.py.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass, field
from typing import List

REQUIRED_COLUMNS = ["dataset_id", "expected_date", "arrived_at", "completed_at", "row_count"]


class SchemaError(Exception):
    """File-level schema problem that makes the whole file unusable."""


@dataclass
class RawRow:
    line_no: int          # physical line in the file (header is line 1)
    fields: List[str]     # original fields, untouched


@dataclass
class ParsedFile:
    columns: List[str]
    rows: List[RawRow]
    warnings: List[str] = field(default_factory=list)


def read_events_file(path: str) -> ParsedFile:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            raise SchemaError("empty_file: no header row") from None
        columns = [c.strip().lower() for c in header]
        if len(set(columns)) != len(columns):
            raise SchemaError(f"duplicate columns in header: {columns}")
        missing = [c for c in REQUIRED_COLUMNS if c not in columns]
        if missing:
            raise SchemaError(f"missing required columns: {missing}")
        warnings = []
        extra = [c for c in columns if c not in REQUIRED_COLUMNS]
        if extra:
            warnings.append(f"ignored unexpected columns: {extra}")
        rows: List[RawRow] = []
        for fields in reader:
            if not fields or all(f.strip() == "" for f in fields) and len(fields) <= 1:
                continue  # blank line: not a record
            rows.append(RawRow(line_no=reader.line_num, fields=fields))
    return ParsedFile(columns=columns, rows=rows, warnings=warnings)
