"""Small shared helpers: deterministic IDs, strict date/time parsing, formatting."""
from __future__ import annotations

import hashlib
import re
import uuid
from datetime import date, datetime, time, timezone
from typing import Optional

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_HHMM_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
_INT_RE = re.compile(r"^\d+$")


def sha1_id(*parts) -> str:
    """Deterministic ID: sha1 over the '|'-joined parts (None -> empty string)."""
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def fmt_ts(dt: Optional[datetime]) -> Optional[str]:
    """ISO-8601 UTC, second precision, e.g. 2026-10-01T06:05:00Z."""
    if dt is None:
        return None
    # isoformat() is C-implemented and ~1.5x faster than strftime(); profiling showed strftime was
    # the single largest cost in the transform stage. Output is identical (asserted in benchmark.py).
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")[:-6] + "Z"


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp into an aware UTC datetime.

    Requires a time part (a bare date is NOT a timestamp). Offsets are converted
    to UTC; a naive timestamp is assumed to be UTC. Returns None if unparseable.
    """
    if value is None:
        return None
    s = value.strip()
    if len(s) <= 10 or ("T" not in s and " " not in s):
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def parse_date(value: Optional[str]) -> Optional[date]:
    """Strict YYYY-MM-DD only."""
    if value is None:
        return None
    s = value.strip()
    if not _DATE_RE.match(s):
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def parse_hhmm(value: str) -> time:
    s = (value or "").strip()
    if not _HHMM_RE.match(s):
        raise ValueError(f"invalid HH:MM time: {value!r}")
    return time(int(s[:2]), int(s[3:]))


def parse_nonneg_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    s = value.strip()
    return int(s) if _INT_RE.match(s) else None


def combine_utc(d: date, t: time) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, tzinfo=timezone.utc)


def minutes_between(later: datetime, earlier: datetime) -> int:
    """Whole minutes (floored) from earlier to later; may be negative."""
    return int((later - earlier).total_seconds() // 60)


def make_run_id(now: Optional[datetime] = None) -> str:
    now = now or utc_now()
    return f"run_{now:%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"
