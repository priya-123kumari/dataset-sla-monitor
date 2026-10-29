"""Pure business logic: derive status, delay and freshness. No I/O, easy to unit-test.

Definitions (all UTC, daily datasets)
  expected_arrival_at  = expected_date + expected_arrival_time
  arrival_deadline_at  = expected_arrival_at + grace_minutes
  sla_deadline_at      = expected_date + sla_completion_time

Canonical load for a (dataset, expected_date) = the earliest arrival. Later arrivals are
re-loads: kept as history (event_count) but they do not change the status. Within the
canonical arrival, the latest non-null completed_at wins (an "in progress" event followed by
a "completed" event for the same arrival is one load).

Status precedence (first match wins)
  no arrival      -> 'pending' if as_of <= arrival_deadline_at else 'missing'
  sla_breached    completed_at > sla_deadline_at, or never completed and as_of > sla_deadline_at
  late            first_arrived_at > arrival_deadline_at   (exactly at the deadline is on time)
  in_progress     arrived on time, not completed, SLA not yet due
  on_time         otherwise
A (dataset, date) that is not yet due (as_of < expected_arrival_at) and has no arrival is skipped.
Dates that have an arrival are always evaluated, even before the dataset's active_from.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from .common import combine_utc, fmt_ts, minutes_between, parse_timestamp
from .registry import DatasetRule

ALERT_STATUSES = ("missing", "late", "sla_breached")


@dataclass(frozen=True)
class Arrival:
    first_arrived_at: datetime
    completed_at: Optional[datetime]
    row_count: int
    event_count: int


@dataclass(frozen=True)
class StatusRow:
    dataset_id: str
    expected_date: str
    expected_arrival_at: str
    arrival_deadline_at: str
    sla_deadline_at: str
    status: str
    first_arrived_at: Optional[str]
    completed_at: Optional[str]
    row_count: Optional[int]
    event_count: int
    delay_minutes: Optional[int]
    sla_overrun_minutes: Optional[int]

    def comparable(self) -> tuple:
        """Everything derived (used to skip unchanged rows on write)."""
        return tuple(getattr(self, f) for f in self.__dataclass_fields__)


@dataclass(frozen=True)
class FreshnessRow:
    dataset_id: str
    last_completed_at: Optional[str]
    freshness_minutes: Optional[int]
    max_staleness_minutes: int
    freshness_status: str


def evaluate(rule: DatasetRule, d: date, arrival: Optional[Arrival], as_of: datetime) -> Optional[StatusRow]:
    expected_at = combine_utc(d, rule.expected_arrival_time)
    deadline = expected_at + timedelta(minutes=rule.grace_minutes)
    sla_at = combine_utc(d, rule.sla_completion_time)

    if arrival is None:
        if as_of < expected_at:
            return None  # not due yet
        status = "pending" if as_of <= deadline else "missing"
        return StatusRow(rule.dataset_id, d.isoformat(), fmt_ts(expected_at), fmt_ts(deadline), fmt_ts(sla_at),
                         status, None, None, None, 0,
                         max(0, minutes_between(as_of, expected_at)), None)

    completed = arrival.completed_at
    if completed is not None and completed > sla_at:
        status = "sla_breached"
    elif completed is None and as_of > sla_at:
        status = "sla_breached"
    elif arrival.first_arrived_at > deadline:
        status = "late"
    elif completed is None:
        status = "in_progress"
    else:
        status = "on_time"

    if completed is not None:
        overrun = max(0, minutes_between(completed, sla_at))
    elif as_of > sla_at:
        overrun = minutes_between(as_of, sla_at)
    else:
        overrun = 0
    return StatusRow(rule.dataset_id, d.isoformat(), fmt_ts(expected_at), fmt_ts(deadline), fmt_ts(sla_at),
                     status, fmt_ts(arrival.first_arrived_at), fmt_ts(completed), arrival.row_count,
                     arrival.event_count,
                     max(0, minutes_between(arrival.first_arrived_at, expected_at)), overrun)


def canonical_arrivals(event_rows: Iterable[Tuple[str, str, str, Optional[str], int]]) -> Dict[Tuple[str, str], Arrival]:
    """Collapse raw events (dataset_id, expected_date, arrived_at, completed_at, row_count) per key."""
    groups: Dict[Tuple[str, str], List[Tuple[str, Optional[str], int]]] = {}
    for ds, ed, arr, comp, rc in event_rows:
        groups.setdefault((ds, ed), []).append((arr, comp, rc))
    out: Dict[Tuple[str, str], Arrival] = {}
    for key, evs in groups.items():
        first = min(e[0] for e in evs)  # ISO-8601 'Z' strings sort chronologically
        same = [e for e in evs if e[0] == first]
        best = max(same, key=lambda e: (e[1] is not None, e[1] or ""))
        out[key] = Arrival(parse_timestamp(first), parse_timestamp(best[1]) if best[1] else None,
                           best[2], len(evs))
    return out


def build_statuses(rules: Dict[str, DatasetRule], arrivals: Dict[Tuple[str, str], Arrival],
                   as_of: datetime, start_date: date) -> List[StatusRow]:
    """One row per (dataset, expected_date) from max(start_date, active_from) to as_of's date, plus any
    date that has an arrival (even before active_from, or for an inactive dataset) so no loaded event is
    ever left without a status."""
    rows: List[StatusRow] = []
    last_day = as_of.date()
    covered = set()
    for rule in rules.values():
        if not rule.is_active:
            continue
        d = max(start_date, rule.active_from)
        while d <= last_day:
            row = evaluate(rule, d, arrivals.get((rule.dataset_id, d.isoformat())), as_of)
            if row is not None:
                rows.append(row)
                covered.add((rule.dataset_id, row.expected_date))
            d += timedelta(days=1)
    for key in sorted(set(arrivals) - covered):
        rule = rules.get(key[0])
        if rule is not None:
            row = evaluate(rule, date.fromisoformat(key[1]), arrivals[key], as_of)
            if row is not None:
                rows.append(row)
    return rows


def build_freshness(rules: Dict[str, DatasetRule], last_completed: Dict[str, str], as_of: datetime) -> List[FreshnessRow]:
    """Freshness = as_of - latest completed_at ever loaded for the dataset."""
    rows = []
    for rule in rules.values():
        if not rule.is_active:
            continue
        last = last_completed.get(rule.dataset_id)
        if last is None:
            rows.append(FreshnessRow(rule.dataset_id, None, None, rule.max_staleness_minutes, "no_data"))
            continue
        minutes = minutes_between(as_of, parse_timestamp(last))
        state = "stale" if minutes > rule.max_staleness_minutes else "fresh"
        rows.append(FreshnessRow(rule.dataset_id, last, minutes, rule.max_staleness_minutes, state))
    return rows
