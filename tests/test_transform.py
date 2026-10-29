import unittest
from datetime import date, time

from sla_monitor.common import parse_timestamp as ts
from sla_monitor.registry import DatasetRule
from sla_monitor.transform import (Arrival, build_freshness, build_statuses, canonical_arrivals, evaluate)

RULE = DatasetRule("ds_a", "daily_a", time(6, 0), time(8, 0), 15, 1560, "o@example.com", date(2026, 10, 1), True)
D = date(2026, 10, 2)
LATE_AS_OF = ts("2026-10-02T12:00:00Z")


def arrival(arr, comp, rc=100, n=1):
    return Arrival(ts(arr), ts(comp) if comp else None, rc, n)


class StatusRules(unittest.TestCase):
    def status(self, arr, comp, as_of=LATE_AS_OF):
        return evaluate(RULE, D, arrival(arr, comp), as_of)

    def test_on_time(self):
        r = self.status("2026-10-02T06:05:00Z", "2026-10-02T06:30:00Z")
        self.assertEqual((r.status, r.delay_minutes, r.sla_overrun_minutes), ("on_time", 5, 0))

    def test_early_arrival_has_zero_delay(self):
        r = self.status("2026-10-02T05:00:00Z", "2026-10-02T05:20:00Z")
        self.assertEqual((r.status, r.delay_minutes), ("on_time", 0))

    def test_boundary_exactly_at_grace_deadline_is_on_time(self):
        self.assertEqual(self.status("2026-10-02T06:15:00Z", "2026-10-02T06:40:00Z").status, "on_time")

    def test_boundary_one_minute_after_grace_is_late(self):
        self.assertEqual(self.status("2026-10-02T06:16:00Z", "2026-10-02T06:40:00Z").status, "late")

    def test_boundary_completed_exactly_at_sla_is_ok(self):
        r = self.status("2026-10-02T06:00:00Z", "2026-10-02T08:00:00Z")
        self.assertEqual((r.status, r.sla_overrun_minutes), ("on_time", 0))

    def test_completed_one_minute_after_sla_is_breached(self):
        r = self.status("2026-10-02T06:00:00Z", "2026-10-02T08:01:00Z")
        self.assertEqual((r.status, r.sla_overrun_minutes), ("sla_breached", 1))

    def test_breach_takes_precedence_over_late(self):
        self.assertEqual(self.status("2026-10-02T06:40:00Z", "2026-10-02T09:00:00Z").status, "sla_breached")

    def test_never_completed_after_sla_is_breached(self):
        r = self.status("2026-10-02T06:00:00Z", None)
        self.assertEqual((r.status, r.sla_overrun_minutes), ("sla_breached", 240))

    def test_in_progress_before_sla(self):
        r = self.status("2026-10-02T06:00:00Z", None, as_of=ts("2026-10-02T07:00:00Z"))
        self.assertEqual(r.status, "in_progress")

    def test_late_and_still_in_progress_is_late(self):
        r = self.status("2026-10-02T06:30:00Z", None, as_of=ts("2026-10-02T07:00:00Z"))
        self.assertEqual(r.status, "late")

    def test_arrival_after_midnight_for_previous_day(self):
        r = self.status("2026-10-03T00:30:00Z", "2026-10-03T01:00:00Z", as_of=ts("2026-10-03T12:00:00Z"))
        self.assertEqual((r.expected_date, r.status, r.delay_minutes), ("2026-10-02", "sla_breached", 1110))

    def test_missing_and_pending(self):
        none = lambda t: evaluate(RULE, D, None, ts(t))  # noqa: E731
        self.assertIsNone(none("2026-10-02T05:59:00Z"))          # not due yet -> no row
        self.assertEqual(none("2026-10-02T06:00:00Z").status, "pending")
        self.assertEqual(none("2026-10-02T06:15:00Z").status, "pending")   # exactly at deadline
        m = none("2026-10-02T06:16:00Z")
        self.assertEqual((m.status, m.delay_minutes), ("missing", 16))


class Collapse(unittest.TestCase):
    def test_in_progress_then_completed_is_one_load_with_completion(self):
        a = canonical_arrivals([
            ("ds_a", "2026-10-02", "2026-10-02T06:00:00Z", None, 10),
            ("ds_a", "2026-10-02", "2026-10-02T06:00:00Z", "2026-10-02T06:20:00Z", 10)])[("ds_a", "2026-10-02")]
        self.assertEqual((a.completed_at, a.event_count), (ts("2026-10-02T06:20:00Z"), 2))

    def test_reload_does_not_change_canonical_arrival(self):
        a = canonical_arrivals([
            ("ds_a", "2026-10-02", "2026-10-02T09:00:00Z", "2026-10-02T09:30:00Z", 99),
            ("ds_a", "2026-10-02", "2026-10-02T06:00:00Z", "2026-10-02T06:20:00Z", 10)])[("ds_a", "2026-10-02")]
        self.assertEqual((a.first_arrived_at, a.completed_at, a.row_count, a.event_count),
                         (ts("2026-10-02T06:00:00Z"), ts("2026-10-02T06:20:00Z"), 10, 2))


class Windows(unittest.TestCase):
    def test_inactive_skipped_and_active_from_respected(self):
        later = DatasetRule("ds_c", "c", time(6, 0), time(8, 0), 0, 1560, "o", date(2026, 10, 3), True)
        off = DatasetRule("ds_d", "d", time(6, 0), time(8, 0), 0, 1560, "o", date(2026, 10, 1), False)
        rows = build_statuses({"ds_c": later, "ds_d": off}, {}, ts("2026-10-03T12:00:00Z"), date(2026, 10, 1))
        self.assertEqual([(r.dataset_id, r.expected_date, r.status) for r in rows], [("ds_c", "2026-10-03", "missing")])

    def test_freshness_states(self):
        as_of = ts("2026-10-03T12:00:00Z")
        rules = {"ds_a": RULE}
        fresh = build_freshness(rules, {"ds_a": "2026-10-03T06:00:00Z"}, as_of)[0]
        stale = build_freshness(rules, {"ds_a": "2026-10-01T06:00:00Z"}, as_of)[0]
        none = build_freshness(rules, {}, as_of)[0]
        self.assertEqual((fresh.freshness_status, fresh.freshness_minutes), ("fresh", 360))
        self.assertEqual(stale.freshness_status, "stale")
        self.assertEqual((none.freshness_status, none.freshness_minutes), ("no_data", None))


if __name__ == "__main__":
    unittest.main()
