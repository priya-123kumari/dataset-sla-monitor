import json
import unittest

from sla_monitor.common import parse_timestamp as ts
from sla_monitor.ingest import REQUIRED_COLUMNS, ParsedFile, RawRow
from sla_monitor.registry import DatasetRule
from sla_monitor.validate import validate_rows
from datetime import date, time

RULES = {"ds_a": DatasetRule("ds_a", "a", time(6, 0), time(8, 0), 15, 1560, "o", date(2026, 10, 1), True)}
AS_OF = ts("2026-10-03T12:00:00Z")
GOOD = ["ds_a", "2026-10-02", "2026-10-02T06:00:00Z", "2026-10-02T06:20:00Z", "100"]


def run(*rows):
    parsed = ParsedFile(REQUIRED_COLUMNS, [RawRow(i + 2, list(r)) for i, r in enumerate(rows)])
    return validate_rows(parsed, RULES, AS_OF)


def with_(**changes):
    r = dict(zip(REQUIRED_COLUMNS, GOOD))
    r.update(changes)
    return [r[c] for c in REQUIRED_COLUMNS]


class Validate(unittest.TestCase):
    def test_happy_path_normalises_values(self):
        res = run(GOOD)
        self.assertEqual(len(res.valid), 1)
        self.assertEqual(res.rejects, [])
        e = res.valid[0]
        self.assertEqual((e.dataset_id, e.arrived_at, e.completed_at, e.row_count),
                         ("ds_a", "2026-10-02T06:00:00Z", "2026-10-02T06:20:00Z", 100))
        self.assertEqual(len(e.event_id), 40)

    def test_empty_input(self):
        res = run()
        self.assertEqual((res.valid, res.rejects), ([], []))

    def test_each_reject_reason(self):
        cases = {
            "malformed_row": GOOD[:3],
            "missing_required_field": with_(dataset_id=""),
            "invalid_date": with_(expected_date="2026-02-30"),
            "invalid_timestamp": with_(arrived_at="garbage"),
            "invalid_row_count": with_(row_count="-1"),
            "completed_before_arrived": with_(completed_at="2026-10-02T05:00:00Z"),
            "timestamp_in_future": with_(arrived_at="2026-10-04T06:00:00Z", completed_at=""),
            "unknown_dataset": with_(dataset_id="ds_zzz"),
        }
        for reason, row in cases.items():
            with self.subTest(reason=reason):
                res = run(row)
                self.assertEqual([r.reason for r in res.rejects], [reason])
                self.assertEqual(res.valid, [])

    def test_each_required_field_blank(self):
        for col in ("dataset_id", "expected_date", "arrived_at", "row_count"):
            with self.subTest(col=col):
                res = run(with_(**{col: "  "}))
                self.assertEqual([(r.reason, r.detail) for r in res.rejects], [("missing_required_field", col)])

    def test_completed_at_is_optional(self):
        res = run(with_(completed_at=""))
        self.assertEqual((len(res.valid), res.valid[0].completed_at), (1, None))

    def test_date_only_is_not_a_timestamp(self):
        self.assertEqual([r.reason for r in run(with_(arrived_at="2026-10-02")).rejects], ["invalid_timestamp"])

    def test_timezone_offset_is_converted_to_utc(self):
        res = run(with_(arrived_at="2026-10-02T11:30:00+05:30", completed_at="2026-10-02T11:50:00+05:30"))
        self.assertEqual(res.valid[0].arrived_at, "2026-10-02T06:00:00Z")

    def test_zero_row_count_and_completed_equals_arrived_are_valid_boundaries(self):
        res = run(with_(row_count="0", completed_at="2026-10-02T06:00:00Z"))
        self.assertEqual((len(res.valid), res.valid[0].row_count), (1, 0))

    def test_arrival_exactly_at_as_of_is_valid(self):
        self.assertEqual(len(run(with_(arrived_at="2026-10-03T12:00:00Z", completed_at="")).valid), 1)

    def test_duplicate_in_file_keeps_first_rejects_later_with_line_number(self):
        res = run(GOOD, with_(row_count="999"), GOOD)   # row 2 differs only in row_count -> same event identity
        self.assertEqual([e.source_row_number for e in res.valid], [2])
        self.assertEqual([(r.source_row_number, r.reason) for r in res.rejects],
                         [(3, "duplicate_event_in_file"), (4, "duplicate_event_in_file")])

    def test_reject_keeps_raw_record_for_traceability(self):
        res = run(with_(row_count="abc"))
        self.assertEqual(json.loads(res.rejects[0].raw_record), with_(row_count="abc"))

    def test_first_failing_check_wins(self):
        res = run(with_(dataset_id="ds_zzz", row_count="abc"))
        self.assertEqual([r.reason for r in res.rejects], ["invalid_row_count"])


if __name__ == "__main__":
    unittest.main()
