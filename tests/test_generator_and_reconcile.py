import filecmp
import os
import sqlite3
import unittest
from datetime import date

from sla_monitor import reconcile
from sla_monitor.common import parse_timestamp
from sla_monitor.generate_data import generate
from sla_monitor.pipeline import run_pipeline
from tests.helpers import PipelineTestCase


class Generator(PipelineTestCase):
    def test_same_seed_gives_identical_files_and_different_seed_differs(self):
        a, b, c = (os.path.join(self.tmp, n) for n in "abc")
        generate(a, seed=11, datasets=4, days=10)
        generate(b, seed=11, datasets=4, days=10)
        generate(c, seed=12, datasets=4, days=10)
        for f in ("registry.csv", "events.csv", "expected/expected_status.csv", "expected/expected_rejects.csv",
                  "expected/expected_summary.json"):
            self.assertTrue(filecmp.cmp(os.path.join(a, f), os.path.join(b, f), shallow=False), f)
        self.assertFalse(filecmp.cmp(os.path.join(a, "events.csv"), os.path.join(c, "events.csv"), shallow=False))

    def test_rows_option_scales_volume(self):
        s = generate(os.path.join(self.tmp, "big"), seed=1, datasets=10, rows=5000)
        self.assertGreaterEqual(s["rows_in"], 5000)

    def test_answer_key_is_kept_out_of_the_input_files(self):
        out = os.path.join(self.tmp, "g")
        generate(out, seed=3)
        with open(os.path.join(out, "events.csv")) as fh:
            header = fh.readline().strip()
        self.assertEqual(header, "dataset_id,expected_date,arrived_at,completed_at,row_count")
        self.assertTrue(os.path.isdir(os.path.join(out, "expected")))


class EndToEndAgainstAnswerKey(PipelineTestCase):
    """Pipeline output must equal the independently constructed answer key, for several seeds and sizes."""

    def check(self, seed, datasets, days):
        out = os.path.join(self.tmp, f"gen_{seed}")
        s = generate(out, seed=seed, datasets=datasets, days=days)
        db_path = os.path.join(self.tmp, f"e2e_{seed}.db")
        res = run_pipeline(db_path, os.path.join(out, "events.csv"), parse_timestamp(s["as_of"]),
                           os.path.join(out, "registry.csv"))
        self.assertEqual(res.status, "success", res.failure_reason)
        self.assertEqual((res.rows_in, res.rows_loaded, res.rows_rejected),
                         (s["rows_in"], s["rows_loaded"], s["rows_rejected"]))
        conn = sqlite3.connect(db_path)
        checks = reconcile.internal_checks(conn) + reconcile.answer_key_checks(conn, os.path.join(out, "expected"), "events.csv")
        failed = [c for c in checks if not c.passed]
        self.assertEqual(failed, [], reconcile.render(checks))
        # rerun is a no-op
        again = run_pipeline(db_path, os.path.join(out, "events.csv"), parse_timestamp(s["as_of"]),
                             os.path.join(out, "registry.csv"))
        self.assertEqual((again.rows_loaded, again.rows_already_loaded, again.status_rows_changed),
                         (0, s["rows_loaded"], 0))
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM arrival_events").fetchone()[0], s["rows_loaded"])
        conn.close()

    def test_seed_42_small(self):
        self.check(42, 5, 8)

    def test_seed_7_medium(self):
        self.check(7, 6, 40)

    def test_seed_2026_wider(self):
        self.check(2026, 12, 25)

    def test_committed_sample_matches_its_answer_key(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sample = os.path.join(root, "data", "sample")
        if not os.path.exists(os.path.join(sample, "events.csv")):
            self.skipTest("sample not present")
        import json
        with open(os.path.join(sample, "expected", "expected_summary.json")) as fh:
            s = json.load(fh)
        res = run_pipeline(self.db_path, os.path.join(sample, "events.csv"), parse_timestamp(s["as_of"]),
                           os.path.join(sample, "registry.csv"))
        conn = sqlite3.connect(self.db_path)
        checks = reconcile.internal_checks(conn) + reconcile.answer_key_checks(conn, os.path.join(sample, "expected"), "events.csv")
        self.assertTrue(all(c.passed for c in checks), reconcile.render(checks))


class ReconcilerCatchesProblems(PipelineTestCase):
    """The checker itself must fail when the data is wrong (a checker that cannot fail is useless)."""

    def setUp(self):
        super().setUp()
        out = os.path.join(self.tmp, "g")
        self.s = generate(out, seed=5, datasets=3, days=6)
        self.out = out
        run_pipeline(self.db_path, os.path.join(out, "events.csv"), parse_timestamp(self.s["as_of"]),
                     os.path.join(out, "registry.csv"))
        self.conn = sqlite3.connect(self.db_path)
        self.addCleanup(self.conn.close)

    def failed(self):
        checks = reconcile.internal_checks(self.conn) + reconcile.answer_key_checks(
            self.conn, os.path.join(self.out, "expected"), "events.csv")
        return {c.name.split(" ")[0] for c in checks if not c.passed}

    def test_clean_state_passes(self):
        self.assertEqual(self.failed(), set())

    def test_detects_lost_event(self):
        self.conn.execute("DELETE FROM arrival_events WHERE rowid = (SELECT MIN(rowid) FROM arrival_events)")
        self.assertIn("events_table_equals_sum_of_loaded", self.failed())
        self.assertIn("rows_loaded_matches_answer_key", self.failed())

    def test_detects_wrong_status(self):
        self.conn.execute("UPDATE arrival_status SET status='missing' WHERE rowid = "
                          "(SELECT MIN(rowid) FROM arrival_status WHERE status='on_time')")
        self.assertIn("status_matches_answer_key", self.failed())

    def test_detects_tampered_reject(self):
        self.conn.execute("UPDATE rejects SET reject_reason='invalid_date' WHERE rowid = (SELECT MIN(rowid) FROM rejects "
                          "WHERE reject_reason != 'invalid_date')")
        self.assertIn("rejects_match_answer_key", self.failed())


if __name__ == "__main__":
    unittest.main()
