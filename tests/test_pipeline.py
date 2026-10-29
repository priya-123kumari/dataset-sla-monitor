import unittest
from unittest import mock

from sla_monitor import publish
from sla_monitor.common import parse_timestamp as ts
from tests.helpers import AS_OF, HEADER, PipelineTestCase

A_OK = ["ds_a", "2026-10-02", "2026-10-02T06:05:00Z", "2026-10-02T06:30:00Z", "100"]
B_OK = ["ds_b", "2026-10-02", "2026-10-02T02:00:00Z", "2026-10-02T02:30:00Z", "50"]


class HappyPathAndCounts(PipelineTestCase):
    def test_happy_path(self):
        res = self.run_pipeline(self.write_csv("e.csv", HEADER, [A_OK, B_OK]))
        self.assertEqual(res.status, "success")
        self.assertEqual((res.rows_in, res.rows_loaded, res.rows_already_loaded, res.rows_rejected), (2, 2, 0, 0))
        self.assertEqual(self.count("arrival_events"), 2)
        # dates evaluated: first event date (10-02) .. as_of date (10-03); 10-03 has no events -> missing
        self.assertEqual(self.statuses(), {("ds_a", "2026-10-02"): "on_time", ("ds_a", "2026-10-03"): "missing",
                                           ("ds_b", "2026-10-02"): "on_time", ("ds_b", "2026-10-03"): "missing"})
        run = self.q("SELECT status, rows_in, rows_loaded, rows_rejected, finished_at IS NOT NULL FROM pipeline_runs")
        self.assertEqual(run, [("success", 2, 2, 0, 1)])

    def test_malformed_nulls_duplicates_and_unknown_are_quarantined_with_lineage(self):
        rows = [A_OK,
                ["ds_a", "2026-10-02", "2026-10-02T06:05:00Z"],               # malformed (short)
                ["", "2026-10-02", "2026-10-02T06:05:00Z", "", "5"],            # null required field
                A_OK,                                                           # duplicate
                ["ds_nope", "2026-10-02", "2026-10-02T06:05:00Z", "", "5"],     # invalid reference key
                ["ds_b", "2026-10-02", "2026-10-02T02:00:00Z", "", "x"]]        # bad row_count
        res = self.run_pipeline(self.write_csv("bad.csv", HEADER, rows))
        self.assertEqual((res.rows_in, res.rows_loaded, res.rows_rejected), (6, 1, 5))
        rej = self.q("SELECT source_file, source_row_number, reject_reason FROM rejects ORDER BY source_row_number")
        self.assertEqual(rej, [("bad.csv", 3, "malformed_row"), ("bad.csv", 4, "missing_required_field"),
                               ("bad.csv", 5, "duplicate_event_in_file"), ("bad.csv", 6, "unknown_dataset"),
                               ("bad.csv", 7, "invalid_row_count")])
        self.assertEqual(self.q("SELECT run_id FROM rejects LIMIT 1")[0][0], res.run_id)

    def test_blank_lines_and_utf8_bom_are_tolerated(self):
        text = "\ufeff" + ",".join(HEADER) + "\n\n" + ",".join(A_OK) + "\n\n"
        res = self.run_pipeline(self.write_text("bom.csv", text, encoding="utf-8"))
        self.assertEqual((res.status, res.rows_in, res.rows_loaded), ("success", 1, 1))

    def test_in_progress_then_completed_events_are_one_load(self):
        rows = [["ds_a", "2026-10-02", "2026-10-02T06:00:00Z", "", "100"],
                ["ds_a", "2026-10-02", "2026-10-02T06:00:00Z", "2026-10-02T06:20:00Z", "100"]]
        res = self.run_pipeline(self.write_csv("e.csv", HEADER, rows))
        self.assertEqual(res.rows_loaded, 2)
        self.assertEqual(self.q("SELECT status, completed_at, event_count FROM arrival_status WHERE dataset_id='ds_a' "
                                "AND expected_date='2026-10-02'"), [("on_time", "2026-10-02T06:20:00Z", 2)])

    def test_reload_is_kept_as_history_but_does_not_change_status(self):
        rows = [A_OK, ["ds_a", "2026-10-02", "2026-10-02T11:00:00Z", "2026-10-02T11:20:00Z", "100"]]
        self.run_pipeline(self.write_csv("e.csv", HEADER, rows))
        self.assertEqual(self.q("SELECT status, first_arrived_at, event_count FROM arrival_status "
                                "WHERE dataset_id='ds_a' AND expected_date='2026-10-02'"),
                         [("on_time", "2026-10-02T06:05:00Z", 2)])


class FileLevelProblems(PipelineTestCase):
    def test_empty_batch_succeeds_and_still_detects_missing_loads(self):
        res = self.run_pipeline(self.write_csv("empty.csv", HEADER, []))
        self.assertEqual((res.status, res.rows_in, res.rows_loaded), ("success", 0, 0))
        self.assertEqual(self.statuses(), {("ds_a", "2026-10-03"): "missing", ("ds_b", "2026-10-03"): "missing"})
        self.assertEqual(self.count("alerts"), 2)

    def test_zero_byte_file_fails_cleanly(self):
        res = self.run_pipeline(self.write_text("zero.csv", ""))
        self.assertEqual(res.status, "failed")
        self.assertIn("empty_file", res.failure_reason)
        self.assertEqual((self.count("arrival_events"), self.count("arrival_status")), (0, 0))
        self.assertEqual(self.q("SELECT status, failure_reason IS NOT NULL FROM pipeline_runs"), [("failed", 1)])

    def test_missing_required_column_fails_and_loads_nothing(self):
        path = self.write_csv("schema.csv", ["dataset_id", "expected_date", "arrived_at", "row_count"],
                              [["ds_a", "2026-10-02", "2026-10-02T06:05:00Z", "1"]])
        res = self.run_pipeline(path)
        self.assertEqual(res.status, "failed")
        self.assertIn("missing required columns", res.failure_reason)
        self.assertIn("completed_at", res.failure_reason)
        self.assertEqual((self.count("arrival_events"), self.count("rejects"), self.count("arrival_status")), (0, 0, 0))

    def test_renamed_column_is_a_schema_change(self):
        header = ["dataset", "expected_date", "arrived_at", "completed_at", "row_count"]
        res = self.run_pipeline(self.write_csv("renamed.csv", header, [A_OK]))
        self.assertEqual(res.status, "failed")
        self.assertIn("dataset_id", res.failure_reason)

    def test_extra_column_is_tolerated_with_warning(self):
        res = self.run_pipeline(self.write_csv("extra.csv", HEADER + ["note"], [A_OK + ["hello"]]))
        self.assertEqual((res.status, res.rows_loaded), ("success", 1))
        self.assertIn("note", res.warnings[0])
        self.assertIn("note", self.q("SELECT warnings FROM pipeline_runs")[0][0])

    def test_reordered_columns_are_read_by_name(self):
        header = ["row_count", "completed_at", "arrived_at", "expected_date", "dataset_id"]
        res = self.run_pipeline(self.write_csv("re.csv", header, [list(reversed(A_OK))]))
        self.assertEqual((res.status, res.rows_loaded), ("success", 1))

    def test_bad_registry_fails_fast(self):
        bad = self.write_csv("badreg.csv", ["dataset_id"], [["x"]])
        self.registry_path = bad
        res = self.run_pipeline(self.write_csv("e.csv", HEADER, [A_OK]))
        self.assertEqual(res.status, "failed")
        self.assertIn("RegistryError", res.failure_reason)

    def test_no_registry_anywhere_fails_with_clear_reason(self):
        res = self.run_pipeline(self.write_csv("e.csv", HEADER, [A_OK]), registry=False)
        self.assertEqual(res.status, "failed")
        self.assertIn("registry is empty", res.failure_reason)


class IdempotencyAndRecovery(PipelineTestCase):
    def snapshot(self):
        snap = {t: self.q(f"SELECT * FROM {t} ORDER BY 1,2") for t in
                ("arrival_events", "rejects", "arrival_status", "arrival_status_history", "alerts")}
        # dataset_freshness is a snapshot rebuilt every run: its run_id legitimately changes, values must not.
        snap["dataset_freshness"] = self.q("SELECT dataset_id, last_completed_at, freshness_minutes, "
                                           "freshness_status FROM dataset_freshness ORDER BY 1")
        return snap

    def test_rerun_of_same_input_creates_no_duplicates(self):
        path = self.write_csv("e.csv", HEADER, [A_OK, B_OK, ["ds_zzz", "2026-10-02", "2026-10-02T02:00:00Z", "", "1"]])
        r1 = self.run_pipeline(path)
        snap1 = self.snapshot()
        r2 = self.run_pipeline(path)
        self.assertEqual((r1.rows_loaded, r1.rows_already_loaded), (2, 0))
        self.assertEqual((r2.rows_loaded, r2.rows_already_loaded, r2.rows_rejected), (0, 2, 1))
        self.assertEqual((r2.status_rows_changed, r2.alerts_opened, r2.alerts_resolved), (0, 0, 0))
        self.assertEqual(self.snapshot(), snap1)          # byte-for-byte identical target state
        self.assertEqual(self.count("pipeline_runs"), 2)  # but both runs are recorded

    def test_failure_rolls_back_everything_and_retry_succeeds(self):
        path = self.write_csv("e.csv", HEADER, [A_OK, ["", "", "", "", ""], B_OK])
        with mock.patch.object(publish, "replace_freshness", side_effect=RuntimeError("disk exploded")):
            failed = self.run_pipeline(path, run_id="attempt_1")
        self.assertEqual(failed.status, "failed")
        self.assertIn("disk exploded", failed.failure_reason)
        for table in ("arrival_events", "rejects", "arrival_status", "arrival_status_history", "alerts", "dataset_registry"):
            self.assertEqual(self.count(table), 0, table)
        self.assertEqual(self.q("SELECT run_id, status FROM pipeline_runs"), [("attempt_1", "failed")])
        ok = self.run_pipeline(path, run_id="attempt_2")   # retry = rerun
        self.assertEqual((ok.status, ok.rows_loaded, ok.rows_rejected), ("success", 2, 1))
        self.assertEqual(self.count("arrival_events"), 2)
        self.assertEqual(self.q("SELECT run_id, status FROM pipeline_runs ORDER BY run_id"),
                         [("attempt_1", "failed"), ("attempt_2", "success")])

    def test_failure_after_a_good_run_leaves_previous_state_intact(self):
        self.run_pipeline(self.write_csv("one.csv", HEADER, [A_OK]))
        before = self.snapshot()
        with mock.patch.object(publish, "upsert_statuses", side_effect=RuntimeError("boom")):
            bad = self.run_pipeline(self.write_csv("two.csv", HEADER, [B_OK]))
        self.assertEqual(bad.status, "failed")
        self.assertEqual(self.snapshot(), before)

    def test_duplicate_run_id_is_refused(self):
        path = self.write_csv("e.csv", HEADER, [A_OK])
        self.run_pipeline(path, run_id="same")
        with self.assertRaises(Exception):
            self.run_pipeline(path, run_id="same")

    def test_loading_files_in_either_order_gives_the_same_result(self):
        f1 = self.write_csv("day1.csv", HEADER, [A_OK, B_OK])
        f2 = self.write_csv("day2.csv", HEADER, [["ds_a", "2026-10-03", "2026-10-03T06:40:00Z", "2026-10-03T07:00:00Z", "7"]])
        self.run_pipeline(f1); self.run_pipeline(f2)
        forward = (self.statuses(), self.count("arrival_events"))
        import os
        os.remove(self.db_path)
        self.run_pipeline(f2); self.run_pipeline(f1)          # backfill the older file last
        self.assertEqual((self.statuses(), self.count("arrival_events")), forward)
        self.assertEqual(forward[0][("ds_a", "2026-10-03")], "late")


class LateArrivingDataAndAlerts(PipelineTestCase):
    def test_missing_load_that_arrives_later_is_corrected_not_overwritten_silently(self):
        t1 = ts("2026-10-02T09:00:00Z")
        r1 = self.run_pipeline(self.write_csv("empty.csv", HEADER, []), as_of=t1, start_date=ts("2026-10-02T00:00:00Z").date())
        self.assertEqual(self.statuses("ds_a"), {("ds_a", "2026-10-02"): "missing"})
        self.assertEqual(self.q("SELECT alert_type, severity, resolved_run_id FROM alerts WHERE dataset_id='ds_a'"),
                         [("missing", "critical", None)])
        late = ["ds_a", "2026-10-02", "2026-10-02T08:30:00Z", "2026-10-02T08:45:00Z", "10"]
        r2 = self.run_pipeline(self.write_csv("late.csv", HEADER, [late]), as_of=t1)
        self.assertEqual(self.statuses("ds_a"), {("ds_a", "2026-10-02"): "sla_breached"})
        self.assertEqual(self.q("SELECT alert_type, resolved_run_id FROM alerts WHERE dataset_id='ds_a' ORDER BY alert_type"),
                         [("missing", r2.run_id), ("sla_breached", None)])
        self.assertEqual(self.q("SELECT previous_status, new_status, run_id FROM arrival_status_history "
                                "WHERE dataset_id='ds_a' ORDER BY rowid"),
                         [(None, "missing", r1.run_id), ("missing", "sla_breached", r2.run_id)])

    def test_pending_turns_into_missing_as_time_passes(self):
        empty = self.write_csv("empty.csv", HEADER, [])
        sd = ts("2026-10-02T00:00:00Z").date()
        self.run_pipeline(empty, as_of=ts("2026-10-02T06:10:00Z"), start_date=sd)
        self.assertEqual(self.statuses("ds_a")[("ds_a", "2026-10-02")], "pending")
        self.assertEqual(self.q("SELECT COUNT(*) FROM alerts WHERE dataset_id='ds_a'")[0][0], 0)   # pending is not alertable
        self.run_pipeline(empty, as_of=ts("2026-10-02T06:30:00Z"), start_date=sd)
        self.assertEqual(self.statuses("ds_a")[("ds_a", "2026-10-02")], "missing")

    def test_alerts_are_not_duplicated_across_runs(self):
        empty = self.write_csv("empty.csv", HEADER, [])
        self.run_pipeline(empty)
        n = self.count("alerts")
        self.run_pipeline(empty)
        self.run_pipeline(empty)
        self.assertEqual(self.count("alerts"), n)

    def test_old_problems_outside_lookback_do_not_raise_new_alerts(self):
        old = ["ds_a", "2026-09-20", "2026-09-20T08:30:00Z", "2026-09-20T09:00:00Z", "1"]
        res = self.run_pipeline(self.write_csv("old.csv", HEADER, [old]), as_of=ts("2026-10-03T12:00:00Z"))
        self.assertEqual(self.statuses("ds_a")[("ds_a", "2026-09-20")], "sla_breached")
        self.assertEqual(self.q("SELECT COUNT(*) FROM alerts WHERE alert_key='2026-09-20'")[0][0], 0)

    def test_stale_dataset_alert_opens_then_resolves_when_fresh_data_arrives(self):
        self.run_pipeline(self.write_csv("a.csv", HEADER, [A_OK]), as_of=ts("2026-10-04T12:00:00Z"))
        self.assertEqual(self.q("SELECT severity FROM alerts WHERE alert_type='stale' AND dataset_id='ds_a' "
                                "AND resolved_run_id IS NULL"), [("high",)])
        fresh = ["ds_a", "2026-10-04", "2026-10-04T06:00:00Z", "2026-10-04T06:20:00Z", "5"]
        self.run_pipeline(self.write_csv("b.csv", HEADER, [fresh]), as_of=ts("2026-10-04T12:00:00Z"))
        self.assertEqual(self.q("SELECT COUNT(*) FROM alerts WHERE alert_type='stale' AND dataset_id='ds_a' "
                                "AND resolved_run_id IS NULL")[0][0], 0)
        self.assertEqual(self.q("SELECT freshness_status FROM dataset_freshness WHERE dataset_id='ds_a'"), [("fresh",)])

    def test_alert_messages_are_actionable(self):
        self.run_pipeline(self.write_csv("empty.csv", HEADER, []))
        msg = self.q("SELECT message FROM alerts WHERE dataset_id='ds_a'")[0][0]
        for needle in ("[CRITICAL]", "ds_a", "2026-10-03", "team_a@example.com", "Action:"):
            self.assertIn(needle, msg)


if __name__ == "__main__":
    unittest.main()
