import csv
import os
import shutil
import sqlite3
import tempfile
import unittest

from sla_monitor import pipeline
from sla_monitor.common import parse_timestamp

HEADER = ["dataset_id", "expected_date", "arrived_at", "completed_at", "row_count"]
REG_HEADER = ["dataset_id", "dataset_name", "expected_arrival_time", "sla_completion_time", "grace_minutes",
              "max_staleness_minutes", "owner", "active_from", "is_active"]
REGISTRY = [
    ["ds_a", "daily_a", "06:00", "08:00", "15", "1560", "team_a@example.com", "2026-10-01", "1"],
    ["ds_b", "daily_b", "02:00", "03:00", "0", "1560", "team_b@example.com", "2026-10-01", "1"],
]
AS_OF = parse_timestamp("2026-10-03T12:00:00Z")


class PipelineTestCase(unittest.TestCase):
    """Gives each test a temp dir, a DB path, a registry file and helpers to write input and run."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="sla_test_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.db_path = os.path.join(self.tmp, "t.db")
        self.registry_path = self.write_csv("registry.csv", REG_HEADER, REGISTRY)

    def write_csv(self, name, header, rows):
        path = os.path.join(self.tmp, name)
        with open(path, "w", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            if header is not None:
                w.writerow(header)
            w.writerows(rows)
        return path

    def write_text(self, name, text, mode="w", encoding=None):
        path = os.path.join(self.tmp, name)
        with open(path, mode, encoding=encoding, newline="") as fh:
            fh.write(text)
        return path

    def run_pipeline(self, path, as_of=AS_OF, registry=True, **kw):
        return pipeline.run_pipeline(self.db_path, path, as_of, self.registry_path if registry else None, **kw)

    def q(self, sql, *args):
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(sql, args).fetchall()
        finally:
            conn.close()

    def count(self, table):
        return self.q(f"SELECT COUNT(*) FROM {table}")[0][0]

    def statuses(self, dataset=None):
        sql = "SELECT dataset_id, expected_date, status FROM arrival_status"
        args = ()
        if dataset:
            sql += " WHERE dataset_id=?"
            args = (dataset,)
        return {(r[0], r[1]): r[2] for r in self.q(sql + " ORDER BY 1,2", *args)}
