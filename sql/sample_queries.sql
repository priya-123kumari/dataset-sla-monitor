-- Sample queries against output/sla_monitor.db   (sqlite3 output/sla_monitor.db < sql/sample_queries.sql)

-- 1. SLA scorecard per dataset
SELECT s.dataset_id, r.dataset_name, COUNT(*) AS days_evaluated,
       SUM(status = 'on_time')      AS on_time,
       SUM(status = 'late')         AS late,
       SUM(status = 'sla_breached') AS sla_breached,
       SUM(status = 'missing')      AS missing,
       ROUND(100.0 * SUM(status = 'on_time') / COUNT(*), 1) AS on_time_pct
FROM arrival_status s JOIN dataset_registry r USING (dataset_id)
GROUP BY s.dataset_id ORDER BY on_time_pct;

-- 2. Everything that needs attention right now (open alerts, worst first)
SELECT severity, dataset_id, alert_type, alert_key, message
FROM alerts WHERE resolved_run_id IS NULL
ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END, dataset_id, alert_key;

-- 3. Freshness of each dataset (how old is the newest completed load?)
SELECT dataset_id, last_completed_at, freshness_minutes, max_staleness_minutes, freshness_status
FROM dataset_freshness ORDER BY freshness_minutes DESC;

-- 4. Reconciliation: source rows = loaded + already loaded + rejected, per run
SELECT run_id, source_file, rows_in, rows_loaded, rows_already_loaded, rows_rejected,
       rows_in - (rows_loaded + rows_already_loaded + rows_rejected) AS unexplained_difference
FROM pipeline_runs WHERE status = 'success';

-- 5. Reconciliation: events in target vs. rows reported loaded by successful runs
SELECT (SELECT COUNT(*) FROM arrival_events) AS events_in_target,
       (SELECT SUM(rows_loaded) FROM pipeline_runs WHERE status = 'success') AS reported_loaded;

-- 6. Reject analysis: why were rows quarantined, and where did they come from?
SELECT reject_reason, COUNT(*) AS n, MIN(source_row_number) AS first_line, MAX(source_row_number) AS last_line
FROM rejects GROUP BY reject_reason ORDER BY n DESC;

-- 7. Trace one rejected row back to the exact source line
SELECT source_file, source_row_number, reject_reason, reject_detail, raw_record
FROM rejects ORDER BY source_file, source_row_number LIMIT 5;

-- 8. Status history: which loads changed state (e.g. missing -> sla_breached when data arrived late)?
SELECT dataset_id, expected_date, previous_status, new_status, run_id, changed_as_of
FROM arrival_status_history WHERE previous_status IS NOT NULL ORDER BY changed_as_of, dataset_id, expected_date;

-- 9. Typical arrival delay and SLA overrun per dataset (loads that arrived)
SELECT dataset_id, ROUND(AVG(delay_minutes), 1) AS avg_delay_min, MAX(delay_minutes) AS max_delay_min,
       ROUND(AVG(NULLIF(sla_overrun_minutes, 0)), 1) AS avg_overrun_when_breached_min
FROM arrival_status WHERE first_arrived_at IS NOT NULL GROUP BY dataset_id ORDER BY avg_delay_min DESC;

-- 10. Loads that were re-delivered (history kept, status unaffected)
SELECT dataset_id, expected_date, event_count, status FROM arrival_status WHERE event_count > 1 ORDER BY dataset_id, expected_date;

-- 11. Integrity: events whose dataset is not in the registry (must return 0 rows)
SELECT e.event_id FROM arrival_events e LEFT JOIN dataset_registry r USING (dataset_id) WHERE r.dataset_id IS NULL;
