# Data dictionary

All timestamps are ISO-8601 UTC strings (`YYYY-MM-DDTHH:MM:SSZ`); dates are `YYYY-MM-DD`; `HH:MM` times are UTC.

## Source files

### events.csv  (producer → pipeline)
| column | type | nullable | notes |
|---|---|---|---|
| dataset_id | text | no | must exist in the registry (referential integrity) |
| expected_date | date | no | calendar date the arrival was scheduled for (not the arrival date) |
| arrived_at | timestamp | no | offsets converted to UTC; naive treated as UTC; a bare date is invalid |
| completed_at | timestamp | **yes** | blank = load still running; must be ≥ arrived_at |
| row_count | integer ≥ 0 | no | 0 is a valid (empty) load |

Extra columns are ignored with a warning; missing/duplicate required columns fail the run. Columns are matched by name, case-insensitively.

### registry.csv  (reference data, upserted every run)
| column | type | notes |
|---|---|---|
| dataset_id | text, PK | |
| dataset_name | text | |
| expected_arrival_time | HH:MM | daily expected arrival (UTC) |
| sla_completion_time | HH:MM | must be ≥ expected_arrival_time, same calendar day |
| grace_minutes | int ≥ 0 | on time if arrival ≤ expected + grace |
| max_staleness_minutes | int > 0 | freshness limit |
| owner | text | named in alert messages |
| active_from | date | first expected_date to evaluate |
| is_active | 0/1 | inactive datasets are not evaluated (events still load) |

An invalid registry file fails the run (reference data is trusted or the run stops).

## Target tables (SQLite)

### arrival_events  (append-only)
| column | type | null | notes |
|---|---|---|---|
| event_id | text PK | no | sha1(dataset_id\|expected_date\|arrived_at\|completed_at) |
| dataset_id | text FK→dataset_registry | no | |
| expected_date | date | no | |
| arrived_at | timestamp | no | |
| completed_at | timestamp | yes | |
| row_count | int ≥ 0 | no | |
| source_file | text | no | file the event was first loaded from |
| source_row_number | int | no | physical line in that file |
| run_id | text | no | run that first loaded it |
| ingested_at | timestamp | no | wall-clock load time |

Uniqueness: `event_id`. Index on `(dataset_id, expected_date)`.

### rejects
| column | type | notes |
|---|---|---|
| reject_id | text PK | sha1(source_file\|source_row_number) |
| run_id, source_file, source_row_number | | lineage back to the exact input line |
| reject_reason | text | one of: malformed_row, missing_required_field, invalid_date, invalid_timestamp, invalid_row_count, completed_before_arrived, timestamp_in_future, unknown_dataset, duplicate_event_in_file |
| reject_detail | text | e.g. which fields were blank, the bad value |
| raw_record | text | original fields as a JSON array |
| rejected_at | timestamp | |

### pipeline_runs
`run_id` PK, `source_file`, `as_of`, `started_at`, `finished_at`, `status` (`running|success|failed`), `rows_in`,
`rows_loaded` (new events), `rows_already_loaded` (valid but already present, i.e. reruns/re-sends), `rows_rejected`,
`status_rows_changed`, `alerts_opened`, `alerts_resolved`, `failure_reason`, `warnings`, `duration_seconds`.
Invariant for success runs: `rows_in = rows_loaded + rows_already_loaded + rows_rejected`. For failed runs `rows_in` and `rows_rejected` show what was observed before the failure; load counters are zeroed because the transaction was rolled back.
A run stuck in `running` means the process died before it could record its outcome. Its data transaction was never committed.

### arrival_status  (derived; PK dataset_id + expected_date)
| column | notes |
|---|---|
| expected_arrival_at / arrival_deadline_at / sla_deadline_at | derived from registry and expected_date (deadline = expected + grace) |
| status | pending, in_progress, on_time, late, sla_breached, missing |
| first_arrived_at, completed_at, row_count | from the canonical load (earliest arrival; latest completed_at among events of that arrival) |
| event_count | all events for the key, including re-loads |
| delay_minutes | max(0, first_arrived_at − expected_arrival_at); for missing/pending: minutes overdue as of `as_of` |
| sla_overrun_minutes | minutes past SLA deadline (0 if met; null if no arrival) |
| evaluated_as_of, run_id | `as_of` and run that last wrote the row (rows are only rewritten when a value changes) |

### arrival_status_history
`(dataset_id, expected_date, run_id)` PK, `previous_status` (null on first evaluation), `new_status`, `changed_as_of`.

### dataset_freshness  (derived snapshot, rebuilt each run)
`dataset_id` PK, `last_completed_at`, `freshness_minutes` (= as_of − last_completed_at), `max_staleness_minutes`,
`freshness_status` (`fresh|stale|no_data`), `evaluated_as_of`, `run_id`.

### alerts
`alert_id` PK = sha1(dataset_id\|alert_type\|alert_key); `alert_type` (`missing|late|sla_breached|stale`);
`alert_key` = expected_date, or last_completed_at for `stale`; `severity` (`critical|high|medium`); `message`
(what, how late, owner, suggested action); `opened_run_id/opened_as_of`; `resolved_run_id/resolved_as_of/resolution_note`
(null while open). New alerts are only raised for expected_dates within the lookback window (default 3 days);
existing open alerts are resolved whenever their condition stops being true.

## Derived-field rules
See the docstring at the top of `sla_monitor/transform.py` (the single source of truth for status precedence).
