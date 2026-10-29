# Dataset Arrival & SLA Monitor

## What this project does

Companies receive data files every day (orders, payments, etc.). Each file should arrive at a set time and finish loading before a deadline. If a file is late, missing, or slow, reports built on it become wrong.

This project is a **watchdog** for that problem. It reads a list of arrival events and checks:

- Did each dataset arrive on time?
- Is it late, missing, or did it miss its deadline (SLA)?
- How old is the newest data (freshness)?
- If something is wrong, it creates an **alert** with the dataset, the date, the owner, and what to do next.

It uses only Python and SQLite, so there is nothing to install.

## How it works

```
events.csv + registry.csv  ->  check schema  ->  validate rows  ->  load  ->  calculate status  ->  alerts
                                                     |
                                                     v
                                          bad rows go to a "rejects" table
```

1. **Read** the events file and make sure the columns are correct.
2. **Validate** each row. Bad rows (missing values, wrong dates, unknown dataset, duplicates) are saved in a `rejects` table with the file name and line number, so nothing disappears silently.
3. **Load** the good rows. Old events are never changed, only added.
4. **Calculate** the status of each dataset for each day from those events.
5. **Create alerts** for late, missing, or breached loads. Alerts close by themselves when the problem is fixed.
6. **Record the run** (counts, status, errors) in a `pipeline_runs` table.

## Status rules

Every dataset has an expected arrival time, a grace period, and an SLA completion time (all in UTC).

| Status | Meaning |
|---|---|
| `on_time` | Arrived within the grace period and finished before the SLA time |
| `late` | Arrived after the grace period, but still finished before the SLA time |
| `sla_breached` | Finished after the SLA time, or never finished |
| `missing` | Nothing arrived and the grace period is over |
| `pending` / `in_progress` | Not due yet / arrived but still loading |

Arriving exactly at a deadline counts as on time.

## How to run it

Run these from the project folder. Use `python` on Windows, or `python3` on Mac/Linux.

```bash
# 1. Run the tests (63 tests, they should all pass)
python -m unittest discover -s tests -t . -v

# 2. Run the pipeline on the sample data
python -m sla_monitor.pipeline --registry data/sample/registry.csv --input data/sample/events.csv --as-of 2026-10-08T23:00:00Z

# 3. Check the results against a separate answer key
python -m sla_monitor.reconcile --expected data/sample/expected

# 4. Show a readable report and save CSV files in output/
python -m sla_monitor.report --out-dir output
```

**Expected results on the sample:**
- Step 1 ends with `OK`.
- Step 2 shows `"status": "success"`, 51 rows in, 42 loaded, 9 rejected.
- Step 3 ends with `12/12 checks passed`.

**Try running step 2 a second time.** It should show `rows_loaded: 0`, because the same data is never added twice.

**Optional: test with a large dataset (about 100,000 rows)**

```bash
python -m sla_monitor.benchmark --rows 100000 --datasets 50
```

## Safe to rerun

- Every event, reject and alert has a fixed ID built from its content, so running the same file again adds nothing new.
- One run is one database transaction. If something fails halfway, everything is rolled back and the failure is saved in `pipeline_runs`. To retry, just run the same command again.
- Files can be loaded in any order. Status is always recalculated from all events, so a late file simply corrects the result (and the change is kept in `arrival_status_history`).

## How I checked it is correct

- **Automated tests (63):** happy path, empty file, malformed rows, duplicates, missing values, schema changes, a failure halfway through, reruns, late and out-of-order files, deadline edge cases (exactly at the deadline vs. one minute after), and unknown dataset IDs.
- **Answer key:** the data generator creates sample data with problems added on purpose, and writes the expected answers to a separate folder (`data/sample/expected/`). The pipeline never reads this folder. `reconcile` compares the pipeline output to it. On the sample, all 12 checks pass. The same check also passes on the 100,000-row dataset.

## Performance

On a 100,000-row dataset the full run takes about 8 seconds. I used a profiler to find the slowest part, which was formatting timestamps, and replaced it with a faster method that gives identical output. The remaining slow part is that status is recalculated for all history on every run.

## Limitations

- Daily schedules only, and all times are UTC (no time zones, weekly schedules, or holidays).
- Status is recalculated over all history each run. This is simple and correct, but slow for very large data.
- One run at a time (SQLite allows a single writer).
- Alerts are saved as database rows. They are not sent to Slack or email.
- If the registry changes (for example a new grace period), old results are recalculated with the new values. Old registry values are not saved.

## Next steps

- Recalculate only the dates affected by a new file, instead of all history.
- Save a history of registry changes.
- Send alerts to Slack or email.
- Run it on a schedule (cron or Airflow).

## Project layout

```
sla_monitor/   the pipeline code (ingest, validate, transform, publish, alerts, pipeline)
tests/         automated tests
data/sample/   small sample data, plus expected/ with the answer key
sql/           example queries
docs/          data dictionary and design notes
output/        saved results from running the sample
```

## Note on tools

I used an AI assistant while building this project. I ran the tests myself, checked the results against the answer key, and I can explain how each part works.