# Design and trade-off note

## Decisions

**SQLite + Python stdlib.** The brief rewards correctness, clarity and reproducibility over scale tech. SQLite gives
real transactions, constraints, foreign keys and SQL reports with zero setup; the whole thing runs from a clean checkout
with no installs. The schema is plain SQL, so moving to Postgres/DuckDB is mostly mechanical (`ON CONFLICT` upserts and
`INSERT OR IGNORE` would become `ON CONFLICT DO NOTHING`).

**Events are facts, status is derived.** `arrival_events` is append-only; `arrival_status` is recomputed as a pure
function of (events, registry, `as_of`). Alternatives, such as updating a status row when a file arrives, make
out-of-order files, late data, and backfills order-dependent and hard to test. The derived approach trades some
recompute cost for being obviously correct. `arrival_status_history` records each transition, so "silently overwriting"
never happens even though the current-state table is rewritten.

**`expected_date` supplied by the source.** Attributing an arrival to a date by its timestamp breaks for loads that
land after midnight. Having the producer state which scheduled slot the load is for removes the ambiguity (tested with
an arrival at 00:30 the next day).

**Event identity includes `completed_at`.** An "in progress" event and the later "completed" event for the same arrival
are two immutable facts, not an update. The status logic collapses them (latest completion within the earliest arrival).
Re-loads (a different `arrived_at`) are kept but ignored for status, since the first arrival is what the SLA measures.

**One run = one file = one transaction.** Simple failure semantics: all-or-nothing, retry = rerun. The `pipeline_runs`
row is written *outside* the data transaction so even a crash leaves evidence.

**Reject, don't crash, for row problems; crash, don't guess, for file problems.** Bad rows are quarantined with
file/line/raw record and counted (a bad row must not block 100k good ones). A missing column means the whole file's
meaning is unknown, so the run fails loudly before anything loads. Extra columns are tolerated (forward-compatible).

**First failing check wins** gives each reject exactly one deterministic reason, which makes reject counts reconcilable
and the answer-key comparison exact.

**Answer key by construction.** The generator decides a scenario per dataset-day and builds timestamps from it, so the
expected status is known independently of the pipeline. This catches logic bugs a self-consistency check would miss. In
fact, writing the tests exposed a real gap (events before `active_from` had no status row); this was fixed in `build_statuses`.

## Trade-offs and what I would change at scale

| Choice | Benefit | Cost / limit |
|---|---|---|
| Recompute status for all history every run | trivially correct, order-independent | O(total days); ~8 s at 100k rows. Next step: recompute only keys touched by the new events plus "open" (pending/missing) keys |
| Skip unchanged status rows on write | rerun writes 0 rows, history stays clean | compares every row in Python first |
| Registry applied retroactively | simple | old registry values not versioned (SCD2 is the fix) |
| UTC, daily cadence only | clear semantics | no DST/local-time schedules, no weekly/monthly |
| Alerts are table rows | testable, de-duplicated, auditable | delivery (Slack/email) is out of scope |
| Single SQLite writer | no concurrency bugs | one run at a time |

## Performance reasoning
Measured with `python -m sla_monitor.benchmark` (results in `output/benchmark_results.txt`). Stage timings showed
`transform` as the dominant cost, so I profiled it with cProfile instead of guessing: `strftime` in `fmt_ts` was ~37%.
`isoformat` produces byte-identical output (the benchmark asserts it) and is ~1.5× faster in isolation; whole-run time
fell ~10.1 s → ~8.4 s (timings vary a little run to run; the 10.1 s figure is from before the change, same machine). A second, structural change is bulk `INSERT OR IGNORE` for the event load instead of a `SELECT`
per row (1.3× here, but it avoids N round-trips and keeps working as the table grows). The honest conclusion is that
neither is transformative; the real win is incremental recompute (above).

## Operational notes
* Exit code: 0 success, 1 failed run (so a scheduler can alert on it). `reconcile` also exits non-zero on any failed check.
* Logs are `key=value` lines on stderr including `run_id`, stage, counts, duration; the JSON summary goes to stdout.
* A `pipeline_runs` row left in `running` means the process was killed mid-run; its transaction never committed, so simply rerun.
