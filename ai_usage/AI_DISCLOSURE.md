# AI disclosure

YottaFlex asked candidates to explain where AI accelerated their work, where they relied heavily on it, and what they
personally reviewed, changed or validated. Here is my honest account.

## Tool
Claude (Anthropic) in a chat interface. Prompt history: [PROMPT_HISTORY.md](PROMPT_HISTORY.md).

## Where I relied heavily on AI
- **Almost all of the first version of the project.** After I pasted the assignment, Claude proposed the design and
  wrote the code (`sla_monitor/`), the automated tests (`tests/`), the data generator and sample data, the SQL queries,
  and the documentation (README, `docs/`).
- **The design decisions** (SQLite, events as append-only facts with status calculated from them, reject table,
  one run = one transaction, deterministic IDs) came from Claude's plan. I did not design these from scratch.

## Where AI accelerated me
- Turning a long assignment brief into a working plan and a complete project well inside the time window.
- Writing the edge-case tests and the "answer key" check, which would have taken me much longer by hand.
- Explaining the project goal and the steps to run and submit it.

## What I personally did and verified
- Ran the full test suite on my own computer (Windows, Python 3.12.10): 63 tests, all OK.
- Ran the pipeline on the sample data, then ran `reconcile` (12/12 checks passed) and `report`.
- Ran the pipeline a second time and confirmed no duplicates were added (`rows_loaded: 0`, `rows_already_loaded: 42`).
- Decided to disclose AI use in this repository.
- Asked Claude to rewrite the README in simpler words so it reads the way I would explain it, then reviewed it and
  used that version.
- Before submitting, gave Claude the Submission Guidelines and FAQ and asked it to check my repo against them.

## What I changed myself
- I did not change the code logic. I changed the README wording only.

## Where AI could be wrong, and how I checked
- AI-written code can look correct and still be wrong. I did not rely on the AI saying it works. I ran the tests and
  compared the pipeline output with the separate answer key (`data/sample/expected/`), which the pipeline never reads.
- Limitation: the answer key and the code were both produced in the same AI session, so they share the same
  understanding of the rules. If a business rule was misunderstood, both could be consistently wrong. The rules are written
  in the README so a reviewer can judge them.

## Understanding
I can explain the main ideas: why status is calculated from events, how reruns avoid duplicates, what happens when a run
fails halfway, and the main limitation (status is recalculated over all history on every run).
