# Prompt history

Tool used: Claude (Anthropic), web/app chat. All prompts I sent are listed below in order, exactly as I typed them
(typos included, on purpose, so this stays an accurate record). AI responses are not included (the guidelines say they
are optional). Large pasted attachments are named instead of copied in full.

Each prompt has a short **Why / What I did next** note so a reviewer can follow how I used the AI. It shows where I
asked AI to do the work and where I checked the result myself. See also [AI_DISCLOSURE.md](AI_DISCLOSURE.md).

---

## How I worked, in short

| Stage | Prompts | What I did |
|---|---|---|
| 1. Understand the task | 1, 3 | Gave the AI the full brief, then asked it to explain the end goal before going further |
| 2. Build | 2 | Accepted the plan to build in Python + SQLite |
| 3. Verify on my machine | 4 | Ran the tests, pipeline, reconcile and report myself and sent back the **real terminal output** |
| 4. Own the docs | 5, 6 | Rewrote the README in my own simple words |
| 5. Check before submitting | 7 | Gave it the Submission Guidelines and FAQ and asked it to check my repo against them |

---

## Prompts

**Prompt 1** (attachment: the full text of the YottaFlex assignment brief, pasted in)
> how i proceed this and what is the test cases of this .

- **Why:** I gave it the whole brief instead of a summary so it had the full context. I asked about test cases
  from the start because I wanted to know how the work would be checked before writing any code.

**Prompt 2**
> yes

*(This was my reply to Claude's offer to build the whole project with Python + SQLite and give me the files.)*

- **Why:** I agreed to the proposed design: SQLite, status calculated from append-only events, a reject table,
  one run = one transaction, deterministic IDs. AI_DISCLOSURE.md says clearly that this design came from Claude.

**Prompt 3**
> first make me understand what i have to done what is the end goal of this project

- **Why:** I did not want to submit code I could not explain, so I stopped and asked for the end goal before
  running anything.

**Prompt 4** (attachment: the terminal output from running the tests, pipeline, reconcile and report commands on my machine)
> every command run good as you give so what is the nnext step i give you the terminal output

- **What I did before this prompt:** I ran everything myself on Windows, Python 3.12.10:
  - 63 tests, all OK
  - pipeline on the sample data, then `reconcile`: 12/12 checks passed
  - `report`
- **Why I sent the output:** I wanted the AI to work from real results on my machine, not from its own guess
  that the code works.
- I also ran the pipeline a second time and saw that no duplicates were added (`rows_loaded: 0`,
  `rows_already_loaded: 42`).

**Prompt 5**
> okay if i cannot change anything just change the readme file in my words then it is good .

- **Why:** I wanted the README to sound like me and be something I can explain line by line.

**Prompt 6** (attachment: the README.md text)
> okay i give you readme file just give me simple form

- **What I did next:** I used the simpler version as the README in the repo.

**Prompt 7** (attachments: the two YottaFlex PDFs, the Submission Guidelines and the FAQ)
> is there any change required in any place in my code befotre submitting my github repo so please give me

- **Why:** Before submitting, I checked the repo against the official rules, not just my own reading of them.

---

## Prompts to other tools

*(None. Add any here if you used another AI tool or another chat.)*

## What I would do better next time

Looking back at my prompts:
- I would ask the AI to **explain the design trade-offs** (for example, why SQLite, and why status is recalculated
  from events) *before* saying "yes" in Prompt 2, so I take part in the design instead of only accepting it.
- I would ask it to **write the answer key separately from the code**, or write some expected results myself. In
  this project both came from the same session, so a misunderstood rule could be wrong in both (see
  AI_DISCLOSURE.md).
- I would write more specific prompts, for example "write a test for a file that arrives after the SLA deadline",
  instead of broad questions.

## Limitations of this record

This is a copy of my prompts from the chat history. If any earlier chats or tools were used that are not listed above,
that is a gap in this record.
