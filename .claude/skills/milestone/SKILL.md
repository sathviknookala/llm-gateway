---
name: milestone
description: Run the next llmgw milestone end to end - orient, plan, adversarial plan review, build in small commits, docs sweep, definition of done. Use when the user asks to start, plan, review or build a milestone, or to "pick up the next milestone".
---

# Milestone workflow

The repeatable loop for completing a milestone in this repo. It has three gates where the user decides. Don't pass a gate on your own unless the user approved it in advance (e.g. "build once the review passes").

## 1. Orient
- Read the status line and Map in the root `CLAUDE.md`, the spec (its location is in `CLAUDE.local.md`; if there is none, ask), and `git log --oneline -15`. Check `git status` for uncommitted work.
- Find the next milestone in the spec's milestone list, the open questions that touch it, and what the code has today (grep for the feature; read the modules it will change).
- Present: what the milestone contains, what already exists, and the open decisions, each with a recommended default.
- **Gate 1:** the user settles the decisions, or asks for the plan.

## 2. Plan
- Write the plan to a file in the scratchpad, not the repo. Read the files each step touches before writing it.
- One step per unit that can land as its own commits. Order by dependency, and put backstops first (e.g. the sweeper went in before the features that rely on it).
- Mark judgment calls **(decision)** so the user can overrule them. List the tests per step and say what each one proves. End with an explicit "Out of scope" list naming the later milestone each item belongs to.
- State concretely: units (micro-credits, tokens), lock and transaction boundaries, which exit paths do what, and every new setting and error code.

## 3. Adversarial plan review
- Spawn one fresh `general-purpose` agent. Give it the plan path and the files to read, not your reasoning or expected answer. Tell it to read only and not edit.
- Scope it tightly: report only correctness bugs, invariant violations, broken existing contracts and tests, tests that can't be deterministic or can't observe the property, and contradictions with the spec. No style, no nice-to-haves, nothing already listed as out of scope. Name the riskiest parts to pressure-test.
- Ask for `VERDICT: PASS|FAIL`, numbered must-fix findings (step, failure scenario, file:line, fix) and at most 5 should-fix items, under ~700 words.
- Verify every finding against the code yourself before accepting it. Revise the plan, then send it back to the **same** agent (SendMessage) for a re-review: are the must-fix items resolved, and did the fixes introduce new ones? Repeat until PASS, then fold in cheap should-fix items.
- Tell the user the verdict and what changed. Don't claim results before the agent reports.
- **Gate 2:** plan approval, unless pre-approved.

## 4. Build
- Build in the main session. Milestone steps share files (`gateway/app.py`, `config.py`, fixtures), so don't delegate the build or split it across parallel sessions unless steps touch disjoint files and don't need each other's results.
- Keep the task list current; one step in progress at a time.
- Per step: code, then tests, then run that test file, then commit. Commits are small and imperative, matching history: `Add …` for code, `Test …` for tests, dependencies (`uv add`) in a commit of their own, docs last.
- Before step 1, commit or set aside pending housekeeping so milestone commits stay clean.

### Read/write practices
- Read a whole module before changing it. Don't re-read a file you just edited; the edit would have failed.
- For edits at several sites, use a short script that asserts each target string occurs exactly once before replacing it. Use Write for new files.
- Never edit an existing migration (add `V{n+1}__…`). New tables go in `TABLES` in `tests/conftest.py` and in `LedgerMachine`'s `TRUNCATE`.
- Pyright "import could not be resolved" diagnostics are editor-environment noise; `uv run` is the truth.
- Keep a backup of anything you mutate for a check (copy to the scratchpad), and restore it straight after.

### Test discipline
- Real Postgres and Redis through testcontainers. Never mock the database or Redis.
- Determinism: inject the clock (e.g. the limiter's `now_ms`) rather than sleeping. Pick limits so refill over a test's duration is far below one request. Poll for eventual effects (`wait_closed`, bucket levels) instead of fixed sleeps.
- Prove the tests bite: for each key behaviour, temporarily break it (e.g. zero the refund) and confirm the tests fail, then restore it and confirm `git diff` is clean.
- Concurrency claims get a `gather` test. Ledger-touching tests end with `check_ledger(conn) == []`.

## 5. Docs sweep
- Root `CLAUDE.md`: status line, Map entries, Commands, CLI modes.
- Each touched directory's `CLAUDE.md`: Responsibility, Contract (including error codes), Invariants, Gotchas. `tests/CLAUDE.md`: fixtures and entry points.
- README: status, layout, a section per user-visible feature with its settings.
- Spec: update the status line, add a dated "<feature> implementation decisions" section, strike the milestone through as done, and resolve any open questions it answered.
- Grep tracked files for stale wording: `git grep -n -i "not built\|not implemented\|milestone N"`.
- The repo is public. Tracked files must not reference private files or personal context. Follow `CLAUDE.local.md` on what stays untracked.

## 6. Definition of done and ship
- Full `uv run pytest` passes. Run Flyway `migrate validate` if migrations changed, and `docker compose config -q` if compose changed.
- Report what was built, the test counts, the mutation checks, and what wasn't verified (e.g. not run by hand end to end).
- **Gate 3:** push only when the user says so. Follow `CLAUDE.local.md` for branch, authorship and pre-push checks. After pushing, check CI (`gh run list --limit 1`).
