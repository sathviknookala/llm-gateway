# src/llmgw/ledger
Responsibility: the Python API over the ledger: orgs and teams with their accounts, idempotent postings (fund, grant), and the hold lifecycle (place, then exactly one of settle, release, expire).
Not responsible for: enforcing invariants (the database does, in `db/migrations/`), pools and transactions (`llmgw/db.py`), provider calls or rate limits.

## Contract
- Every function takes an `asyncpg.Connection` and runs inside the caller's transaction. Never open, commit or roll back a transaction here.
- Public errors are in `errors.py`: `InsufficientCredits`, `IdempotencyConflict`, `InvalidHoldState`. Raw asyncpg errors for those cases must not escape.
- `models.py` enums mirror the Postgres enums in `db/migrations/V1__ledger_tables.sql`; change both together.
- Most functions are stubs. Their docstrings are the spec, and the "To cover" list at the top of each `tests/ledger/test_*.py` is the acceptance criteria.

## Invariants
- Idempotency: same key + same `request_hash` → return the existing posting, write nothing. Same key + different hash → `IdempotencyConflict`. `request_hash` must be deterministic over kind and entries.
- Insert a posting's entries in `account_id` order (consistent row-lock order, no deadlocks).
- Close a hold only via `UPDATE holds ... WHERE id = $1 AND state = 'held'`. Zero rows → write no posting and raise `InvalidHoldState`, unless it is a replay of the same close.
- Settle: `0 <= actual <= amount`; team_held −amount, provider_spend +actual, team_available +(amount − actual).
- SQL only. No network calls, no retries, no sleeps.

## Entry points
- `accounts.py` — `create_org`, `create_team`, `account_id`, `balance`.
- `postings.py` — `post`, `request_hash`, `fund_org`, `grant`.
- `holds.py` — `place_hold`, `settle`, `release`, `expire`.
- `models.py` — value types and enums; `errors.py` — ledger exceptions.

## Testing here
- `uv run pytest tests/ledger` (Docker required).
- `test_postings.py`, `test_holds.py`, `test_concurrency.py` and `test_ledger_properties.py` are skipped until implemented; remove the module-level `pytest.skip` when you implement the code they cover.

## Gotchas
- `entries.amount` can't be 0. Leave out zero legs: a settle with `actual == amount` has no team_available leg, and one with `actual == 0` has no provider_spend leg.
- An overdraft fails immediately at the entry INSERT as `CheckViolationError` with `constraint_name == 'balance_non_negative'`. Map only that constraint to `InsufficientCredits`; an unbalanced posting is also a `check_violation`, and is a bug, not a user error.
- An unbalanced posting fails at the caller's COMMIT, not inside `post()`.
- `expire` writes the same entries as `release` but sets state `expired`.
- A hold row and its hold posting go in the same transaction (`hold_posting_id` is NOT NULL).
