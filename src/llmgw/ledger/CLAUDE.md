# src/llmgw/ledger
Responsibility: the Python API over the ledger: orgs and teams with their accounts, idempotent postings (fund, grant), the hold lifecycle (place, then exactly one of settle, release, expire), and the whole-ledger consistency check.
Not responsible for: enforcing invariants (the database does, in `db/migrations/`), pools and transactions (`llmgw/db.py`), provider calls or rate limits.

## Contract
- Every function takes an `asyncpg.Connection` and runs inside the caller's transaction. Never open, commit or roll back a transaction here. Writers call `llmgw.db.require_transaction(conn)` first and raise `RuntimeError` without one.
- A ledger error leaves the caller's transaction aborted; the caller rolls back.
- Public errors are in `errors.py`: `InsufficientCredits`, `IdempotencyConflict`, `InvalidHoldState`, `HoldNotFound`. Raw asyncpg errors for those cases must not escape.
- `models.py` enums mirror the Postgres enums in `db/migrations/V1__ledger_tables.sql`; change both together.

## Invariants
- Idempotency: same key + same `request_hash` → return the existing posting, write nothing. Same key + different hash → `IdempotencyConflict`. `request_hash` covers the operation's inputs (canonical JSON: op plus its arguments), not its entries, because the inputs are what a retry repeats. `place_hold` leaves `expires_at` out of it, since a retry recomputes it.
- Keys: fund and grant take a caller key. `place_hold` builds `hold:{request_id}`. Closes build `settle:{hold_id}`, `release:{hold_id}`, `expire:{hold_id}`; callers never pass close keys.
- Lock order: the hold row (`FOR UPDATE`) first, then balance rows, which the entry inserts lock in `account_id` order. Insert a posting's entries in `account_id` order.
- Close order: lock hold → replay check on its own key → require state `held` (else `InvalidHoldState`, no posting) → post → `UPDATE holds ... WHERE id = $1 AND state = 'held'`.
- Settle charges `min(actual, amount)`: team_held −amount, provider_spend +charged, team_available +(amount − charged). `actual > amount` charges the full hold, flags the posting, and reports `uncovered`; reconciliation books the rest. A settle on an expired hold raises `InvalidHoldState`; that cost is also left to reconciliation.
- Python checks arguments only (positive amounts, `actual >= 0`). Overdrafts, sum-zero and append-only are the database's job; don't re-check them here.
- SQL only. No network calls, no retries, no sleeps.

## Entry points
- `accounts.py` — `create_org`, `create_team`, `org_accounts`, `team_accounts`, `balance`, `team_summary`.
- `postings.py` — `post`, `request_hash`, `find_posting`, `check_replay`, `fund_org`, `grant`.
- `holds.py` — `place_hold`, `settle`, `release`, `expire`, `get_hold`.
- `checks.py` — `check_ledger`: returns a list of violations, empty when consistent.
- `models.py` — value types and enums; `errors.py` — ledger exceptions.

## Testing here
- `uv run pytest tests/ledger` (Docker required).
- Fixtures in `tests/ledger/conftest.py`: `org` (pool funded 10,000), `team` (granted 1,000), and `tx`, which runs one ledger call in its own transaction.

## Gotchas
- `entries.amount` can't be 0. Leave out zero legs (`holds._legs` does): a settle with `charged == amount` has no team_available leg, and one with `actual == 0` has no provider_spend leg.
- An overdraft fails immediately at the entry INSERT as `CheckViolationError` with `constraint_name == 'balance_non_negative'`. Map only that constraint to `InsufficientCredits`; an unbalanced posting is also a `check_violation`, and is a bug, not a user error.
- An unbalanced posting fails at the caller's COMMIT, not inside `post()`.
- The replay check runs before the state check, so a settle retried after it succeeded returns the stored result rather than `InvalidHoldState`.
- `expire` writes the same entries as `release` but sets state `expired`, and refuses a hold before its `expires_at`.
- A hold row and its hold posting go in the same transaction (`hold_posting_id` is NOT NULL).
