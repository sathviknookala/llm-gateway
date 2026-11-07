# tests
Responsibility: proving the ledger can't lose or invent credits — schema invariants, ledger operations, concurrency, and property tests.
Not responsible for: the Flyway check (CI's `migrations` job does that), load or fault tests (later milestones).

## Contract
- `conftest.py` fixtures:
  - `database_url` (session) — one Postgres 17 container per run; migrations applied in version order with plain asyncpg, no Flyway.
  - `conn` — a fresh connection with every table in `TABLES` truncated. Autocommit unless the test opens a transaction.
  - `pool` — up to 8 connections for concurrency tests; depends on `conn` so tables are truncated first.
- `ledger/conftest.py` fixtures, built through the ledger API: `org` (pool funded with 10,000), `team` (team A granted 1,000), and `tx` (`await tx(fn, **kwargs)` runs one ledger call in its own transaction). `in_pool_tx` does the same on a pool connection for concurrency tests.

## Invariants
- Real Postgres only; never mock the database.
- `ledger/test_db_invariants.py` bypasses `llmgw.ledger` on purpose and uses raw SQL. Keep it that way: it proves the schema holds even if application code is wrong.
- Every invariant in `db/migrations/` has a raw-SQL test there.

## Entry points
- `ledger/test_db_invariants.py` — schema invariants, raw SQL.
- `ledger/test_accounts.py`, `test_postings.py`, `test_holds.py` — operation tests.
- `ledger/test_checks.py` — `check_ledger` finds corruption.
- `ledger/test_concurrency.py` — races via `pool` and `asyncio.gather`.
- `ledger/test_ledger_properties.py` — Hypothesis `RuleBasedStateMachine` over the ledger API, checked against a Python model of every balance.

## Testing here
- `uv run pytest` (Docker required); one file: `uv run pytest tests/ledger/test_db_invariants.py`.
- `asyncio_mode = "auto"`: async tests and fixtures need no `@pytest.mark.asyncio`.

## Gotchas
- Ledger functions refuse to run outside a transaction; use `tx` / `in_pool_tx`, not a bare `conn`.
- A ledger error aborts the transaction; with `tx` the block rolls back on its own.
- New tables must be added to `TABLES` in `conftest.py`, or rows leak between tests.
- Fixture loop scope is `function`, so a session-scoped async fixture won't work. That's why `database_url` is sync and runs migrations with `asyncio.run`.
- Hypothesis `RuleBasedStateMachine` rules are sync; `LedgerMachine` owns a private event loop and one connection, and truncates the tables itself. If you add a table, add it to its `TRUNCATE` too.
- The first test in a run is slow: it starts the container and applies migrations.
