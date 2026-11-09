# db/migrations
Responsibility: the ledger schema and every invariant the database enforces (triggers, CHECKs, unique indexes), plus gateway tables (`api_keys`).
Not responsible for: ledger operations or mapping DB errors to Python errors (that's `src/llmgw/ledger/`).

## Contract
- Files are `V{n}__snake_case.sql`, applied in numeric order by Flyway (docker compose, CI) and by `tests/conftest.py` with plain asyncpg.
- Plain SQL only: no `${placeholders}`, no callbacks. Only `V` files are picked up by conftest, so no `R__` repeatable migrations.
- Names and messages other code depends on: tests assert the `balance_non_negative` constraint name, the `unbalanced` / `append-only` messages, and the `check_violation` / `restrict_violation` error codes; `llmgw.ledger` maps `balance_non_negative` to `InsufficientCredits`. Renaming any of them needs matching changes there.
- Postgres enums (`account_kind`, `posting_kind`, `hold_state`) mirror the `StrEnum`s in `src/llmgw/ledger/models.py`; change both together.

## Invariants
- Never edit or rename a migration once written; fix forward with the next `V{n}`.
- Amounts are `bigint` micro-credits.
- `entries` and `postings` are append-only; corrections are new postings. `holds` is the only ledger table updated in place, and its CHECKs keep state, `close_posting_id`, `closed_at` and `settled_amount` consistent.
- A new account kind is non-negative by default (`ledger_create_balance` sets `non_negative = kind <> 'external'`); decide explicitly if it may go negative.
- Every new invariant gets a raw-SQL test in `tests/ledger/test_db_invariants.py`.

## Entry points
- `V1__ledger_tables.sql` — tables, enums, indexes, CHECKs.
- `V2__ledger_invariants.sql` — triggers: balance row per account, balances moved by entries, deferred sum-zero check, append-only guard.
- `V3__api_keys.sql` — team API keys: SHA-256 hash (32 bytes, unique), display prefix, `revoked_at`.

## Testing here
- `uv run pytest tests/ledger/test_db_invariants.py` — applies all migrations to a fresh Postgres 17.
- Real Flyway: `docker compose up -d postgres && docker compose run --rm flyway`. CI runs `migrate validate`.

## Gotchas
- Each file runs in one transaction (Flyway and conftest). `ALTER TYPE ... ADD VALUE` is allowed there on Postgres 17, but the new value can't be used in the same transaction; use it in the next migration.
- The local `pgdata` volume keeps applied migrations; Flyway fails on checksum if an applied file changed. Reset with `docker compose down -v`.
- New tables must be added to `TABLES` in `tests/conftest.py`, or rows leak between tests.
- Tests reset with `TRUNCATE`, which skips the row-level append-only triggers. A statement-level `TRUNCATE` trigger would break the test reset.
- The sum-zero trigger fires from both `postings` and `entries` inserts, so a posting with no entries also fails at COMMIT.
- `posting_kind` has no value for retry-loss postings yet; the `retry_loss` account exists but nothing posts to it.
