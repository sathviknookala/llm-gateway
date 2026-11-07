# llmgw
LLM gateway with a double-entry credit ledger and a Postgres-backed job queue.
Currently milestone 1 (ledger schema + invariants); the spec is `docs/design.md`.

## Commands
- Install: `uv sync`
- Test: `uv run pytest` (needs Docker; testcontainers starts a throwaway Postgres 17)
- One file: `uv run pytest tests/ledger/test_db_invariants.py`
- Local DB with migrations: `docker compose up -d postgres && docker compose run --rm flyway`
- Definition of done: `uv run pytest` passes, and migrations still pass Flyway `migrate validate` (CI `migrations` job)

## Map
- `db/migrations/` — Flyway SQL; source of truth for schema and ledger invariants (see its CLAUDE.md)
- `src/llmgw/ledger/` — accounts, idempotent postings, the hold lifecycle, and `check_ledger` (see its CLAUDE.md)
- `src/llmgw/db.py` — asyncpg pool, `transaction()` helper, `require_transaction()`
- `src/llmgw/config.py` — settings from `LLMGW_*` env vars
- `src/llmgw/__main__.py` — CLI modes: gateway, worker, sweeper, reconciler (none implemented)
- `tests/` — `conftest.py` applies migrations with plain asyncpg (no Flyway) and truncates tables per test (see its CLAUDE.md)
- `docs/design.md` — living design spec; check it before making design decisions

## Hard rules
- Never edit an existing migration; add `V{n+1}__name.sql`.
- Migrations are plain SQL: no Flyway `${placeholders}` (tests run them without Flyway).
- Ledger invariants live in the database (triggers, CHECKs). Don't duplicate or bypass them in Python.
- `entries` and `postings` are append-only. Never UPDATE or DELETE them, including in tests.
- Nothing network-shaped (provider, Redis, retries) inside `transaction()`.

## Conventions that differ from defaults
- Amounts are integer micro-credits. Never floats.
- Explicit SQL via asyncpg; no ORM.
- Async drivers only on request/job paths (asyncpg, redis.asyncio, httpx.AsyncClient); never block the event loop.
- Insert a posting's entries in `account_id` order (consistent lock order, avoids deadlocks).
- Map DB errors to `llmgw.ledger.errors` types (e.g. CHECK violation → `InsufficientCredits`), not raw asyncpg errors.

## Gotchas
- The sum-zero check is deferred to COMMIT, so an unbalanced posting fails when `transaction()` exits, not at the INSERT.
- Settles after a cancelled stream must run under `anyio.CancelScope(shield=True)`; see "Runtime notes" in `docs/design.md`.
