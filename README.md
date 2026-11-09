# llmgw

LLM gateway with a double-entry credit ledger and a Postgres-backed job queue. The design is in [docs/design.md](docs/design.md).

**Status:** milestones 1 and 2 implemented: the ledger (schema, API, concurrency tests, a Hypothesis state machine) and the synchronous path, an OpenAI-compatible streaming endpoint that holds credits up front and settles exact usage, tested against a deterministic mock provider.

## Layout

| Path | What |
|---|---|
| `db/migrations/` | Flyway migrations, plain SQL only (tests apply them without Flyway, so no `${placeholders}`) |
| `src/llmgw/ledger/` | Ledger operations: accounts, idempotent postings, holds, and `check_ledger` |
| `src/llmgw/gateway/` | `POST /v1/chat/completions`: authenticate, hold, stream through, settle |
| `src/llmgw/pricing.py` | Price table and token estimates |
| `src/llmgw/mock_provider.py` | Deterministic OpenAI-compatible provider with injectable faults |
| `src/llmgw/db.py` | asyncpg pool and transaction helpers |
| `tests/ledger/` | Schema invariant tests, operation tests, concurrency tests, Hypothesis state machine |
| `tests/gateway/` | End-to-end tests against the mock provider on real sockets |

## Invariants enforced by the database

- Every posting's entries sum to zero, with at least two entries (deferred constraint trigger, checked at `COMMIT`).
- `entries` and `postings` are append-only (triggers reject `UPDATE` and `DELETE`).
- Each account's balance row is updated by a trigger in the same transaction as its entries, and a `CHECK` keeps every balance except the `external` funding account at or above zero.
- A hold's state, closing posting, and settled amount stay consistent (`CHECK`s on `holds`).

On top of that, `llmgw.ledger.checks.check_ledger` verifies the whole ledger: balances match their entries, every posting balances, team held balances match open holds, and every hold and close posting has its hold row.

## Running

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync
uv run pytest
```

Tests start a throwaway Postgres with testcontainers. For a local database with migrations applied:

```bash
docker compose up -d postgres && docker compose run --rm flyway
```

## The synchronous path

Every request places a hold for its worst case (estimated prompt plus the output cap), streams the provider's bytes through untouched, and closes the hold exactly once:

| What happened | Hold closes as |
|---|---|
| Stream finished with a usage block | settle at exact cost (capped at the hold, flagged if above it) |
| Provider unreachable, or answered with an error status | release, full refund |
| Client disconnected, upstream cut off, timeout, or no usage block | settle on prompt estimate plus counted output, flagged for reconciliation |

The close runs in its own task under a shielded cancel scope, so a cancelled request still settles. Tests cover each row, cancellation of the request task, and concurrent mixed traffic.

To try it locally (with the database up and migrated):

```bash
uv run llmgw mock-provider &
uv run llmgw create-key --team-id 1
uv run llmgw gateway
```
