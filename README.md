# llmgw

LLM gateway with a double-entry credit ledger and a Postgres-backed job queue.

**Status:** milestones 1–3 implemented: the ledger (schema, API, concurrency tests, a Hypothesis state machine); the synchronous path, an OpenAI-compatible streaming endpoint that holds credits up front and settles exact usage, tested against a deterministic mock provider; Redis rate limits per team and a shared provider budget per model; and a sweeper that expires abandoned holds.

## Layout

| Path | What |
|---|---|
| `db/migrations/` | Flyway migrations, plain SQL only (tests apply them without Flyway, so no `${placeholders}`) |
| `src/llmgw/ledger/` | Ledger operations: accounts, idempotent postings, holds, and `check_ledger` |
| `src/llmgw/gateway/` | `POST /v1/chat/completions`: authenticate, hold, stream through, settle |
| `src/llmgw/ratelimit.py` | Redis token buckets: per-team limits and the shared provider budget |
| `src/llmgw/sweeper.py` | Expires holds nobody closed |
| `src/llmgw/pricing.py` | Price table and token estimates |
| `src/llmgw/mock_provider.py` | Deterministic OpenAI-compatible provider with injectable faults |
| `src/llmgw/db.py` | asyncpg pool and transaction helpers |
| `tests/ledger/` | Schema invariant tests, operation tests, concurrency tests, Hypothesis state machine |
| `tests/gateway/` | End-to-end tests against the mock provider on real sockets |
| `tests/test_ratelimit.py`, `tests/test_sweeper.py` | Limiter against real Redis; sweeper against real Postgres |

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

Tests start a throwaway Postgres and Redis with testcontainers. For a local database with migrations applied:

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

To try it locally, start the database and apply migrations, then start the mock provider:

```bash
docker compose up -d postgres && docker compose run --rm flyway
uv run llmgw mock-provider &
```

There's no CLI for orgs and teams yet, so seed one through the ledger API. This prints the team id and an API key:

```bash
uv run python - <<'PY'
import asyncio

import asyncpg

from llmgw import auth, config
from llmgw.ledger import accounts, postings


async def main():
    conn = await asyncpg.connect(config.load().database_url)
    async with conn.transaction():
        org = await accounts.create_org(conn, "demo")
        await postings.fund_org(conn, org_id=org.org_id, amount=10_000_000, idempotency_key="demo-fund")
        team = await accounts.create_team(conn, org.org_id, "demo-team")
        await postings.grant(conn, team_id=team.team_id, amount=1_000_000, idempotency_key="demo-grant")
        print(f"team {team.team_id}: {await auth.create_api_key(conn, team.team_id)}")
    await conn.close()


asyncio.run(main())
PY
```

Further keys for an existing team: `uv run llmgw create-key --team-id N`. Then start the gateway and stream a reply (the hold settles at the mock's reported usage):

```bash
uv run llmgw gateway &
curl -N localhost:8000/v1/chat/completions -H "authorization: Bearer $KEY" \
  -d '{"model": "mock-small", "messages": [{"role": "user", "content": "hello"}], "stream": true}'
```

## Rate limits

With `LLMGW_REDIS_URL` set, each request is checked against token buckets in Redis before its hold is placed:

| Bucket | Setting | Refused with |
|---|---|---|
| Team tokens and requests per minute (same for every team) | `LLMGW_TEAM_TPM`, `LLMGW_TEAM_RPM` | 429 `team_rate_limited` |
| Per-model provider budget, shared by all teams | `LLMGW_PROVIDER_LIMITS='{"mock-small": {"tpm": 200000, "rpm": 500}}'` | 429 `provider_budget_exhausted` |

A request is charged its worst case (estimated prompt plus the output cap, the same number that sizes the hold) against every bucket at once, all or nothing, and the unused part is refunded when the call ends. A request that could never fit a bucket gets 400 `request_exceeds_rate_limit`. If Redis is unreachable the gateway lets requests through: rate limiting goes loose for a while, but the ledger stays correct.

The mock provider can enforce its own limits (`LLMGW_MOCK_TPM`, `LLMGW_MOCK_RPM`), returning 429 like a real provider. A test checks that a gateway budget below the provider's limit means the provider never refuses.

## The sweeper

`uv run llmgw sweeper` expires holds past their `expires_at` (every `LLMGW_SWEEP_INTERVAL_S`, default 5 s), returning the credits to the team. It is the backstop for a close that failed or a gateway that died mid-stream. Each hold is expired in its own transaction under `FOR UPDATE SKIP LOCKED`, so a hold being settled is skipped and several sweepers can run at once.

Ports: gateway `LLMGW_PORT` (8000), mock `LLMGW_MOCK_PORT` (8100), and the gateway's upstream `LLMGW_UPSTREAM_URL` (`http://127.0.0.1:8100`). Settings are in `src/llmgw/config.py`.
