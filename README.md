# llmgw

LLM gateway with a double-entry credit ledger and a Postgres-backed job queue.

**Status:** milestones 1–4 implemented: the ledger (schema, API, concurrency tests, a Hypothesis state machine); the synchronous path, an OpenAI-compatible streaming endpoint that holds credits up front and settles exact usage, tested against a deterministic mock provider; Redis rate limits per team and a shared provider budget per model; a sweeper that expires abandoned holds; and a Postgres job queue with atomic enqueue, fenced leases, retries, dead letters and retry-loss accounting.

## Layout

| Path | What |
|---|---|
| `db/migrations/` | Flyway migrations, plain SQL only (tests apply them without Flyway, so no `${placeholders}`) |
| `src/llmgw/ledger/` | Ledger operations: accounts, idempotent postings, holds, and `check_ledger` |
| `src/llmgw/gateway/` | `POST /v1/chat/completions`: authenticate, hold, stream through, settle; `POST /v1/jobs`, `GET /v1/jobs/{id}` |
| `src/llmgw/jobs/` | Job queue operations and the worker |
| `src/llmgw/ratelimit.py` | Redis token buckets: per-team limits and the shared provider budget |
| `src/llmgw/sweeper.py` | Expires holds nobody closed, reclaims lost job leases, dead-letters overdue jobs |
| `src/llmgw/pricing.py` | Price table and token estimates |
| `src/llmgw/mock_provider.py` | Deterministic OpenAI-compatible provider with injectable faults |
| `src/llmgw/db.py` | asyncpg pool and transaction helpers |
| `tests/ledger/` | Schema invariant tests, operation tests, concurrency tests, Hypothesis state machine |
| `tests/gateway/` | End-to-end tests against the mock provider on real sockets |
| `tests/jobs/` | Queue operations, sweeper passes and the worker against Postgres and the mock |
| `tests/test_ratelimit.py`, `tests/test_sweeper.py` | Limiter against real Redis; sweeper against real Postgres |

## Invariants enforced by the database

- Every posting's entries sum to zero, with at least two entries (deferred constraint trigger, checked at `COMMIT`).
- `entries` and `postings` are append-only (triggers reject `UPDATE` and `DELETE`).
- Each account's balance row is updated by a trigger in the same transaction as its entries, and a `CHECK` keeps every balance except the `external` funding account at or above zero.
- A hold's state, closing posting, and settled amount stay consistent (`CHECK`s on `holds`).
- A job has exactly one hold; a job's lease, result and finish time match its state (`CHECK`s on `jobs`); each attempt is recorded once and backs at most one retry-loss posting.

On top of that, `llmgw.ledger.checks.check_ledger` verifies the whole ledger: balances match their entries, every posting balances, team held balances match open holds, every hold and close posting has its hold row, every retry-loss posting has its job attempt, and every job's hold is open while the job is live and closed the matching way once it finishes.

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

## The job queue

`POST /v1/jobs` takes the same body as chat completions, without streaming. The job's hold (estimated prompt plus the output cap) and the job row are written in one Postgres transaction, so a job never exists without its hold or the other way round. The response is 202 with the job; poll `GET /v1/jobs/{id}` for its state and, once it succeeded, its `chat.completion` result. An `Idempotency-Key` header makes a resubmit return the stored job (200), or 409 if the body differs.

```bash
uv run llmgw worker &
curl localhost:8000/v1/jobs -H "authorization: Bearer $KEY" \
  -d '{"model": "mock-small", "messages": [{"role": "user", "content": "classify this"}]}'
curl localhost:8000/v1/jobs/1 -H "authorization: Bearer $KEY"
```

Workers lease jobs with `FOR UPDATE SKIP LOCKED` and renew the lease with a heartbeat while the call runs. Every lease bumps a fencing token; a worker that lost its lease can't settle, requeue or fail the job, only report its cost. Each attempt ends one of these ways:

| What happened | Job | Ledger |
|---|---|---|
| Stream finished | `succeeded`, result stored | hold settled at usage (or a flagged estimate), in the same transaction |
| Provider unreachable, 429, 5xx | queued again after backoff | nothing: providers don't bill those |
| Stream cut, timeout, lease lost, worker shut down | queued again after backoff | the counted estimate is posted to the org's `retry_loss` account, flagged; the team isn't charged |
| Provider 4xx | `failed` | hold released |
| Out of attempts, or past the deadline | `dead` | hold released |
| Rate limit refused before sending | queued again, attempt given back | nothing |

The team pays only for the attempt that succeeds. Retry losses are posted against the org's `external` account, so they can't fail when the org pool is empty; org spend to reconcile against the provider's bill is provider spend plus retry loss.

| Setting | Default | |
|---|---|---|
| `LLMGW_JOB_MAX_ATTEMPTS` | 5 | attempts before a job goes `dead` |
| `LLMGW_JOB_TTL_S` | 86400 | deadline from submission |
| `LLMGW_JOB_LEASE_S`, `LLMGW_JOB_HEARTBEAT_S` | 30, 10 | lease length and renewal interval (heartbeat ≤ half the lease) |
| `LLMGW_JOB_RETRY_BASE_S`, `LLMGW_JOB_RETRY_CAP_S` | 1, 60 | full-jitter backoff, at least the provider's `retry-after` |
| `LLMGW_WORKER_CONCURRENCY`, `LLMGW_WORKER_POLL_S` | 8, 1 | jobs in flight per worker process, idle poll interval |
| `LLMGW_WORKER_ID` | `host:pid` | recorded on leased jobs |

Scheduling is FIFO for now; fair queuing across teams and headroom for interactive traffic come later.

## The sweeper

`uv run llmgw sweeper` runs every `LLMGW_SWEEP_INTERVAL_S` (default 5 s), one row per transaction under `FOR UPDATE SKIP LOCKED`, so rows being worked on are skipped and several sweepers can run at once:

- expires holds past their `expires_at`, returning the credits to the team: the backstop for a close that failed or a gateway that died mid-stream. Job holds are left to the job paths;
- reclaims jobs whose lease expired (a worker died), recording the attempt as lost and queueing the job again, or dead-lettering it when it is out of attempts;
- dead-letters queued jobs past their deadline and releases their holds.

Ports: gateway `LLMGW_PORT` (8000), mock `LLMGW_MOCK_PORT` (8100), and the gateway's upstream `LLMGW_UPSTREAM_URL` (`http://127.0.0.1:8100`). Settings are in `src/llmgw/config.py`.
