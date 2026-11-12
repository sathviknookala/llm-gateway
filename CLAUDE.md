# llmgw
LLM gateway with a double-entry credit ledger and a Postgres-backed job queue.
Milestones 1 (ledger), 2 (synchronous path against the mock provider), 3 (Redis rate limits and the shared provider budget, plus the hold-expiry sweeper), 4 (job queue: atomic enqueue, fenced leases, retries, dead letters, retry-loss postings) and 5 (container image, Compose stack and single-VM deploy, Prometheus metrics and JSON logs, a stated load test and fault test) are done.

## Commands
- Install: `uv sync`
- Test: `uv run pytest` (needs Docker; testcontainers starts a throwaway Postgres 17, and redis:7 for tests that use it)
- One file: `uv run pytest tests/ledger/test_db_invariants.py`
- Local DB with migrations: `docker compose up -d postgres && docker compose run --rm flyway`; Redis: `docker compose up -d redis`
- Run locally: `uv run llmgw mock-provider` (port 8100), then `uv run llmgw gateway` (port 8000), `uv run llmgw worker` and `uv run llmgw sweeper`. Rate limits are on only when `LLMGW_REDIS_URL` is set. `uv run llmgw seed --teams N` creates a funded org and teams and prints their keys; `uv run llmgw create-key --team-id N` adds a key; `uv run llmgw check-ledger` exits 1 on any inconsistency
- Whole stack in Docker: `docker compose --profile stack up -d --build` (gateway on `LLMGW_GATEWAY_PORT`, default 8000; Prometheus 127.0.0.1:9090, Grafana 127.0.0.1:3000)
- Load test: `uv run python bench/run_load.py --rate 500 --duration 60s`; fault test: `uv run python bench/faults.py --seed 42 --duration 300`. Both bring the stack up themselves, write `bench/results/` (gitignored), and are not part of CI
- Definition of done: `uv run pytest` passes, migrations still pass Flyway `migrate validate` (CI `migrations` job), and the CI `image` job's checks pass (image builds; Compose files and overlays resolve; only Caddy is public in the deploy overlay)
- Milestone work (plan, adversarial review, build, docs sweep): use the `/milestone` skill in `.claude/skills/milestone/`

## Map
- `db/migrations/` — Flyway SQL; source of truth for schema and ledger invariants (see its CLAUDE.md)
- `src/llmgw/ledger/` — accounts, idempotent postings, the hold lifecycle, and `check_ledger` (see its CLAUDE.md)
- `src/llmgw/gateway/` — `/v1/chat/completions`: hold, stream through, close the hold exactly once; `/v1/jobs` submit and poll (see its CLAUDE.md)
- `src/llmgw/jobs/` — job queue operations (enqueue, fenced leases, finishes) and the worker (see its CLAUDE.md)
- `src/llmgw/ratelimit.py` — Redis token buckets (team TPM/RPM, per-model provider budget), one all-or-nothing Lua script; fails open
- `src/llmgw/sweeper.py` — expires overdue non-job holds, reclaims expired job leases, dead-letters jobs past their deadline; one row per transaction, `FOR UPDATE SKIP LOCKED`
- `src/llmgw/pricing.py` — price table (micro-credits per token) and pessimistic token estimates
- `src/llmgw/auth.py` — team API keys, stored as SHA-256 hashes
- `src/llmgw/mock_provider.py` — deterministic OpenAI-compatible provider; fault scenarios via a `mock` body field (per-attempt with `key` + `script`); optional fixed-window TPM/RPM limits
- `src/llmgw/db.py` — asyncpg pool, `transaction()` helper, `require_transaction()`
- `src/llmgw/config.py` — settings from `LLMGW_*` env vars
- `src/llmgw/metrics.py` — every Prometheus metric, on the default registry; `looplag.py` — event-loop lag monitor; `stats.py` — gauges read from the database and the periodic `check_ledger`, run by the sweeper
- `src/llmgw/logs.py` — structlog + stdlib logging to one stream, JSON or console, with bound request/job context
- `src/llmgw/__main__.py` — CLI modes: gateway, mock-provider, create-key, seed, check-ledger, worker, sweeper (reconciler: milestone 7); SIGTERM handling for each
- `Dockerfile`, `docker-compose.yml` — one image for every mode; the `stack` profile runs the app with Prometheus and Grafana
- `ops/` — Prometheus scrape config and alert rules, Grafana provisioning and dashboard
- `deploy/` — single-VM overlay (Caddy for TLS, no public database ports), `deploy.sh`, setup notes
- `bench/` — k6 load test and its runner, the fault harness (`faults.py`, with `docker-compose.faults.yml` and toxiproxy), stated results in `bench/RESULTS.md`
- `tests/` — `conftest.py` applies migrations with plain asyncpg (no Flyway) and truncates tables per test (see its CLAUDE.md)

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
- Hold closes after a cancelled stream, and the worker's attempt finishes, run as their own task under `anyio.CancelScope(shield=True)`; see `src/llmgw/gateway/CLAUDE.md` and `src/llmgw/jobs/CLAUDE.md`.
- Job fencing uses `jobs.lease_token`, not `attempts`: a rate-limit requeue gives an attempt back.
- Metrics live on prometheus_client's process-global registry: tests assert deltas with `metric()` from `tests/conftest.py`, never absolute counter values.
- Call `logs.configure` only from CLI modes, and start uvicorn with `log_config=None`, or uvicorn's dictConfig replaces our handler.
- uvicorn re-raises SIGTERM after shutting down; `__main__` installs a `sys.exit(0)` handler first so the gateway exits 0.
