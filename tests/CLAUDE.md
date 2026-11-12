# tests
Responsibility: proving the ledger can't lose or invent credits — schema invariants, ledger operations, concurrency, and property tests — and that the gateway closes every hold exactly once and the job queue runs and charges each job once.
Not responsible for: the Flyway check (CI's `migrations` job does that), the load test and fault harness (`bench/`, run on demand, timing-based by nature).

## Contract
- `conftest.py` fixtures:
  - `database_url` (session) — one Postgres 17 container per run; migrations applied in version order with plain asyncpg, no Flyway.
  - `conn` — a fresh connection with every table in `TABLES` truncated. Autocommit unless the test opens a transaction.
  - `pool` — up to 8 connections for concurrency tests; depends on `conn` so tables are truncated first.
  - `redis_url` (session) — one redis:7 container, started only when a test asks for it; `redis_client` — a client on a flushed database.
  - `metric(name, **labels)` (a function, not a fixture; `from conftest import metric`) — a sample's current value on the process-global Prometheus registry, 0 if unset.
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
- `test_sweeper.py` — sweeper: expires overdue holds once, skips locked ones, survives a poisoned hold.
- `jobs/test_queue.py` — queue operations against Postgres: atomic enqueue, idempotency (sequential and concurrent), FIFO and SKIP LOCKED leases, fencing, finish replays, retries, dead letters, attempt-number reuse.
- `jobs/test_sweeper_jobs.py` — lease reclaim, deadlines, concurrent reclaims, the hold sweeper skipping job holds, lost → fenced.
- `jobs/test_worker.py` — the worker end to end against the mock: success, missing usage, 503/cut then success, 429 retry-after, 400, dead letters, lost lease mid-stream, crash before the finish, repeated cancellation, shutdown drain, rate limits, concurrent workers.
- `jobs/conftest.py` — `team` (granted 100,000), `enqueue` / `fail` / `complete` helpers (each in its own transaction), `expire_lease`, `make_due`, `NO_BACKOFF`, `mock`, `worker`, and `worker_env(...)` for settings or a limiter.
- `gateway/test_jobs_api.py` — `POST /v1/jobs` and `GET /v1/jobs/{id}`.
- `gateway/test_meter.py` — `StreamMeter` counting and result capture.
- `test_config.py` — settings guards.
- `test_logs.py` — JSON log lines with bound context, stdlib records, exceptions.
- `test_looplag.py` — a blocked loop shows up in the lag histogram.
- `gateway/test_metrics.py` — gateway counters per exit path, `/metrics`, `/readyz` (DB down → 503, Redis down → still 200).
- `jobs/test_worker_metrics.py` — attempts by outcome, queue wait, job hold closes (including only the last retryable attempt releasing), lost leases, failed finishes.
- `jobs/test_stats.py` — database gauges, sweeper action counters, the periodic ledger check.
- `test_cli.py` — `seed`, `check-ledger`, and SIGTERM mid-stream for the worker (attempt finished, job requeued) and the gateway (hold settled, exit 0, uvicorn logs in JSON), as real `python -m llmgw` processes.
- `test_ratelimit.py` — limiter against real Redis with explicit `now_ms`: refill, all-or-nothing, wait, refunds, script flush, fail-open.
- `test_mock_provider.py` — the mock's own TPM/RPM limits and per-key attempt scripts.
- `gateway/test_rate_limits.py` — rate limits end to end: 429s before the hold, refunds after the close, shared provider budget, fail-open, zero provider 429s under a lower gateway budget.
- `gateway/test_chat_completions.py` — `/v1/chat/completions` end to end: usage settle, release on provider errors, flagged estimates on cut-offs and disconnects, cancellation of the request task, mixed concurrent traffic.
- `gateway/conftest.py` — `mock` (mock provider on a real uvicorn server, `mock.state` records what it saw), `funded` (org, team A granted 1,000,000, API key), `env` (gateway on uvicorn plus an authenticated httpx client), and `gateway_env(...)` for settings overrides.

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
- Gateway tests run uvicorn via `server._serve(sockets=...)` to skip its signal-handler capture.
- The gateway closes a hold after the client has seen the end of the stream, so poll with `wait_closed` before asserting on holds or balances.
- The token refund lands after the hold close commits; poll the bucket (`wait_level`) rather than relying on `wait_closed`.
- Worker tests step with `run_one()` and make jobs due with SQL; a lost lease is simulated by bumping `lease_token` once the mock's `sent_chunks` shows the stream is under way.
- `tests/` has no `__init__.py`, so `jobs/conftest.py` imports `gateway.conftest` (the rootdir-relative package name) for `serve` and `make_settings`.
- Metric tests assert deltas (read, act, read): counters outlive tests. Gauges set by `stats.collect` are absolute, so a test that needs to see a fresh `collect` first sets a sentinel value.
- `test_cli.py` children write output to a file, not a pipe, and are killed in `finally`. They reach the in-test mock provider over 127.0.0.1.
- In-process uvicorn servers (gateway tests) reconfigure the `uvicorn.*` loggers, so `test_logs.py` uses its own logger name.
- Limiter tests pass `now_ms` so refill is deterministic; gateway tests use limits whose refill over a test (e.g. TPM 600 → 10 tokens/s) is far below one request.
