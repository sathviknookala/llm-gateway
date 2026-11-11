# src/llmgw/jobs
Responsibility: the asynchronous path. Atomic enqueue (hold + job row in one transaction), leases with a fencing token, the worker's attempt loop, retries with backoff, dead letters, and booking failed attempts' provider cost to retry_loss.
Not responsible for: the HTTP routes (`llmgw/gateway/jobs.py`), ledger rules (`llmgw/ledger`), lease reclaim and deadline sweeps as a loop (`llmgw/sweeper.py` calls `queue.reclaim` / `queue.expire_deadline`), fair scheduling across teams (milestone 6).

## Contract
- `queue.py`: SQL only. Writers take a connection inside the caller's transaction (`require_transaction`); `lease` and `renew` are single statements and take a pool or connection.
  - `enqueue` → `(Job, created)`. With an idempotency key: same request → stored job; different → `JobConflict`. `InsufficientCredits` and, for a concurrent same-key submit with a different amount, `IdempotencyConflict` come from `place_hold`.
  - `lease` → next queued job with `run_after <= now()` and `deadline > now()`, FIFO by `(run_after, id)`, `FOR UPDATE SKIP LOCKED`; bumps `attempts` and `lease_token`.
  - `renew` raises `LeaseLost` on a stale token.
  - Finishes take `(job_id, token, attempt)`: `complete` (settle + result + `succeeded`), `fail_attempt` (retry loss if `cost > 0`, then queued / `failed` / `dead`), `requeue_unstarted` (nothing sent: give the attempt back). They return an `Outcome`.
- `worker.py`: `Worker.run_one()` does one attempt and returns its `Outcome` (None if nothing was ready); `Worker.run()` runs `worker_concurrency` loops; `serve(settings)` is the CLI mode.
- `last_error` codes: `upstream_<status>`, `upstream_unreachable`, `upstream_cut`, `upstream_timeout`, `lease_lost`, `deadline`, `hold_closed`, `request_exceeds_rate_limit`, `model_not_found`, `team_rate_limited`, `provider_budget_exhausted`, `interrupted`.

## Invariants
- Lock order: job row → hold row → balance rows. Every finish locks the job row first (`_lock_attempt`), even when fenced.
- `lease_token` is the fencing token: +1 per lease, never decremented. A finish whose token doesn't match a running job is fenced: it books its cost to retry_loss and touches neither the job nor its hold.
- `attempts` is the retry budget and numbers `job_attempts` rows and `retry_loss:{job}:{attempt}` keys. An attempt number is reused only after `requeue_unstarted`, which writes no attempt row and no posting; that is what keeps those keys unique.
- A finish retried after it committed finds its own attempt row (outcome ≠ `lost`) and returns the stored outcome, writing nothing. Without this, a retried `complete` would charge retry_loss for a call already settled.
- The team pays only for the attempt that succeeds. Billed failures (cut, timeout, lost lease, shutdown after sending) are costed at prompt estimate + counted output, flagged, and posted external → retry_loss. Connect errors, 429 and 5xx cost 0; other 4xx are permanent and cost 0 (`fail_attempt` refuses a permanent failure with a cost).
- A job's hold is closed only by job paths: `complete` settles; `failed` / `dead` release (only if still held). The hold sweeper skips job holds; `check_ledger` flags a job whose hold state doesn't match.
- Lease reclaim counts as an attempt (outcome `lost`, cost unknown, left to reconciliation), so a job that kills its workers still dead-letters.
- The worker's finish runs as its own task under `anyio.CancelScope(shield=True)` + `asyncio.shield`, like the gateway's close; `run()` drains pending finishes before returning. The provider call and heartbeat share an anyio task group; `LeaseLost` cancels the call and is caught with `except*`.
- Nothing network-shaped inside a transaction: lease, rate-limit acquire, call + heartbeat, finish, refund are separate steps.

## Entry points
- `queue.py` — `enqueue`, `lease`, `renew`, `complete`, `fail_attempt`, `requeue_unstarted`, `reclaim`, `expire_deadline`, `get_job`, `backoff`; `Job`, `JobState`, `Outcome`.
- `worker.py` — `Worker`, `serve`.
- `errors.py` — `JobNotFound`, `JobConflict`, `LeaseLost`.

## Testing here
- `uv run pytest tests/jobs`. Queue tests run against Postgres; worker tests also run the mock provider on uvicorn and call `run_one()` directly, so each attempt is a deterministic step.
- Make a requeued job due with SQL (`make_due`), and expire a lease with `expire_lease`; never sleep for it.
- A job re-sends the same body each attempt; vary behaviour per attempt with the mock's `key` + `script`.

## Gotchas
- `requeue_unstarted` decrements `attempts`, which is why fencing can't use `attempts`.
- A lease past `locked_until` that nobody reclaimed can still be renewed and finished; correctness rests on the token.
- A heartbeat that can't reach the database for a whole lease raises `LeaseLost`, but the lease may still be ours. The worker then calls the token-checked `fail_attempt`, never `_record_fenced` directly.
- The worker's rate-limit acquire uses the same buckets as the synchronous path; holding headroom back for interactive traffic is milestone 6.
- `Worker.before_finish` is a test hook awaited inside the shielded finish.
