# src/llmgw/gateway
Responsibility: the synchronous path, and the HTTP side of jobs (submit and poll). Authenticate, check rate limits, place a hold, stream the provider's reply through, close the hold exactly once on every exit path, and refund unused rate-limit tokens.
Not responsible for: ledger rules (`llmgw/ledger`), prices and token estimates (`llmgw/pricing.py`), API keys (`llmgw/auth.py`), bucket logic (`llmgw/ratelimit.py`), the hold-expiry sweeper (`llmgw/sweeper.py`), running jobs (`llmgw/jobs`).

## Contract
- `POST /v1/chat/completions`, OpenAI-compatible, `stream: true` only.
- `GET /healthz` (process alive), `GET /readyz` (200 `{"db": "ok", "redis": "ok"|"off"|"down"}`; 503 only when Postgres doesn't answer within 1 s — Redis down never fails readiness because the limiter fails open), `GET /metrics` (Prometheus text; blocked publicly by Caddy).
- Metrics (all in `llmgw.metrics`): `llmgw_http_requests_total{route,status}` (route is a fixed template; `status="none"` if nothing was sent), `llmgw_refusals_total{code}` for every error before a hold or enqueue, `llmgw_admission_seconds` (arrival → upstream send), `llmgw_inflight_streams`, `llmgw_hold_closes_total{path="sync",op,flagged}`, `llmgw_hold_close_failures_total`, `llmgw_upstream_errors_total{path="sync",kind}`, `llmgw_ratelimit_unavailable_total{op}`, `llmgw_event_loop_lag_seconds`.
- `POST /v1/jobs`: 401, then the shared body checks (`stream` must be absent or false → else 400 `stream_not_supported`; `n` must be 1), 400 bad `Idempotency-Key`, 400 `request_exceeds_rate_limit`, then `enqueue` in one transaction → 202 (200 for an idempotent replay), 402 `insufficient_credits`, 409 `idempotency_conflict`. No inflight slot or rate-limit acquire at enqueue. The stored request is the body with the cap field set and `stream`/`stream_options` removed.
- `GET /v1/jobs/{id}`: the job (state, attempts, `last_error`, `result`); 404 `job_not_found` for a missing job or another team's.
- Errors before a hold, in this order: 401 bad key, 400 bad body or `stream` not true, 404 unknown model, 429 `capacity` at `max_inflight`, 400 `request_exceeds_rate_limit` (worst case bigger than a configured TPM bucket), 429 `team_rate_limited` / `provider_budget_exhausted` with `retry-after` (max wait over the blocking buckets; the model bucket names the code if both block), 402 insufficient credits.
- Rate limits are on only when `settings.redis_url` is set. TPM buckets are charged `reserved_tokens` = prompt estimate + the forwarded cap, the same worst case the hold covers.
- Upstream body: the client's body, plus `max_tokens` (or `max_completion_tokens` if the client used it) set to the cap the hold covers, and `stream_options.include_usage = true`.
- How each hold closes:
  - **release**: connect failure, or the provider answered with an error status (429 → 429 with `retry-after`, other 4xx passed through, 5xx → 502).
  - **settle on usage**: the stream carried a usage block.
  - **settle on a flagged estimate**: anything else once the request was sent (client disconnect, upstream cut, timeout, no usage block). Prompt estimate plus counted output.

## Invariants
- Token refunds: any failure between a debit and a placed hold (402 or any `place_hold` exception) refunds the whole reservation. After a hold, `_close` refunds `max(0, reserved − used)` after the ledger write, outside its timeout. `used` follows what happened upstream (release → 0, usage block → reported prompt + completion, otherwise prompt estimate + counted output), not whether the ledger write succeeded. No refund unless the acquire actually debited (`_Call.debited`); a failed-open acquire never refunds.
- The close runs as its own task, awaited under `anyio.CancelScope(shield=True)` plus `asyncio.shield`. The shield covers anyio's level-triggered scope cancels; the separate task covers repeated `Task.cancel()`. Each has a test that fails without it. Don't simplify it to an `await` in `finally`.
- `_close` never raises. A close that fails is logged and left to the sweeper.
- Shutdown: the lifespan first waits for `Gateway.idle` (set whenever no chat request is in flight; created set), then drains `closing`. uvicorn cancels request tasks after `shutdown_grace_s` and goes straight to lifespan shutdown without awaiting them; a cancelled request creates its close task only after it unwinds, so draining `closing` alone would miss it and close the pool under it. `tests/test_cli.py` fails without the idle wait.
- Request context (`request_id`, `team_id`) is bound with `structlog.contextvars` around the stream and the close; the close task copies it.
- Nothing network-shaped inside `transaction()`: hold, then stream, then close, each on its own.
- Pass upstream bytes through untouched. `StreamMeter` reads a copy and never re-encodes.

## Entry points
- `app.py`: `create_app(settings, pool=, http=)`, `ChatCompletions` (raw ASGI app), `Gateway` (shared state: pool, http client, in-flight count, pending closes).
- `meter.py`: `StreamMeter`, an incremental SSE parser that counts content chunks and chars and keeps the usage block and finish_reason; with `collect=True` it also rebuilds a `chat.completion` (`result()`).
- `request.py`: `parse_chat(raw, settings, stream=)`, the body checks both paths share, in the order of the error list above.
- `jobs.py`: the job routes.

## Testing here
- `uv run pytest tests/gateway`. The mock provider and the gateway each run on a real uvicorn server, so disconnects are real socket closes.
- Fault scenarios go in the request body's `mock` object; see `llmgw/mock_provider.py`.

## Gotchas
- `ChatCompletions` is a raw ASGI app, not a StreamingResponse. Its own watcher task reads `receive()` for `http.disconnect`, because uvicorn silently drops `send` after a disconnect and Starlette only listens for one on ASGI spec < 2.4.
- After the final body is sent, uvicorn's `receive()` returns `http.disconnect` too. `call.finished` keeps that from being logged as a client disconnect.
- The client sees the end of the stream before the close commits. Tests poll with `wait_closed`.
- `Gateway.before_close` is a test hook awaited inside the shielded close.
- The shutdown drain waits `settle_timeout_s + redis_timeout_s + 1`: a close is the ledger write plus a refund, each with its own timeout.
- The oversized check uses config only, so it still answers 400 when Redis is down.
- `hold_ttl_s` must exceed `upstream_timeout_s + settle_timeout_s` (checked in `Settings`), or the sweeper could expire a hold whose stream is still running.
