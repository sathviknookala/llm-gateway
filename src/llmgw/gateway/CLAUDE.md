# src/llmgw/gateway
Responsibility: the synchronous path. Authenticate, place a hold, stream the provider's reply through, and close the hold exactly once on every exit path.
Not responsible for: ledger rules (`llmgw/ledger`), prices and token estimates (`llmgw/pricing.py`), API keys (`llmgw/auth.py`), Redis rate limits (milestone 3), the hold-expiry sweeper.

## Contract
- `POST /v1/chat/completions`, OpenAI-compatible, `stream: true` only. `GET /healthz`.
- Errors before a hold: 401 bad key, 400 bad body or `stream` not true, 404 unknown model, 402 insufficient credits, 429 at `max_inflight`.
- Upstream body: the client's body, plus `max_tokens` (or `max_completion_tokens` if the client used it) set to the cap the hold covers, and `stream_options.include_usage = true`.
- How each hold closes:
  - **release**: connect failure, or the provider answered with an error status (429 → 429 with `retry-after`, other 4xx passed through, 5xx → 502).
  - **settle on usage**: the stream carried a usage block.
  - **settle on a flagged estimate**: anything else once the request was sent (client disconnect, upstream cut, timeout, no usage block). Prompt estimate plus counted output.

## Invariants
- The close runs as its own task, awaited under `anyio.CancelScope(shield=True)` plus `asyncio.shield`. The shield covers anyio's level-triggered scope cancels; the separate task covers repeated `Task.cancel()`. Each has a test that fails without it. Don't simplify it to an `await` in `finally`.
- `_close` never raises. A close that fails is logged and left to the sweeper.
- Nothing network-shaped inside `transaction()`: hold, then stream, then close, each on its own.
- Pass upstream bytes through untouched. `StreamMeter` reads a copy and never re-encodes.

## Entry points
- `app.py`: `create_app(settings, pool=, http=)`, `ChatCompletions` (raw ASGI app), `Gateway` (shared state: pool, http client, in-flight count, pending closes).
- `meter.py`: `StreamMeter`, an incremental SSE parser that counts content chunks and chars and keeps the usage block.

## Testing here
- `uv run pytest tests/gateway`. The mock provider and the gateway each run on a real uvicorn server, so disconnects are real socket closes.
- Fault scenarios go in the request body's `mock` object; see `llmgw/mock_provider.py`.

## Gotchas
- `ChatCompletions` is a raw ASGI app, not a StreamingResponse. Its own watcher task reads `receive()` for `http.disconnect`, because uvicorn silently drops `send` after a disconnect and Starlette only listens for one on ASGI spec < 2.4.
- After the final body is sent, uvicorn's `receive()` returns `http.disconnect` too. `call.finished` keeps that from being logged as a client disconnect.
- The client sees the end of the stream before the close commits. Tests poll with `wait_closed`.
- `Gateway.before_close` is a test hook awaited inside the shielded close.
- `hold_ttl_s` must exceed `upstream_timeout_s + settle_timeout_s` (checked in `Settings`), or the sweeper could expire a hold whose stream is still running.
