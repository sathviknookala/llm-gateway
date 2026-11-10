"""Synchronous path: authenticate, hold, stream through, settle.

POST /v1/chat/completions (OpenAI-compatible, `stream: true` only for now).

Every hold placed here is closed exactly once, on every exit path:

- release: the call never reached the provider (connect failure) or the
  provider refused it with an error status. Providers don't bill those.
- settle on reported usage: the stream finished with a usage block.
- settle on an estimate, flagged: anything else after the request was sent
  (client disconnect, upstream cut, timeout, no usage block). The estimate is
  the hold's prompt estimate plus the output counted so far; reconciliation
  corrects flagged postings later.

The close runs in `finally` under `anyio.CancelScope(shield=True)` with a
timeout inside it. Cancellation in anyio is level-triggered: without the
shield, the first await in the settle would be cancelled too. If the process
dies before the close commits, the hold-expiry sweeper refunds the hold.

The endpoint is a raw ASGI app rather than a StreamingResponse so that it
owns disconnect handling: a watcher task reads `receive()` and cancels the
stream when the client leaves, whatever ASGI spec version the server speaks.
Nothing network-shaped runs inside a ledger transaction.

Rate limits (when `LLMGW_REDIS_URL` is set) run after validation and before
the hold, so a refused request never touches the ledger. The TPM buckets are
charged the same worst case that sizes the hold; once the call is over, the
close task refunds the unused tokens. The refund follows what happened
upstream (tokens the provider used), not whether the ledger write succeeded,
and runs only if the acquire actually debited Redis.
"""

import asyncio
import contextlib
import math
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import anyio
import asyncpg
import httpx
import orjson
import structlog
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.types import Receive, Scope, Send

from llmgw import auth
from llmgw.config import Settings
from llmgw.db import create_pool, transaction
from llmgw.gateway.meter import StreamMeter
from llmgw.ledger import holds
from llmgw.ledger.errors import InsufficientCredits
from llmgw.pricing import Price, cost, estimate_output_tokens, estimate_prompt_tokens
from llmgw.ratelimit import RateLimiter

log = structlog.get_logger("llmgw.gateway")

CLOSE_ATTEMPTS = 3


@dataclass
class Gateway:
    settings: Settings
    pool: asyncpg.Pool | None = None
    http: httpx.AsyncClient | None = None
    limiter: RateLimiter | None = None
    inflight: int = 0
    # Test hook: awaited inside the shielded close, before the ledger write.
    before_close: Any = None
    # Hold closes still running; shutdown waits for them before closing the pool.
    closing: set[asyncio.Future[None]] = field(default_factory=set)


@dataclass
class _Call:
    """What the close needs to know about one request."""

    hold_id: int
    request_id: str
    team_id: int
    model: str
    price: Price
    prompt_estimate: int
    reserved_tokens: int  # charged to the TPM buckets at admission
    debited: bool  # the rate-limit acquire debited Redis, so a refund is owed
    meter: StreamMeter = field(default_factory=StreamMeter)
    sent_upstream: bool = False
    refused: bool = False  # connect failure or upstream error status: release
    client_gone: bool = False
    started: bool = False  # response headers sent to the client
    finished: bool = False


def _error_body(message: str, code: str) -> bytes:
    return orjson.dumps({"error": {"message": message, "type": "llmgw", "code": code}})


async def _send_json(send: Send, status: int, body: bytes, headers: dict[str, str] | None = None) -> None:
    raw_headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
    raw_headers += [(k.encode(), v.encode()) for k, v in (headers or {}).items()]
    await send({"type": "http.response.start", "status": status, "headers": raw_headers})
    await send({"type": "http.response.body", "body": body})


async def _send_error(send: Send, status: int, message: str, code: str, headers: dict[str, str] | None = None) -> None:
    await _send_json(send, status, _error_body(message, code), headers)


class ChatCompletions:
    def __init__(self, gw: Gateway) -> None:
        self.gw = gw

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        gw, settings = self.gw, self.gw.settings
        request = Request(scope, receive)

        key = request.headers.get("authorization", "").removeprefix("Bearer ").strip()
        team_id = await auth.team_for_key(gw.pool, key) if key else None
        if team_id is None:
            return await _send_error(send, 401, "invalid API key", "invalid_api_key")

        try:
            body = orjson.loads(await request.body())
        except orjson.JSONDecodeError:
            return await _send_error(send, 400, "body is not valid JSON", "invalid_json")
        if not isinstance(body, dict) or not isinstance(body.get("messages"), list):
            return await _send_error(send, 400, "`messages` must be a list", "invalid_request")
        if body.get("stream") is not True:
            return await _send_error(send, 400, "only `stream: true` is supported", "stream_required")
        model = body.get("model")
        price = settings.prices.get(model) if isinstance(model, str) else None
        if price is None:
            return await _send_error(send, 404, f"unknown model {model!r}", "model_not_found")

        cap_field = "max_completion_tokens" if "max_completion_tokens" in body else "max_tokens"
        max_tokens = body.get(cap_field)
        if max_tokens is None:
            max_tokens = settings.default_max_tokens
        if not isinstance(max_tokens, int) or isinstance(max_tokens, bool) or max_tokens <= 0:
            return await _send_error(send, 400, f"`{cap_field}` must be a positive integer", "invalid_request")

        if gw.inflight >= settings.max_inflight:
            return await _send_error(send, 429, "gateway at capacity", "capacity", {"retry-after": "1"})

        gw.inflight += 1
        try:
            prompt_estimate = estimate_prompt_tokens(body["messages"])
            reserved_tokens = prompt_estimate + max_tokens
            debited = False
            if gw.limiter is not None:
                if gw.limiter.oversized(team_id, model, reserved_tokens):
                    return await _send_error(
                        send, 400,
                        f"request needs up to {reserved_tokens} tokens, more than a rate-limit bucket holds",
                        "request_exceeds_rate_limit",
                    )
                decision = await gw.limiter.acquire(team_id, model, reserved_tokens)
                if not decision.allowed:
                    provider = "model" in decision.blocked
                    retry = str(max(1, math.ceil(decision.wait_ms / 1000)))
                    return await _send_error(
                        send, 429,
                        "provider budget exhausted" if provider else "team rate limit exceeded",
                        "provider_budget_exhausted" if provider else "team_rate_limited",
                        {"retry-after": retry},
                    )
                debited = decision.debited

            amount = max(1, cost(price, prompt_estimate, max_tokens))
            request_id = uuid.uuid4().hex
            try:
                async with transaction(gw.pool) as conn:
                    hold = await holds.place_hold(
                        conn,
                        team_id=team_id,
                        amount=amount,
                        expires_at=datetime.now(UTC) + timedelta(seconds=settings.hold_ttl_s),
                        request_id=request_id,
                    )
            except InsufficientCredits:
                await self._refund_tokens(team_id, model, reserved_tokens, debited)
                return await _send_error(
                    send, 402, f"insufficient credits: this request holds {amount}", "insufficient_credits"
                )
            except BaseException:
                # No hold, so no close task to refund later: give the tokens back now.
                await self._refund_tokens(team_id, model, reserved_tokens, debited)
                raise

            upstream_body = {
                **body,
                cap_field: max_tokens,
                "stream": True,
                "stream_options": {**(body.get("stream_options") or {}), "include_usage": True},
            }
            call = _Call(hold.id, request_id, team_id, model, price, prompt_estimate, reserved_tokens, debited)
            try:
                await self._proxy(call, upstream_body, receive, send)
            finally:
                # Its own task, so even repeated Task.cancel() on this request
                # can't stop the commit; the lifespan drains these on shutdown.
                close = asyncio.ensure_future(self._close(call))
                gw.closing.add(close)
                close.add_done_callback(gw.closing.discard)
                with anyio.CancelScope(shield=True):
                    await asyncio.shield(close)
        finally:
            gw.inflight -= 1

    async def _proxy(self, call: _Call, upstream_body: dict[str, Any], receive: Receive, send: Send) -> None:
        """Forward the request and stream the reply. Returns normally on cut-offs and disconnects."""
        async with anyio.create_task_group() as tg:
            tg.start_soon(self._watch_disconnect, receive, tg.cancel_scope, call)
            try:
                with anyio.fail_after(self.gw.settings.upstream_timeout_s):
                    await self._stream(call, upstream_body, send)
            except TimeoutError:
                log.warning("upstream_timeout", request_id=call.request_id)
                await self._end_stream(call, send, 504, "provider timed out", "upstream_timeout")
            except httpx.TransportError as e:
                # The request was sent, so the provider may bill it: settle on an estimate.
                log.warning("upstream_cut", request_id=call.request_id, error=repr(e))
                await self._end_stream(call, send, 502, "provider connection failed", "upstream_error")
            tg.cancel_scope.cancel()

    async def _watch_disconnect(self, receive: Receive, scope: anyio.CancelScope, call: _Call) -> None:
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                if not call.finished:
                    call.client_gone = True
                    log.info("client_disconnected", request_id=call.request_id)
                scope.cancel()
                return

    async def _stream(self, call: _Call, upstream_body: dict[str, Any], send: Send) -> None:
        gw, settings = self.gw, self.gw.settings
        headers = {"content-type": "application/json", "accept-encoding": "identity"}
        if settings.upstream_api_key:
            headers["authorization"] = f"Bearer {settings.upstream_api_key}"
        req = gw.http.build_request(
            "POST", settings.upstream_url.rstrip("/") + "/v1/chat/completions",
            content=orjson.dumps(upstream_body), headers=headers,
        )
        call.sent_upstream = True
        try:
            resp = await gw.http.send(req, stream=True)
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            call.refused = True
            log.warning("upstream_unreachable", request_id=call.request_id, error=repr(e))
            return await _send_error(send, 502, "provider unreachable", "upstream_unreachable")
        try:
            if resp.status_code != 200:
                call.refused = True
                detail = await resp.aread()
                log.warning("upstream_error", request_id=call.request_id, status=resp.status_code)
                if resp.status_code == 429:
                    retry = {"retry-after": resp.headers["retry-after"]} if "retry-after" in resp.headers else None
                    return await _send_error(send, 429, "provider rate limited", "upstream_rate_limited", retry)
                if 400 <= resp.status_code < 500:
                    return await _send_json(send, resp.status_code, detail)
                return await _send_error(send, 502, f"provider returned {resp.status_code}", "upstream_error")

            call.started = True
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/event-stream"),
                    (b"cache-control", b"no-cache"),
                    (b"x-llmgw-request-id", call.request_id.encode()),
                ],
            })
            async for raw in resp.aiter_raw():
                call.meter.feed(raw)
                await send({"type": "http.response.body", "body": raw, "more_body": True})
            await self._end_stream(call, send)
        finally:
            with anyio.CancelScope(shield=True), anyio.move_on_after(1):
                await resp.aclose()

    async def _end_stream(
        self, call: _Call, send: Send, status: int = 502, message: str = "", code: str = ""
    ) -> None:
        """Finish the client response: end the stream, or send an error if it never started."""
        call.finished = True
        with contextlib.suppress(OSError):
            if call.started:
                await send({"type": "http.response.body", "body": b"", "more_body": False})
            else:
                await _send_error(send, status, message, code)

    async def _refund_tokens(self, team_id: int, model: str, tokens: int, debited: bool) -> None:
        """Give unused tokens back to the TPM buckets. Best effort, shielded, never raises."""
        if not debited or tokens <= 0 or self.gw.limiter is None:
            return
        with anyio.CancelScope(shield=True):
            await self.gw.limiter.refund(team_id, model, tokens)

    async def _close(self, call: _Call) -> None:
        """Close the hold exactly once, then refund unused tokens. Runs shielded; never raises."""
        meter = call.meter
        if call.refused:
            op, actual, flagged = "release", 0, False
            used_tokens = 0
        elif meter.usage is not None:
            op, flagged = "settle", False
            actual = cost(call.price, meter.usage.prompt_tokens, meter.usage.completion_tokens)
            used_tokens = meter.usage.prompt_tokens + meter.usage.completion_tokens
        else:
            op, flagged = "settle", True
            output = estimate_output_tokens(meter.content_chunks, meter.content_chars)
            actual = cost(call.price, call.prompt_estimate, output)
            used_tokens = call.prompt_estimate + output if call.sent_upstream else 0

        try:
            await self._close_hold(call, op, actual, flagged)
        finally:
            # Outside the close's timeout, with its own: tokens follow what the
            # provider used, whether or not the ledger write went through.
            await self._refund_tokens(
                call.team_id, call.model, max(0, call.reserved_tokens - used_tokens), call.debited
            )

    async def _close_hold(self, call: _Call, op: str, actual: int, flagged: bool) -> None:
        fields = dict(request_id=call.request_id, hold_id=call.hold_id, team_id=call.team_id,
                      op=op, actual=actual, flagged=flagged, client_gone=call.client_gone)
        try:
            with anyio.fail_after(self.gw.settings.settle_timeout_s):
                if self.gw.before_close is not None:
                    await self.gw.before_close(call)
                for attempt in range(1, CLOSE_ATTEMPTS + 1):
                    try:
                        async with transaction(self.gw.pool) as conn:
                            if op == "release":
                                await holds.release(conn, hold_id=call.hold_id)
                            else:
                                await holds.settle(conn, hold_id=call.hold_id, actual=actual, flagged=flagged)
                        break
                    except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError):
                        # Closes are idempotent by key, so a retry after an unknown outcome is safe.
                        if attempt == CLOSE_ATTEMPTS:
                            raise
                        await asyncio.sleep(0.05 * attempt)
            log.info("hold_closed", **fields)
        except Exception:
            # Left open: the sweeper expires it and reconciliation books the cost.
            log.exception("hold_close_failed", **fields)


async def _healthz(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True})


def create_app(
    settings: Settings,
    *,
    pool: asyncpg.Pool | None = None,
    http: httpx.AsyncClient | None = None,
    limiter: RateLimiter | None = None,
) -> Starlette:
    gw = Gateway(settings, pool=pool, http=http, limiter=limiter)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async with contextlib.AsyncExitStack() as stack:
            if gw.pool is None:
                gw.pool = await create_pool(settings)
                stack.push_async_callback(gw.pool.close)
            if gw.limiter is None and settings.redis_url:
                gw.limiter = RateLimiter.from_url(
                    settings.redis_url, settings.rate_limits, timeout_s=settings.redis_timeout_s
                )
                stack.push_async_callback(gw.limiter.aclose)
            if gw.http is None:
                gw.http = httpx.AsyncClient(
                    timeout=httpx.Timeout(settings.upstream_timeout_s, connect=settings.upstream_connect_timeout_s),
                    limits=httpx.Limits(max_connections=None, max_keepalive_connections=100),
                )
                stack.push_async_callback(gw.http.aclose)
            try:
                yield
            finally:
                if gw.closing:
                    # A close is the ledger write plus a token refund, each with its own timeout.
                    await asyncio.wait(gw.closing, timeout=settings.settle_timeout_s + settings.redis_timeout_s + 1)

    app = Starlette(
        routes=[
            Route("/v1/chat/completions", ChatCompletions(gw), methods=["POST"]),
            Route("/healthz", _healthz),
        ],
        lifespan=lifespan,
    )
    app.state.gateway = gw
    return app
