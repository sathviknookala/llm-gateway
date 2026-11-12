"""Job worker: lease a job, call the provider, finish the attempt exactly once.

One attempt, each step on its own (nothing network-shaped in a transaction):

1. lease (one statement; bumps the fencing token);
2. rate limits: the same acquire as the synchronous path. Refused → the job is
   requeued with its attempt given back, nothing sent;
3. the provider call (streamed, metered, collected into a chat.completion)
   and the lease heartbeat, as siblings in one task group. A heartbeat that
   finds the lease gone raises LeaseLost, which cancels the call;
4. finish, in one transaction: `complete` (settle + result) or `fail_attempt`
   (retry loss, then requeue / failed / dead). It runs as its own task under a
   shield, like the gateway's close, so cancelling the worker can't stop the
   commit; `run()` drains pending finishes on shutdown;
5. refund unused rate-limit tokens.

How an attempt is classified:
- never sent (rate limited, or cancelled before sending) → requeue, attempt given back;
- connect failure, 429, 5xx → retryable, cost 0 (providers don't bill those);
- other 4xx → permanent: the job fails and its hold is released;
- the stream ended normally → success, settled on usage, or on a flagged
  counted estimate if usage never came;
- anything else after sending (cut, timeout, lease lost, shutdown) →
  retryable, costed at the counted estimate (prompt estimate + output so far),
  flagged, and booked to retry_loss. If the lease really was lost,
  `fail_attempt`'s token check turns it into a fenced report.
"""

import asyncio
import contextlib
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import anyio
import asyncpg
import httpx
import orjson
import structlog

from llmgw.config import Settings
from llmgw.db import transaction
from llmgw.gateway.meter import StreamMeter
from llmgw.jobs import queue
from llmgw.jobs.errors import LeaseLost
from llmgw.jobs.queue import Job, Outcome
from llmgw.ledger.errors import InvalidHoldState
from llmgw.pricing import Price, cost, estimate_output_tokens
from llmgw.ratelimit import RateLimiter

log = structlog.get_logger("llmgw.worker")

FINISH_ATTEMPTS = 3
_DB_ERRORS = (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError)


@dataclass
class _Attempt:
    job: Job
    price: Price | None
    reserved_tokens: int
    debited: bool = False
    meter: StreamMeter = field(default_factory=lambda: StreamMeter(collect=True))
    sent: bool = False
    unreachable: bool = False
    status: int | None = None
    retry_after_s: float | None = None
    completed: bool = False  # the stream ended normally
    lease_lost: bool = False
    error: str | None = None  # what cut the call short, if anything
    refusal: str | None = None  # refused before sending: permanent
    requeue_after_s: float | None = None  # rate limited before sending

    @property
    def counted_cost(self) -> int:
        if not self.sent or self.price is None:
            return 0
        return cost(self.price, self.job.prompt_estimate, self.counted_output)

    @property
    def counted_output(self) -> int:
        return estimate_output_tokens(self.meter.content_chunks, self.meter.content_chars)


def _retry_after(raw: str | None) -> float | None:
    try:
        return max(0.0, float(raw)) if raw is not None else None
    except ValueError:
        return None


class Worker:
    def __init__(
        self,
        settings: Settings,
        *,
        pool: asyncpg.Pool,
        http: httpx.AsyncClient,
        limiter: RateLimiter | None = None,
        rand: Callable[[], float] = random.random,
    ) -> None:
        self.settings = settings
        self.pool = pool
        self.http = http
        self.limiter = limiter
        self.rand = rand
        # Finishes still running; run() waits for them on shutdown.
        self.finishing: set[asyncio.Future[Outcome]] = set()
        # Test hook: awaited inside the shielded finish, before the ledger write.
        self.before_finish: Any = None

    # --- loop ---

    async def run(self) -> None:
        """Run `worker_concurrency` loops until cancelled, then drain pending finishes."""
        try:
            async with asyncio.TaskGroup() as tg:
                for _ in range(self.settings.worker_concurrency):
                    tg.create_task(self._loop())
        finally:
            if self.finishing:
                await asyncio.wait(
                    self.finishing, timeout=self.settings.settle_timeout_s + self.settings.redis_timeout_s + 1
                )

    async def _loop(self) -> None:
        while True:
            try:
                outcome = await self.run_one()
            except Exception:
                log.exception("worker_iteration_failed")
                outcome = None
            if outcome is None:
                await asyncio.sleep(self.settings.worker_poll_s)

    async def run_one(self) -> Outcome | None:
        """Lease and run one job. None if nothing was ready."""
        s = self.settings
        job = await queue.lease(self.pool, worker=s.worker_id, lease_s=s.job_lease_s)
        if job is None:
            return None
        # Bound for the call and the finish: the finish task copies this context.
        with structlog.contextvars.bound_contextvars(job_id=job.id, attempt=job.attempts, lease_token=job.lease_token):
            log.info("job_leased", job_id=job.id, attempt=job.attempts, token=job.lease_token)
            a = _Attempt(job, s.prices.get(job.model), job.prompt_estimate + job.max_tokens)
            try:
                await self._admit(a)
                if a.refusal is None and a.requeue_after_s is None:
                    await self._call(a)
            finally:
                finish = asyncio.ensure_future(self._finish(a))
                self.finishing.add(finish)
                finish.add_done_callback(self.finishing.discard)
                with anyio.CancelScope(shield=True):
                    outcome = await asyncio.shield(finish)
        return outcome

    # --- before the call ---

    async def _admit(self, a: _Attempt) -> None:
        job = a.job
        if a.price is None:
            a.refusal = "model_not_found"
            return
        if self.limiter is None:
            return
        if self.limiter.oversized(job.team_id, job.model, a.reserved_tokens):
            a.refusal = "request_exceeds_rate_limit"
            return
        decision = await self.limiter.acquire(job.team_id, job.model, a.reserved_tokens)
        if not decision.allowed:
            a.requeue_after_s = max(0.001, decision.wait_ms / 1000)
            a.error = "provider_budget_exhausted" if "model" in decision.blocked else "team_rate_limited"
            return
        a.debited = decision.debited

    # --- the call ---

    async def _call(self, a: _Attempt) -> None:
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._heartbeat, a)
                try:
                    with anyio.fail_after(self.settings.upstream_timeout_s):
                        await self._stream(a)
                except TimeoutError:
                    a.error = "upstream_timeout"
                except httpx.TransportError as e:
                    a.error = "upstream_cut"
                    log.warning("upstream_cut", job_id=a.job.id, error=repr(e))
                tg.cancel_scope.cancel()
        except* LeaseLost:
            a.lease_lost = True
            a.error = "lease_lost"
            log.warning("lease_lost", job_id=a.job.id, token=a.job.lease_token)

    async def _heartbeat(self, a: _Attempt) -> None:
        s = self.settings
        last_ok = time.monotonic()
        while True:
            await anyio.sleep(s.job_heartbeat_s)
            try:
                async with asyncio.timeout(s.settle_timeout_s):
                    await queue.renew(self.pool, job_id=a.job.id, token=a.job.lease_token, lease_s=s.job_lease_s)
                last_ok = time.monotonic()
            except LeaseLost:
                raise
            except (*_DB_ERRORS, TimeoutError) as e:
                log.warning("lease_renew_failed", job_id=a.job.id, error=repr(e))
                if time.monotonic() - last_ok >= s.job_lease_s:
                    raise LeaseLost(f"job {a.job.id}: couldn't renew the lease for {s.job_lease_s}s") from e

    async def _stream(self, a: _Attempt) -> None:
        s = self.settings
        request = a.job.request
        body = {
            **request,
            "stream": True,
            "stream_options": {**(request.get("stream_options") or {}), "include_usage": True},
        }
        headers = {"content-type": "application/json", "accept-encoding": "identity"}
        if s.upstream_api_key:
            headers["authorization"] = f"Bearer {s.upstream_api_key}"
        req = self.http.build_request(
            "POST", s.upstream_url.rstrip("/") + "/v1/chat/completions",
            content=orjson.dumps(body), headers=headers,
        )
        a.sent = True
        try:
            resp = await self.http.send(req, stream=True)
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            a.unreachable = True
            log.warning("upstream_unreachable", job_id=a.job.id, error=repr(e))
            return
        try:
            a.status = resp.status_code
            if resp.status_code != 200:
                a.retry_after_s = _retry_after(resp.headers.get("retry-after"))
                await resp.aread()
                return
            async for raw in resp.aiter_raw():
                a.meter.feed(raw)
            a.completed = a.meter.finish_reason is not None or a.meter.done
        finally:
            with anyio.CancelScope(shield=True), anyio.move_on_after(1):
                await resp.aclose()

    # --- the finish ---

    def _used_tokens(self, a: _Attempt) -> int:
        if not a.sent or a.unreachable or (a.status is not None and a.status != 200):
            return 0
        if a.meter.usage is not None:
            return a.meter.usage.prompt_tokens + a.meter.usage.completion_tokens
        return a.job.prompt_estimate + a.counted_output

    async def _finish(self, a: _Attempt) -> Outcome:
        """Record the attempt, then refund unused tokens. Never raises."""
        try:
            outcome = await self._record(a)
        except Exception:
            # Left running: the lease expires and the sweeper reclaims it.
            log.exception("job_finish_failed", job_id=a.job.id, attempt=a.job.attempts)
            outcome = Outcome.LOST
        if a.debited and self.limiter is not None:
            unused = max(0, a.reserved_tokens - self._used_tokens(a))
            if unused:
                with anyio.CancelScope(shield=True):
                    await self.limiter.refund(a.job.team_id, a.job.model, unused)
        return outcome

    async def _record(self, a: _Attempt) -> Outcome:
        with anyio.fail_after(self.settings.settle_timeout_s):
            if self.before_finish is not None:
                await self.before_finish(a)
            for n in range(1, FINISH_ATTEMPTS + 1):
                try:
                    outcome = await self._write(a)
                    log.info("job_attempt_finished", job_id=a.job.id, attempt=a.job.attempts,
                             outcome=str(outcome), error=a.error, status=a.status)
                    return outcome
                except _DB_ERRORS:
                    # Every finish is fenced by token and replays by attempt, so a retry is safe.
                    if n == FINISH_ATTEMPTS:
                        raise
                    await asyncio.sleep(0.05 * n)
        raise AssertionError("unreachable")

    async def _write(self, a: _Attempt) -> Outcome:
        job, s = a.job, self.settings
        backoff = dict(base_s=s.job_retry_base_s, cap_s=s.job_retry_cap_s, rand=self.rand)

        async def fail(*, permanent: bool, cost_: int, flagged: bool, error: str) -> Outcome:
            async with transaction(self.pool) as conn:
                return await queue.fail_attempt(
                    conn, job_id=job.id, token=job.lease_token, attempt=job.attempts,
                    permanent=permanent, cost=cost_, flagged=flagged, error=error,
                    retry_after_s=a.retry_after_s, **backoff,
                )

        if a.refusal is not None:
            return await fail(permanent=True, cost_=0, flagged=False, error=a.refusal)
        if not a.sent:
            # Rate limited, or cancelled before anything went out: give the attempt back.
            async with transaction(self.pool) as conn:
                await queue.requeue_unstarted(
                    conn, job_id=job.id, token=job.lease_token,
                    delay_s=a.requeue_after_s or 0.0, error=a.error or "interrupted",
                )
            return Outcome.RATE_LIMITED
        if a.unreachable:
            return await fail(permanent=False, cost_=0, flagged=False, error="upstream_unreachable")
        if a.status is not None and a.status != 200:
            permanent = 400 <= a.status < 500 and a.status != 429
            return await fail(permanent=permanent, cost_=0, flagged=False, error=f"upstream_{a.status}")
        if a.completed:
            usage = a.meter.usage
            if usage is not None:
                actual, flagged = cost(a.price, usage.prompt_tokens, usage.completion_tokens), False
            else:
                actual, flagged = a.counted_cost, True
            try:
                async with transaction(self.pool) as conn:
                    return await queue.complete(
                        conn, job_id=job.id, token=job.lease_token, attempt=job.attempts,
                        actual=actual, flagged=flagged, result=a.meter.result(),
                    )
            except InvalidHoldState:
                # The hold was closed by another path; its provider cost goes to reconciliation.
                log.error("job_hold_closed", job_id=job.id, hold_id=job.hold_id)
                return await fail(permanent=True, cost_=0, flagged=False, error="hold_closed")
        return await fail(permanent=False, cost_=a.counted_cost, flagged=True, error=a.error or "interrupted")


@contextlib.asynccontextmanager
async def _resources(settings: Settings):
    from llmgw.db import create_pool

    async with contextlib.AsyncExitStack() as stack:
        pool = await create_pool(settings)
        stack.push_async_callback(pool.close)
        limiter = None
        if settings.redis_url:
            limiter = RateLimiter.from_url(settings.redis_url, settings.rate_limits, timeout_s=settings.redis_timeout_s)
            stack.push_async_callback(limiter.aclose)
        http = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.upstream_timeout_s, connect=settings.upstream_connect_timeout_s),
            limits=httpx.Limits(max_connections=None, max_keepalive_connections=100),
        )
        stack.push_async_callback(http.aclose)
        yield Worker(settings, pool=pool, http=http, limiter=limiter)


async def serve(settings: Settings) -> None:
    """`llmgw worker`: run until cancelled; resources close only after the drain."""
    async with _resources(settings) as worker:
        await worker.run()
