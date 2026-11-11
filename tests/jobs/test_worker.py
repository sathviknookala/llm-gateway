"""The job worker end to end: real Postgres, the mock provider on uvicorn.

Tests step the worker with run_one() so each attempt is deterministic, and
make a requeued job due again with SQL rather than sleeping.
"""

import asyncio

import pytest

from llmgw import sweeper
from llmgw.jobs import queue
from llmgw.jobs.queue import JobState, Outcome
from llmgw.ledger import accounts, holds
from llmgw.ledger.checks import check_ledger
from llmgw.ledger.models import HoldState
from llmgw.mock_provider import count_prompt_tokens
from llmgw.pricing import DEFAULT_PRICES, cost, estimate_output_tokens
from llmgw.ratelimit import RateLimiter, RateLimits

from .conftest import GRANT, body, enqueue, expire_lease, make_due, worker_env

PRICE = DEFAULT_PRICES["mock-small"]
PROMPT_EST = 10  # what the enqueue helper records
MESSAGES = body()["messages"]


async def submit(conn, team, **mock):
    j, _ = await enqueue(conn, team, request=body(mock=mock) if mock else body())
    return j


async def job(conn, job_id):
    return await queue.get_job(conn, job_id)


async def attempts(conn, job_id):
    return [tuple(r) for r in await conn.fetch(
        "SELECT attempt, outcome, cost, flagged FROM job_attempts WHERE job_id = $1 ORDER BY attempt", job_id
    )]


async def org(conn, team):
    return await accounts.org_accounts(conn, team.org_id)


def usage_cost(completion: int) -> int:
    return cost(PRICE, count_prompt_tokens(MESSAGES), completion)


def counted_cost(chunks: int) -> int:
    chars = sum(len(f"tok{i} ") for i in range(chunks))
    return cost(PRICE, PROMPT_EST, estimate_output_tokens(chunks, chars))


async def poll(predicate, timeout=5.0):
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.02)


async def test_success_stores_the_result_and_settles_on_usage(conn, team, worker, mock):
    j = await submit(conn, team, tokens=5)
    assert await worker.run_one() == Outcome.SUCCEEDED

    done = await job(conn, j.id)
    assert done.state == JobState.SUCCEEDED
    msg = done.result["choices"][0]
    assert msg["message"]["content"] == "tok0 tok1 tok2 tok3 tok4 " and msg["finish_reason"] == "stop"
    assert done.result["object"] == "chat.completion" and done.result["usage"]["completion_tokens"] == 5
    sent = mock.state.requests[0]
    assert sent["stream"] is True and sent["stream_options"]["include_usage"] is True
    assert sent["max_tokens"] == 50
    assert await accounts.balance(conn, team.available) == GRANT - usage_cost(5)
    assert await attempts(conn, j.id) == [(1, "succeeded", usage_cost(5), False)]
    assert await worker.run_one() is None
    assert await check_ledger(conn) == []


async def test_missing_usage_settles_on_a_flagged_estimate(conn, team, worker):
    j = await submit(conn, team, tokens=4, no_usage=True)
    assert await worker.run_one() == Outcome.SUCCEEDED
    hold = await holds.get_hold(conn, j.hold_id)
    assert hold.settled_amount == counted_cost(4)
    flagged = await conn.fetchval("SELECT flagged FROM postings WHERE id = $1", hold.close_posting_id)
    assert flagged
    assert await check_ledger(conn) == []


async def test_503_then_success_charges_once_with_no_loss(conn, team, worker, mock):
    j = await submit(conn, team, tokens=3, key="k503", script=[{"status": 503}, {}])
    assert await worker.run_one() == Outcome.RETRYABLE
    assert (await job(conn, j.id)).last_error == "upstream_503"
    assert await worker.run_one() == Outcome.SUCCEEDED

    assert await attempts(conn, j.id) == [(1, "retryable", 0, False), (2, "succeeded", usage_cost(3), False)]
    assert await accounts.balance(conn, team.available) == GRANT - usage_cost(3)
    assert await accounts.balance(conn, (await org(conn, team)).retry_loss) == 0
    assert await check_ledger(conn) == []


async def test_cut_then_success_books_the_cut_to_retry_loss(conn, team, worker):
    o = await org(conn, team)
    external_before = await accounts.balance(conn, o.external)
    j = await submit(conn, team, tokens=6, key="kcut", script=[{"cut_after": 3}, {}])

    assert await worker.run_one() == Outcome.RETRYABLE
    assert (await job(conn, j.id)).last_error == "upstream_cut"
    assert await worker.run_one() == Outcome.SUCCEEDED

    loss = counted_cost(3)
    assert await attempts(conn, j.id) == [(1, "retryable", loss, True), (2, "succeeded", usage_cost(6), False)]
    assert await accounts.balance(conn, o.retry_loss) == loss
    assert await accounts.balance(conn, o.external) == external_before - loss
    assert await accounts.balance(conn, team.available) == GRANT - usage_cost(6)  # the team pays once
    assert await check_ledger(conn) == []


async def test_429_honours_retry_after(conn, team, worker):
    j = await submit(conn, team, status=429)
    assert await worker.run_one() == Outcome.RETRYABLE
    wait = await conn.fetchval("SELECT extract(epoch FROM run_after - now()) FROM jobs WHERE id = $1", j.id)
    assert 0.5 < wait <= 1.0
    assert await worker.run_one() is None  # not due yet
    assert await attempts(conn, j.id) == [(1, "retryable", 0, False)]


async def test_provider_400_fails_the_job_and_releases(conn, team, worker):
    j = await submit(conn, team, status=400)
    assert await worker.run_one() == Outcome.PERMANENT
    assert (await job(conn, j.id)).state == JobState.FAILED
    assert (await holds.get_hold(conn, j.hold_id)).state == HoldState.RELEASED
    assert await accounts.balance(conn, team.available) == GRANT
    assert await check_ledger(conn) == []


async def test_always_503_goes_dead_after_max_attempts(conn, team, worker):
    j = await submit(conn, team, status=503)  # the helper's max_attempts is 3
    outcomes = [await worker.run_one() for _ in range(4)]
    assert outcomes == [Outcome.RETRYABLE] * 3 + [None]
    done = await job(conn, j.id)
    assert done.state == JobState.DEAD and done.attempts == 3
    assert (await holds.get_hold(conn, j.hold_id)).state == HoldState.RELEASED
    assert len(await attempts(conn, j.id)) == 3
    assert await check_ledger(conn) == []


async def test_lost_lease_cancels_the_call_and_books_its_cost(conn, team, worker, mock):
    j = await submit(conn, team, tokens=200, delay_ms=20)
    run = asyncio.create_task(worker.run_one())
    await poll(lambda: _at_least(mock, 2))
    # Another worker took the job: the token moves on.
    await conn.execute("UPDATE jobs SET lease_token = lease_token + 100 WHERE id = $1", j.id)

    assert await run == Outcome.FENCED
    await poll(lambda: _disconnected(mock))
    (row,) = await attempts(conn, j.id)
    assert row[1] == "fenced" and row[2] > 0 and row[3] is True
    assert await accounts.balance(conn, (await org(conn, team)).retry_loss) == row[2]
    current = await job(conn, j.id)
    assert current.state == JobState.RUNNING  # left for the lease holder
    assert (await holds.get_hold(conn, j.hold_id)).state == HoldState.HELD
    assert await check_ledger(conn) == []


async def _at_least(mock, n):
    return mock.state.sent_chunks >= n


async def _disconnected(mock):
    return mock.state.disconnected == 1


async def test_crash_after_reply_is_reclaimed_and_charged_once(conn, pool, team, worker, mock):
    j = await submit(conn, team, tokens=3)

    async def die(a):
        worker.before_finish = None
        raise RuntimeError("worker died before the finish committed")

    worker.before_finish = die
    assert await worker.run_one() == Outcome.LOST
    assert (await job(conn, j.id)).state == JobState.RUNNING
    assert (await holds.get_hold(conn, j.hold_id)).state == HoldState.HELD

    await expire_lease(conn, j.id)
    assert await sweeper.reclaim_once(pool, base_s=0, cap_s=0) == 1
    assert await worker.run_one() == Outcome.SUCCEEDED

    assert len(mock.state.requests) == 2
    assert await attempts(conn, j.id) == [(1, "lost", 0, False), (2, "succeeded", usage_cost(3), False)]
    assert await accounts.balance(conn, team.available) == GRANT - usage_cost(3)
    assert await check_ledger(conn) == []


async def test_finish_survives_repeated_cancellation(conn, team, worker):
    j = await submit(conn, team, tokens=3)
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow(a):
        entered.set()
        await release.wait()

    worker.before_finish = slow
    run = asyncio.create_task(worker.run_one())
    await entered.wait()
    run.cancel()
    await asyncio.sleep(0.05)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    release.set()
    await poll(lambda: _state_is(conn, j.id, JobState.SUCCEEDED))
    assert await check_ledger(conn) == []


async def _state_is(conn, job_id, state):
    return (await job(conn, job_id)).state == state


async def test_shutdown_waits_for_pending_finishes(conn, team, database_url, pool, mock):
    j = await submit(conn, team, tokens=3)
    async with worker_env(database_url, pool, mock.url, worker_concurrency=1) as w:
        entered = asyncio.Event()

        async def slow(a):
            entered.set()
            await asyncio.sleep(0.3)

        w.before_finish = slow
        run = asyncio.create_task(w.run())
        await entered.wait()
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run
        # run() returned only after the finish committed.
        assert (await job(conn, j.id)).state == JobState.SUCCEEDED
    assert await check_ledger(conn) == []


async def test_rate_limited_job_is_requeued_with_its_attempt_back(conn, team, database_url, pool, mock, redis_client):
    limiter = RateLimiter(redis_client, RateLimits(team_tpm=100), timeout_s=1)
    j = await submit(conn, team)
    reserved = PROMPT_EST + 50
    assert (await limiter.acquire(team.team_id, "mock-small", reserved)).allowed  # drain the bucket

    async with worker_env(database_url, pool, mock.url, limiter=limiter) as w:
        assert await w.run_one() == Outcome.RATE_LIMITED

    after = await job(conn, j.id)
    assert after.state == JobState.QUEUED and after.attempts == 0 and after.last_error == "team_rate_limited"
    wait = await conn.fetchval("SELECT extract(epoch FROM run_after - now()) FROM jobs WHERE id = $1", j.id)
    assert wait > 5  # 20 tokens short at 100 per minute
    assert await attempts(conn, j.id) == [] and mock.state.requests == []
    assert await check_ledger(conn) == []


async def test_rate_limited_success_refunds_unused_tokens(conn, team, database_url, pool, mock, redis_client):
    limiter = RateLimiter(redis_client, RateLimits(team_tpm=1000), timeout_s=1)
    await submit(conn, team, tokens=3)
    async with worker_env(database_url, pool, mock.url, limiter=limiter) as w:
        assert await w.run_one() == Outcome.SUCCEEDED
    level = await limiter.level(f"llmgw:rl:team:{team.team_id}:tpm")
    used = count_prompt_tokens(MESSAGES) + 3
    assert level == pytest.approx(1000 - used, abs=1)


async def test_concurrent_workers_run_each_job_once(conn, team, database_url, pool, mock):
    ids = [(await submit(conn, team, tokens=2)).id for _ in range(20)]
    async with worker_env(database_url, pool, mock.url) as w:
        async def drain():
            while await w.run_one() is not None:
                pass

        await asyncio.gather(*(drain() for _ in range(4)))

    states = await conn.fetch("SELECT state FROM jobs WHERE id = ANY($1)", ids)
    assert {r["state"] for r in states} == {"succeeded"}
    assert len(mock.state.requests) == 20
    assert await conn.fetchval("SELECT count(*) FROM postings WHERE kind = 'settle'") == 20
    assert await conn.fetchval("SELECT count(*) FROM postings WHERE kind = 'retry_loss'") == 0
    assert await accounts.balance(conn, team.held) == 0
    assert await check_ledger(conn) == []
