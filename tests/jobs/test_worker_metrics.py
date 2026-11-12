"""Worker metrics: attempts by outcome, queue wait, hold closes, lost leases, failed finishes.

The registry is process-global, so every assertion is a delta.
"""

import asyncio

from conftest import metric
from llmgw.jobs.queue import Outcome
from llmgw.ledger import holds

from .conftest import enqueue
from .test_worker import _at_least, poll, submit


def attempts(outcome: str) -> float:
    return metric("llmgw_job_attempts_total", outcome=outcome)


def closes(op: str, path: str = "job", flagged: str = "false") -> float:
    return metric("llmgw_hold_closes_total", path=path, op=op, flagged=flagged)


async def test_success_counts_an_attempt_a_wait_and_a_settle(conn, team, worker):
    ok0, settle0 = attempts("succeeded"), closes("settle")
    waits0 = metric("llmgw_job_queue_wait_seconds_count")
    await submit(conn, team)
    assert await worker.run_one() == Outcome.SUCCEEDED
    assert attempts("succeeded") == ok0 + 1
    assert closes("settle") == settle0 + 1
    assert metric("llmgw_job_queue_wait_seconds_count") == waits0 + 1


async def test_queue_wait_reflects_time_spent_ready(conn, team, worker):
    j = await submit(conn, team)
    await conn.execute("UPDATE jobs SET run_after = now() - interval '7 seconds' WHERE id = $1", j.id)
    over5 = lambda: (metric("llmgw_job_queue_wait_seconds_count")  # noqa: E731
                     - metric("llmgw_job_queue_wait_seconds_bucket", le="5.0"))
    before = over5()
    assert await worker.run_one() == Outcome.SUCCEEDED
    assert over5() == before + 1  # waited ~7 s: above the 5 s bucket, on the DB clock


async def test_provider_400_counts_a_release(conn, team, worker):
    release0, permanent0 = closes("release"), attempts("permanent")
    await submit(conn, team, status=400)
    assert await worker.run_one() == Outcome.PERMANENT
    assert closes("release") == release0 + 1
    assert attempts("permanent") == permanent0 + 1


async def test_retryable_releases_only_on_the_last_attempt(conn, team, worker):
    j, _ = await enqueue(conn, team, request={"model": "mock-small", "messages": [{"role": "user", "content": "x"}],
                                              "max_tokens": 50, "mock": {"status": 503}}, max_attempts=2)
    release0, retry0 = closes("release"), attempts("retryable")
    assert await worker.run_one() == Outcome.RETRYABLE
    assert closes("release") == release0  # requeued, hold still held
    assert await worker.run_one() == Outcome.RETRYABLE  # out of attempts: dead, released
    assert closes("release") == release0 + 1
    assert attempts("retryable") == retry0 + 2


async def test_hold_closed_elsewhere_counts_no_release(conn, team, worker):
    j = await submit(conn, team)

    async def close_it_first(a):
        worker.before_finish = None
        async with conn.transaction():
            await holds.release(conn, hold_id=j.hold_id)

    worker.before_finish = close_it_first
    release0, settle0 = closes("release"), closes("settle")
    assert await worker.run_one() == Outcome.PERMANENT
    assert (closes("release"), closes("settle")) == (release0, settle0)


async def test_lost_lease_is_counted(conn, team, worker, mock):
    j = await submit(conn, team, tokens=200, delay_ms=20)
    lost0, fenced0 = metric("llmgw_lease_lost_total"), attempts("fenced")
    run = asyncio.create_task(worker.run_one())
    await poll(lambda: _at_least(mock, 2))
    await conn.execute("UPDATE jobs SET lease_token = lease_token + 100 WHERE id = $1", j.id)
    assert await run == Outcome.FENCED
    assert metric("llmgw_lease_lost_total") == lost0 + 1
    assert attempts("fenced") == fenced0 + 1


async def test_failed_finish_is_counted_as_lost(conn, team, worker):
    await submit(conn, team)

    async def die(a):
        worker.before_finish = None
        raise RuntimeError("finish failed")

    worker.before_finish = die
    failures0, lost0 = metric("llmgw_job_finish_failures_total"), attempts("lost")
    assert await worker.run_one() == Outcome.LOST
    assert metric("llmgw_job_finish_failures_total") == failures0 + 1
    assert attempts("lost") == lost0 + 1

