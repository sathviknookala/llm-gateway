"""llmgw.sweeper on the job queue: lease reclaim, deadlines, and leaving job holds alone."""

import asyncio

from llmgw import sweeper
from llmgw.jobs import queue
from llmgw.jobs.queue import JobState, Outcome
from llmgw.ledger import accounts, holds
from llmgw.ledger.checks import check_ledger
from llmgw.ledger.models import HoldState

from .conftest import GRANT, NO_BACKOFF, complete, enqueue, expire_lease, fail


async def state(conn, hold_id) -> HoldState:
    return (await holds.get_hold(conn, hold_id)).state


LEASE = dict(worker="w", lease_s=30.0)


async def leased(conn, team, n=1, **kw):
    for _ in range(n):
        await enqueue(conn, team, **kw)
    return [await queue.lease(conn, **LEASE) for _ in range(n)]


async def test_reclaims_an_expired_lease(conn, pool, team):
    (j,) = await leased(conn, team)
    await expire_lease(conn, j.id)

    assert await sweeper.reclaim_once(pool, **NO_BACKOFF) == 1

    after = await queue.get_job(conn, j.id)
    assert after.state == JobState.QUEUED and after.attempts == 1 and after.locked_until is None
    assert await conn.fetchval("SELECT outcome FROM job_attempts WHERE job_id = $1", j.id) == "lost"
    assert await state(conn, j.hold_id) == HoldState.HELD
    assert await sweeper.reclaim_once(pool) == 0
    assert await check_ledger(conn) == []


async def test_leaves_a_live_lease_alone(conn, pool, team):
    await leased(conn, team)
    assert await sweeper.reclaim_once(pool) == 0


async def test_reclaim_out_of_attempts_is_dead_and_released(conn, pool, team):
    (j,) = await leased(conn, team, max_attempts=1)
    await expire_lease(conn, j.id)

    assert await sweeper.reclaim_once(pool) == 1

    after = await queue.get_job(conn, j.id)
    assert after.state == JobState.DEAD and after.last_error == "lease_lost" and after.finished_at
    assert await state(conn, j.hold_id) == HoldState.RELEASED
    assert await accounts.balance(conn, team.available) == GRANT
    assert await check_ledger(conn) == []


async def test_reclaim_past_deadline_is_dead(conn, pool, team):
    (j,) = await leased(conn, team)
    await expire_lease(conn, j.id)
    await conn.execute("UPDATE jobs SET deadline = now() - interval '1 second' WHERE id = $1", j.id)
    assert await sweeper.reclaim_once(pool) == 1
    assert (await queue.get_job(conn, j.id)).state == JobState.DEAD
    assert await check_ledger(conn) == []


async def test_reclaim_skips_a_job_whose_finish_holds_the_lock(conn, pool, team):
    (j,) = await leased(conn, team)
    await expire_lease(conn, j.id)

    async with pool.acquire() as other:
        tr = other.transaction()
        await tr.start()
        # A worker finishing: the job row is locked until it commits.
        assert await queue.complete(other, job_id=j.id, token=j.lease_token, attempt=1, actual=100,
                                    flagged=False, result={"ok": True}) == Outcome.SUCCEEDED
        assert await sweeper.reclaim_once(pool) == 0
        await tr.commit()

    assert (await queue.get_job(conn, j.id)).state == JobState.SUCCEEDED
    assert await sweeper.reclaim_once(pool) == 0
    assert await check_ledger(conn) == []


async def test_concurrent_reclaims_take_each_lease_once(conn, pool, team):
    jobs = await leased(conn, team, n=20)
    await conn.execute("UPDATE jobs SET locked_until = now() - interval '1 second'")

    counts = await asyncio.gather(*(sweeper.reclaim_once(pool) for _ in range(4)))

    assert sum(counts) == len(jobs)
    assert await conn.fetchval("SELECT count(*) FROM job_attempts WHERE outcome = 'lost'") == 20
    assert await check_ledger(conn) == []


async def test_deadline_dead_letters_queued_jobs(conn, pool, team):
    due, _ = await enqueue(conn, team)
    live, _ = await enqueue(conn, team)
    await conn.execute("UPDATE jobs SET deadline = now() - interval '1 second' WHERE id = $1", due.id)

    assert await sweeper.expire_deadlines_once(pool) == 1

    gone = await queue.get_job(conn, due.id)
    assert gone.state == JobState.DEAD and gone.last_error == "deadline"
    assert await state(conn, due.hold_id) == HoldState.RELEASED
    assert (await queue.get_job(conn, live.id)).state == JobState.QUEUED
    assert await check_ledger(conn) == []


async def test_hold_sweeper_leaves_job_holds_alone(conn, pool, team):
    j, _ = await enqueue(conn, team)
    await conn.execute("UPDATE holds SET expires_at = now() - interval '1 second' WHERE id = $1", j.hold_id)
    assert await sweeper.sweep_once(pool) == 0
    assert await state(conn, j.hold_id) == HoldState.HELD


async def test_reclaimed_worker_reporting_later_upgrades_lost_to_fenced(conn, pool, team):
    (j,) = await leased(conn, team)
    await expire_lease(conn, j.id)
    await sweeper.reclaim_once(pool, **NO_BACKOFF)

    # The old worker was alive after all and reports a cut stream with a cost.
    assert await fail(conn, j, cost=30, flagged=True, error="upstream_cut") == Outcome.FENCED
    row = await conn.fetchrow("SELECT outcome, cost, retry_loss_posting_id FROM job_attempts WHERE job_id = $1", j.id)
    assert row["outcome"] == "fenced" and row["cost"] == 30 and row["retry_loss_posting_id"] is not None
    assert (await queue.get_job(conn, j.id)).state == JobState.QUEUED
    assert await complete(conn, j) == Outcome.FENCED  # still a replay of the fenced report
    assert await check_ledger(conn) == []
