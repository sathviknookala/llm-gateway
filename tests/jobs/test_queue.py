"""llmgw.jobs.queue against real Postgres: enqueue, lease, fencing, retries, dead letters."""

import asyncio

import pytest

from llmgw.jobs import queue
from llmgw.jobs.errors import JobConflict, LeaseLost
from llmgw.jobs.queue import JobState, Outcome
from llmgw.ledger import accounts, holds
from llmgw.ledger.checks import check_ledger
from llmgw.ledger.errors import IdempotencyConflict, InsufficientCredits
from llmgw.ledger.models import HoldState

from .conftest import AMOUNT, GRANT, NO_BACKOFF, body, complete, enqueue, expire_lease, fail, in_tx, make_due

LEASE = dict(worker="w", lease_s=30.0)


async def counts(c):
    return await c.fetchrow(
        "SELECT (SELECT count(*) FROM jobs) AS jobs, (SELECT count(*) FROM holds) AS holds,"
        " (SELECT count(*) FROM postings WHERE kind = 'hold') AS hold_postings,"
        " (SELECT count(*) FROM postings WHERE kind = 'retry_loss') AS losses,"
        " (SELECT count(*) FROM postings WHERE kind = 'settle') AS settles"
    )


async def hold_state(c, job):
    return (await holds.get_hold(c, job.hold_id)).state


async def job(c, job_id):
    return await queue.get_job(c, job_id)


# --- enqueue ---

async def test_enqueue_writes_job_and_hold_together(conn, team):
    j, created = await enqueue(conn, team)
    assert created and j.state == JobState.QUEUED and j.attempts == 0
    assert await hold_state(conn, j) == HoldState.HELD
    assert await accounts.balance(conn, team.held) == AMOUNT
    assert await check_ledger(conn) == []


async def test_enqueue_rolls_back_as_one(conn, team):
    with pytest.raises(RuntimeError, match="after"):
        async with conn.transaction():
            await queue.enqueue(
                conn, team_id=team.team_id, model="mock-small", request=body(), prompt_estimate=10,
                max_tokens=50, amount=AMOUNT, idempotency_key=None, max_attempts=3, ttl_s=60, hold_grace_s=60,
            )
            raise RuntimeError("after enqueue")
    c = await counts(conn)
    assert (c["jobs"], c["holds"], c["hold_postings"]) == (0, 0, 0)


async def test_insufficient_credits_writes_nothing(conn, team):
    with pytest.raises(InsufficientCredits):
        await enqueue(conn, team, amount=GRANT + 1)
    c = await counts(conn)
    assert (c["jobs"], c["holds"], c["hold_postings"]) == (0, 0, 0)


async def test_idempotent_replay_and_conflict(conn, team):
    first, created = await enqueue(conn, team, key="k")
    again, created_again = await enqueue(conn, team, key="k")
    assert created and not created_again and again.id == first.id
    # A different body is a conflict, whatever its amount; caught before any hold.
    with pytest.raises(JobConflict):
        await enqueue(conn, team, key="k", request=body("other"))
    with pytest.raises(JobConflict):
        await enqueue(conn, team, key="k", request=body("other"), amount=AMOUNT + 1)
    c = await counts(conn)
    assert (c["jobs"], c["holds"]) == (1, 1)
    assert await check_ledger(conn) == []


async def test_concurrent_submits_with_one_key_make_one_job(conn, pool, team):
    async def submit():
        async with pool.acquire() as c:
            return await enqueue(c, team, key="same")

    results = await asyncio.gather(*(submit() for _ in range(8)))
    assert len({j.id for j, _ in results}) == 1
    assert sum(created for _, created in results) == 1
    c = await counts(conn)
    assert (c["jobs"], c["holds"], c["hold_postings"]) == (1, 1, 1)
    assert await check_ledger(conn) == []


async def _wait_until_blocked(conn, pid):
    for _ in range(500):
        if await conn.fetchval(
            "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = $1", pid
        ):
            return
        await asyncio.sleep(0.01)
    raise AssertionError("second submit never blocked")


async def test_concurrent_conflict_with_a_different_amount(conn, pool, team):
    async with pool.acquire() as c1, pool.acquire() as c2:
        t1 = c1.transaction()
        await t1.start()
        await queue.enqueue(
            c1, team_id=team.team_id, model="mock-small", request=body(), prompt_estimate=10,
            max_tokens=50, amount=AMOUNT, idempotency_key="k", max_attempts=3, ttl_s=60, hold_grace_s=60,
        )
        second = asyncio.create_task(enqueue(c2, team, key="k", request=body("other"), amount=AMOUNT + 1))
        await _wait_until_blocked(conn, c2.get_server_pid())
        await t1.commit()
        with pytest.raises(IdempotencyConflict):
            await second
    c = await counts(conn)
    assert (c["jobs"], c["holds"]) == (1, 1)
    assert await check_ledger(conn) == []


# --- lease and renew ---

async def test_lease_is_fifo_and_skips_not_ready(conn, team):
    a, _ = await enqueue(conn, team)
    b, _ = await enqueue(conn, team)
    later, _ = await enqueue(conn, team)
    stale, _ = await enqueue(conn, team)
    await conn.execute("UPDATE jobs SET run_after = now() + interval '1 hour' WHERE id = $1", later.id)
    await conn.execute("UPDATE jobs SET deadline = now() - interval '1 second' WHERE id = $1", stale.id)

    first = await queue.lease(conn, **LEASE)
    second = await queue.lease(conn, **LEASE)
    assert [first.id, second.id] == [a.id, b.id]
    assert first.state == JobState.RUNNING and first.attempts == 1 and first.lease_token == 1
    assert await queue.lease(conn, **LEASE) is None


async def test_concurrent_leases_take_distinct_jobs(conn, pool, team):
    for _ in range(5):
        await enqueue(conn, team)
    got = await asyncio.gather(*(queue.lease(pool, **LEASE) for _ in range(8)))
    leased = [j.id for j in got if j is not None]
    assert len(leased) == len(set(leased)) == 5


async def test_renew_with_a_stale_token_is_lease_lost(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    await queue.renew(conn, job_id=j.id, token=j.lease_token, lease_s=30)
    with pytest.raises(LeaseLost):
        await queue.renew(conn, job_id=j.id, token=j.lease_token + 1, lease_s=30)


# --- complete ---

async def test_complete_settles_and_stores_the_result(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    assert await complete(conn, j, actual=200) == Outcome.SUCCEEDED

    done = await job(conn, j.id)
    assert done.state == JobState.SUCCEEDED and done.result == {"ok": True}
    assert done.locked_until is None and done.finished_at is not None
    assert await hold_state(conn, j) == HoldState.SETTLED
    assert await accounts.balance(conn, team.available) == GRANT - 200
    row = await conn.fetchrow("SELECT outcome, cost FROM job_attempts WHERE job_id = $1", j.id)
    assert (row["outcome"], row["cost"]) == ("succeeded", 200)
    assert await check_ledger(conn) == []


async def test_complete_is_one_transaction(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    with pytest.raises(RuntimeError, match="after"):
        async with conn.transaction():
            await queue.complete(conn, job_id=j.id, token=j.lease_token, attempt=1, actual=200,
                                 flagged=False, result={"ok": True})
            raise RuntimeError("after complete")
    assert (await job(conn, j.id)).state == JobState.RUNNING
    assert await hold_state(conn, j) == HoldState.HELD
    assert await check_ledger(conn) == []


async def test_retried_complete_is_a_replay(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    assert await complete(conn, j, actual=200) == Outcome.SUCCEEDED
    # The ack was lost and the finish retried: nothing more is written.
    assert await complete(conn, j, actual=200) == Outcome.SUCCEEDED
    c = await counts(conn)
    assert (c["settles"], c["losses"]) == (1, 0)
    assert await check_ledger(conn) == []


async def test_fenced_complete_books_retry_loss_and_leaves_the_job(conn, team):
    await enqueue(conn, team)
    old = await queue.lease(conn, **LEASE)
    await expire_lease(conn, old.id)
    assert await in_tx(conn, queue.reclaim, job_id=old.id, **NO_BACKOFF) == JobState.QUEUED
    new = await queue.lease(conn, **LEASE)
    assert (new.id, new.lease_token, new.attempts) == (old.id, 2, 2)

    assert await complete(conn, old, actual=150) == Outcome.FENCED
    current = await job(conn, old.id)
    assert current.state == JobState.RUNNING and current.lease_token == 2
    assert await hold_state(conn, old) == HoldState.HELD
    row = await conn.fetchrow("SELECT outcome, cost FROM job_attempts WHERE job_id = $1 AND attempt = 1", old.id)
    assert (row["outcome"], row["cost"]) == ("fenced", 150)
    assert await accounts.balance(conn, (await accounts.org_accounts(conn, team.org_id)).retry_loss) == 150

    assert await complete(conn, new, actual=200) == Outcome.SUCCEEDED
    assert await hold_state(conn, new) == HoldState.SETTLED
    assert await accounts.balance(conn, team.available) == GRANT - 200
    assert await check_ledger(conn) == []


# --- fail_attempt ---

async def test_retryable_without_cost_requeues_with_no_posting(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    assert await fail(conn, j, retry_after_s=5) == Outcome.RETRYABLE
    after = await job(conn, j.id)
    assert after.state == JobState.QUEUED and after.attempts == 1 and after.last_error == "upstream_503"
    wait = await conn.fetchval("SELECT extract(epoch FROM run_after - now()) FROM jobs WHERE id = $1", j.id)
    assert 4 < wait <= 5
    assert (await counts(conn))["losses"] == 0
    assert await check_ledger(conn) == []


async def test_retryable_with_cost_posts_a_linked_retry_loss(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    assert await fail(conn, j, cost=40, flagged=True, error="upstream_cut") == Outcome.RETRYABLE
    row = await conn.fetchrow(
        "SELECT a.cost, a.flagged, p.kind, p.flagged AS pf FROM job_attempts a"
        " JOIN postings p ON p.id = a.retry_loss_posting_id WHERE a.job_id = $1", j.id,
    )
    assert (row["cost"], row["flagged"], row["kind"], row["pf"]) == (40, True, "retry_loss", True)
    # Retried after it committed: still one posting.
    assert await fail(conn, j, cost=40, flagged=True, error="upstream_cut") == Outcome.RETRYABLE
    assert (await counts(conn))["losses"] == 1
    assert await accounts.balance(conn, team.held) == AMOUNT  # the team isn't charged
    assert await check_ledger(conn) == []


async def test_permanent_fails_the_job_and_releases(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    assert await fail(conn, j, permanent=True, error="upstream_400") == Outcome.PERMANENT
    after = await job(conn, j.id)
    assert after.state == JobState.FAILED and after.finished_at is not None
    assert await hold_state(conn, j) == HoldState.RELEASED
    assert await accounts.balance(conn, team.available) == GRANT
    assert await check_ledger(conn) == []


async def test_permanent_with_a_cost_is_refused(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    with pytest.raises(ValueError):
        await fail(conn, j, permanent=True, cost=5)


async def test_out_of_attempts_is_dead_and_released(conn, team):
    await enqueue(conn, team, max_attempts=2)
    for n in (1, 2):
        j = await queue.lease(conn, **LEASE)
        assert j.attempts == n
        await fail(conn, j, cost=10)
    after = await job(conn, j.id)
    assert after.state == JobState.DEAD and after.attempts == 2
    assert await hold_state(conn, j) == HoldState.RELEASED
    assert await accounts.balance(conn, team.available) == GRANT
    assert (await counts(conn))["losses"] == 2
    assert await check_ledger(conn) == []


# --- requeue_unstarted ---

async def test_requeue_unstarted_gives_the_attempt_back(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    assert await in_tx(conn, queue.requeue_unstarted, job_id=j.id, token=j.lease_token,
                       delay_s=0, error="team_rate_limited")
    after = await job(conn, j.id)
    assert after.state == JobState.QUEUED and after.attempts == 0 and after.lease_token == 1
    assert await conn.fetchval("SELECT count(*) FROM job_attempts") == 0


async def test_retried_requeue_cannot_touch_a_newer_lease(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    kw = dict(job_id=j.id, token=j.lease_token, delay_s=0, error="team_rate_limited")
    assert await in_tx(conn, queue.requeue_unstarted, **kw)
    newer = await queue.lease(conn, **LEASE)
    assert not await in_tx(conn, queue.requeue_unstarted, **kw)
    after = await job(conn, j.id)
    assert after.state == JobState.RUNNING and after.lease_token == newer.lease_token == 2


async def test_reused_attempt_number_gets_one_row_and_one_posting(conn, team):
    await enqueue(conn, team)
    j = await queue.lease(conn, **LEASE)
    await in_tx(conn, queue.requeue_unstarted, job_id=j.id, token=j.lease_token, delay_s=0, error="rl")
    again = await queue.lease(conn, **LEASE)
    assert again.attempts == 1 and again.lease_token == 2
    await fail(conn, again, cost=25)
    assert await conn.fetchval("SELECT count(*) FROM job_attempts WHERE job_id = $1", j.id) == 1
    assert (await counts(conn))["losses"] == 1
    assert await check_ledger(conn) == []


# --- backoff ---

def test_backoff_bounds():
    kw = dict(base_s=1.0, cap_s=60.0)
    assert queue.backoff(3, None, rand=lambda: 0.0, **kw) == 0.0
    assert queue.backoff(3, None, rand=lambda: 1.0, **kw) == 4.0
    assert queue.backoff(3, 9.0, rand=lambda: 1.0, **kw) == 9.0  # retry-after wins when larger
    assert queue.backoff(20, None, rand=lambda: 1.0, **kw) == 60.0  # capped
    assert queue.backoff(1, None, rand=lambda: 0.5, **kw) == 0.5
