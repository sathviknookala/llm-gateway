"""Database gauges (llmgw.stats), the sweeper's counters, and the periodic ledger check."""

import asyncio
from datetime import UTC, datetime, timedelta

from conftest import metric
from llmgw import metrics, stats, sweeper
from llmgw.jobs import queue
from llmgw.ledger import holds

from .conftest import enqueue, expire_lease, fail

LEASE = dict(worker="w", lease_s=30.0)


def gauge(name: str, **labels: str) -> float:
    return metric(name, **labels)


async def sync_hold(conn, team, request_id: str, *, minutes: float = 10):
    async with conn.transaction():
        return await holds.place_hold(
            conn, team_id=team.team_id, amount=100,
            expires_at=datetime.now(UTC) + timedelta(minutes=minutes), request_id=request_id,
        )


async def test_empty_database_sets_zeros(conn, pool):
    await stats.collect(pool)
    for state in stats.JOB_STATES:
        assert gauge("llmgw_jobs", state=state) == 0
    for kind in ("sync", "job"):
        assert gauge("llmgw_holds_open", kind=kind) == 0
        assert gauge("llmgw_hold_oldest_open_seconds", kind=kind) == 0
    assert gauge("llmgw_job_oldest_ready_seconds") == 0
    assert gauge("llmgw_retry_loss_microcredits") == 0


async def test_counts_jobs_and_open_holds(conn, pool, team):
    await enqueue(conn, team)
    await queue.lease(conn, **LEASE)  # running
    ready, _ = await enqueue(conn, team)
    later, _ = await enqueue(conn, team)
    await conn.execute("UPDATE jobs SET run_after = now() - interval '30 seconds' WHERE id = $1", ready.id)
    await conn.execute("UPDATE jobs SET run_after = now() + interval '1 hour' WHERE id = $1", later.id)
    await sync_hold(conn, team, "s1")
    await conn.execute("UPDATE holds SET created_at = now() - interval '45 seconds' WHERE NOT EXISTS"
                       " (SELECT 1 FROM jobs WHERE jobs.hold_id = holds.id)")

    await stats.collect(pool)

    assert gauge("llmgw_jobs", state="queued") == 2
    assert gauge("llmgw_jobs", state="running") == 1
    assert gauge("llmgw_jobs", state="dead") == 0
    assert gauge("llmgw_holds_open", kind="sync") == 1
    assert gauge("llmgw_holds_open", kind="job") == 3
    assert 45 <= gauge("llmgw_hold_oldest_open_seconds", kind="sync") < 60
    # Only the job that is due counts as backlog; the future one isn't ready yet.
    assert 30 <= gauge("llmgw_job_oldest_ready_seconds") < 45


async def test_retry_loss_gauge_follows_the_account(conn, pool, team):
    await enqueue(conn, team)
    job = await queue.lease(conn, **LEASE)
    await fail(conn, job, cost=123, flagged=True, error="upstream_cut")
    await stats.collect(pool)
    assert gauge("llmgw_retry_loss_microcredits") == 123
    assert gauge("llmgw_flagged_postings") == 1


async def test_ledger_check_exports_the_violation_count(conn, pool, team):
    assert await stats.check(pool) == []
    assert gauge("llmgw_ledger_violations") == 0
    # balances is the one ledger table that isn't append-only; corrupt it directly.
    await conn.execute("UPDATE balances SET amount = amount + 7 WHERE account_id = $1", team.available)
    assert await stats.check(pool)
    assert gauge("llmgw_ledger_violations") >= 1


async def test_sweeper_loop_refreshes_gauges_and_checks_the_ledger(conn, pool, team):
    await enqueue(conn, team)
    await stats.check(pool)  # gauges are process-global: start from a clean ledger's 0
    assert gauge("llmgw_ledger_violations") == 0
    metrics.JOBS.labels("queued").set(-1)  # a value only the loop's collect can replace
    await conn.execute("UPDATE balances SET amount = amount + 7 WHERE account_id = $1", team.available)

    task = asyncio.create_task(sweeper.run(pool, interval_s=0.05, batch=10, ledger_check_interval_s=60))
    try:
        async with asyncio.timeout(5):
            while gauge("llmgw_ledger_violations") == 0 or gauge("llmgw_jobs", state="queued") != 1:
                await asyncio.sleep(0.02)
    finally:
        task.cancel()
        await asyncio.wait([task])


async def test_expired_hold_is_counted(conn, pool, team):
    await sync_hold(conn, team, "old", minutes=-1)
    expired0 = metric("llmgw_sweeper_actions_total", action="hold_expired")
    assert await sweeper.sweep_once(pool) == 1
    assert metric("llmgw_sweeper_actions_total", action="hold_expired") == expired0 + 1


async def test_dead_letter_by_reclaim_counts_a_release(conn, pool, team):
    await enqueue(conn, team, max_attempts=1)
    job = await queue.lease(conn, **LEASE)
    await expire_lease(conn, job.id)
    release0 = metric("llmgw_hold_closes_total", path="job_sweeper", op="release", flagged="false")
    reclaimed0 = metric("llmgw_sweeper_actions_total", action="lease_reclaimed")
    assert await sweeper.reclaim_once(pool, base_s=0, cap_s=0) == 1  # out of attempts: dead
    assert metric("llmgw_hold_closes_total", path="job_sweeper", op="release", flagged="false") == release0 + 1
    assert metric("llmgw_sweeper_actions_total", action="lease_reclaimed") == reclaimed0 + 1


async def test_requeue_by_reclaim_counts_no_release(conn, pool, team):
    await enqueue(conn, team, max_attempts=3)
    job = await queue.lease(conn, **LEASE)
    await expire_lease(conn, job.id)
    release0 = metric("llmgw_hold_closes_total", path="job_sweeper", op="release", flagged="false")
    assert await sweeper.reclaim_once(pool, base_s=0, cap_s=0) == 1  # queued again
    assert metric("llmgw_hold_closes_total", path="job_sweeper", op="release", flagged="false") == release0


async def test_deadline_counts_a_release(conn, pool, team):
    j, _ = await enqueue(conn, team)
    await conn.execute("UPDATE jobs SET deadline = now() - interval '1 second' WHERE id = $1", j.id)
    release0 = metric("llmgw_hold_closes_total", path="job_sweeper", op="release", flagged="false")
    assert await sweeper.expire_deadlines_once(pool) == 1
    assert metric("llmgw_hold_closes_total", path="job_sweeper", op="release", flagged="false") == release0 + 1
