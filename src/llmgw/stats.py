"""Gauges read from the database, set by the sweeper once per pass.

One process (the sweeper) runs these, so N gateways don't repeat the same
queries. Plain reads, outside any write transaction. `check_ledger` runs in its
own REPEATABLE READ READ ONLY transaction for a consistent snapshot; every
invariant it checks is written atomically, so live traffic can't trip it.

A failed collect or check is logged by the caller and leaves the previous
values in place.
"""

import asyncpg
import structlog

from llmgw import metrics
from llmgw.ledger.checks import check_ledger

log = structlog.get_logger("llmgw.stats")

JOB_STATES = ("queued", "running", "succeeded", "failed", "dead")


async def collect(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        by_state = {r["state"]: r["n"] for r in await conn.fetch(
            "SELECT state::text AS state, count(*) AS n FROM jobs GROUP BY state"
        )}
        oldest_ready = await conn.fetchval(
            """
            SELECT coalesce(extract(epoch FROM now() - min(run_after)), 0)::float8
              FROM jobs WHERE state = 'queued' AND run_after <= now()
            """
        )
        open_holds = {r["kind"]: r for r in await conn.fetch(
            """
            SELECT CASE WHEN j.id IS NULL THEN 'sync' ELSE 'job' END AS kind, count(*) AS n,
                   extract(epoch FROM now() - min(h.created_at))::float8 AS oldest
              FROM holds h LEFT JOIN jobs j ON j.hold_id = h.id
             WHERE h.state = 'held'
             GROUP BY 1
            """
        )}
        retry_loss = await conn.fetchval(
            """
            SELECT coalesce(sum(b.amount), 0) FROM accounts a JOIN balances b ON b.account_id = a.id
             WHERE a.kind = 'retry_loss'
            """
        )
        flagged = await conn.fetchval("SELECT count(*) FROM postings WHERE flagged")

    for state in JOB_STATES:
        metrics.JOBS.labels(state).set(by_state.get(state, 0))
    metrics.JOB_OLDEST_READY.set(oldest_ready)
    for kind in ("sync", "job"):
        row = open_holds.get(kind)
        metrics.HOLDS_OPEN.labels(kind).set(row["n"] if row else 0)
        metrics.HOLD_OLDEST_OPEN.labels(kind).set(row["oldest"] if row else 0)
    metrics.RETRY_LOSS.set(retry_loss)
    metrics.FLAGGED_POSTINGS.set(flagged)


async def check(pool: asyncpg.Pool) -> list[str]:
    """Run check_ledger on a consistent snapshot; export and log what it finds."""
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            problems = await check_ledger(conn)
    metrics.LEDGER_VIOLATIONS.set(len(problems))
    for p in problems:
        log.error("ledger_violation", problem=p)
    return problems
