"""Hold-expiry sweeper: the backstop for holds nobody closed.

A gateway close that failed, or a process that died mid-stream, leaves a hold
in `held`. Once its `expires_at` passes, the sweeper expires it, which returns
the full amount to the team. Any provider cost for that call is left to
reconciliation (a settle after expiry is rejected by the ledger).

Each hold is expired in its own transaction, on the connection that selected
it with `FOR UPDATE SKIP LOCKED`:

- a hold whose settle currently holds the row lock is skipped, not waited on;
- concurrent sweepers split the work instead of racing for the same rows;
- `holds.expire` re-locks a row this transaction already holds, which Postgres
  grants at once, and its due check uses the same `now()` as the select.

Correctness still rests on `expire`'s own lock, replay and state checks; the
SKIP LOCKED select only keeps sweepers from queueing behind each other.

A job's hold is never expired here: job paths close it (finish, lease reclaim,
deadline). `check_ledger` flags a job whose hold state doesn't match.

The same loop backs the job queue, one job per transaction, picked the same way:
- lease reclaim: a running job whose lease expired is queued again (or
  dead-lettered when out of attempts or past its deadline); the attempt is
  recorded as `lost`.
- deadlines: a queued job past its deadline is dead-lettered, hold released.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable

import asyncpg
import structlog

from llmgw.jobs import queue
from llmgw.jobs.errors import JobNotFound
from llmgw.ledger import holds
from llmgw.ledger.errors import HoldNotFound, InvalidHoldState

log = structlog.get_logger("llmgw.sweeper")

_PICK = """
SELECT id FROM holds
 WHERE state = 'held' AND expires_at <= now() AND id <> ALL($1::bigint[])
   AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.hold_id = holds.id)
 ORDER BY expires_at
 LIMIT 1
   FOR UPDATE SKIP LOCKED
"""


async def sweep_once(pool: asyncpg.Pool, *, batch: int = 500) -> int:
    """Expire up to `batch` overdue holds; return how many were expired.

    A hold that fails unexpectedly is skipped for the rest of the pass, so one
    poisoned row can't stall the holds queued behind it.
    """
    expired = 0
    failed: list[int] = []
    async with pool.acquire() as conn:
        for _ in range(batch):
            hold_id = None
            try:
                async with conn.transaction():
                    hold_id = await conn.fetchval(_PICK, failed)
                    if hold_id is None:
                        break
                    await holds.expire(conn, hold_id=hold_id)
                expired += 1
                log.info("hold_expired", hold_id=hold_id)
            except (InvalidHoldState, HoldNotFound) as e:
                # Closed by another path between pick and expire; nothing to do.
                log.info("hold_expire_skipped", hold_id=hold_id, reason=str(e))
                if hold_id is not None:
                    failed.append(hold_id)
            except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError):
                raise
            except Exception:
                log.exception("hold_expire_failed", hold_id=hold_id)
                if hold_id is None:
                    raise
                failed.append(hold_id)
    return expired


_PICK_LEASE = """
SELECT id FROM jobs
 WHERE state = 'running' AND locked_until <= now() AND id <> ALL($1::bigint[])
 ORDER BY locked_until
 LIMIT 1
   FOR UPDATE SKIP LOCKED
"""

_PICK_DEADLINE = """
SELECT id FROM jobs
 WHERE state = 'queued' AND deadline <= now() AND id <> ALL($1::bigint[])
 ORDER BY deadline
 LIMIT 1
   FOR UPDATE SKIP LOCKED
"""


async def _per_job(
    pool: asyncpg.Pool, pick: str, act: Callable[[asyncpg.Connection, int], Awaitable[object]],
    *, batch: int, event: str,
) -> int:
    """Run `act` on up to `batch` picked jobs, one transaction each; count those it acted on."""
    done = 0
    failed: list[int] = []
    async with pool.acquire() as conn:
        for _ in range(batch):
            job_id = None
            try:
                async with conn.transaction():
                    job_id = await conn.fetchval(pick, failed)
                    if job_id is None:
                        break
                    acted = await act(conn, job_id)
                if acted:
                    done += 1
                    log.info(event, job_id=job_id, result=str(acted))
                else:
                    failed.append(job_id)
            except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError):
                raise
            except Exception:
                log.exception(f"{event}_failed", job_id=job_id)
                if job_id is None:
                    raise
                failed.append(job_id)
    return done


async def reclaim_once(
    pool: asyncpg.Pool, *, batch: int = 500, base_s: float = 1.0, cap_s: float = 60.0,
    rand: Callable[[], float] = random.random,
) -> int:
    """Take back up to `batch` jobs whose lease expired; return how many."""

    async def act(conn: asyncpg.Connection, job_id: int) -> object:
        try:
            return await queue.reclaim(conn, job_id, base_s=base_s, cap_s=cap_s, rand=rand)
        except JobNotFound:
            return None

    return await _per_job(pool, _PICK_LEASE, act, batch=batch, event="lease_reclaimed")


async def expire_deadlines_once(pool: asyncpg.Pool, *, batch: int = 500) -> int:
    """Dead-letter up to `batch` queued jobs past their deadline; return how many."""
    return await _per_job(pool, _PICK_DEADLINE, queue.expire_deadline, batch=batch, event="job_deadline")


async def run(
    pool: asyncpg.Pool, *, interval_s: float, batch: int, retry_base_s: float = 1.0, retry_cap_s: float = 60.0
) -> None:
    """Sweep forever. A lost connection ends the pass; the next one retries."""
    while True:
        try:
            expired = await sweep_once(pool, batch=batch)
            reclaimed = await reclaim_once(pool, batch=batch, base_s=retry_base_s, cap_s=retry_cap_s)
            dead = await expire_deadlines_once(pool, batch=batch)
            if expired or reclaimed or dead:
                log.info("sweep_pass", expired=expired, reclaimed=reclaimed, deadline=dead)
        except Exception:
            log.exception("sweep_pass_failed")
        await asyncio.sleep(interval_s)
