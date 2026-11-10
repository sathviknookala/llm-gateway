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
"""

import asyncio

import asyncpg
import structlog

from llmgw.ledger import holds
from llmgw.ledger.errors import HoldNotFound, InvalidHoldState

log = structlog.get_logger("llmgw.sweeper")

_PICK = """
SELECT id FROM holds
 WHERE state = 'held' AND expires_at <= now() AND id <> ALL($1::bigint[])
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


async def run(pool: asyncpg.Pool, *, interval_s: float, batch: int) -> None:
    """Sweep forever. A lost connection ends the pass; the next one retries."""
    while True:
        try:
            n = await sweep_once(pool, batch=batch)
            if n:
                log.info("sweep_pass", expired=n)
        except Exception:
            log.exception("sweep_pass_failed")
        await asyncio.sleep(interval_s)
