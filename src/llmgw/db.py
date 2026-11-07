"""asyncpg pool and transaction helper.

Rule: nothing network-shaped (provider, Redis, retries) inside `transaction()`.
The window between `SELECT ... FOR UPDATE` and `COMMIT` must stay short, and
every await in it holds a pool connection.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg

from llmgw.config import Settings


async def create_pool(settings: Settings) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        settings.database_url,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
    )


def require_transaction(conn: asyncpg.Connection) -> None:
    """Ledger writes are multi-statement; outside a transaction they'd commit piecemeal."""
    if not conn.is_in_transaction():
        raise RuntimeError("ledger operations must run inside the caller's transaction")


@asynccontextmanager
async def transaction(pool: asyncpg.Pool) -> AsyncIterator[asyncpg.Connection]:
    """Acquire a connection and run the block in one transaction.

    The deferred sum-zero trigger runs at COMMIT, so an unbalanced posting
    surfaces as an exception when this block exits.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            yield conn
