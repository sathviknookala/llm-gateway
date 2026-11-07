"""Ledger fixtures built through the llmgw.ledger API (test_db_invariants.py uses raw SQL instead)."""

import functools
from datetime import UTC, datetime, timedelta

import pytest

from llmgw.ledger import accounts, postings
from llmgw.ledger.models import OrgAccounts, TeamAccounts

POOL_FUNDING = 10_000
TEAM_GRANT = 1_000


def future() -> datetime:
    return datetime.now(UTC) + timedelta(hours=1)


def past() -> datetime:
    return datetime.now(UTC) - timedelta(minutes=1)


async def in_tx(conn, fn, /, **kwargs):
    """Run one ledger operation in its own transaction (ledger functions require one)."""
    async with conn.transaction():
        return await fn(conn, **kwargs)


async def in_pool_tx(pool, fn, /, **kwargs):
    async with pool.acquire() as c:
        async with c.transaction():
            return await fn(c, **kwargs)


@pytest.fixture
def tx(conn):
    """`await tx(fn, **kwargs)` runs fn(conn, **kwargs) in its own transaction."""
    return functools.partial(in_tx, conn)


@pytest.fixture
async def org(conn) -> OrgAccounts:
    """An org whose pool is funded with POOL_FUNDING."""
    async with conn.transaction():
        o = await accounts.create_org(conn, "org")
        await postings.fund_org(conn, org_id=o.org_id, amount=POOL_FUNDING, idempotency_key="fund-0")
    return o


@pytest.fixture
async def team(conn, org) -> TeamAccounts:
    """Team A with TEAM_GRANT available."""
    async with conn.transaction():
        t = await accounts.create_team(conn, org.org_id, "A")
        await postings.grant(conn, team_id=t.team_id, amount=TEAM_GRANT, idempotency_key="grant-A")
    return t
