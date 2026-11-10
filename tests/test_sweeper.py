"""llmgw.sweeper: expire overdue holds, skip locked and unexpired ones."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from llmgw import sweeper
from llmgw.ledger import accounts, holds, postings
from llmgw.ledger.checks import check_ledger
from llmgw.ledger.models import HoldState

GRANT = 100_000


@pytest.fixture
async def team(conn):
    async with conn.transaction():
        org = await accounts.create_org(conn, "org")
        await postings.fund_org(conn, org_id=org.org_id, amount=10 * GRANT, idempotency_key="fund")
        t = await accounts.create_team(conn, org.org_id, "A")
        await postings.grant(conn, team_id=t.team_id, amount=GRANT, idempotency_key="grant")
    return t


async def place(conn, team, request_id, *, minutes):
    async with conn.transaction():
        return await holds.place_hold(
            conn,
            team_id=team.team_id,
            amount=100,
            expires_at=datetime.now(UTC) + timedelta(minutes=minutes),
            request_id=request_id,
        )


async def state(conn, hold_id) -> HoldState:
    return (await holds.get_hold(conn, hold_id)).state


async def test_expires_overdue_and_leaves_unexpired(conn, pool, team):
    overdue = await place(conn, team, "old", minutes=-1)
    live = await place(conn, team, "new", minutes=10)

    assert await sweeper.sweep_once(pool) == 1

    assert await state(conn, overdue.id) == HoldState.EXPIRED
    assert await state(conn, live.id) == HoldState.HELD
    assert await accounts.balance(conn, team.available) == GRANT - 100
    assert await accounts.balance(conn, team.held) == 100
    assert await check_ledger(conn) == []


async def test_second_pass_is_a_no_op(conn, pool, team):
    await place(conn, team, "old", minutes=-1)
    assert await sweeper.sweep_once(pool) == 1
    assert await sweeper.sweep_once(pool) == 0


async def test_batch_limits_one_pass(conn, pool, team):
    for i in range(5):
        await place(conn, team, f"r{i}", minutes=-1)
    assert await sweeper.sweep_once(pool, batch=3) == 3
    assert await sweeper.sweep_once(pool, batch=3) == 2


async def test_concurrent_sweepers_expire_each_hold_once(conn, pool, team):
    ids = [(await place(conn, team, f"r{i}", minutes=-1)).id for i in range(40)]

    counts = await asyncio.gather(*(sweeper.sweep_once(pool) for _ in range(4)))

    assert sum(counts) == len(ids)
    expire_postings = await conn.fetchval("SELECT count(*) FROM postings WHERE kind = 'expire'")
    assert expire_postings == len(ids)
    assert await accounts.balance(conn, team.available) == GRANT
    assert await check_ledger(conn) == []


async def test_skips_a_hold_whose_settle_holds_the_lock(conn, pool, team):
    hold = await place(conn, team, "busy", minutes=-1)

    async with pool.acquire() as other:
        tr = other.transaction()
        await tr.start()
        # A settle in progress: the hold row is locked until it commits.
        await other.execute("SELECT 1 FROM holds WHERE id = $1 FOR UPDATE", hold.id)

        assert await sweeper.sweep_once(pool) == 0
        await holds.settle(other, hold_id=hold.id, actual=40)
        await tr.commit()

    assert await state(conn, hold.id) == HoldState.SETTLED
    assert await sweeper.sweep_once(pool) == 0
    assert await check_ledger(conn) == []


async def test_poisoned_hold_does_not_stall_the_rest(conn, pool, team, monkeypatch):
    first = await place(conn, team, "first", minutes=-2)
    second = await place(conn, team, "second", minutes=-1)
    real_expire = holds.expire

    async def flaky_expire(c, *, hold_id):
        if hold_id == first.id:
            raise RuntimeError("boom")
        return await real_expire(c, hold_id=hold_id)

    monkeypatch.setattr(holds, "expire", flaky_expire)

    assert await sweeper.sweep_once(pool) == 1
    assert await state(conn, first.id) == HoldState.HELD
    assert await state(conn, second.id) == HoldState.EXPIRED
