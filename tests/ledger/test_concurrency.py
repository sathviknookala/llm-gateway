"""Concurrent ledger operations: separate pool connections, asyncio.gather."""

import asyncio
import random

from llmgw.ledger import accounts, holds, postings
from llmgw.ledger.checks import check_ledger
from llmgw.ledger.errors import InsufficientCredits, InvalidHoldState
from llmgw.ledger.models import Hold, SettleResult

from .conftest import future, in_pool_tx, past


async def make_team(tx, org, name: str, grant: int):
    team = await tx(accounts.create_team, org_id=org.org_id, name=name)
    await tx(postings.grant, team_id=team.team_id, amount=grant, idempotency_key=f"grant-{name}")
    return team


async def test_two_holds_race_for_the_same_credits(conn, pool, tx, org):
    """docs/design.md: 100 available, two calls worth 80 arrive together."""
    team = await make_team(tx, org, "B", 100)
    results = await asyncio.gather(
        *(
            in_pool_tx(pool, holds.place_hold, team_id=team.team_id, amount=80,
                       expires_at=future(), request_id=f"r{i}")
            for i in range(2)
        ),
        return_exceptions=True,
    )
    assert sorted(type(r).__name__ for r in results) == ["Hold", "InsufficientCredits"]
    assert await accounts.balance(conn, team.held) == 80
    assert await accounts.balance(conn, team.available) == 20


async def test_settle_races_expire(conn, pool, tx, team):
    hold = await tx(holds.place_hold, team_id=team.team_id, amount=120, expires_at=past(), request_id="r1")
    results = await asyncio.gather(
        in_pool_tx(pool, holds.settle, hold_id=hold.id, actual=45),
        in_pool_tx(pool, holds.expire, hold_id=hold.id),
        return_exceptions=True,
    )
    winners = [r for r in results if isinstance(r, (Hold, SettleResult))]
    losers = [r for r in results if isinstance(r, InvalidHoldState)]
    assert len(winners) == 1 and len(losers) == 1, results
    closes = await conn.fetchval(
        "SELECT count(*) FROM postings WHERE kind IN ('settle', 'expire')"
    )
    assert closes == 1
    assert await check_ledger(conn) == []


async def test_identical_place_hold_requests_create_one_hold(conn, pool, team):
    results = await asyncio.gather(
        *(
            in_pool_tx(pool, holds.place_hold, team_id=team.team_id, amount=50,
                       expires_at=future(), request_id="same")
            for _ in range(4)
        )
    )
    assert len({h.id for h in results}) == 1
    assert await conn.fetchval("SELECT count(*) FROM holds") == 1
    assert await accounts.balance(conn, team.held) == 50


async def test_mixed_load_has_no_deadlocks(conn, pool, tx, org):
    rng = random.Random(7)
    teams = [await make_team(tx, org, f"T{i}", 1_000) for i in range(3)]

    placed = await asyncio.gather(
        *(
            in_pool_tx(pool, holds.place_hold, team_id=rng.choice(teams).team_id,
                       amount=rng.randint(1, 300), expires_at=past(), request_id=f"p{i}")
            for i in range(30)
        ),
        return_exceptions=True,
    )
    assert all(isinstance(r, (Hold, InsufficientCredits)) for r in placed), placed
    open_holds = [h for h in placed if isinstance(h, Hold)]

    ops = []
    for h in open_holds:
        choice = rng.choice(["settle", "release", "expire"])
        if choice == "settle":
            ops.append(in_pool_tx(pool, holds.settle, hold_id=h.id, actual=rng.randint(0, 2 * h.amount)))
        else:
            ops.append(in_pool_tx(pool, getattr(holds, choice), hold_id=h.id))
        # a competing close on the same hold, so closes really race
        ops.append(in_pool_tx(pool, holds.release, hold_id=h.id))
    for i in range(10):
        ops.append(in_pool_tx(pool, postings.grant, team_id=rng.choice(teams).team_id,
                              amount=rng.randint(1, 200), idempotency_key=f"g{i}"))
        ops.append(in_pool_tx(pool, holds.place_hold, team_id=rng.choice(teams).team_id,
                              amount=rng.randint(1, 200), expires_at=future(), request_id=f"q{i}"))
    rng.shuffle(ops)

    results = await asyncio.gather(*ops, return_exceptions=True)
    unexpected = [r for r in results if isinstance(r, Exception)
                  and not isinstance(r, (InvalidHoldState, InsufficientCredits))]
    assert unexpected == []
    assert await check_ledger(conn) == []
