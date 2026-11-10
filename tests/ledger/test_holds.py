"""llmgw.ledger.holds: place, settle, release, expire."""

import pytest

from llmgw.ledger import accounts, holds
from llmgw.ledger.errors import (
    HoldNotFound,
    IdempotencyConflict,
    InsufficientCredits,
    InvalidHoldState,
)
from llmgw.ledger.models import HoldState

from .conftest import POOL_FUNDING, TEAM_GRANT, future, past


async def place(tx, team, amount=120, request_id="r1", expires_at=None):
    return await tx(
        holds.place_hold,
        team_id=team.team_id,
        amount=amount,
        expires_at=expires_at or future(),
        request_id=request_id,
    )


async def balances(conn, org, team) -> dict[str, int]:
    return {
        "pool": await accounts.balance(conn, org.org_pool),
        "spend": await accounts.balance(conn, org.provider_spend),
        "available": await accounts.balance(conn, team.available),
        "held": await accounts.balance(conn, team.held),
    }


async def test_worked_example(conn, tx, org, team):
    """Worked example: pool funded 10,000, grant 1,000, hold 120, settle at 45."""
    hold = await place(tx, team, amount=120)
    assert await balances(conn, org, team) == {"pool": 9_000, "spend": 0, "available": 880, "held": 120}

    result = await tx(holds.settle, hold_id=hold.id, actual=45)

    assert await balances(conn, org, team) == {"pool": 9_000, "spend": 45, "available": 955, "held": 0}
    assert result.charged == 45 and result.uncovered == 0
    assert result.hold.state == HoldState.SETTLED and result.hold.settled_amount == 45
    assert result.posting.flagged is False


@pytest.mark.parametrize("actual", [0, 120])
async def test_settle_edges(conn, tx, org, team, actual):
    hold = await place(tx, team, amount=120)
    await tx(holds.settle, hold_id=hold.id, actual=actual)
    b = await balances(conn, org, team)
    assert b["spend"] == actual
    assert b["available"] == TEAM_GRANT - actual
    assert b["held"] == 0


async def test_overage_is_capped_at_hold_and_flagged(conn, tx, org, team):
    hold = await place(tx, team, amount=120)
    result = await tx(holds.settle, hold_id=hold.id, actual=150)

    assert result.charged == 120 and result.uncovered == 30
    assert result.posting.flagged is True
    assert result.hold.settled_amount == 120
    assert await balances(conn, org, team) == {"pool": 9_000, "spend": 120, "available": 880, "held": 0}


async def test_caller_can_flag_a_settle(tx, team):
    hold = await place(tx, team)
    result = await tx(holds.settle, hold_id=hold.id, actual=45, flagged=True)
    assert result.posting.flagged is True


async def test_place_hold_over_available_raises(conn, tx, org, team):
    with pytest.raises(InsufficientCredits):
        await place(tx, team, amount=TEAM_GRANT + 1)
    assert await conn.fetchval("SELECT count(*) FROM holds") == 0
    assert (await balances(conn, org, team))["available"] == TEAM_GRANT


@pytest.mark.parametrize("amount", [0, -1])
async def test_place_hold_nonpositive_amount(tx, team, amount):
    with pytest.raises(ValueError):
        await place(tx, team, amount=amount)


async def test_release_refunds_everything(conn, tx, org, team):
    hold = await place(tx, team)
    released = await tx(holds.release, hold_id=hold.id)
    assert released.state == HoldState.RELEASED
    assert await balances(conn, org, team) == {"pool": 9_000, "spend": 0, "available": TEAM_GRANT, "held": 0}


async def test_expire_before_due_rejected(conn, tx, org, team):
    hold = await place(tx, team, expires_at=future())
    with pytest.raises(InvalidHoldState, match="before"):
        await tx(holds.expire, hold_id=hold.id)
    assert (await holds.get_hold(conn, hold.id)).state == HoldState.HELD


async def test_expire_after_due_refunds(conn, tx, org, team):
    hold = await place(tx, team, expires_at=past())
    expired = await tx(holds.expire, hold_id=hold.id)
    assert expired.state == HoldState.EXPIRED
    assert (await balances(conn, org, team))["available"] == TEAM_GRANT


async def test_settle_past_expiry_before_sweeper_is_normal(conn, tx, org, team):
    hold = await place(tx, team, amount=120, expires_at=past())
    result = await tx(holds.settle, hold_id=hold.id, actual=45)
    assert result.hold.state == HoldState.SETTLED


async def test_settle_after_expire_rejected(conn, tx, org, team):
    """The late-charge case: the cost is left to reconciliation, not booked here."""
    hold = await place(tx, team, expires_at=past())
    await tx(holds.expire, hold_id=hold.id)
    with pytest.raises(InvalidHoldState):
        await tx(holds.settle, hold_id=hold.id, actual=45)
    assert (await balances(conn, org, team))["spend"] == 0


async def test_settle_after_release_rejected(tx, team):
    hold = await place(tx, team)
    await tx(holds.release, hold_id=hold.id)
    with pytest.raises(InvalidHoldState):
        await tx(holds.settle, hold_id=hold.id, actual=45)


async def test_release_after_settle_rejected(tx, team):
    hold = await place(tx, team)
    await tx(holds.settle, hold_id=hold.id, actual=45)
    with pytest.raises(InvalidHoldState):
        await tx(holds.release, hold_id=hold.id)


async def test_place_hold_replay_returns_same_hold(conn, tx, org, team):
    first = await place(tx, team, request_id="r1")
    again = await place(tx, team, request_id="r1", expires_at=future())  # recomputed expiry
    assert again.id == first.id
    assert await conn.fetchval("SELECT count(*) FROM holds") == 1
    assert (await balances(conn, org, team))["held"] == 120


async def test_place_hold_replay_with_different_amount_conflicts(tx, team):
    await place(tx, team, amount=120, request_id="r1")
    with pytest.raises(IdempotencyConflict):
        await place(tx, team, amount=121, request_id="r1")


async def test_settle_replay_writes_nothing(conn, tx, org, team):
    hold = await place(tx, team)
    first = await tx(holds.settle, hold_id=hold.id, actual=45)
    again = await tx(holds.settle, hold_id=hold.id, actual=45)
    assert again.posting == first.posting
    assert (again.charged, again.uncovered) == (45, 0)
    assert await conn.fetchval("SELECT count(*) FROM postings WHERE kind = 'settle'") == 1
    assert (await balances(conn, org, team))["spend"] == 45


async def test_settle_replay_with_different_actual_conflicts(tx, team):
    hold = await place(tx, team)
    await tx(holds.settle, hold_id=hold.id, actual=45)
    with pytest.raises(IdempotencyConflict):
        await tx(holds.settle, hold_id=hold.id, actual=46)


@pytest.mark.parametrize("op", ["release", "expire"])
async def test_refund_replay_writes_nothing(conn, tx, org, team, op):
    hold = await place(tx, team, expires_at=past())
    fn = getattr(holds, op)
    first = await tx(fn, hold_id=hold.id)
    again = await tx(fn, hold_id=hold.id)
    assert again == first
    assert await conn.fetchval("SELECT count(*) FROM postings WHERE kind = $1::posting_kind", op) == 1
    assert (await balances(conn, org, team))["available"] == TEAM_GRANT


async def test_unknown_hold(tx):
    with pytest.raises(HoldNotFound):
        await tx(holds.release, hold_id=999)


async def test_requires_transaction(conn, team):
    with pytest.raises(RuntimeError, match="transaction"):
        await holds.place_hold(conn, team_id=team.team_id, amount=1, expires_at=future(), request_id="x")


async def test_pool_untouched_by_hold_lifecycle(conn, tx, org, team):
    hold = await place(tx, team)
    await tx(holds.settle, hold_id=hold.id, actual=45)
    assert await accounts.balance(conn, org.org_pool) == POOL_FUNDING - TEAM_GRANT
