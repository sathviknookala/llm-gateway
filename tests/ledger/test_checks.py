"""llmgw.ledger.checks.check_ledger."""

from llmgw.ledger import holds
from llmgw.ledger.checks import check_ledger

from .conftest import future


async def test_clean_ledger_has_no_problems(conn, tx, team):
    hold = await tx(holds.place_hold, team_id=team.team_id, amount=100, expires_at=future(), request_id="r1")
    await tx(holds.place_hold, team_id=team.team_id, amount=50, expires_at=future(), request_id="r2")
    await tx(holds.settle, hold_id=hold.id, actual=30)
    assert await check_ledger(conn) == []


async def test_detects_corrupted_balance(conn, team):
    # balances is the one ledger table that isn't append-only; corrupt it directly.
    await conn.execute("UPDATE balances SET amount = amount + 7 WHERE account_id = $1", team.available)
    problems = await check_ledger(conn)
    assert any(f"account {team.available}" in p for p in problems)
    assert any("balances sum to 7" in p for p in problems)


async def test_detects_held_balance_without_open_hold(conn, tx, team):
    hold = await tx(holds.place_hold, team_id=team.team_id, amount=100, expires_at=future(), request_id="r1")
    # A hold marked closed without a refund posting (bypassing the API).
    await conn.execute(
        "UPDATE holds SET state = 'released', close_posting_id = hold_posting_id, closed_at = now()"
        " WHERE id = $1",
        hold.id,
    )
    problems = await check_ledger(conn)
    assert any(f"team {team.team_id}: held balance 100 != open holds 0" in p for p in problems)
