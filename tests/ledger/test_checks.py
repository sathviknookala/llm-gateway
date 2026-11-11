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


async def test_detects_retry_loss_without_job_attempt(conn, tx, team):
    from llmgw.ledger import losses

    p = await tx(losses.post_retry_loss, team_id=team.team_id, job_id=9, attempt=1, amount=5, flagged=False)
    problems = await check_ledger(conn)
    assert any(f"retry_loss posting {p.id} has no matching job attempt" in p_ for p_ in problems)


async def test_detects_job_whose_hold_state_does_not_match(conn, tx, team):
    hold = await tx(holds.place_hold, team_id=team.team_id, amount=100, expires_at=future(), request_id="j")
    job_id = await conn.fetchval(
        "INSERT INTO jobs (team_id, hold_id, request_hash, model, request, prompt_estimate,"
        " max_tokens, max_attempts, deadline, state, result, finished_at)"
        " VALUES ($1, $2, '\\x00', 'm', '{}', 1, 1, 1, now(), 'succeeded', '{}', now()) RETURNING id",
        team.team_id, hold.id,
    )
    problems = await check_ledger(conn)
    assert f"job {job_id} is succeeded but its hold is held" in problems
