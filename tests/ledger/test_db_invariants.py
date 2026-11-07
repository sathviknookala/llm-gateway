"""The database enforces the ledger invariants on its own, even against raw SQL.

These tests bypass llmgw.ledger on purpose: if application code has a bug,
the schema must still refuse to lose or invent credits.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

HASH = b"\x00"


@dataclass
class Accounts:
    org_id: int
    team_id: int
    external: int
    org_pool: int
    provider_spend: int
    team_available: int
    team_held: int


@pytest.fixture
async def accts(conn) -> Accounts:
    org_id = await conn.fetchval("INSERT INTO orgs (name) VALUES ('org') RETURNING id")
    team_id = await conn.fetchval(
        "INSERT INTO teams (org_id, name) VALUES ($1, 'A') RETURNING id", org_id
    )
    ids = {}
    for kind in ("external", "org_pool", "provider_spend", "retry_loss"):
        ids[kind] = await conn.fetchval(
            "INSERT INTO accounts (org_id, kind) VALUES ($1, $2) RETURNING id", org_id, kind
        )
    for kind in ("team_available", "team_held"):
        ids[kind] = await conn.fetchval(
            "INSERT INTO accounts (org_id, team_id, kind) VALUES ($1, $2, $3) RETURNING id",
            org_id, team_id, kind,
        )
    return Accounts(
        org_id=org_id,
        team_id=team_id,
        external=ids["external"],
        org_pool=ids["org_pool"],
        provider_spend=ids["provider_spend"],
        team_available=ids["team_available"],
        team_held=ids["team_held"],
    )


async def insert_posting(conn, org_id: int, kind: str, key: str, legs: list[tuple[int, int]]) -> int:
    pid = await conn.fetchval(
        "INSERT INTO postings (org_id, kind, idempotency_key, request_hash)"
        " VALUES ($1, $2, $3, $4) RETURNING id",
        org_id, kind, key, HASH,
    )
    for account_id, amount in sorted(legs):
        await conn.execute(
            "INSERT INTO entries (posting_id, account_id, amount) VALUES ($1, $2, $3)",
            pid, account_id, amount,
        )
    return pid


async def post(conn, *args) -> int:
    async with conn.transaction():
        return await insert_posting(conn, *args)


async def balance(conn, account_id: int) -> int:
    return await conn.fetchval("SELECT amount FROM balances WHERE account_id = $1", account_id)


async def test_every_account_gets_a_zero_balance_row(conn, accts):
    rows = await conn.fetch("SELECT amount, non_negative FROM balances ORDER BY account_id")
    assert len(rows) == 6
    assert all(r["amount"] == 0 for r in rows)
    external = await conn.fetchval(
        "SELECT non_negative FROM balances WHERE account_id = $1", accts.external
    )
    assert external is False


async def test_balanced_posting_commits_and_moves_balances(conn, accts):
    await post(conn, accts.org_id, "fund", "fund-1", [(accts.external, -10_000), (accts.org_pool, 10_000)])
    await post(conn, accts.org_id, "grant", "grant-1", [(accts.org_pool, -1_000), (accts.team_available, 1_000)])

    assert await balance(conn, accts.external) == -10_000
    assert await balance(conn, accts.org_pool) == 9_000
    assert await balance(conn, accts.team_available) == 1_000


async def test_unbalanced_posting_fails_at_commit_not_before(conn, accts):
    tx = conn.transaction()
    await tx.start()
    # The inserts succeed: the sum-zero check is deferred.
    await insert_posting(conn, accts.org_id, "fund", "bad", [(accts.external, -100), (accts.org_pool, 90)])
    with pytest.raises(asyncpg.CheckViolationError, match="unbalanced"):
        await tx.commit()

    assert await conn.fetchval("SELECT count(*) FROM postings") == 0
    assert await balance(conn, accts.org_pool) == 0


async def test_posting_with_no_entries_fails_at_commit(conn, accts):
    with pytest.raises(asyncpg.CheckViolationError, match="0 entries"):
        async with conn.transaction():
            await insert_posting(conn, accts.org_id, "fund", "empty", [])


async def test_zero_amount_entry_rejected(conn, accts):
    with pytest.raises(asyncpg.CheckViolationError):
        await post(conn, accts.org_id, "fund", "zero", [(accts.external, 0), (accts.org_pool, 0)])


async def test_overdraft_rejected_immediately(conn, accts):
    await post(conn, accts.org_id, "fund", "fund-1", [(accts.external, -100), (accts.org_pool, 100)])

    with pytest.raises(asyncpg.CheckViolationError) as exc:
        await post(conn, accts.org_id, "grant", "grant-big", [(accts.org_pool, -101), (accts.team_available, 101)])
    assert exc.value.constraint_name == "balance_non_negative"
    assert await balance(conn, accts.org_pool) == 100
    assert await balance(conn, accts.team_available) == 0


async def test_duplicate_idempotency_key_rejected(conn, accts):
    legs = [(accts.external, -100), (accts.org_pool, 100)]
    await post(conn, accts.org_id, "fund", "same-key", legs)
    with pytest.raises(asyncpg.UniqueViolationError):
        await post(conn, accts.org_id, "fund", "same-key", legs)
    assert await balance(conn, accts.org_pool) == 100


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE entries SET amount = amount * 2",
        "DELETE FROM entries",
        "UPDATE postings SET flagged = true",
        "DELETE FROM postings",
    ],
)
async def test_ledger_is_append_only(conn, accts, sql):
    await post(conn, accts.org_id, "fund", "fund-1", [(accts.external, -100), (accts.org_pool, 100)])
    with pytest.raises(asyncpg.RestrictViolationError, match="append-only"):
        await conn.execute(sql)


async def test_team_account_requires_team(conn, accts):
    with pytest.raises(asyncpg.CheckViolationError):
        await conn.execute(
            "INSERT INTO accounts (org_id, kind) VALUES ($1, 'team_available')", accts.org_id
        )


async def test_one_account_per_kind(conn, accts):
    with pytest.raises(asyncpg.UniqueViolationError):
        await conn.execute("INSERT INTO accounts (org_id, kind) VALUES ($1, 'org_pool')", accts.org_id)


async def _open_hold(conn, accts, amount: int = 120) -> tuple[int, int]:
    await post(conn, accts.org_id, "fund", "fund-1", [(accts.external, -1_000), (accts.org_pool, 1_000)])
    await post(conn, accts.org_id, "grant", "grant-1", [(accts.org_pool, -1_000), (accts.team_available, 1_000)])
    hold_pid = await post(
        conn, accts.org_id, "hold", "hold-1", [(accts.team_available, -amount), (accts.team_held, amount)]
    )
    hold_id = await conn.fetchval(
        "INSERT INTO holds (team_id, amount, hold_posting_id, expires_at)"
        " VALUES ($1, $2, $3, $4) RETURNING id",
        accts.team_id, amount, hold_pid, datetime.now(UTC) + timedelta(minutes=5),
    )
    return hold_id, hold_pid


async def test_hold_cannot_close_without_posting(conn, accts):
    hold_id, _ = await _open_hold(conn, accts)
    with pytest.raises(asyncpg.CheckViolationError):
        await conn.execute(
            "UPDATE holds SET state = 'released', closed_at = now() WHERE id = $1", hold_id
        )


async def test_settled_amount_cannot_exceed_hold(conn, accts):
    hold_id, _ = await _open_hold(conn, accts, amount=120)
    settle_pid = await post(
        conn, accts.org_id, "settle", "settle-1",
        [(accts.team_held, -120), (accts.provider_spend, 120)],
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await conn.execute(
            "UPDATE holds SET state = 'settled', settled_amount = 121, close_posting_id = $2,"
            " closed_at = now() WHERE id = $1",
            hold_id, settle_pid,
        )


async def test_worked_example_from_design_doc(conn, accts):
    """docs/design.md: fund 10,000, grant 1,000, hold 120, settle at 45."""
    await post(conn, accts.org_id, "fund", "fund-1", [(accts.external, -10_000), (accts.org_pool, 10_000)])
    await post(conn, accts.org_id, "grant", "grant-1", [(accts.org_pool, -1_000), (accts.team_available, 1_000)])
    await post(conn, accts.org_id, "hold", "hold-1", [(accts.team_available, -120), (accts.team_held, 120)])
    await post(
        conn, accts.org_id, "settle", "settle-1",
        [(accts.team_available, 75), (accts.team_held, -120), (accts.provider_spend, 45)],
    )

    assert await balance(conn, accts.org_pool) == 9_000
    assert await balance(conn, accts.team_available) == 955
    assert await balance(conn, accts.team_held) == 0
    assert await balance(conn, accts.provider_spend) == 45
    total = await conn.fetchval("SELECT sum(amount) FROM balances")
    assert total == 0
