"""llmgw.ledger.postings: fund, grant, idempotent replay."""

import asyncpg
import pytest

from llmgw.ledger import accounts, postings
from llmgw.ledger.errors import IdempotencyConflict, InsufficientCredits

from .conftest import POOL_FUNDING, TEAM_GRANT


async def count(conn, table: str) -> int:
    return await conn.fetchval(f"SELECT count(*) FROM {table}")


async def test_fund_moves_external_to_pool(conn, org):
    assert await accounts.balance(conn, org.org_pool) == POOL_FUNDING
    assert await accounts.balance(conn, org.external) == -POOL_FUNDING


async def test_grant_moves_pool_to_team(conn, org, team):
    assert await accounts.balance(conn, org.org_pool) == POOL_FUNDING - TEAM_GRANT
    assert await accounts.balance(conn, team.available) == TEAM_GRANT


async def test_replay_returns_same_posting_and_writes_nothing(conn, tx, team):
    first = await tx(postings.grant, team_id=team.team_id, amount=100, idempotency_key="g")
    postings_before, entries_before = await count(conn, "postings"), await count(conn, "entries")

    again = await tx(postings.grant, team_id=team.team_id, amount=100, idempotency_key="g")

    assert again == first
    assert await count(conn, "postings") == postings_before
    assert await count(conn, "entries") == entries_before
    assert await accounts.balance(conn, team.available) == TEAM_GRANT + 100


async def test_same_key_different_request_conflicts(conn, tx, team):
    await tx(postings.grant, team_id=team.team_id, amount=100, idempotency_key="g")
    with pytest.raises(IdempotencyConflict):
        await tx(postings.grant, team_id=team.team_id, amount=101, idempotency_key="g")
    assert await accounts.balance(conn, team.available) == TEAM_GRANT + 100


async def test_key_is_global_across_operation_kinds(tx, org, team):
    with pytest.raises(IdempotencyConflict):
        await tx(postings.fund_org, org_id=org.org_id, amount=TEAM_GRANT, idempotency_key="grant-A")


async def test_grant_over_pool_raises_insufficient_credits(conn, tx, org, team):
    with pytest.raises(InsufficientCredits):
        await tx(postings.grant, team_id=team.team_id, amount=POOL_FUNDING, idempotency_key="big")
    assert await accounts.balance(conn, org.org_pool) == POOL_FUNDING - TEAM_GRANT
    assert await conn.fetchval("SELECT count(*) FROM postings WHERE idempotency_key = 'big'") == 0


async def test_insufficient_credits_aborts_callers_transaction(conn, team):
    """The documented contract: after a ledger error the caller must roll back."""
    tr = conn.transaction()
    await tr.start()
    with pytest.raises(InsufficientCredits):
        await postings.grant(conn, team_id=team.team_id, amount=10**9, idempotency_key="big")
    with pytest.raises(asyncpg.InFailedSQLTransactionError):
        await conn.fetchval("SELECT 1")
    await tr.rollback()
    assert await conn.fetchval("SELECT 1") == 1


async def test_requires_transaction(conn, org):
    with pytest.raises(RuntimeError, match="transaction"):
        await postings.fund_org(conn, org_id=org.org_id, amount=1, idempotency_key="x")


@pytest.mark.parametrize("amount", [0, -5])
async def test_nonpositive_amount_rejected(tx, org, amount):
    with pytest.raises(ValueError):
        await tx(postings.fund_org, org_id=org.org_id, amount=amount, idempotency_key="x")


def test_request_hash_ignores_key_order():
    a = postings.request_hash({"op": "grant", "team_id": 1, "amount": 5})
    b = postings.request_hash({"amount": 5, "team_id": 1, "op": "grant"})
    assert a == b
    assert a != postings.request_hash({"op": "grant", "team_id": 1, "amount": 6})
