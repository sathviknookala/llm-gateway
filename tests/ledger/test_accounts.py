"""llmgw.ledger.accounts: orgs and teams with their accounts."""

import asyncpg
import pytest

from llmgw.ledger import accounts, postings


async def test_create_org_makes_its_accounts(conn, tx):
    org = await tx(accounts.create_org, name="acme")
    ids = {org.external, org.org_pool, org.provider_spend, org.retry_loss}
    assert len(ids) == 4
    for account_id in ids:
        assert await accounts.balance(conn, account_id) == 0
    assert await accounts.org_accounts(conn, org.org_id) == org


async def test_create_team_makes_its_accounts(conn, tx):
    org = await tx(accounts.create_org, name="acme")
    team = await tx(accounts.create_team, org_id=org.org_id, name="A", weight=3)
    assert team.org_id == org.org_id
    assert team.available != team.held
    assert await accounts.team_accounts(conn, team.team_id) == team
    assert await conn.fetchval("SELECT weight FROM teams WHERE id = $1", team.team_id) == 3


async def test_duplicate_team_name_rejected(tx, org):
    await tx(accounts.create_team, org_id=org.org_id, name="A")
    with pytest.raises(asyncpg.UniqueViolationError):
        await tx(accounts.create_team, org_id=org.org_id, name="A")


async def test_summary_of_new_team_is_zero(conn, tx, org):
    team = await tx(accounts.create_team, org_id=org.org_id, name="A")
    s = await accounts.team_summary(conn, team.team_id)
    assert (s.available, s.held, s.granted, s.spent) == (0, 0, 0, 0)


async def test_summary_counts_grants(conn, tx, team):
    await tx(postings.grant, team_id=team.team_id, amount=500, idempotency_key="grant-2")
    s = await accounts.team_summary(conn, team.team_id)
    assert (s.available, s.held, s.granted, s.spent) == (1_500, 0, 1_500, 0)


async def test_unknown_team_raises(conn):
    with pytest.raises(LookupError):
        await accounts.team_accounts(conn, 999)


async def test_create_requires_transaction(conn):
    with pytest.raises(RuntimeError, match="transaction"):
        await accounts.create_org(conn, "acme")
