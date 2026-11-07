"""Creating orgs and teams together with their ledger accounts.

Balance rows are created by the `accounts_create_balance` trigger.
"""

import asyncpg

from llmgw.ledger.models import (
    ORG_ACCOUNT_KINDS,
    TEAM_ACCOUNT_KINDS,
    OrgAccounts,
    PostingKind,
    TeamAccounts,
    TeamSummary,
)
from llmgw.db import require_transaction


async def create_org(conn: asyncpg.Connection, name: str) -> OrgAccounts:
    """Insert an org and its accounts (ORG_ACCOUNT_KINDS)."""
    require_transaction(conn)
    org_id = await conn.fetchval("INSERT INTO orgs (name) VALUES ($1) RETURNING id", name)
    await conn.execute(
        "INSERT INTO accounts (org_id, kind) SELECT $1, k::account_kind FROM unnest($2::text[]) AS k",
        org_id,
        [str(k) for k in ORG_ACCOUNT_KINDS],
    )
    return await org_accounts(conn, org_id)


async def create_team(
    conn: asyncpg.Connection, org_id: int, name: str, weight: int = 1
) -> TeamAccounts:
    """Insert a team and its accounts (TEAM_ACCOUNT_KINDS)."""
    require_transaction(conn)
    team_id = await conn.fetchval(
        "INSERT INTO teams (org_id, name, weight) VALUES ($1, $2, $3) RETURNING id",
        org_id, name, weight,
    )
    await conn.execute(
        "INSERT INTO accounts (org_id, team_id, kind)"
        " SELECT $1, $2, k::account_kind FROM unnest($3::text[]) AS k",
        org_id, team_id, [str(k) for k in TEAM_ACCOUNT_KINDS],
    )
    return await team_accounts(conn, team_id)


async def org_accounts(conn: asyncpg.Connection, org_id: int) -> OrgAccounts:
    row = await conn.fetchrow(
        """
        SELECT max(id) FILTER (WHERE kind = 'external')       AS external,
               max(id) FILTER (WHERE kind = 'org_pool')       AS org_pool,
               max(id) FILTER (WHERE kind = 'provider_spend') AS provider_spend,
               max(id) FILTER (WHERE kind = 'retry_loss')     AS retry_loss
        FROM accounts WHERE org_id = $1 AND team_id IS NULL
        """,
        org_id,
    )
    if row is None or row["org_pool"] is None:
        raise LookupError(f"org {org_id} has no accounts")
    return OrgAccounts(org_id=org_id, **dict(row))


async def team_accounts(conn: asyncpg.Connection, team_id: int) -> TeamAccounts:
    row = await conn.fetchrow(
        """
        SELECT max(org_id) AS org_id,
               max(id) FILTER (WHERE kind = 'team_available') AS available,
               max(id) FILTER (WHERE kind = 'team_held')      AS held
        FROM accounts WHERE team_id = $1
        """,
        team_id,
    )
    if row is None or row["available"] is None:
        raise LookupError(f"team {team_id} has no accounts")
    return TeamAccounts(team_id=team_id, **dict(row))


async def balance(conn: asyncpg.Connection, account_id: int) -> int:
    amount = await conn.fetchval("SELECT amount FROM balances WHERE account_id = $1", account_id)
    if amount is None:
        raise LookupError(f"account {account_id} not found")
    return amount


async def team_summary(conn: asyncpg.Connection, team_id: int) -> TeamSummary:
    """Available, held, total granted, and spent (granted - available - held)."""
    team = await team_accounts(conn, team_id)
    row = await conn.fetchrow(
        """
        SELECT (SELECT amount FROM balances WHERE account_id = $1) AS available,
               (SELECT amount FROM balances WHERE account_id = $2) AS held,
               (SELECT coalesce(sum(e.amount), 0)
                  FROM entries e JOIN postings p ON p.id = e.posting_id
                 WHERE e.account_id = $1 AND p.kind = $3::posting_kind) AS granted
        """,
        team.available, team.held, str(PostingKind.GRANT),
    )
    available, held, granted = row["available"], row["held"], int(row["granted"])
    return TeamSummary(
        available=available, held=held, granted=granted, spent=granted - available - held
    )
