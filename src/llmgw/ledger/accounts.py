"""Creating orgs and teams together with their ledger accounts.

Balance rows are created by the `accounts_create_balance` trigger.
"""

import asyncpg

from llmgw.ledger.models import AccountKind


async def create_org(conn: asyncpg.Connection, name: str) -> int:
    """Insert an org and its accounts (ORG_ACCOUNT_KINDS). Returns the org id."""
    raise NotImplementedError


async def create_team(conn: asyncpg.Connection, org_id: int, name: str, weight: int = 1) -> int:
    """Insert a team and its accounts (TEAM_ACCOUNT_KINDS). Returns the team id."""
    raise NotImplementedError


async def account_id(
    conn: asyncpg.Connection, kind: AccountKind, *, org_id: int | None = None, team_id: int | None = None
) -> int:
    """Look up an account by owner and kind."""
    raise NotImplementedError


async def balance(conn: asyncpg.Connection, account_id: int) -> int:
    raise NotImplementedError
