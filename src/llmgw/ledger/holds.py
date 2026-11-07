"""Hold lifecycle: place, then exactly one of settle, release or expire.

Every close is guarded by `UPDATE holds ... WHERE id = $1 AND state = 'held'`.
Zero rows updated means another path (a retry, the sweeper) closed it first;
the loser writes no posting and raises InvalidHoldState, unless it is an
idempotent replay of the same close, which returns the existing result.
"""

from datetime import datetime

import asyncpg

from llmgw.ledger.models import Hold


async def place_hold(
    conn: asyncpg.Connection,
    *,
    team_id: int,
    amount: int,
    expires_at: datetime,
    idempotency_key: str,
) -> Hold:
    """team_available -> team_held. Raises InsufficientCredits if the team can't cover it."""
    raise NotImplementedError


async def settle(
    conn: asyncpg.Connection,
    *,
    hold_id: int,
    actual: int,
    idempotency_key: str,
    flagged: bool = False,
) -> Hold:
    """team_held -amount; provider_spend +actual; team_available +(amount - actual)."""
    raise NotImplementedError


async def release(conn: asyncpg.Connection, *, hold_id: int, idempotency_key: str) -> Hold:
    """team_held -> team_available, full amount. Used when the call never reached the provider."""
    raise NotImplementedError


async def expire(conn: asyncpg.Connection, *, hold_id: int, idempotency_key: str) -> Hold:
    """Sweeper path for holds past expires_at: same entries as release, state 'expired'."""
    raise NotImplementedError
