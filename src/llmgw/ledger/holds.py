"""Hold lifecycle: place, then exactly one of settle, release or expire.

Idempotency keys are built here, not by callers:
- place_hold: `hold:{request_id}`
- closes: `settle:{hold_id}`, `release:{hold_id}`, `expire:{hold_id}`
A retry of the same close therefore reuses its key; a settle racing an expire
has a different key and loses on the state check.

Every close:
1. locks the hold row (`FOR UPDATE`), before any balance row;
2. checks its own key first: a stored posting with the same request is a
   replay and returns the stored result; a different request raises
   IdempotencyConflict;
3. requires state 'held', else InvalidHoldState (no posting is written);
4. writes the close posting, then sets state, close posting, closed_at and
   settled_amount in one guarded UPDATE (`WHERE state = 'held'`).

Settle charges min(actual, amount). A reported cost above the hold is not
charged: the posting is flagged, and reconciliation books the difference.
A settle on an expired hold is rejected; that cost is also left to
reconciliation.
"""

from datetime import datetime

import asyncpg

from llmgw.db import require_transaction
from llmgw.ledger.accounts import org_accounts, team_accounts
from llmgw.ledger.errors import HoldNotFound, InvalidHoldState
from llmgw.ledger.models import EntrySpec, Hold, HoldState, Posting, PostingKind, SettleResult
from llmgw.ledger.postings import check_replay, find_posting, post

_HOLD_COLUMNS = (
    "id, team_id, amount, state, hold_posting_id, close_posting_id, settled_amount, expires_at"
)


def _hold(row: asyncpg.Record) -> Hold:
    return Hold(
        id=row["id"],
        team_id=row["team_id"],
        amount=row["amount"],
        state=HoldState(row["state"]),
        hold_posting_id=row["hold_posting_id"],
        close_posting_id=row["close_posting_id"],
        settled_amount=row["settled_amount"],
        expires_at=row["expires_at"],
    )


def _legs(*legs: tuple[int, int]) -> list[EntrySpec]:
    """Entry specs with zero-amount legs left out (entries.amount can't be 0)."""
    return [EntrySpec(account_id, amount) for account_id, amount in legs if amount != 0]


async def get_hold(conn: asyncpg.Connection, hold_id: int) -> Hold:
    row = await conn.fetchrow(f"SELECT {_HOLD_COLUMNS} FROM holds WHERE id = $1", hold_id)
    if row is None:
        raise HoldNotFound(f"hold {hold_id} not found")
    return _hold(row)


async def place_hold(
    conn: asyncpg.Connection,
    *,
    team_id: int,
    amount: int,
    expires_at: datetime,
    request_id: str,
) -> Hold:
    """team_available -> team_held. InsufficientCredits if the team can't cover it.

    A replay (same request_id, team and amount) returns the existing hold.
    expires_at is not part of the request hash: a retry recomputes it.
    """
    if amount <= 0:
        raise ValueError("amount must be positive")
    require_transaction(conn)
    team = await team_accounts(conn, team_id)
    posting, created = await post(
        conn,
        org_id=team.org_id,
        kind=PostingKind.HOLD,
        idempotency_key=f"hold:{request_id}",
        request={"op": "hold", "team_id": team_id, "amount": amount},
        entries=_legs((team.available, -amount), (team.held, amount)),
    )
    if not created:
        row = await conn.fetchrow(
            f"SELECT {_HOLD_COLUMNS} FROM holds WHERE hold_posting_id = $1", posting.id
        )
    else:
        row = await conn.fetchrow(
            f"""
            INSERT INTO holds (team_id, amount, hold_posting_id, expires_at)
            VALUES ($1, $2, $3, $4)
            RETURNING {_HOLD_COLUMNS}
            """,
            team_id, amount, posting.id, expires_at,
        )
    return _hold(row)


async def _lock_hold(conn: asyncpg.Connection, hold_id: int) -> Hold:
    row = await conn.fetchrow(
        f"SELECT {_HOLD_COLUMNS} FROM holds WHERE id = $1 FOR UPDATE", hold_id
    )
    if row is None:
        raise HoldNotFound(f"hold {hold_id} not found")
    return _hold(row)


async def _replay(
    conn: asyncpg.Connection, key: str, request: dict[str, object]
) -> Posting | None:
    found = await find_posting(conn, key)
    return None if found is None else check_replay(found, request)


async def _close(
    conn: asyncpg.Connection,
    hold: Hold,
    *,
    kind: PostingKind,
    key: str,
    request: dict[str, object],
    state: HoldState,
    entries: list[EntrySpec],
    flagged: bool = False,
    settled_amount: int | None = None,
) -> tuple[Hold, Posting]:
    posting, _ = await post(
        conn,
        org_id=(await team_accounts(conn, hold.team_id)).org_id,
        kind=kind,
        idempotency_key=key,
        request=request,
        entries=entries,
        flagged=flagged,
    )
    row = await conn.fetchrow(
        f"""
        UPDATE holds
           SET state = $2::hold_state, close_posting_id = $3, closed_at = now(), settled_amount = $4
         WHERE id = $1 AND state = 'held'
        RETURNING {_HOLD_COLUMNS}
        """,
        hold.id, str(state), posting.id, settled_amount,
    )
    if row is None:
        raise InvalidHoldState(f"hold {hold.id} was closed concurrently")
    return _hold(row), posting


def _require_held(hold: Hold, op: str) -> None:
    if hold.state != HoldState.HELD:
        raise InvalidHoldState(f"can't {op} hold {hold.id}: it is {hold.state}")


async def settle(
    conn: asyncpg.Connection, *, hold_id: int, actual: int, flagged: bool = False
) -> SettleResult:
    """Charge min(actual, amount) and refund the rest.

    team_held -amount; provider_spend +charged; team_available +(amount - charged).
    actual > amount: charge the full hold, flag the posting, report `uncovered`.
    """
    if actual < 0:
        raise ValueError("actual must be non-negative")
    require_transaction(conn)
    hold = await _lock_hold(conn, hold_id)
    charged = min(actual, hold.amount)
    uncovered = actual - charged
    key = f"settle:{hold_id}"
    request = {"op": "settle", "hold_id": hold_id, "actual": actual, "flagged": flagged}

    replayed = await _replay(conn, key, request)
    if replayed is not None:
        return SettleResult(hold=hold, posting=replayed, charged=charged, uncovered=uncovered)
    _require_held(hold, "settle")

    team = await team_accounts(conn, hold.team_id)
    org = await org_accounts(conn, team.org_id)
    closed, posting = await _close(
        conn,
        hold,
        kind=PostingKind.SETTLE,
        key=key,
        request=request,
        state=HoldState.SETTLED,
        entries=_legs(
            (team.held, -hold.amount),
            (org.provider_spend, charged),
            (team.available, hold.amount - charged),
        ),
        flagged=flagged or uncovered > 0,
        settled_amount=charged,
    )
    return SettleResult(hold=closed, posting=posting, charged=charged, uncovered=uncovered)


async def _refund(
    conn: asyncpg.Connection, hold_id: int, *, op: str, kind: PostingKind, state: HoldState
) -> Hold:
    require_transaction(conn)
    hold = await _lock_hold(conn, hold_id)
    key = f"{op}:{hold_id}"
    request = {"op": op, "hold_id": hold_id}
    if await _replay(conn, key, request) is not None:
        return hold
    _require_held(hold, op)
    if state == HoldState.EXPIRED:
        due = await conn.fetchval("SELECT $1::timestamptz <= now()", hold.expires_at)
        if not due:
            raise InvalidHoldState(f"can't expire hold {hold_id} before {hold.expires_at}")
    team = await team_accounts(conn, hold.team_id)
    closed, _ = await _close(
        conn,
        hold,
        kind=kind,
        key=key,
        request=request,
        state=state,
        entries=_legs((team.held, -hold.amount), (team.available, hold.amount)),
    )
    return closed


async def release(conn: asyncpg.Connection, *, hold_id: int) -> Hold:
    """team_held -> team_available, full amount. Used when the call never reached the provider."""
    return await _refund(conn, hold_id, op="release", kind=PostingKind.RELEASE, state=HoldState.RELEASED)


async def expire(conn: asyncpg.Connection, *, hold_id: int) -> Hold:
    """Sweeper path for holds past expires_at: same entries as release, state 'expired'."""
    return await _refund(conn, hold_id, op="expire", kind=PostingKind.EXPIRE, state=HoldState.EXPIRED)
