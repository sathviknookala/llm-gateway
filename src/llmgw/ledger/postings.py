"""Idempotent double-entry postings.

Every public function runs inside the caller's transaction and refuses to run
without one (`require_transaction`). An error raised here leaves that
transaction aborted; the caller rolls back.

`post()`:
- Inserts the posting with `ON CONFLICT (idempotency_key) DO NOTHING`. A
  concurrent insert of the same key waits on the unique index until the first
  transaction finishes, so no extra locking is needed.
- If the key already exists: same request hash -> return that posting and
  write nothing; different hash -> IdempotencyConflict.
- Otherwise inserts the entries in account_id order (consistent row-lock order,
  so concurrent postings don't deadlock). The balance trigger moves balances;
  a `balance_non_negative` violation becomes InsufficientCredits.

The request hash covers the operation's inputs, not its entries: the inputs
are what a retry repeats. The sum-zero check is deferred to COMMIT, so a bug
that writes unbalanced entries fails when the caller's transaction ends.
"""

import hashlib
import json
from collections.abc import Mapping, Sequence

import asyncpg

from llmgw.db import require_transaction
from llmgw.ledger.accounts import org_accounts, team_accounts
from llmgw.ledger.errors import IdempotencyConflict, InsufficientCredits
from llmgw.ledger.models import EntrySpec, Posting, PostingKind

_POSTING_COLUMNS = "id, org_id, kind, idempotency_key, flagged, created_at, request_hash"


def request_hash(request: Mapping[str, object]) -> bytes:
    """sha256 of the request's canonical JSON (sorted keys, no whitespace)."""
    canonical = json.dumps(request, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).digest()


def _posting(row: asyncpg.Record) -> Posting:
    return Posting(
        id=row["id"],
        org_id=row["org_id"],
        kind=PostingKind(row["kind"]),
        idempotency_key=row["idempotency_key"],
        flagged=row["flagged"],
        created_at=row["created_at"],
    )


async def find_posting(
    conn: asyncpg.Connection, idempotency_key: str
) -> tuple[Posting, bytes] | None:
    """The posting stored under a key, with its request hash."""
    row = await conn.fetchrow(
        f"SELECT {_POSTING_COLUMNS} FROM postings WHERE idempotency_key = $1", idempotency_key
    )
    return None if row is None else (_posting(row), bytes(row["request_hash"]))


def check_replay(found: tuple[Posting, bytes], request: Mapping[str, object]) -> Posting:
    """Return the stored posting if `request` matches it, else raise IdempotencyConflict."""
    posting, stored_hash = found
    if stored_hash != request_hash(request):
        raise IdempotencyConflict(
            f"idempotency key {posting.idempotency_key!r} was used for a different request"
        )
    return posting


async def post(
    conn: asyncpg.Connection,
    *,
    org_id: int,
    kind: PostingKind,
    idempotency_key: str,
    request: Mapping[str, object],
    entries: Sequence[EntrySpec],
    flagged: bool = False,
) -> tuple[Posting, bool]:
    """Write a posting, or return the existing one for a replay. The bool is True if written."""
    require_transaction(conn)
    row = await conn.fetchrow(
        f"""
        INSERT INTO postings (org_id, kind, idempotency_key, request_hash, flagged)
        VALUES ($1, $2::posting_kind, $3, $4, $5)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING {_POSTING_COLUMNS}
        """,
        org_id, str(kind), idempotency_key, request_hash(request), flagged,
    )
    if row is None:
        found = await find_posting(conn, idempotency_key)
        if found is None:
            raise RuntimeError(f"idempotency key {idempotency_key!r} conflicted but no posting found")
        return check_replay(found, request), False

    posting = _posting(row)
    try:
        await conn.executemany(
            "INSERT INTO entries (posting_id, account_id, amount) VALUES ($1, $2, $3)",
            [(posting.id, e.account_id, e.amount) for e in sorted(entries, key=lambda e: e.account_id)],
        )
    except asyncpg.CheckViolationError as exc:
        if exc.constraint_name == "balance_non_negative":
            raise InsufficientCredits(f"{kind} {idempotency_key!r}: balance would go negative") from exc
        raise
    return posting, True


async def fund_org(
    conn: asyncpg.Connection, *, org_id: int, amount: int, idempotency_key: str
) -> Posting:
    """external -> org_pool."""
    if amount <= 0:
        raise ValueError("amount must be positive")
    require_transaction(conn)
    org = await org_accounts(conn, org_id)
    posting, _ = await post(
        conn,
        org_id=org_id,
        kind=PostingKind.FUND,
        idempotency_key=idempotency_key,
        request={"op": "fund", "org_id": org_id, "amount": amount},
        entries=[EntrySpec(org.external, -amount), EntrySpec(org.org_pool, amount)],
    )
    return posting


async def grant(
    conn: asyncpg.Connection, *, team_id: int, amount: int, idempotency_key: str
) -> Posting:
    """org_pool -> team_available. InsufficientCredits if the pool can't cover it."""
    if amount <= 0:
        raise ValueError("amount must be positive")
    require_transaction(conn)
    team = await team_accounts(conn, team_id)
    org = await org_accounts(conn, team.org_id)
    posting, _ = await post(
        conn,
        org_id=team.org_id,
        kind=PostingKind.GRANT,
        idempotency_key=idempotency_key,
        request={"op": "grant", "team_id": team_id, "amount": amount},
        entries=[EntrySpec(org.org_pool, -amount), EntrySpec(team.available, amount)],
    )
    return posting
