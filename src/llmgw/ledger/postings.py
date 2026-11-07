"""Idempotent double-entry postings.

`post()` runs inside the caller's transaction:
- If `idempotency_key` exists with the same request hash, return that posting
  and write nothing.
- If it exists with a different hash, raise IdempotencyConflict.
- Otherwise insert the posting and its entries in account_id order (consistent
  row-lock order, so concurrent postings don't deadlock). The trigger moves the
  balances; a balance CHECK violation becomes InsufficientCredits.

The sum-zero check is deferred to COMMIT, so a bug that writes unbalanced
entries fails when the transaction ends, not here.
"""

from collections.abc import Sequence

import asyncpg

from llmgw.ledger.models import EntrySpec, Posting, PostingKind


def request_hash(kind: PostingKind, entries: Sequence[EntrySpec]) -> bytes:
    """Stable hash of what a posting asks for, used to detect key reuse."""
    raise NotImplementedError


async def post(
    conn: asyncpg.Connection,
    *,
    org_id: int,
    kind: PostingKind,
    idempotency_key: str,
    entries: Sequence[EntrySpec],
    flagged: bool = False,
) -> Posting:
    raise NotImplementedError


async def fund_org(conn: asyncpg.Connection, *, org_id: int, amount: int, idempotency_key: str) -> Posting:
    """external -> org_pool."""
    raise NotImplementedError


async def grant(
    conn: asyncpg.Connection, *, org_id: int, team_id: int, amount: int, idempotency_key: str
) -> Posting:
    """org_pool -> team_available."""
    raise NotImplementedError
