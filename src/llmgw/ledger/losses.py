"""Retry-loss postings: provider cost of a job attempt the team doesn't pay for.

The team pays only for the attempt that succeeds. An attempt that failed after
the request reached the provider (a cut stream, a timeout, a lease lost
mid-call) may still be billed; its cost goes to the org's `retry_loss` account
so it shows on the books instead of disappearing.

The counterparty is the org's `external` account, not its pool: the posting
must never fail with InsufficientCredits, because the provider bills the org
whether or not it has ungranted credits left. Org spend to reconcile against
the provider's bill is provider_spend + retry_loss.

Key: `retry_loss:{job_id}:{attempt}`. A replay with the same inputs returns
the stored posting.
"""

import asyncpg

from llmgw.db import require_transaction
from llmgw.ledger.accounts import org_accounts, team_accounts
from llmgw.ledger.models import EntrySpec, Posting, PostingKind
from llmgw.ledger.postings import post


async def post_retry_loss(
    conn: asyncpg.Connection,
    *,
    team_id: int,
    job_id: int,
    attempt: int,
    amount: int,
    flagged: bool,
) -> Posting:
    """external -> retry_loss for one failed attempt of a job."""
    if amount <= 0:
        raise ValueError("amount must be positive")
    require_transaction(conn)
    team = await team_accounts(conn, team_id)
    org = await org_accounts(conn, team.org_id)
    posting, _ = await post(
        conn,
        org_id=team.org_id,
        kind=PostingKind.RETRY_LOSS,
        idempotency_key=f"retry_loss:{job_id}:{attempt}",
        request={
            "op": "retry_loss", "job_id": job_id, "attempt": attempt,
            "amount": amount, "flagged": flagged,
        },
        entries=[EntrySpec(org.external, -amount), EntrySpec(org.retry_loss, amount)],
        flagged=flagged,
    )
    return posting
