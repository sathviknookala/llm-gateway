"""Job queue fixtures: an org and team built through the ledger API, and helpers
that run one queue operation in its own transaction."""

import pytest

from llmgw.jobs import queue
from llmgw.ledger import accounts, postings
from llmgw.ledger.models import TeamAccounts

GRANT = 100_000
AMOUNT = 500  # micro-credits each test job holds

# Backoff off unless a test is about backoff: retries are due at once.
NO_BACKOFF = dict(base_s=0.0, cap_s=0.0, rand=lambda: 0.0)


@pytest.fixture
async def team(conn) -> TeamAccounts:
    async with conn.transaction():
        org = await accounts.create_org(conn, "org")
        await postings.fund_org(conn, org_id=org.org_id, amount=10 * GRANT, idempotency_key="fund")
        t = await accounts.create_team(conn, org.org_id, "A")
        await postings.grant(conn, team_id=t.team_id, amount=GRANT, idempotency_key="grant")
    return t


def body(text: str = "hi", **extra) -> dict:
    return {"model": "mock-small", "messages": [{"role": "user", "content": text}], "max_tokens": 50, **extra}


async def enqueue(c, team, *, key=None, request=None, amount=AMOUNT, max_attempts=3, ttl_s=3600.0):
    async with c.transaction():
        return await queue.enqueue(
            c, team_id=team.team_id, model="mock-small", request=request or body(),
            prompt_estimate=10, max_tokens=50, amount=amount, idempotency_key=key,
            max_attempts=max_attempts, ttl_s=ttl_s, hold_grace_s=60.0,
        )


async def in_tx(c, fn, /, **kwargs):
    async with c.transaction():
        return await fn(c, **kwargs)


async def fail(c, job, *, permanent=False, cost=0, flagged=False, error="upstream_503",
               retry_after_s=None, token=None, attempt=None, **backoff):
    return await in_tx(
        c, queue.fail_attempt, job_id=job.id, token=job.lease_token if token is None else token,
        attempt=job.attempts if attempt is None else attempt, permanent=permanent, cost=cost,
        flagged=flagged, error=error, retry_after_s=retry_after_s, **(backoff or NO_BACKOFF),
    )


async def complete(c, job, *, actual=200, flagged=False, token=None, attempt=None):
    return await in_tx(
        c, queue.complete, job_id=job.id, token=job.lease_token if token is None else token,
        attempt=job.attempts if attempt is None else attempt, actual=actual, flagged=flagged,
        result={"ok": True},
    )


async def expire_lease(c, job_id):
    """Push the lease into the past (jobs isn't append-only; tests may do this)."""
    await c.execute("UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id = $1", job_id)


async def make_due(c, job_id):
    await c.execute("UPDATE jobs SET run_after = now() WHERE id = $1", job_id)
