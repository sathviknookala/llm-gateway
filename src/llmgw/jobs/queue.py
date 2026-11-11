"""Job queue operations. SQL only: no network calls, no sleeps.

Writers run inside the caller's transaction (`require_transaction`); `lease`
and `renew` are single statements and run on a pool or a bare connection.

Lock order, on every path: the job row first, then its hold row, then balance
rows (which the ledger locks in account_id order). Nothing locks a hold and
then its job.

Fencing: every lease bumps `lease_token`, which is never decremented. A worker
finishes its attempt with the token it leased; a finish whose token no longer
matches a running job is *fenced*: it books its provider cost to retry_loss
(when there is one) and leaves the job and its hold alone. `attempts` is the
retry budget, and numbers the attempt rows and retry-loss keys. An attempt
number is reused only after `requeue_unstarted`, which writes no attempt row
and no posting, so `(job_id, attempt)` keys stay unique.

A finish retried after it already committed (the ack was lost) finds its own
attempt row and returns the stored outcome without writing anything.
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import asyncpg
import orjson

from llmgw.db import require_transaction
from llmgw.jobs.errors import JobConflict, JobNotFound, LeaseLost
from llmgw.ledger import holds, losses
from llmgw.ledger.models import HoldState
from llmgw.ledger.postings import request_hash


class JobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD = "dead"


class Outcome(StrEnum):
    """What became of one attempt. The first five mirror the `attempt_outcome` enum."""

    SUCCEEDED = "succeeded"
    RETRYABLE = "retryable"
    PERMANENT = "permanent"
    LOST = "lost"
    FENCED = "fenced"
    RATE_LIMITED = "rate_limited"  # requeued before anything was sent; no attempt row


@dataclass(frozen=True)
class Job:
    id: int
    team_id: int
    hold_id: int
    idempotency_key: str | None
    model: str
    request: dict[str, Any]
    prompt_estimate: int
    max_tokens: int
    state: JobState
    attempts: int
    max_attempts: int
    lease_token: int
    run_after: datetime
    locked_until: datetime | None
    deadline: datetime
    last_error: str | None
    result: dict[str, Any] | None
    created_at: datetime
    finished_at: datetime | None
    request_hash: bytes


_COLUMNS = (
    "id, team_id, hold_id, idempotency_key, model, request, prompt_estimate, max_tokens,"
    " state, attempts, max_attempts, lease_token, run_after, locked_until, deadline,"
    " last_error, result, created_at, finished_at, request_hash"
)


def _job(row: asyncpg.Record) -> Job:
    d = dict(row)
    d["request"] = orjson.loads(d["request"])
    d["result"] = None if d["result"] is None else orjson.loads(d["result"])
    d["state"] = JobState(d["state"])
    d["request_hash"] = bytes(d["request_hash"])
    return Job(**d)


def backoff(attempt: int, retry_after_s: float | None, *, base_s: float, cap_s: float,
            rand: Callable[[], float]) -> float:
    """Seconds before retry number `attempt` (1-based): full jitter, at least retry-after."""
    ceiling = min(cap_s, base_s * 2 ** min(attempt - 1, 62))
    return max(retry_after_s or 0.0, rand() * ceiling)


async def get_job(conn: asyncpg.Connection | asyncpg.Pool, job_id: int,
                  team_id: int | None = None) -> Job | None:
    """The job, or None if it doesn't exist or (with team_id) belongs to another team."""
    row = await conn.fetchrow(f"SELECT {_COLUMNS} FROM jobs WHERE id = $1", job_id)
    if row is None or (team_id is not None and row["team_id"] != team_id):
        return None
    return _job(row)


async def _by_key(conn: asyncpg.Connection, team_id: int, key: str) -> Job | None:
    row = await conn.fetchrow(
        f"SELECT {_COLUMNS} FROM jobs WHERE team_id = $1 AND idempotency_key = $2", team_id, key
    )
    return None if row is None else _job(row)


def _same_request(job: Job, digest: bytes) -> Job:
    if job.request_hash != digest:
        raise JobConflict(f"idempotency key {job.idempotency_key!r} was used for a different request")
    return job


async def enqueue(
    conn: asyncpg.Connection,
    *,
    team_id: int,
    model: str,
    request: dict[str, Any],
    prompt_estimate: int,
    max_tokens: int,
    amount: int,
    idempotency_key: str | None,
    max_attempts: int,
    ttl_s: float,
    hold_grace_s: float,
) -> tuple[Job, bool]:
    """Place the job's hold and insert the job, in the caller's transaction.

    Returns (job, created). With an idempotency key, a replay of the same
    request returns the stored job; a different request raises JobConflict.
    InsufficientCredits (and, for a concurrent submit with the same key and a
    different amount, IdempotencyConflict) come from the hold and abort the
    transaction.
    """
    require_transaction(conn)
    digest = request_hash(request)
    if idempotency_key is not None:
        found = await _by_key(conn, team_id, idempotency_key)
        if found is not None:
            return _same_request(found, digest), False

    deadline = datetime.now(UTC) + timedelta(seconds=ttl_s)
    hold = await holds.place_hold(
        conn,
        team_id=team_id,
        amount=amount,
        expires_at=deadline + timedelta(seconds=hold_grace_s),
        request_id=f"job:{team_id}:{idempotency_key}" if idempotency_key else f"job:{uuid.uuid4().hex}",
    )
    row = await conn.fetchrow(
        f"""
        INSERT INTO jobs (team_id, hold_id, idempotency_key, request_hash, model, request,
                          prompt_estimate, max_tokens, max_attempts, deadline)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10)
        ON CONFLICT (team_id, idempotency_key) DO NOTHING
        RETURNING {_COLUMNS}
        """,
        team_id, hold.id, idempotency_key, digest, model, orjson.dumps(request).decode(),
        prompt_estimate, max_tokens, max_attempts, deadline,
    )
    if row is not None:
        return _job(row), True
    # A concurrent submit with the same key committed first. Our place_hold
    # waited on the posting's unique index and replayed its hold.
    found = await _by_key(conn, team_id, idempotency_key)  # type: ignore[arg-type]
    if found is None:
        raise RuntimeError(f"job with key {idempotency_key!r} conflicted but was not found")
    return _same_request(found, digest), False


async def lease(conn: asyncpg.Connection | asyncpg.Pool, *, worker: str, lease_s: float) -> Job | None:
    """Claim the next ready job (FIFO by run_after, id), skipping rows other workers hold."""
    row = await conn.fetchrow(
        f"""
        UPDATE jobs
           SET state = 'running', attempts = attempts + 1, lease_token = lease_token + 1,
               locked_until = now() + make_interval(secs => $2), worker = $1, updated_at = now()
         WHERE id = (SELECT id FROM jobs
                      WHERE state = 'queued' AND run_after <= now() AND deadline > now()
                      ORDER BY run_after, id
                      LIMIT 1
                        FOR UPDATE SKIP LOCKED)
        RETURNING {_COLUMNS}
        """,
        worker, lease_s,
    )
    return None if row is None else _job(row)


async def renew(conn: asyncpg.Connection | asyncpg.Pool, *, job_id: int, token: int, lease_s: float) -> None:
    """Extend the lease. LeaseLost if the token is stale.

    An expired lease that nobody reclaimed yet is still renewed: correctness
    rests on the token, not on the clock.
    """
    renewed = await conn.fetchval(
        """
        UPDATE jobs SET locked_until = now() + make_interval(secs => $3), updated_at = now()
         WHERE id = $1 AND lease_token = $2 AND state = 'running'
        RETURNING id
        """,
        job_id, token, lease_s,
    )
    if renewed is None:
        raise LeaseLost(f"job {job_id}: lease {token} is no longer held")


async def _lock(conn: asyncpg.Connection, job_id: int) -> Job:
    row = await conn.fetchrow(f"SELECT {_COLUMNS} FROM jobs WHERE id = $1 FOR UPDATE", job_id)
    if row is None:
        raise JobNotFound(f"job {job_id} not found")
    return _job(row)


async def _lock_attempt(conn: asyncpg.Connection, job_id: int, token: int, attempt: int) -> tuple[Job, bool]:
    """Lock the job row (always, first) and say whether this lease still owns it."""
    job = await _lock(conn, job_id)
    current = job.state == JobState.RUNNING and job.lease_token == token
    if current and job.attempts != attempt:
        raise RuntimeError(f"job {job_id}: lease {token} is attempt {job.attempts}, not {attempt}")
    return job, current


async def _insert_attempt(
    conn: asyncpg.Connection, job_id: int, attempt: int, outcome: Outcome, *,
    cost: int = 0, flagged: bool = False, posting_id: int | None = None, error: str | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO job_attempts (job_id, attempt, outcome, cost, flagged, retry_loss_posting_id, error)
        VALUES ($1, $2, $3::attempt_outcome, $4, $5, $6, $7)
        """,
        job_id, attempt, str(outcome), cost, flagged, posting_id, error,
    )


async def _loss(conn: asyncpg.Connection, job: Job, attempt: int, cost: int, flagged: bool) -> int | None:
    if cost <= 0:
        return None
    posting = await losses.post_retry_loss(
        conn, team_id=job.team_id, job_id=job.id, attempt=attempt, amount=cost, flagged=flagged
    )
    return posting.id


async def _record_fenced(
    conn: asyncpg.Connection, job: Job, attempt: int, cost: int, flagged: bool, error: str | None
) -> Outcome:
    """A finish whose lease is gone. The job row is already locked by the caller."""
    row = await conn.fetchrow(
        "SELECT outcome FROM job_attempts WHERE job_id = $1 AND attempt = $2 FOR UPDATE", job.id, attempt
    )
    if row is not None and row["outcome"] != Outcome.LOST:
        # This attempt's own earlier finish committed (its ack was lost): a replay.
        return Outcome(row["outcome"])
    posting_id = await _loss(conn, job, attempt, cost, flagged)
    if row is None:
        await _insert_attempt(conn, job.id, attempt, Outcome.FENCED, cost=cost, flagged=flagged,
                              posting_id=posting_id, error=error)
    else:
        await conn.execute(
            """
            UPDATE job_attempts
               SET outcome = 'fenced', cost = $3, flagged = $4, retry_loss_posting_id = $5,
                   error = $6, finished_at = now()
             WHERE job_id = $1 AND attempt = $2 AND outcome = 'lost'
            """,
            job.id, attempt, cost, flagged, posting_id, error,
        )
    return Outcome.FENCED


async def complete(
    conn: asyncpg.Connection,
    *,
    job_id: int,
    token: int,
    attempt: int,
    actual: int,
    flagged: bool,
    result: dict[str, Any],
) -> Outcome:
    """Settle the hold, store the result and mark the job succeeded, in one transaction.

    A fenced worker's actual cost goes to retry_loss instead. InvalidHoldState
    from the settle (the hold was closed by another path) aborts the transaction.
    """
    require_transaction(conn)
    job, current = await _lock_attempt(conn, job_id, token, attempt)
    if not current:
        return await _record_fenced(conn, job, attempt, actual, flagged, "lease_lost")
    settled = await holds.settle(conn, hold_id=job.hold_id, actual=actual, flagged=flagged)
    await _insert_attempt(conn, job_id, attempt, Outcome.SUCCEEDED, cost=settled.charged,
                          flagged=flagged or settled.uncovered > 0)
    await conn.execute(
        """
        UPDATE jobs SET state = 'succeeded', result = $2::jsonb, locked_until = NULL,
                        finished_at = now(), updated_at = now(), last_error = NULL
         WHERE id = $1
        """,
        job_id, orjson.dumps(result).decode(),
    )
    return Outcome.SUCCEEDED


async def _release_if_held(conn: asyncpg.Connection, hold_id: int) -> None:
    hold = await holds.get_hold(conn, hold_id)
    if hold.state == HoldState.HELD:
        await holds.release(conn, hold_id=hold_id)


async def _finish(conn: asyncpg.Connection, job_id: int, state: JobState, error: str) -> None:
    await conn.execute(
        """
        UPDATE jobs SET state = $2::job_state, locked_until = NULL, finished_at = now(),
                        updated_at = now(), last_error = $3
         WHERE id = $1
        """,
        job_id, str(state), error,
    )


async def _requeue(conn: asyncpg.Connection, job_id: int, delay_s: float, error: str | None) -> None:
    await conn.execute(
        """
        UPDATE jobs SET state = 'queued', locked_until = NULL, updated_at = now(),
                        run_after = now() + make_interval(secs => $2), last_error = $3
         WHERE id = $1
        """,
        job_id, delay_s, error,
    )


async def fail_attempt(
    conn: asyncpg.Connection,
    *,
    job_id: int,
    token: int,
    attempt: int,
    permanent: bool,
    cost: int,
    flagged: bool,
    error: str,
    retry_after_s: float | None,
    base_s: float,
    cap_s: float,
    rand: Callable[[], float],
) -> Outcome:
    """Record a failed attempt and decide what happens to the job.

    cost > 0 (the provider may have billed it) posts a retry loss. Permanent →
    failed; out of attempts → dead; both release the hold (if still held).
    Otherwise the job is queued again after a backoff.
    """
    if cost < 0 or (permanent and cost > 0):
        raise ValueError("cost must be >= 0, and 0 for a permanent failure")
    require_transaction(conn)
    job, current = await _lock_attempt(conn, job_id, token, attempt)
    if not current:
        return await _record_fenced(conn, job, attempt, cost, flagged, error)
    outcome = Outcome.PERMANENT if permanent else Outcome.RETRYABLE
    posting_id = await _loss(conn, job, attempt, cost, flagged)
    await _insert_attempt(conn, job_id, attempt, outcome, cost=cost, flagged=flagged,
                          posting_id=posting_id, error=error)
    if permanent or job.attempts >= job.max_attempts:
        await _release_if_held(conn, job.hold_id)
        await _finish(conn, job_id, JobState.FAILED if permanent else JobState.DEAD, error)
    else:
        delay = backoff(attempt, retry_after_s, base_s=base_s, cap_s=cap_s, rand=rand)
        await _requeue(conn, job_id, delay, error)
    return outcome


async def requeue_unstarted(
    conn: asyncpg.Connection, *, job_id: int, token: int, delay_s: float, error: str
) -> bool:
    """Put back a leased job that never reached the provider, returning its attempt.

    No attempt row, no posting. The token stays bumped, so a retry of this call
    after it committed is fenced and can't touch a newer lease. Returns False if fenced.
    """
    require_transaction(conn)
    job = await _lock(conn, job_id)
    if job.state != JobState.RUNNING or job.lease_token != token:
        return False
    await conn.execute("UPDATE jobs SET attempts = attempts - 1 WHERE id = $1", job_id)
    await _requeue(conn, job_id, delay_s, error)
    return True


async def reclaim(
    conn: asyncpg.Connection, job_id: int, *, base_s: float, cap_s: float, rand: Callable[[], float]
) -> JobState | None:
    """Sweeper: take back a running job whose lease expired. None if it no longer qualifies.

    The attempt is recorded as `lost` (its cost is unknown: reconciliation).
    Out of attempts or past the deadline → dead, hold released; else queued
    after a backoff. Counting it as an attempt keeps a job that kills its
    workers from cycling forever.
    """
    require_transaction(conn)
    job = await _lock(conn, job_id)
    expired = await conn.fetchval(
        "SELECT state = 'running' AND locked_until <= now() FROM jobs WHERE id = $1", job_id
    )
    if not expired:
        return None
    await _insert_attempt(conn, job_id, job.attempts, Outcome.LOST, error="lease_lost")
    past_deadline = await conn.fetchval("SELECT $1::timestamptz <= now()", job.deadline)
    if job.attempts >= job.max_attempts or past_deadline:
        await _release_if_held(conn, job.hold_id)
        await _finish(conn, job_id, JobState.DEAD, "lease_lost")
        return JobState.DEAD
    await _requeue(conn, job_id, backoff(job.attempts, None, base_s=base_s, cap_s=cap_s, rand=rand), "lease_lost")
    return JobState.QUEUED


async def expire_deadline(conn: asyncpg.Connection, job_id: int) -> bool:
    """Sweeper: dead-letter a queued job past its deadline and release its hold."""
    require_transaction(conn)
    await _lock(conn, job_id)
    due = await conn.fetchval(
        "SELECT state = 'queued' AND deadline <= now() FROM jobs WHERE id = $1", job_id
    )
    if not due:
        return False
    job = await _lock(conn, job_id)
    await _release_if_held(conn, job.hold_id)
    await _finish(conn, job_id, JobState.DEAD, "deadline")
    return True
