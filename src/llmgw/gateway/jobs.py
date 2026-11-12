"""Job submission and polling on the gateway.

POST /v1/jobs   a chat-completions body (no streaming). Places the job's hold
                and inserts the job in one transaction; 202 with the job.
                `Idempotency-Key` makes a resubmit return the stored job (200).
GET /v1/jobs/{id}  the job and, once it succeeded, its chat.completion result.

No inflight slot and no rate-limit acquire at enqueue: the worker acquires
before it calls the provider. Only the oversized check runs here, so a job
that could never fit a bucket is refused up front.
"""

from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse

from llmgw import auth, metrics
from llmgw.db import transaction
from llmgw.gateway.request import Refusal, parse_chat
from llmgw.jobs import queue
from llmgw.jobs.errors import JobConflict
from llmgw.ledger.errors import IdempotencyConflict, InsufficientCredits
from llmgw.pricing import cost

MAX_KEY_LENGTH = 255


def _error(status: int, message: str, code: str) -> JSONResponse:
    """Every job-route error is a refusal: nothing was enqueued."""
    metrics.REFUSALS.labels(code).inc()
    return JSONResponse({"error": {"message": message, "type": "llmgw", "code": code}}, status_code=status)


def job_json(job: queue.Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "object": "job",
        "state": str(job.state),
        "model": job.model,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "created_at": job.created_at.isoformat(),
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        "last_error": job.last_error,
        "result": job.result,
    }


async def _team(request: Request) -> int | None:
    gw = request.app.state.gateway
    key = request.headers.get("authorization", "").removeprefix("Bearer ").strip()
    return await auth.team_for_key(gw.pool, key) if key else None


async def submit(request: Request) -> JSONResponse:
    gw = request.app.state.gateway
    settings = gw.settings
    team_id = await _team(request)
    if team_id is None:
        return _error(401, "invalid API key", "invalid_api_key")
    parsed = parse_chat(await request.body(), settings, stream=False)
    if isinstance(parsed, Refusal):
        return _error(parsed.status, parsed.message, parsed.code)
    key = request.headers.get("idempotency-key")
    if key is not None and not 0 < len(key) <= MAX_KEY_LENGTH:
        return _error(400, f"`Idempotency-Key` must be 1-{MAX_KEY_LENGTH} characters", "invalid_request")
    if gw.limiter is not None and gw.limiter.oversized(team_id, parsed.model, parsed.reserved_tokens):
        return _error(
            400, f"request needs up to {parsed.reserved_tokens} tokens, more than a rate-limit bucket holds",
            "request_exceeds_rate_limit",
        )

    stored = {k: v for k, v in parsed.body.items() if k not in ("stream", "stream_options")}
    stored[parsed.cap_field] = parsed.max_tokens
    amount = max(1, cost(parsed.price, parsed.prompt_estimate, parsed.max_tokens))
    try:
        async with transaction(gw.pool) as conn:
            job, created = await queue.enqueue(
                conn,
                team_id=team_id,
                model=parsed.model,
                request=stored,
                prompt_estimate=parsed.prompt_estimate,
                max_tokens=parsed.max_tokens,
                amount=amount,
                idempotency_key=key,
                max_attempts=settings.job_max_attempts,
                ttl_s=settings.job_ttl_s,
                hold_grace_s=settings.job_hold_grace_s,
            )
    except InsufficientCredits:
        return _error(402, f"insufficient credits: this job holds {amount}", "insufficient_credits")
    except (JobConflict, IdempotencyConflict):
        return _error(409, "Idempotency-Key was used for a different request", "idempotency_conflict")
    return JSONResponse(job_json(job), status_code=202 if created else 200)


async def get(request: Request) -> JSONResponse:
    gw = request.app.state.gateway
    team_id = await _team(request)
    if team_id is None:
        return _error(401, "invalid API key", "invalid_api_key")
    job = await queue.get_job(gw.pool, request.path_params["job_id"], team_id)
    if job is None:
        return _error(404, "job not found", "job_not_found")
    return JSONResponse(job_json(job))
