"""POST /v1/jobs and GET /v1/jobs/{id}: enqueue writes job + hold together, or nothing."""

from llmgw import auth
from llmgw.ledger import accounts
from llmgw.ledger.checks import check_ledger
from llmgw.pricing import DEFAULT_PRICES, cost, estimate_prompt_tokens

from .conftest import TEAM_GRANT

MESSAGES = [{"role": "user", "content": "classify this"}]
PRICE = DEFAULT_PRICES["mock-small"]


def body(**extra) -> dict:
    return {"model": "mock-small", "messages": MESSAGES, "max_tokens": 40, **extra}


async def counts(conn):
    return tuple(await conn.fetchrow("SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM holds)"))


async def test_submit_writes_a_queued_job_and_its_hold(env, conn):
    r = await env.client.post("/v1/jobs", json=body(mock={"tokens": 3}))
    assert r.status_code == 202
    job = r.json()
    assert job["object"] == "job" and job["state"] == "queued" and job["attempts"] == 0
    assert job["result"] is None

    row = await conn.fetchrow("SELECT j.request, h.amount, h.state FROM jobs j JOIN holds h ON h.id = j.hold_id")
    expected = cost(PRICE, estimate_prompt_tokens(MESSAGES), 40)
    assert row["amount"] == expected and row["state"] == "held"
    assert await accounts.balance(conn, env.team.available) == TEAM_GRANT - expected
    assert '"mock"' in row["request"] and '"stream"' not in row["request"]
    assert await check_ledger(conn) == []


async def test_default_cap_and_max_completion_tokens_are_stored(env, conn):
    r = await env.client.post("/v1/jobs", json={"model": "mock-small", "messages": MESSAGES,
                                                "max_completion_tokens": 7, "stream": False})
    assert r.status_code == 202
    stored = await conn.fetchval("SELECT request::text FROM jobs")
    assert '"max_completion_tokens": 7' in stored and '"stream"' not in stored


async def test_insufficient_credits_writes_nothing(env, conn):
    r = await env.client.post("/v1/jobs", json=body(max_tokens=TEAM_GRANT))
    assert r.status_code == 402 and r.json()["error"]["code"] == "insufficient_credits"
    assert await counts(conn) == (0, 0)
    assert await check_ledger(conn) == []


async def test_idempotency_key_replays_and_conflicts(env, conn):
    first = await env.client.post("/v1/jobs", json=body(), headers={"idempotency-key": "k1"})
    again = await env.client.post("/v1/jobs", json=body(), headers={"idempotency-key": "k1"})
    assert (first.status_code, again.status_code) == (202, 200)
    assert again.json()["id"] == first.json()["id"]
    other = await env.client.post("/v1/jobs", json=body(max_tokens=41), headers={"idempotency-key": "k1"})
    assert other.status_code == 409 and other.json()["error"]["code"] == "idempotency_conflict"
    assert await counts(conn) == (1, 1)
    assert await check_ledger(conn) == []


async def test_refusals(env, conn):
    cases = [
        (body(stream=True), 400, "stream_not_supported"),
        (body(n=2), 400, "invalid_request"),
        (body(model="nope"), 404, "model_not_found"),
        (body(max_tokens=0), 400, "invalid_request"),
        ({"model": "mock-small"}, 400, "invalid_request"),
    ]
    for payload, status, code in cases:
        r = await env.client.post("/v1/jobs", json=payload)
        assert (r.status_code, r.json()["error"]["code"]) == (status, code), payload
    r = await env.client.post("/v1/jobs", json=body(), headers={"authorization": "Bearer nope"})
    assert r.status_code == 401
    assert await counts(conn) == (0, 0)


async def test_get_returns_own_jobs_only(env, conn):
    job_id = (await env.client.post("/v1/jobs", json=body())).json()["id"]
    r = await env.client.get(f"/v1/jobs/{job_id}")
    assert r.status_code == 200 and r.json()["id"] == job_id

    async with conn.transaction():
        other = await accounts.create_team(conn, env.org.org_id, "B")
        other_key = await auth.create_api_key(conn, other.team_id)
    r = await env.client.get(f"/v1/jobs/{job_id}", headers={"authorization": f"Bearer {other_key}"})
    assert r.status_code == 404 and r.json()["error"]["code"] == "job_not_found"
    assert (await env.client.get("/v1/jobs/999999")).status_code == 404
