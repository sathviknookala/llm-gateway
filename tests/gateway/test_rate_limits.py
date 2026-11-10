"""Rate limits on /v1/chat/completions: refused before the hold, refunded after the close.

Limits are chosen so refill during a test (team TPM 600 → 10 tokens/s) is far
below one request's cost, so outcomes don't depend on timing.
"""

import asyncio
import socket

import pytest

from llmgw import auth, mock_provider
from llmgw.ledger import accounts, postings
from llmgw.ratelimit import ModelLimit, RateLimits, model_key, team_key
from llmgw.pricing import estimate_prompt_tokens

from .conftest import TEAM_GRANT, gateway_env, serve
from .test_chat_completions import MESSAGES, assert_consistent, available, body, chat, holds, wait_closed

PROMPT_EST = estimate_prompt_tokens(MESSAGES)
BIG = 400 - PROMPT_EST  # max_tokens that makes a request reserve exactly 400 tokens


@pytest.fixture
def limited(database_url, pool, mock, funded, redis_url, redis_client):
    """`async with limited(**rate_limits) as env`: a gateway with the limiter on."""

    def make(*, upstream=None, redis=None, **limits):
        return gateway_env(
            database_url, pool, upstream or mock.url, funded,
            redis_url=redis or redis_url, rate_limits=RateLimits(**limits),
        )

    return make


async def second_team_key(conn, env) -> str:
    async with conn.transaction():
        team = await accounts.create_team(conn, env.org.org_id, "B")
        await postings.grant(conn, team_id=team.team_id, amount=TEAM_GRANT, idempotency_key="grant-B")
        return await auth.create_api_key(conn, team.team_id)


async def wait_level(env, key: str, at_least: float, timeout: float = 5.0) -> float:
    """The refund runs after the ledger close commits; poll the bucket until it lands."""
    async with asyncio.timeout(timeout):
        while True:
            level = await env.gw.limiter.level(key)
            if level is not None and level >= at_least:
                return level
            await asyncio.sleep(0.02)


async def test_over_team_limit_is_429_before_any_hold(conn, limited, mock):
    async with limited(team_tpm=600) as env:
        # Uses almost all it reserved, so the refund is tiny.
        status, _, _ = await chat(env.client, body(max_tokens=BIG, tokens=BIG))
        assert status == 200
        await wait_closed(conn)

        r = await env.client.post("/v1/chat/completions", json=body(max_tokens=BIG))

    assert r.status_code == 429
    assert r.json()["error"]["code"] == "team_rate_limited"
    assert int(r.headers["retry-after"]) >= 1
    assert len(await holds(conn)) == 1
    assert len(mock.state.requests) == 1
    await assert_consistent(conn)


async def test_one_team_limited_does_not_block_another(conn, limited):
    async with limited(team_tpm=600) as env:
        await chat(env.client, body(max_tokens=BIG, tokens=BIG))
        assert (await env.client.post("/v1/chat/completions", json=body(max_tokens=BIG))).status_code == 429

        key_b = await second_team_key(conn, env)
        async with env.client.stream(
            "POST", "/v1/chat/completions", json=body(max_tokens=BIG),
            headers={"authorization": f"Bearer {key_b}"},
        ) as r:
            await r.aread()
            assert r.status_code == 200


async def test_provider_budget_is_shared_across_teams(conn, limited):
    async with limited(provider={"mock-small": ModelLimit(tpm=600)}) as env:
        status, _, _ = await chat(env.client, body(max_tokens=BIG, tokens=BIG))
        assert status == 200
        key_b = await second_team_key(conn, env)

        r = await env.client.post(
            "/v1/chat/completions", json=body(max_tokens=BIG),
            headers={"authorization": f"Bearer {key_b}"},
        )

    assert r.status_code == 429
    assert r.json()["error"]["code"] == "provider_budget_exhausted"


async def test_unused_tokens_are_refunded_after_the_close(limited):
    async with limited(team_tpm=600) as env:
        key = team_key(env.team.team_id, "tpm")
        status, _, _ = await chat(env.client, body(max_tokens=BIG, tokens=4))
        assert status == 200
        # Reserved 400, used about 12: without the refund the bucket would sit near 200.
        assert await wait_level(env, key, 500) <= 600

        status, _, _ = await chat(env.client, body(max_tokens=BIG, tokens=4))
        assert status == 200


async def test_refused_by_provider_refunds_everything(limited):
    async with limited(team_tpm=600) as env:
        status, _, _ = await chat(env.client, body(max_tokens=BIG, status=500))
        assert status == 502
        assert await wait_level(env, team_key(env.team.team_id, "tpm"), 599) <= 600


async def test_insufficient_credits_refunds_the_reservation(conn, limited):
    tpm = 2 * TEAM_GRANT
    async with limited(team_tpm=tpm) as env:
        r = await env.client.post("/v1/chat/completions", json=body(max_tokens=TEAM_GRANT))
        assert r.status_code == 402
        assert await env.gw.limiter.level(team_key(env.team.team_id, "tpm")) == pytest.approx(tpm, abs=1)
    assert await holds(conn) == []
    assert await available(conn, env.team) == TEAM_GRANT


async def test_request_bigger_than_a_bucket_is_400(conn, limited, mock):
    async with limited(provider={"mock-small": ModelLimit(tpm=300)}) as env:
        r = await env.client.post("/v1/chat/completions", json=body(max_tokens=BIG))
        assert r.status_code == 400
        assert r.json()["error"]["code"] == "request_exceeds_rate_limit"
        assert await env.gw.limiter.level(model_key("mock-small", "tpm")) is None
    assert await holds(conn) == []
    assert mock.state.requests == []


async def test_redis_down_fails_open(conn, limited):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        dead = f"redis://127.0.0.1:{s.getsockname()[1]}/0"
    async with limited(redis=dead, team_tpm=10_000) as env:
        status, _, _ = await chat(env.client, body())
        assert status == 200
        [hold] = await wait_closed(conn)
    assert hold["state"] == "settled"
    await assert_consistent(conn)


async def test_provider_budget_below_provider_limit_means_no_provider_429s(
    conn, database_url, pool, funded, redis_url, redis_client
):
    """Gateway budget 2,000 TPM against a mock that allows 4,000: the gateway does the limiting."""
    app = mock_provider.create_app(tpm=4_000)
    async with serve(app) as mock_url:
        async with gateway_env(
            database_url, pool, mock_url, funded, redis_url=redis_url,
            rate_limits=RateLimits(provider={"mock-small": ModelLimit(tpm=2_000)}),
        ) as env:
            # Each reserves 100 tokens and streams slowly, so the burst is all in flight at once.
            payload = body(max_tokens=100 - PROMPT_EST, tokens=50, delay_ms=5)
            results = await asyncio.gather(*(chat(env.client, payload) for _ in range(40)))
            await wait_closed(conn)

    statuses = [s for s, _, _ in results]
    assert app.state.mock.rate_limited == 0
    assert statuses.count(429) > 0 and statuses.count(200) > 0
    assert set(statuses) == {200, 429}
    for s, text, _ in results:
        if s == 429:
            assert "provider_budget_exhausted" in text
    await assert_consistent(conn)
