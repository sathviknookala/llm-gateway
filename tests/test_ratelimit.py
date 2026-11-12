"""llmgw.ratelimit against a real Redis. Tests pass `now_ms`, so refill is deterministic."""

import asyncio
import socket

import pytest

from conftest import metric

from llmgw.ratelimit import ModelLimit, RateLimiter, RateLimits, model_key, team_key

TEAM = 1
MODEL = "mock-small"


def limiter(client, **kw) -> RateLimiter:
    provider = kw.pop("provider", {})
    return RateLimiter(client, RateLimits(provider=provider, **kw), timeout_s=1)


async def test_refill_is_linear_and_wait_is_exact(redis_client):
    rl = limiter(redis_client, team_tpm=600)  # 0.01 tokens per ms

    assert (await rl.acquire(TEAM, MODEL, 600, now_ms=0)).allowed
    d = await rl.acquire(TEAM, MODEL, 1, now_ms=0)
    assert not d.allowed and d.blocked == ("team",) and d.wait_ms == 100
    assert not (await rl.acquire(TEAM, MODEL, 1, now_ms=99)).allowed
    assert (await rl.acquire(TEAM, MODEL, 1, now_ms=100)).allowed


async def test_slow_bucket_refills_under_frequent_calls(redis_client):
    """RPM 60 refills 0.001 per ms; calls every 0.5 ms must not round that away."""
    rl = limiter(redis_client, team_rpm=60)
    for _ in range(60):
        assert (await rl.acquire(TEAM, MODEL, 0, now_ms=0)).allowed

    first_allowed = None
    for i in range(1, 2001):
        if (await rl.acquire(TEAM, MODEL, 0, now_ms=i * 0.5)).allowed:
            first_allowed = i * 0.5
            break
    assert first_allowed == 1000


async def test_all_or_nothing_across_team_and_model(redis_client):
    rl = limiter(redis_client, team_tpm=1000, provider={MODEL: ModelLimit(tpm=200)})
    assert (await rl.acquire(TEAM, MODEL, 150, now_ms=0)).allowed

    d = await rl.acquire(TEAM, MODEL, 100, now_ms=0)

    assert not d.allowed and d.blocked == ("model",)
    assert await rl.level(team_key(TEAM, "tpm")) == 850
    assert await rl.level(model_key(MODEL, "tpm")) == 50


async def test_model_budget_is_shared_across_teams(redis_client):
    rl = limiter(redis_client, team_tpm=1000, provider={MODEL: ModelLimit(tpm=200)})
    assert (await rl.acquire(1, MODEL, 150, now_ms=0)).allowed
    d = await rl.acquire(2, MODEL, 100, now_ms=0)
    assert not d.allowed and d.blocked == ("model",)
    assert (await rl.acquire(2, "mock-large", 100, now_ms=0)).allowed


async def test_wait_is_the_max_over_blocking_buckets(redis_client):
    # team refills 0.02/ms, model 0.01/ms
    rl = limiter(redis_client, team_tpm=1200, provider={MODEL: ModelLimit(tpm=600)})
    assert (await rl.acquire(TEAM, MODEL, 600, now_ms=0)).allowed  # team 600 left, model 0

    d = await rl.acquire(TEAM, MODEL, 700, now_ms=0)

    assert set(d.blocked) == {"team", "model"}
    assert d.wait_ms == 70_000  # model: 700 / 0.01; team alone would be 100 / 0.02 = 5,000


async def test_rpm_counts_requests_not_tokens(redis_client):
    rl = limiter(redis_client, provider={MODEL: ModelLimit(rpm=2)})
    assert (await rl.acquire(TEAM, MODEL, 10_000, now_ms=0)).allowed
    assert (await rl.acquire(TEAM, MODEL, 10_000, now_ms=0)).allowed
    d = await rl.acquire(TEAM, MODEL, 1, now_ms=0)
    assert not d.allowed and d.blocked == ("model",) and d.wait_ms == 30_000


async def test_refund_is_capped_and_never_negative(redis_client):
    rl = limiter(redis_client, team_tpm=1000, team_rpm=10)
    key = team_key(TEAM, "tpm")
    assert (await rl.acquire(TEAM, MODEL, 800, now_ms=0)).allowed

    await rl.refund(TEAM, MODEL, 500, now_ms=0)
    assert await rl.level(key) == 700
    await rl.refund(TEAM, MODEL, -50, now_ms=0)
    assert await rl.level(key) == 700
    await rl.refund(TEAM, MODEL, 10_000, now_ms=0)
    assert await rl.level(key) == 1000
    # RPM is never refunded
    assert await rl.level(team_key(TEAM, "rpm")) == 9


async def test_refund_into_a_missing_bucket_is_a_no_op(redis_client):
    rl = limiter(redis_client, team_tpm=1000)
    await rl.refund(TEAM, MODEL, 100, now_ms=0)
    assert not await redis_client.exists(team_key(TEAM, "tpm"))


async def test_concurrent_acquires_never_overspend(redis_client):
    rl = limiter(redis_client, team_tpm=1000)
    results = await asyncio.gather(*(rl.acquire(TEAM, MODEL, 30, now_ms=0) for _ in range(100)))
    assert sum(d.allowed for d in results) == 1000 // 30
    assert await rl.level(team_key(TEAM, "tpm")) == pytest.approx(1000 % 30)


async def test_survives_script_flush(redis_client):
    rl = limiter(redis_client, team_tpm=1000)
    assert (await rl.acquire(TEAM, MODEL, 1, now_ms=0)).allowed
    await redis_client.script_flush()
    assert (await rl.acquire(TEAM, MODEL, 1, now_ms=0)).debited


async def test_redis_clock_path(redis_client):
    rl = limiter(redis_client, team_tpm=600)
    assert (await rl.acquire(TEAM, MODEL, 600)).allowed
    d = await rl.acquire(TEAM, MODEL, 1)
    assert not d.allowed and 0 < d.wait_ms <= 100


async def test_oversized_checks_configured_token_buckets_only(redis_client):
    rl = limiter(redis_client, team_tpm=1000, provider={MODEL: ModelLimit(tpm=500)})
    assert rl.oversized(TEAM, MODEL, 501)
    assert not rl.oversized(TEAM, MODEL, 500)
    assert not rl.oversized(TEAM, "mock-large", 900)
    assert not limiter(redis_client).oversized(TEAM, MODEL, 10**9)


async def test_no_buckets_allows_without_debit(redis_client):
    d = await limiter(redis_client).acquire(TEAM, MODEL, 100)
    assert d.allowed and not d.debited


def _closed_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def test_fails_open_when_redis_is_unreachable():
    rl = RateLimiter.from_url(
        f"redis://127.0.0.1:{_closed_port()}/0", RateLimits(team_tpm=10), timeout_s=0.25
    )
    try:
        acquire0 = metric("llmgw_ratelimit_unavailable_total", op="acquire")
        refund0 = metric("llmgw_ratelimit_unavailable_total", op="refund")
        d = await rl.acquire(TEAM, MODEL, 1_000)
        assert d.allowed and not d.debited
        await rl.refund(TEAM, MODEL, 1_000)  # never raises
        assert metric("llmgw_ratelimit_unavailable_total", op="acquire") == acquire0 + 1
        assert metric("llmgw_ratelimit_unavailable_total", op="refund") == refund0 + 1
        assert await rl.ping() is False
    finally:
        await rl.aclose()
