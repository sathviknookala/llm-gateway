"""Gateway metrics, /metrics and /readyz.

The registry is process-global, so every assertion is a delta around one action.
"""

import asyncio
import socket

import asyncpg

from conftest import metric
from llmgw.ratelimit import RateLimiter, RateLimits

from .conftest import TEAM_GRANT, gateway_env
from .test_chat_completions import body, chat, holds, wait_closed

CHAT = "/v1/chat/completions"


async def eventually(check, timeout: float = 5.0) -> None:
    """Poll `check()` until it is true: some counters move after the client's response."""
    async with asyncio.timeout(timeout):
        while not check():
            await asyncio.sleep(0.02)


def closes(op: str, flagged: str) -> float:
    return metric("llmgw_hold_closes_total", path="sync", op=op, flagged=flagged)


def responses(status: str, route: str = CHAT) -> float:
    return metric("llmgw_http_requests_total", route=route, status=status)


async def test_successful_stream(conn, env):
    ok0, settle0 = responses("200"), closes("settle", "false")
    admitted0 = metric("llmgw_admission_seconds_count")
    inflight0 = metric("llmgw_inflight_streams")

    status, _, _ = await chat(env.client, body())
    assert status == 200
    await wait_closed(conn)

    await eventually(lambda: responses("200") == ok0 + 1 and closes("settle", "false") == settle0 + 1)
    assert metric("llmgw_admission_seconds_count") == admitted0 + 1
    await eventually(lambda: metric("llmgw_inflight_streams") == inflight0)


async def test_refusal_is_counted_by_code_and_closes_nothing(conn, env):
    refused0 = metric("llmgw_refusals_total", code="insufficient_credits")
    settle0, release0 = closes("settle", "false"), closes("release", "false")
    admitted0 = metric("llmgw_admission_seconds_count")

    r = await env.client.post(CHAT, json=body(max_tokens=TEAM_GRANT))
    assert r.status_code == 402

    assert metric("llmgw_refusals_total", code="insufficient_credits") == refused0 + 1
    await eventually(lambda: responses("402") >= 1)
    assert await holds(conn) == []
    assert (closes("settle", "false"), closes("release", "false")) == (settle0, release0)
    assert metric("llmgw_admission_seconds_count") == admitted0


async def test_provider_5xx_counts_a_release_and_an_upstream_error(conn, env):
    release0 = closes("release", "false")
    errors0 = metric("llmgw_upstream_errors_total", path="sync", kind="status_5xx")

    r = await env.client.post(CHAT, json=body(status=503))
    assert r.status_code == 502
    await wait_closed(conn)

    await eventually(lambda: closes("release", "false") == release0 + 1)
    assert metric("llmgw_upstream_errors_total", path="sync", kind="status_5xx") == errors0 + 1


async def test_client_disconnect_counts_a_flagged_settle(conn, env):
    flagged0 = closes("settle", "true")
    async with env.client.stream("POST", CHAT, json=body(max_tokens=200, tokens=200, delay_ms=20)) as r:
        seen = ""
        async for piece in r.aiter_text():
            seen += piece
            if "tok3 " in seen:
                break
    await wait_closed(conn)
    await eventually(lambda: closes("settle", "true") == flagged0 + 1)


async def test_job_route_refusals_are_counted(env):
    refused0 = metric("llmgw_refusals_total", code="job_not_found")
    r = await env.client.get("/v1/jobs/999999")
    assert r.status_code == 404
    assert metric("llmgw_refusals_total", code="job_not_found") == refused0 + 1
    await eventually(lambda: responses("404", "/v1/jobs/{id}") >= 1)


async def test_metrics_endpoint_serves_the_registry(env):
    r = await env.client.get("/metrics")
    assert r.status_code == 200
    assert "llmgw_event_loop_lag_seconds_bucket" in r.text
    assert "llmgw_hold_closes_total" in r.text


async def test_readyz_without_redis(env):
    r = await env.client.get("/readyz")
    assert r.status_code == 200
    assert r.json() == {"db": "ok", "redis": "off"}


async def test_readyz_is_503_when_the_database_is_gone(database_url, conn, mock, funded):
    own = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
    async with gateway_env(database_url, own, mock.url, funded) as env:
        await own.close()
        r = await env.client.get("/readyz")
    assert r.status_code == 503
    assert r.json()["db"] == "down"


async def test_readyz_reports_redis_down_but_stays_ready(database_url, pool, mock, funded):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        dead = f"redis://127.0.0.1:{s.getsockname()[1]}/0"
    limiter = RateLimiter.from_url(dead, RateLimits(team_tpm=10_000), timeout_s=0.2)
    try:
        async with gateway_env(database_url, pool, mock.url, funded, limiter=limiter) as env:
            r = await env.client.get("/readyz")
    finally:
        await limiter.aclose()
    assert r.status_code == 200
    assert r.json() == {"db": "ok", "redis": "down"}
