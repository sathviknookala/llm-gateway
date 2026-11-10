"""The mock provider's own TPM/RPM limits."""

import asyncio

import httpx
import orjson

from llmgw import mock_provider


def client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://mock")


def body(tokens: int = 4) -> dict:
    return {"model": "mock-small", "stream": True, "messages": [{"role": "user", "content": "hi"}],
            "stream_options": {"include_usage": True}, "mock": {"tokens": tokens}}


async def stream(c: httpx.AsyncClient, tokens: int = 4) -> httpx.Response:
    r = await c.post("/v1/chat/completions", content=orjson.dumps(body(tokens)))
    await r.aread()
    return r


async def test_rpm_limit_returns_429_with_retry_after():
    app = mock_provider.create_app(rpm=2)
    async with client(app) as c:
        assert (await stream(c)).status_code == 200
        assert (await stream(c)).status_code == 200
        r = await stream(c)
    assert r.status_code == 429
    assert 1 <= int(r.headers["retry-after"]) <= 60
    assert app.state.mock.rate_limited == 1


async def test_tpm_counts_reported_usage_at_completion():
    prompt = mock_provider.count_prompt_tokens(body()["messages"])
    app = mock_provider.create_app(tpm=2 * (prompt + 4))
    async with client(app) as c:
        assert (await stream(c)).status_code == 200
        assert (await stream(c)).status_code == 200
        assert (await stream(c)).status_code == 429


async def test_window_resets():
    app = mock_provider.create_app(rpm=1, window_s=0.05)
    async with client(app) as c:
        assert (await stream(c)).status_code == 200
        assert (await stream(c)).status_code == 429
        await asyncio.sleep(0.06)
        assert (await stream(c)).status_code == 200


async def test_no_limits_by_default():
    app = mock_provider.create_app()
    async with client(app) as c:
        for _ in range(20):
            assert (await stream(c)).status_code == 200
