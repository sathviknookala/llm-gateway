"""The mock provider: its own TPM/RPM limits and per-key attempt scripts."""

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


def scripted(key: str) -> dict:
    b = body()
    b["mock"] = {"tokens": 4, "key": key, "script": [{"status": 503}, {"tokens": 2}, {}]}
    return b


async def test_script_steps_per_key_and_repeats_the_last_step():
    app = mock_provider.create_app()
    async with client(app) as c:
        async def send(key):
            r = await c.post("/v1/chat/completions", content=orjson.dumps(scripted(key)))
            await r.aread()
            return r

        assert (await send("a")).status_code == 503
        second = await send("a")
        assert second.status_code == 200 and second.text.count('"content":"tok') == 2
        assert (await send("a")).text.count('"content":"tok') == 4  # last step {} keeps the top-level tokens
        assert (await send("a")).text.count('"content":"tok') == 4
        assert (await send("b")).status_code == 503  # a new key starts at step 0
    assert app.state.mock.script_calls == {"a": 4, "b": 1}
    assert app.state.mock.sent_chunks == 2 + 4 + 4
