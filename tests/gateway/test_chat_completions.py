"""POST /v1/chat/completions end to end: every hold is closed exactly once, on every exit path."""

import asyncio
import socket

import anyio
import pytest

from llmgw import auth
from llmgw.gateway.app import ChatCompletions
from llmgw.ledger import accounts
from llmgw.ledger.checks import check_ledger
from llmgw.mock_provider import count_prompt_tokens
from llmgw.pricing import DEFAULT_PRICES, cost, estimate_output_tokens, estimate_prompt_tokens

from .conftest import TEAM_GRANT, gateway_env

MESSAGES = [{"role": "user", "content": "hello"}]
PRICE = DEFAULT_PRICES["mock-small"]
PROMPT_EST = estimate_prompt_tokens(MESSAGES)  # what the gateway holds for
PROMPT_REAL = count_prompt_tokens(MESSAGES)  # what the mock reports


def body(max_tokens: int | None = 32, **mock) -> dict:
    b = {"model": "mock-small", "messages": MESSAGES, "stream": True}
    if max_tokens is not None:
        b["max_tokens"] = max_tokens
    if mock:
        b["mock"] = mock
    return b


def tokens(n: int, start: int = 0) -> tuple[int, int]:
    """(chunks, chars) of the mock's output `tok{start} ... tok{start+n-1} `."""
    return n, sum(len(f"tok{i} ") for i in range(start, start + n))


async def chat(client, payload: dict):
    async with client.stream("POST", "/v1/chat/completions", json=payload) as r:
        return r.status_code, (await r.aread()).decode(), r.headers


async def holds(conn):
    return await conn.fetch("SELECT * FROM holds ORDER BY id")


async def wait_closed(conn, timeout: float = 5.0):
    """The client sees the end of the stream before the gateway's close commits."""
    async with asyncio.timeout(timeout):
        while True:
            rows = await holds(conn)
            if rows and all(r["state"] != "held" for r in rows):
                return rows
            await asyncio.sleep(0.02)


async def close_posting(conn, hold):
    return await conn.fetchrow("SELECT * FROM postings WHERE id = $1", hold["close_posting_id"])


async def available(conn, team) -> int:
    return await accounts.balance(conn, team.available)


async def assert_consistent(conn):
    assert await check_ledger(conn) == []
    assert await conn.fetchval("SELECT count(*) FROM holds WHERE state = 'held'") == 0


# --- happy path -------------------------------------------------------------


async def test_stream_settles_on_reported_usage(conn, env, mock):
    status, text, headers = await chat(env.client, body())

    assert status == 200
    assert headers["content-type"].startswith("text/event-stream")
    assert all(f"tok{i} " in text for i in range(16))
    assert text.rstrip().endswith("data: [DONE]")

    [hold] = await wait_closed(conn)
    charged = cost(PRICE, PROMPT_REAL, 16)
    assert hold["amount"] == cost(PRICE, PROMPT_EST, 32)
    assert hold["state"] == "settled"
    assert hold["settled_amount"] == charged
    assert not (await close_posting(conn, hold))["flagged"]
    assert await available(conn, env.team) == TEAM_GRANT - charged
    await assert_consistent(conn)


async def test_forwards_output_cap_and_asks_for_usage(conn, env, mock):
    await chat(env.client, body(max_tokens=None))
    [sent] = mock.state.requests
    assert sent["max_tokens"] == env.gw.settings.default_max_tokens
    assert sent["stream_options"] == {"include_usage": True}
    [hold] = await wait_closed(conn)
    assert hold["amount"] == cost(PRICE, PROMPT_EST, env.gw.settings.default_max_tokens)


async def test_max_completion_tokens_is_the_cap_when_given(conn, env, mock):
    payload = body(max_tokens=None)
    payload["max_completion_tokens"] = 5
    status, text, _ = await chat(env.client, payload)
    assert status == 200 and "tok4 " in text and "tok5 " not in text
    assert mock.state.requests[0]["max_completion_tokens"] == 5
    [hold] = await wait_closed(conn)
    assert hold["amount"] == cost(PRICE, PROMPT_EST, 5)
    assert hold["settled_amount"] == cost(PRICE, PROMPT_REAL, 5)


async def test_gateway_estimate_covers_the_mock_tokenizer():
    for content in ["", "a", "hello", "x" * 1000, "word " * 333]:
        msgs = [{"role": "system", "content": "be brief"}, {"role": "user", "content": content}]
        assert estimate_prompt_tokens(msgs) >= count_prompt_tokens(msgs)


# --- refused before a hold ----------------------------------------------------


@pytest.mark.parametrize("header", ["", "Bearer nope", "Basic abc"])
async def test_rejects_missing_or_bad_key(conn, env, mock, header):
    r = await env.client.post("/v1/chat/completions", json=body(), headers={"authorization": header})
    assert r.status_code == 401
    assert await holds(conn) == []
    assert mock.state.requests == []


async def test_rejects_revoked_key(conn, env, mock):
    assert await auth.revoke_api_key(conn, env.key)
    r = await env.client.post("/v1/chat/completions", json=body())
    assert r.status_code == 401


@pytest.mark.parametrize(
    ("payload", "status", "code"),
    [
        ({**body(), "model": "gpt-nope"}, 404, "model_not_found"),
        ({**body(), "stream": False}, 400, "stream_required"),
        ({**body(), "messages": "hi"}, 400, "invalid_request"),
        ({**body(), "max_tokens": 0}, 400, "invalid_request"),
        ({**body(), "max_tokens": "10"}, 400, "invalid_request"),
    ],
)
async def test_rejects_bad_requests(conn, env, mock, payload, status, code):
    r = await env.client.post("/v1/chat/completions", json=payload)
    assert r.status_code == status
    assert r.json()["error"]["code"] == code
    assert await holds(conn) == []


async def test_invalid_json(conn, env):
    r = await env.client.post("/v1/chat/completions", content=b"{not json")
    assert r.status_code == 400


async def test_insufficient_credits_is_402_and_nothing_is_sent(conn, env, mock):
    r = await env.client.post("/v1/chat/completions", json=body(max_tokens=TEAM_GRANT))
    assert r.status_code == 402
    assert r.json()["error"]["code"] == "insufficient_credits"
    assert await holds(conn) == []
    assert mock.state.requests == []
    assert await available(conn, env.team) == TEAM_GRANT


async def test_at_capacity_is_429(conn, env, mock):
    env.gw.inflight = env.gw.settings.max_inflight
    try:
        r = await env.client.post("/v1/chat/completions", json=body())
    finally:
        env.gw.inflight = 0
    assert r.status_code == 429 and r.headers["retry-after"] == "1"
    assert await holds(conn) == []


# --- provider refused: release ------------------------------------------------


async def test_provider_429_releases_and_passes_retry_after(conn, env, mock):
    status, text, headers = await chat(env.client, body(status=429))
    assert status == 429 and headers["retry-after"] == "1"
    [hold] = await wait_closed(conn)
    assert hold["state"] == "released"
    assert await available(conn, env.team) == TEAM_GRANT
    await assert_consistent(conn)


async def test_provider_400_is_passed_through_and_released(conn, env, mock):
    status, text, _ = await chat(env.client, body(status=400))
    assert status == 400 and "mock error 400" in text
    [hold] = await wait_closed(conn)
    assert hold["state"] == "released"


async def test_provider_500_is_502_and_released(conn, env, mock):
    status, _, _ = await chat(env.client, body(status=500))
    assert status == 502
    [hold] = await wait_closed(conn)
    assert hold["state"] == "released"


async def test_provider_unreachable_releases(conn, database_url, pool, funded):
    with socket.socket() as s:  # a port with nothing listening
        s.bind(("127.0.0.1", 0))
        dead = f"http://127.0.0.1:{s.getsockname()[1]}"
    async with gateway_env(database_url, pool, dead, funded) as env:
        status, _, _ = await chat(env.client, body())
        assert status == 502
        [hold] = await wait_closed(conn)
    assert hold["state"] == "released"
    assert await available(conn, env.team) == TEAM_GRANT
    await assert_consistent(conn)


# --- no usage block: settle on a flagged estimate -----------------------------


async def test_upstream_cut_settles_flagged_estimate(conn, env, mock):
    status, text, _ = await chat(env.client, body(cut_after=5))

    assert status == 200  # headers went out before the cut
    assert "tok4 " in text and "tok5 " not in text and "[DONE]" not in text
    [hold] = await wait_closed(conn)
    expected = cost(PRICE, PROMPT_EST, estimate_output_tokens(*tokens(5)))
    assert hold["state"] == "settled"
    assert hold["settled_amount"] == expected
    assert (await close_posting(conn, hold))["flagged"]
    assert await available(conn, env.team) == TEAM_GRANT - expected
    await assert_consistent(conn)


async def test_missing_usage_block_settles_flagged_estimate(conn, env, mock):
    status, text, _ = await chat(env.client, body(no_usage=True))
    assert status == 200 and "[DONE]" in text
    [hold] = await wait_closed(conn)
    assert hold["settled_amount"] == cost(PRICE, PROMPT_EST, estimate_output_tokens(*tokens(16)))
    assert (await close_posting(conn, hold))["flagged"]


async def test_usage_above_hold_is_capped_and_flagged(conn, env, mock):
    status, _, _ = await chat(env.client, body(prompt_tokens=10_000))
    assert status == 200
    [hold] = await wait_closed(conn)
    assert hold["settled_amount"] == hold["amount"]  # capped at the hold
    assert (await close_posting(conn, hold))["flagged"]
    assert await available(conn, env.team) == TEAM_GRANT - hold["amount"]
    await assert_consistent(conn)


async def test_upstream_timeout_settles_flagged_estimate(conn, database_url, pool, mock, funded):
    async with gateway_env(
        database_url, pool, mock.url, funded, upstream_timeout_s=0.3
    ) as env:
        status, text, _ = await chat(env.client, body(tokens=30, delay_ms=50))
        assert status == 200 and "[DONE]" not in text
        [hold] = await wait_closed(conn)
    assert hold["state"] == "settled"
    assert (await close_posting(conn, hold))["flagged"]
    await assert_consistent(conn)


# --- client goes away ---------------------------------------------------------


async def test_client_disconnect_settles_and_closes_upstream(conn, env, mock):
    async with env.client.stream(
        "POST", "/v1/chat/completions", json=body(max_tokens=200, tokens=200, delay_ms=20)
    ) as r:
        assert r.status_code == 200
        seen = ""
        async for piece in r.aiter_text():
            seen += piece
            if "tok3 " in seen:
                break  # leaving the block closes the connection mid-stream

    [hold] = await wait_closed(conn)
    assert hold["state"] == "settled"
    assert (await close_posting(conn, hold))["flagged"]
    # Charged for the prompt estimate and some, not all, of the output.
    assert cost(PRICE, PROMPT_EST, 4) <= hold["settled_amount"] < hold["amount"]
    async with asyncio.timeout(5):
        while mock.state.disconnected == 0:  # the gateway closed its upstream stream
            await asyncio.sleep(0.02)
    assert mock.state.completed == 0
    await assert_consistent(conn)


def _asgi_request(key: str, payload: dict):
    """Drive ChatCompletions directly, to cancel its task the way a server shutdown would."""
    import orjson

    scope = {
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1", "method": "POST", "scheme": "http",
        "path": "/v1/chat/completions", "raw_path": b"/v1/chat/completions",
        "query_string": b"", "root_path": "",
        "headers": [(b"authorization", f"Bearer {key}".encode()),
                    (b"content-type", b"application/json")],
        "client": ("127.0.0.1", 1), "server": ("127.0.0.1", 2),
    }
    sent_body = False

    async def receive():
        nonlocal sent_body
        if not sent_body:
            sent_body = True
            return {"type": "http.request", "body": orjson.dumps(payload), "more_body": False}
        await asyncio.Event().wait()  # the client never leaves

    chunks: list[bytes] = []
    streaming = asyncio.Event()

    async def send(message):
        chunks.append(message.get("body", b""))
        if b"tok2 " in b"".join(chunks):
            streaming.set()

    return scope, receive, send, streaming


@pytest.mark.parametrize("how", ["task-cancel-once", "task-cancel-repeatedly", "anyio-scope"])
async def test_settle_survives_request_task_cancellation(conn, env, mock, how):
    """The close must commit however the request is cancelled. anyio scope
    cancellation (Starlette's task groups) is level-triggered and re-cancels every
    await, including in `finally`; repeated Task.cancel() (a forced server
    shutdown) breaks through a plain shield. The slow close below makes both
    land while the close is in flight."""
    closing = asyncio.Event()

    async def slow_close(call):
        closing.set()
        await asyncio.sleep(0.3)

    env.gw.before_close = slow_close
    scope, receive, send, streaming = _asgi_request(env.key, body(max_tokens=100, tokens=100, delay_ms=20))
    handler = ChatCompletions(env.gw)

    if how == "anyio-scope":
        async with anyio.create_task_group() as tg:
            tg.start_soon(handler, scope, receive, send)
            await asyncio.wait_for(streaming.wait(), 5)
            tg.cancel_scope.cancel()
    else:
        task = asyncio.create_task(handler(scope, receive, send))
        await asyncio.wait_for(streaming.wait(), 5)
        task.cancel()
        if how == "task-cancel-repeatedly":
            await asyncio.wait_for(closing.wait(), 5)
            for _ in range(3):
                await asyncio.sleep(0.05)
                task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    [hold] = await wait_closed(conn)
    assert hold["state"] == "settled"
    assert (await close_posting(conn, hold))["flagged"]
    await assert_consistent(conn)


# --- concurrency --------------------------------------------------------------


async def test_concurrent_mixed_outcomes_keep_the_ledger_consistent(conn, env, mock):
    scenarios = [{}, {"status": 429}, {"cut_after": 3}, {"no_usage": True}, {"prompt_tokens": 10_000}]
    payloads = [body(**scenarios[i % len(scenarios)]) for i in range(40)]
    results = await asyncio.gather(*(chat(env.client, p) for p in payloads))
    assert {s for s, _, _ in results} == {200, 429}

    rows = await wait_closed(conn)
    assert len(rows) == 40
    spent = await accounts.balance(conn, env.org.provider_spend)
    assert spent == sum(r["settled_amount"] or 0 for r in rows)
    assert await available(conn, env.team) == TEAM_GRANT - spent
    assert await accounts.balance(conn, env.team.held) == 0
    await assert_consistent(conn)
