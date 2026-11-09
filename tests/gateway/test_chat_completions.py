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
