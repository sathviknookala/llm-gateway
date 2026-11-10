"""Deterministic OpenAI-compatible provider for tests, CI and load tests.

POST /v1/chat/completions. Output is `tok0 tok1 ...`, one token per chunk.
Behaviour is chosen per request by a `mock` object in the body, which the
gateway forwards untouched:

    tokens         output tokens to generate (default 16), capped by max_tokens
    delay_ms       sleep before each token
    status         reply with this HTTP error instead (429 adds Retry-After: 1)
    cut_after      drop the connection after this many tokens, before usage
    no_usage       finish normally but leave out the usage chunk
    prompt_tokens  report this prompt count instead of the mock's own count

Provider limits (off unless set): `create_app(tpm=, rpm=)`, or
LLMGW_MOCK_TPM / LLMGW_MOCK_RPM for `llmgw mock-provider`. Fixed 60 s windows.
RPM counts each admitted request; TPM counts prompt + completion tokens when a
stream ends, which is what the gateway refunds against. A request arriving
when the window's RPM is used up, or its counted tokens already reach TPM, gets
429 with `retry-after` = seconds to the window's end.

The mock's tokenizer: ceil(chars / 4) per message plus 3, plus 3 per request.
The gateway's estimate (llmgw.pricing) is built to stay at or above it.

`app.state.mock` records requests, finished streams and client disconnects
so tests can assert what the provider saw.
"""

import asyncio
import math
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import orjson
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

DEFAULT_TOKENS = 16


class MockCut(Exception):
    """Raised mid-stream to make the server drop the connection."""


@dataclass
class MockState:
    requests: list[dict[str, Any]] = field(default_factory=list)
    completed: int = 0
    cut: int = 0
    disconnected: int = 0
    rate_limited: int = 0  # 429s from the mock's own TPM/RPM limits


@dataclass
class _Window:
    """Fixed-window TPM/RPM counters, like a provider's per-minute limits."""

    tpm: int | None
    rpm: int | None
    window_s: float
    start: float = 0.0
    requests: int = 0
    tokens: int = 0

    def _roll(self) -> float:
        now = time.monotonic()
        if now - self.start >= self.window_s:
            self.start, self.requests, self.tokens = now, 0, 0
        return now

    def admit(self) -> float | None:
        """Count a request; return seconds until the window resets if it's refused."""
        now = self._roll()
        if (self.rpm is not None and self.requests >= self.rpm) or (
            self.tpm is not None and self.tokens >= self.tpm
        ):
            return self.start + self.window_s - now
        self.requests += 1
        return None

    def used(self, tokens: int) -> None:
        self._roll()
        self.tokens += tokens


def count_prompt_tokens(messages: list[dict[str, Any]]) -> int:
    total = 3
    for m in messages:
        content = m.get("content") or ""
        chars = len(content) if isinstance(content, str) else len(orjson.dumps(content))
        total += 3 + math.ceil(chars / 4)
    return total


def _sse(obj: object) -> bytes:
    return b"data: " + orjson.dumps(obj) + b"\n\n"


def create_app(*, tpm: int | None = None, rpm: int | None = None, window_s: float = 60.0) -> Starlette:
    state = MockState()
    window = _Window(tpm, rpm, window_s)

    async def chat(request: Request) -> Response:
        body = await request.json()
        state.requests.append(body)
        sc: dict[str, Any] = body.get("mock") or {}

        if (wait := window.admit()) is not None:
            state.rate_limited += 1
            return JSONResponse(
                {"error": {"message": "mock rate limit", "type": "rate_limit"}},
                status_code=429, headers={"retry-after": str(max(1, math.ceil(wait)))},
            )

        if status := sc.get("status"):
            headers = {"retry-after": "1"} if status == 429 else None
            return JSONResponse(
                {"error": {"message": f"mock error {status}", "type": "mock"}},
                status_code=status, headers=headers,
            )

        model = body.get("model", "mock")
        cap = body.get("max_completion_tokens") or body.get("max_tokens")
        wanted = sc.get("tokens", DEFAULT_TOKENS)
        n = wanted if cap is None else min(wanted, cap)
        finish = "length" if cap is not None and wanted > cap else "stop"
        prompt = sc.get("prompt_tokens") or count_prompt_tokens(body.get("messages", []))
        usage = {"prompt_tokens": prompt, "completion_tokens": n, "total_tokens": prompt + n}
        cid = f"chatcmpl-mock-{uuid.uuid4().hex[:12]}"
        created = int(time.time())
        delay = sc.get("delay_ms", 0) / 1000
        cut_after = sc.get("cut_after")

        if not body.get("stream"):
            window.used(prompt + n)
            text = "".join(f"tok{i} " for i in range(n))
            return JSONResponse({
                "id": cid, "object": "chat.completion", "created": created, "model": model,
                "choices": [{"index": 0, "finish_reason": finish,
                             "message": {"role": "assistant", "content": text}}],
                "usage": usage,
            })

        include_usage = bool((body.get("stream_options") or {}).get("include_usage"))

        def chunk(delta: dict[str, Any], finish_reason: str | None = None) -> bytes:
            return _sse({
                "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
            })

        async def events() -> AsyncIterator[bytes]:
            sent = 0
            try:
                yield chunk({"role": "assistant", "content": ""})
                for i in range(n):
                    if cut_after is not None and i >= cut_after:
                        state.cut += 1
                        raise MockCut(f"cut after {cut_after} tokens")
                    if delay:
                        await asyncio.sleep(delay)
                    yield chunk({"content": f"tok{i} "})
                    sent += 1
                yield chunk({}, finish)
                if include_usage and not sc.get("no_usage"):
                    yield _sse({
                        "id": cid, "object": "chat.completion.chunk", "created": created,
                        "model": model, "choices": [], "usage": usage,
                    })
                yield b"data: [DONE]\n\n"
                state.completed += 1
            except asyncio.CancelledError:
                state.disconnected += 1
                raise
            finally:
                window.used(prompt + sent)

        return StreamingResponse(events(), media_type="text/event-stream")

    app = Starlette(routes=[Route("/v1/chat/completions", chat, methods=["POST"])])
    app.state.mock = state
    return app
