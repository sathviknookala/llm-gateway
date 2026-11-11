"""Checks a chat-completions body before anything touches Redis or the ledger.

Shared by the synchronous path (`stream: true` required) and job submission
(`stream` absent or false, one choice). The checks and their codes run in
this order: invalid JSON, `messages` not a list, the stream rule, unknown
model, a bad output cap.
"""

from dataclasses import dataclass
from typing import Any

import orjson

from llmgw.config import Settings
from llmgw.pricing import Price, estimate_prompt_tokens


@dataclass(frozen=True)
class Parsed:
    body: dict[str, Any]
    model: str
    price: Price
    cap_field: str  # "max_tokens", or "max_completion_tokens" if the client used it
    max_tokens: int  # the cap forwarded upstream; the hold covers it
    prompt_estimate: int

    @property
    def reserved_tokens(self) -> int:
        """Worst-case tokens: what the TPM buckets are charged and the hold is sized on."""
        return self.prompt_estimate + self.max_tokens


@dataclass(frozen=True)
class Refusal:
    status: int
    message: str
    code: str


def parse_chat(raw: bytes, settings: Settings, *, stream: bool) -> Parsed | Refusal:
    try:
        body = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return Refusal(400, "body is not valid JSON", "invalid_json")
    if not isinstance(body, dict) or not isinstance(body.get("messages"), list):
        return Refusal(400, "`messages` must be a list", "invalid_request")
    if stream:
        if body.get("stream") is not True:
            return Refusal(400, "only `stream: true` is supported", "stream_required")
    else:
        if body.get("stream") not in (None, False):
            return Refusal(400, "jobs don't stream; poll GET /v1/jobs/{id} for the result", "stream_not_supported")
        if body.get("n") not in (None, 1):
            return Refusal(400, "jobs support one choice (`n` = 1)", "invalid_request")
    model = body.get("model")
    price = settings.prices.get(model) if isinstance(model, str) else None
    if price is None:
        return Refusal(404, f"unknown model {model!r}", "model_not_found")

    cap_field = "max_completion_tokens" if "max_completion_tokens" in body else "max_tokens"
    max_tokens = body.get(cap_field)
    if max_tokens is None:
        max_tokens = settings.default_max_tokens
    if not isinstance(max_tokens, int) or isinstance(max_tokens, bool) or max_tokens <= 0:
        return Refusal(400, f"`{cap_field}` must be a positive integer", "invalid_request")
    return Parsed(body, model, price, cap_field, max_tokens, estimate_prompt_tokens(body["messages"]))
