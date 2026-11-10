"""Price table and token estimates. Everything is integer micro-credits.

Estimates here are deliberately pessimistic. The prompt estimate sizes the
hold, so it should come out at or above what the provider reports: an
overshoot is refunded at settle, an undershoot is capped and left to
reconciliation.
"""

import math
from collections.abc import Mapping
from dataclasses import dataclass

import orjson


@dataclass(frozen=True)
class Price:
    """Micro-credits per token."""

    input: int
    output: int

    def __post_init__(self) -> None:
        if self.input < 0 or self.output < 0:
            raise ValueError("prices must be non-negative")


# Models the mock provider serves. Real models are added through LLMGW_PRICES.
DEFAULT_PRICES: dict[str, Price] = {
    "mock-small": Price(input=1, output=4),
    "mock-large": Price(input=5, output=20),
}

# Fixed tokens per message (role and separators) and per request (reply priming).
# Real tokenizers use 3-4 and 3; these round up.
PER_MESSAGE_TOKENS = 4
PER_REQUEST_TOKENS = 3
# Real tokenizers average ~4 characters per token on English text. Dividing by
# 3 overestimates the prompt; dividing output by 4 is a middling guess, and
# output is only estimated when the provider's usage never arrived.
PROMPT_CHARS_PER_TOKEN = 3
OUTPUT_CHARS_PER_TOKEN = 4


def load_prices(raw: str | None) -> dict[str, Price]:
    """Parse LLMGW_PRICES: {"model": {"input": n, "output": n}, ...}. Unset → defaults."""
    if not raw:
        return dict(DEFAULT_PRICES)
    table = orjson.loads(raw)
    return {model: Price(int(p["input"]), int(p["output"])) for model, p in table.items()}


def cost(price: Price, prompt_tokens: int, completion_tokens: int) -> int:
    return price.input * prompt_tokens + price.output * completion_tokens


def _content_chars(content: object) -> int:
    if content is None:
        return 0
    if isinstance(content, str):
        return len(content)
    if isinstance(content, list):
        total = 0
        for part in content:
            if isinstance(part, Mapping) and part.get("type") == "text":
                total += len(str(part.get("text", "")))
            else:
                # Non-text parts aren't priced yet; count their serialised size.
                total += len(orjson.dumps(part))
        return total
    return len(orjson.dumps(content))


def estimate_prompt_tokens(messages: list[Mapping[str, object]]) -> int:
    tokens = PER_REQUEST_TOKENS
    for m in messages:
        chars = _content_chars(m.get("content")) + len(str(m.get("name", "")))
        tokens += PER_MESSAGE_TOKENS + math.ceil(chars / PROMPT_CHARS_PER_TOKEN)
    return tokens


def estimate_output_tokens(content_chunks: int, content_chars: int) -> int:
    """Output tokens for a stream cut off before usage arrived.

    Providers send about one token per content chunk, so the chunk count is a
    floor; the character count covers providers that batch tokens per chunk.
    """
    return max(content_chunks, math.ceil(content_chars / OUTPUT_CHARS_PER_TOKEN))
