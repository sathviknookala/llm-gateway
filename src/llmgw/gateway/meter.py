"""Reads a passing SSE stream far enough to bill it, without re-encoding it.

The gateway forwards upstream bytes untouched; the meter sees a copy. It
counts content chunks and characters (for the cut-off estimate) and keeps the
final usage block when the provider sends one.

With `collect=True` (the job worker) it also keeps choice 0's content, its
finish_reason and the completion's id, model and created, so `result()` can
rebuild a non-streaming `chat.completion`.
"""

from dataclasses import dataclass
from typing import Any

import orjson


@dataclass(frozen=True)
class Usage:
    prompt_tokens: int
    completion_tokens: int


class StreamMeter:
    def __init__(self, *, collect: bool = False) -> None:
        self._buf = b""
        self.content_chunks = 0
        self.content_chars = 0
        self.usage: Usage | None = None
        self.done = False
        self.collect = collect
        self.parts: list[str] = []
        self.finish_reason: str | None = None
        self.meta: dict[str, Any] = {}

    def feed(self, data: bytes) -> None:
        self._buf += data
        *lines, self._buf = self._buf.split(b"\n")
        for line in lines:
            self._line(line.rstrip(b"\r"))

    def _line(self, line: bytes) -> None:
        if not line.startswith(b"data:"):
            return
        payload = line[5:].strip()
        if payload == b"[DONE]":
            self.done = True
            return
        try:
            event = orjson.loads(payload)
        except orjson.JSONDecodeError:
            return
        if not isinstance(event, dict):
            return
        usage = event.get("usage")
        if isinstance(usage, dict):
            try:
                self.usage = Usage(
                    int(usage["prompt_tokens"]), int(usage["completion_tokens"])
                )
            except (KeyError, TypeError, ValueError):
                pass
        if self.collect:
            for k in ("id", "model", "created"):
                if k in event and k not in self.meta:
                    self.meta[k] = event[k]
        for choice in event.get("choices") or ():
            if not isinstance(choice, dict):
                continue
            content = (choice.get("delta") or {}).get("content")
            if content:
                self.content_chunks += 1
                self.content_chars += len(content)
                if self.collect and choice.get("index", 0) == 0:
                    self.parts.append(content)
            if choice.get("finish_reason") and choice.get("index", 0) == 0:
                self.finish_reason = choice["finish_reason"]

    def result(self) -> dict[str, Any]:
        """The stream as a non-streaming chat.completion (needs collect=True)."""
        out: dict[str, Any] = {
            "id": self.meta.get("id"),
            "object": "chat.completion",
            "created": self.meta.get("created"),
            "model": self.meta.get("model"),
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "".join(self.parts)},
                "finish_reason": self.finish_reason,
            }],
        }
        if self.usage is not None:
            out["usage"] = {
                "prompt_tokens": self.usage.prompt_tokens,
                "completion_tokens": self.usage.completion_tokens,
                "total_tokens": self.usage.prompt_tokens + self.usage.completion_tokens,
            }
        return out
