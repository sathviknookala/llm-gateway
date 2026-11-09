"""Reads a passing SSE stream far enough to bill it, without re-encoding it.

The gateway forwards upstream bytes untouched; the meter sees a copy. It
counts content chunks and characters (for the cut-off estimate) and keeps the
final usage block when the provider sends one.
"""

from dataclasses import dataclass

import orjson


@dataclass(frozen=True)
class Usage:
    prompt_tokens: int
    completion_tokens: int


class StreamMeter:
    def __init__(self) -> None:
        self._buf = b""
        self.content_chunks = 0
        self.content_chars = 0
        self.usage: Usage | None = None
        self.done = False

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
        for choice in event.get("choices") or ():
            content = (choice.get("delta") or {}).get("content") if isinstance(choice, dict) else None
            if content:
                self.content_chunks += 1
                self.content_chars += len(content)
