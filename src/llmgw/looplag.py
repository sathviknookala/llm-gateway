"""Event-loop lag: how late a short sleep wakes up.

Anything that blocks the loop (a sync call, heavy CPU) delays every stream in
the process; this shows up here before it shows up as a mystery in p95.
Every long-running mode runs `monitor` as a background task.
"""

import asyncio
import time

from llmgw.metrics import EVENT_LOOP_LAG


async def monitor(interval_s: float) -> None:
    while True:
        t0 = time.monotonic()
        await asyncio.sleep(interval_s)
        EVENT_LOOP_LAG.observe(max(0.0, time.monotonic() - t0 - interval_s))
