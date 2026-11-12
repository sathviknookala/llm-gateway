"""Loop-lag monitor: a blocked loop shows up as a late wake-up."""

import asyncio
import contextlib
import time

from conftest import metric
from llmgw.looplag import monitor

NAME = "llmgw_event_loop_lag_seconds"


def slow_observations() -> float:
    """Observations above 0.25 s so far: total count minus the le=0.25 bucket."""
    return metric(f"{NAME}_count") - metric(f"{NAME}_bucket", le="0.25")


async def test_blocked_loop_is_observed():
    before = slow_observations()
    task = asyncio.create_task(monitor(0.02))
    try:
        await asyncio.sleep(0.05)  # the monitor is now inside its sleep
        time.sleep(0.3)  # block the loop on purpose: 15x the interval
        deadline = time.monotonic() + 2
        while slow_observations() - before < 1:
            assert time.monotonic() < deadline, "no late wake-up recorded"
            await asyncio.sleep(0.01)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

