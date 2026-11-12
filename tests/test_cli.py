"""CLI modes as real processes: seed, check-ledger, and SIGTERM for worker, sweeper and gateway.

Each process runs `python -m llmgw <mode>` against the test database. The mock
provider runs in this test's event loop on 127.0.0.1, which the child reaches
over a socket. Output goes to a file, never a pipe, so a chatty child can't
block on a full pipe buffer.
"""

import asyncio
import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
from pathlib import Path

import httpx
import orjson
import pytest

from gateway.conftest import Mock, serve
from llmgw import auth, mock_provider
from llmgw.jobs import queue
from llmgw.jobs.queue import JobState
from llmgw.ledger import accounts, postings
from llmgw.ledger.checks import check_ledger
from llmgw.ledger.models import HoldState

GRANT = 1_000_000


def child_env(database_url: str, **extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("LLMGW_")}
    return {**env, "LLMGW_DATABASE_URL": database_url, "LLMGW_LOG_FORMAT": "json", **extra}


def run(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "llmgw", *args], env=child_env(database_url),
        capture_output=True, text=True, timeout=60,
    )


@contextlib.asynccontextmanager
async def child(tmp_path: Path, env: dict[str, str], *args: str):
    """A long-running mode; yields (process, log path). Killed on the way out if still running."""
    log = tmp_path / f"{args[0]}.log"
    with log.open("wb") as out:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "llmgw", *args, env=env, stdout=out, stderr=asyncio.subprocess.STDOUT,
        )
        try:
            yield proc, log
        finally:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()


async def until(predicate, timeout: float = 10.0, what: str = "condition") -> None:
    try:
        async with asyncio.timeout(timeout):
            while not await predicate():
                await asyncio.sleep(0.05)
    except TimeoutError:
        raise AssertionError(f"timed out waiting for {what}") from None


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def json_lines(log: Path) -> list[dict]:
    """Every line that looks like JSON must be JSON; return them parsed."""
    return [orjson.loads(line) for line in log.read_text().splitlines() if line.startswith("{")]


@pytest.fixture
async def mock():
    app = mock_provider.create_app()
    async with serve(app) as url:
        yield Mock(url, app.state.mock)


@pytest.fixture
async def team(conn):
    async with conn.transaction():
        org = await accounts.create_org(conn, "org")
        await postings.fund_org(conn, org_id=org.org_id, amount=10 * GRANT, idempotency_key="fund")
        t = await accounts.create_team(conn, org.org_id, "A")
        await postings.grant(conn, team_id=t.team_id, amount=GRANT, idempotency_key="grant")
        key = await auth.create_api_key(conn, t.team_id)
    return t, key


# --- seed and check-ledger ----------------------------------------------------


async def test_seed_creates_funded_teams_with_working_keys(database_url, conn):
    r = await asyncio.to_thread(run, database_url, "seed", "--org", "acme", "--teams", "3", "--fund", "1000",
                                "--grant", "300")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert len(out["teams"]) == 3
    org = await accounts.org_accounts(conn, out["org_id"])
    assert await accounts.balance(conn, org.org_pool) == 1000 - 3 * 300
    for t in out["teams"]:
        team = await accounts.team_accounts(conn, t["team_id"])
        assert await accounts.balance(conn, team.available) == 300
        assert await auth.team_for_key(conn, t["key"]) == t["team_id"]
    assert await check_ledger(conn) == []


async def test_seed_refuses_grants_the_fund_cannot_cover(database_url, conn):
    r = await asyncio.to_thread(run, database_url, "seed", "--teams", "3", "--fund", "500", "--grant", "300")
    assert r.returncode != 0
    assert "can't cover" in r.stderr
    assert await conn.fetchval("SELECT count(*) FROM orgs") == 0


async def test_check_ledger_exit_codes(database_url, conn, team):
    r = await asyncio.to_thread(run, database_url, "check-ledger")
    assert r.returncode == 0, r.stdout + r.stderr
    t, _ = team
    # balances is the one ledger table that isn't append-only; corrupt it directly.
    await conn.execute("UPDATE balances SET amount = amount + 7 WHERE account_id = $1", t.available)
    r = await asyncio.to_thread(run, database_url, "check-ledger")
    assert r.returncode == 1
    assert f"account {t.available}" in r.stdout


# --- SIGTERM ----------------------------------------------------------------------


async def test_worker_sigterm_mid_stream_finishes_the_attempt(database_url, conn, team, mock, tmp_path):
    t, _ = team
    request = {"model": "mock-small", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 400,
               "mock": {"tokens": 400, "delay_ms": 50}}  # a 20 s stream
    async with conn.transaction():
        job, _ = await queue.enqueue(
            conn, team_id=t.team_id, model="mock-small", request=request, prompt_estimate=10,
            max_tokens=400, amount=10_000, idempotency_key=None, max_attempts=3, ttl_s=3600, hold_grace_s=60,
        )
    env = child_env(database_url, LLMGW_UPSTREAM_URL=mock.url, LLMGW_WORKER_POLL_S="0.05",
                    LLMGW_JOB_LEASE_S="10", LLMGW_JOB_HEARTBEAT_S="2", LLMGW_WORKER_CONCURRENCY="1")
    async with child(tmp_path, env, "worker") as (proc, log):
        async def streaming():
            return mock.state.sent_chunks >= 3

        await until(streaming, what="the worker's stream to start")
        proc.send_signal(signal.SIGTERM)
        async with asyncio.timeout(10):
            assert await proc.wait() == 0, log.read_text()

    # The shielded finish ran (not the sweeper): requeued, lease cleared, cost booked.
    done = await queue.get_job(conn, job.id)
    assert done.state == JobState.QUEUED and done.locked_until is None
    (outcome, cost, flagged, posting) = await conn.fetchrow(
        "SELECT outcome::text, cost, flagged, retry_loss_posting_id FROM job_attempts WHERE job_id = $1", job.id
    )
    # cost > 0 because the default price for mock-small times the prompt estimate is positive.
    assert (outcome, flagged) == ("retryable", True) and cost > 0 and posting is not None
    assert await check_ledger(conn) == []
    json_lines(log)


async def test_sweeper_sigterm_exits_cleanly(database_url, conn, tmp_path):
    env = child_env(database_url, LLMGW_SWEEP_INTERVAL_S="0.1")
    async with child(tmp_path, env, "sweeper") as (proc, log):
        await asyncio.sleep(1)  # let it get through a few passes
        assert proc.returncode is None, log.read_text()
        proc.send_signal(signal.SIGTERM)
        async with asyncio.timeout(5):
            assert await proc.wait() == 0, log.read_text()
    json_lines(log)


async def test_gateway_sigterm_mid_stream_settles_the_hold(database_url, conn, team, mock, tmp_path):
    _, key = team
    port = free_port()
    env = child_env(database_url, LLMGW_UPSTREAM_URL=mock.url, LLMGW_HOST="127.0.0.1", LLMGW_PORT=str(port),
                    LLMGW_SHUTDOWN_GRACE_S="1")
    base = f"http://127.0.0.1:{port}"
    async with child(tmp_path, env, "gateway") as (proc, log), httpx.AsyncClient(base_url=base, timeout=30) as c:
        async def up():
            try:
                return (await c.get("/healthz")).status_code == 200
            except httpx.TransportError:
                assert proc.returncode is None, log.read_text()
                return False

        await until(up, what="the gateway to listen")

        async def stream():
            payload = {"model": "mock-small", "messages": [{"role": "user", "content": "hi"}], "stream": True,
                       "max_tokens": 400, "mock": {"tokens": 400, "delay_ms": 50}}
            with contextlib.suppress(httpx.HTTPError):
                async with c.stream("POST", "/v1/chat/completions", json=payload,
                                    headers={"authorization": f"Bearer {key}"}) as r:
                    async for _ in r.aiter_raw():
                        pass

        reader = asyncio.create_task(stream())

        async def streaming():
            return mock.state.sent_chunks >= 3

        await until(streaming, what="the stream to start")
        proc.send_signal(signal.SIGTERM)
        async with asyncio.timeout(10):
            assert await proc.wait() == 0, log.read_text()
        await reader

    # Closed by the gateway's own shutdown path, not left for the sweeper.
    [hold] = await conn.fetch("SELECT * FROM holds")
    assert hold["state"] == HoldState.SETTLED
    assert await conn.fetchval("SELECT flagged FROM postings WHERE id = $1", hold["close_posting_id"])
    assert await check_ledger(conn) == []
    lines = json_lines(log)
    # uvicorn's own records come through our JSON handler (log_config=None).
    assert any(line.get("logger", "").startswith("uvicorn") for line in lines), log.read_text()
