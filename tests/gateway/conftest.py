"""Gateway fixtures: the mock provider and the gateway, each on a real uvicorn
server on a local port, so client disconnects and upstream cuts are real
socket events rather than simulated ones."""

import asyncio
import socket
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
import pytest
import uvicorn

from llmgw import auth, mock_provider
from llmgw.config import Settings
from llmgw.gateway.app import Gateway, create_app
from llmgw.ledger import accounts, postings
from llmgw.ledger.models import OrgAccounts, TeamAccounts
from llmgw.mock_provider import MockState

TEAM_GRANT = 1_000_000


@asynccontextmanager
async def serve(app):
    """Run an ASGI app on 127.0.0.1:<free port> in this event loop; yield its base URL."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="warning", timeout_graceful_shutdown=1)
    )
    # _serve skips uvicorn's signal-handler capture, which a test must not install.
    task = asyncio.create_task(server._serve(sockets=[sock]))
    while not server.started:
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    try:
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        server.should_exit = True
        await task
        sock.close()


@dataclass
class Mock:
    url: str
    state: MockState


@dataclass
class Env:
    url: str
    key: str
    gw: Gateway
    org: OrgAccounts
    team: TeamAccounts
    client: httpx.AsyncClient


@pytest.fixture
async def mock():
    app = mock_provider.create_app()
    async with serve(app) as url:
        yield Mock(url, app.state.mock)


def make_settings(database_url: str, upstream_url: str, **overrides) -> Settings:
    fields = dict(
        database_url=database_url,
        db_pool_min=1,
        db_pool_max=8,
        upstream_url=upstream_url,
        upstream_connect_timeout_s=1,
        upstream_timeout_s=10,
        settle_timeout_s=3,
        hold_ttl_s=60,
    )
    return Settings(**{**fields, **overrides})


@pytest.fixture
async def funded(conn):
    """An org, team A granted TEAM_GRANT micro-credits, and an API key for A."""
    async with conn.transaction():
        org = await accounts.create_org(conn, "org")
        await postings.fund_org(conn, org_id=org.org_id, amount=10 * TEAM_GRANT, idempotency_key="fund")
        team = await accounts.create_team(conn, org.org_id, "A")
        await postings.grant(conn, team_id=team.team_id, amount=TEAM_GRANT, idempotency_key="grant")
        key = await auth.create_api_key(conn, team.team_id)
    return org, team, key


@asynccontextmanager
async def gateway_env(database_url, pool, upstream_url, funded, *, limiter=None, **overrides):
    org, team, key = funded
    settings = make_settings(database_url, upstream_url, **overrides)
    async with httpx.AsyncClient(timeout=httpx.Timeout(10, connect=1)) as upstream:
        app = create_app(settings, pool=pool, http=upstream, limiter=limiter)
        async with serve(app) as url:
            async with httpx.AsyncClient(
                base_url=url, headers={"authorization": f"Bearer {key}"}, timeout=10
            ) as client:
                yield Env(url, key, app.state.gateway, org, team, client)


@pytest.fixture
async def env(database_url, pool, mock, funded):
    async with gateway_env(database_url, pool, mock.url, funded) as e:
        yield e
