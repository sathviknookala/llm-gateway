"""Test database: one throwaway Postgres per session, migrations applied in
version order with plain asyncpg (no Flyway), tables truncated per test.
Redis: one throwaway redis:7 per session (started only by tests that use it),
flushed per test."""

import asyncio
import re
from pathlib import Path

import asyncpg
import pytest
import redis.asyncio as aioredis
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "db" / "migrations"
TABLES = ("api_keys", "holds", "entries", "postings", "balances", "accounts", "teams", "orgs")
_VERSIONED = re.compile(r"^V(\d+)__.+\.sql$")


def migration_files() -> list[Path]:
    found = []
    for path in MIGRATIONS_DIR.iterdir():
        m = _VERSIONED.match(path.name)
        if m:
            found.append((int(m.group(1)), path))
    return [path for _, path in sorted(found)]


async def _migrate(url: str) -> None:
    conn = await asyncpg.connect(url)
    try:
        for path in migration_files():
            async with conn.transaction():
                await conn.execute(path.read_text())
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def database_url():
    with PostgresContainer("postgres:17-alpine", driver=None) as pg:
        url = pg.get_connection_url()
        asyncio.run(_migrate(url))
        yield url


@pytest.fixture
async def conn(database_url):
    """A fresh, empty ledger. Autocommit unless the test opens a transaction."""
    c = await asyncpg.connect(database_url)
    await c.execute(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE")
    try:
        yield c
    finally:
        await c.close()


@pytest.fixture
async def pool(database_url, conn):
    """Pool for concurrency tests. Depends on `conn` so the tables are truncated first."""
    p = await asyncpg.create_pool(database_url, min_size=1, max_size=8)
    try:
        yield p
    finally:
        await p.close()


@pytest.fixture(scope="session")
def redis_url():
    with RedisContainer("redis:7-alpine") as r:
        yield f"redis://{r.get_container_host_ip()}:{r.get_exposed_port(6379)}/0"


@pytest.fixture
async def redis_client(redis_url):
    """A client on an empty Redis."""
    c = aioredis.from_url(redis_url)
    await c.flushdb()
    try:
        yield c
    finally:
        await c.aclose()
