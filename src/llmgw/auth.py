"""Team API keys. Only a SHA-256 hash is stored; the plaintext is returned once.

Keys are 32 random bytes, so a plain SHA-256 (no salt, no slow hash) is enough:
there is nothing to brute-force, and the lookup is one indexed equality.
"""

import hashlib
import secrets

import asyncpg

from llmgw.db import require_transaction

KEY_PREFIX = "llmgw_"


def hash_key(key: str) -> bytes:
    return hashlib.sha256(key.encode()).digest()


async def create_api_key(conn: asyncpg.Connection, team_id: int) -> str:
    """Create a key for the team and return its plaintext (not stored)."""
    require_transaction(conn)
    key = KEY_PREFIX + secrets.token_urlsafe(32)
    await conn.execute(
        "INSERT INTO api_keys (team_id, key_hash, prefix) VALUES ($1, $2, $3)",
        team_id, hash_key(key), key[: len(KEY_PREFIX) + 6],
    )
    return key


async def revoke_api_key(conn: asyncpg.Connection, key: str) -> bool:
    status = await conn.execute(
        "UPDATE api_keys SET revoked_at = now() WHERE key_hash = $1 AND revoked_at IS NULL",
        hash_key(key),
    )
    return status == "UPDATE 1"


async def team_for_key(conn: asyncpg.Connection | asyncpg.Pool, key: str) -> int | None:
    """The team that owns an active key, or None."""
    return await conn.fetchval(
        "SELECT team_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        hash_key(key),
    )
