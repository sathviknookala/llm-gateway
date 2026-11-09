"""Settings read from the environment."""

import os
from dataclasses import dataclass, field

from llmgw.pricing import Price, load_prices


@dataclass(frozen=True)
class Settings:
    database_url: str
    db_pool_min: int
    db_pool_max: int
    # Gateway
    host: str = "127.0.0.1"
    port: int = 8000
    upstream_url: str = "http://127.0.0.1:8100"
    upstream_api_key: str | None = None
    prices: dict[str, Price] = field(default_factory=lambda: load_prices(None))
    # Output cap forwarded upstream when the client sets none; the hold covers it.
    default_max_tokens: int = 1024
    # Open holds outlive the longest stream plus the settle, or the sweeper
    # would expire a hold whose stream is still running.
    hold_ttl_s: float = 900.0
    upstream_connect_timeout_s: float = 5.0
    # Whole-stream deadline; a stream past it is cut and settled on an estimate.
    upstream_timeout_s: float = 600.0
    settle_timeout_s: float = 5.0
    # Streams in flight per process; past it the gateway answers 429.
    max_inflight: int = 256

    def __post_init__(self) -> None:
        if self.upstream_timeout_s + self.settle_timeout_s >= self.hold_ttl_s:
            raise ValueError("hold_ttl_s must exceed upstream_timeout_s + settle_timeout_s")


def load() -> Settings:
    env = os.environ.get
    return Settings(
        database_url=env("LLMGW_DATABASE_URL", "postgresql://llmgw:llmgw@localhost:5432/llmgw"),
        db_pool_min=int(env("LLMGW_DB_POOL_MIN", "1")),
        db_pool_max=int(env("LLMGW_DB_POOL_MAX", "10")),
        host=env("LLMGW_HOST", "127.0.0.1"),
        port=int(env("LLMGW_PORT", "8000")),
        upstream_url=env("LLMGW_UPSTREAM_URL", "http://127.0.0.1:8100"),
        upstream_api_key=env("LLMGW_UPSTREAM_API_KEY") or None,
        prices=load_prices(env("LLMGW_PRICES")),
        default_max_tokens=int(env("LLMGW_DEFAULT_MAX_TOKENS", "1024")),
        hold_ttl_s=float(env("LLMGW_HOLD_TTL_S", "900")),
        upstream_connect_timeout_s=float(env("LLMGW_UPSTREAM_CONNECT_TIMEOUT_S", "5")),
        upstream_timeout_s=float(env("LLMGW_UPSTREAM_TIMEOUT_S", "600")),
        settle_timeout_s=float(env("LLMGW_SETTLE_TIMEOUT_S", "5")),
        max_inflight=int(env("LLMGW_MAX_INFLIGHT", "256")),
    )
