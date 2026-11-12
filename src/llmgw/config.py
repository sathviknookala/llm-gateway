"""Settings read from the environment."""

import os
import socket
from dataclasses import dataclass, field

from llmgw.pricing import Price, load_prices
from llmgw.ratelimit import RateLimits, parse_provider_limits


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
    # Rate limits (Redis). No redis_url → limiter off.
    redis_url: str | None = None
    redis_timeout_s: float = 0.25
    rate_limits: RateLimits = field(default_factory=RateLimits)
    # Sweeper
    sweep_interval_s: float = 5.0
    sweep_batch: int = 500
    # Job queue
    job_max_attempts: int = 5
    # A job not finished by its deadline is dead-lettered and its hold released.
    job_ttl_s: float = 86_400.0
    job_lease_s: float = 30.0
    job_heartbeat_s: float = 10.0
    # Backoff before retry n: uniform(0, min(cap, base * 2**(n-1))), at least retry-after.
    job_retry_base_s: float = 1.0
    job_retry_cap_s: float = 60.0
    worker_concurrency: int = 8
    worker_poll_s: float = 1.0
    worker_id: str = field(default_factory=lambda: f"{socket.gethostname()}:{os.getpid()}")
    # Observability
    log_format: str = "console"
    log_level: str = "info"
    # Worker and sweeper metrics side port; None = off (the gateway serves /metrics itself).
    metrics_port: int | None = None
    metrics_host: str = "0.0.0.0"
    loop_lag_interval_s: float = 0.5
    # Sweeper: how often to run check_ledger and export the violation count; 0 = off.
    ledger_check_interval_s: float = 300.0
    # Gateway: on SIGTERM, how long streams get before uvicorn cancels them.
    shutdown_grace_s: float = 20.0

    def __post_init__(self) -> None:
        if self.log_format not in ("console", "json"):
            raise ValueError("log_format must be 'console' or 'json'")
        if self.upstream_timeout_s + self.settle_timeout_s >= self.hold_ttl_s:
            raise ValueError("hold_ttl_s must exceed upstream_timeout_s + settle_timeout_s")
        positive = (
            self.job_max_attempts, self.job_ttl_s, self.job_lease_s, self.job_heartbeat_s,
            self.worker_concurrency, self.worker_poll_s, self.loop_lag_interval_s,
        )
        if any(x <= 0 for x in positive):
            raise ValueError("job and worker settings must be positive")
        if self.ledger_check_interval_s < 0 or self.shutdown_grace_s < 0:
            raise ValueError("ledger_check_interval_s and shutdown_grace_s must be non-negative")
        if self.job_retry_base_s < 0 or self.job_retry_cap_s < self.job_retry_base_s:
            raise ValueError("job_retry_cap_s must be at least job_retry_base_s, both non-negative")
        if self.job_heartbeat_s * 2 > self.job_lease_s:
            raise ValueError("job_heartbeat_s must be at most half of job_lease_s")

    @property
    def job_hold_grace_s(self) -> float:
        """How long a job's hold outlives its deadline: one last attempt plus its finish.

        The hold sweeper skips job holds, so this only bounds the hold on paper;
        the job paths (finish, reclaim, deadline) are what close it.
        """
        return self.upstream_timeout_s + self.settle_timeout_s + self.job_lease_s + 60.0


def _opt_int(raw: str | None) -> int | None:
    return int(raw) if raw else None


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
        redis_url=env("LLMGW_REDIS_URL") or None,
        redis_timeout_s=float(env("LLMGW_REDIS_TIMEOUT_S", "0.25")),
        rate_limits=RateLimits(
            team_tpm=_opt_int(env("LLMGW_TEAM_TPM")),
            team_rpm=_opt_int(env("LLMGW_TEAM_RPM")),
            provider=parse_provider_limits(env("LLMGW_PROVIDER_LIMITS")),
        ),
        sweep_interval_s=float(env("LLMGW_SWEEP_INTERVAL_S", "5")),
        sweep_batch=int(env("LLMGW_SWEEP_BATCH", "500")),
        job_max_attempts=int(env("LLMGW_JOB_MAX_ATTEMPTS", "5")),
        job_ttl_s=float(env("LLMGW_JOB_TTL_S", "86400")),
        job_lease_s=float(env("LLMGW_JOB_LEASE_S", "30")),
        job_heartbeat_s=float(env("LLMGW_JOB_HEARTBEAT_S", "10")),
        job_retry_base_s=float(env("LLMGW_JOB_RETRY_BASE_S", "1")),
        job_retry_cap_s=float(env("LLMGW_JOB_RETRY_CAP_S", "60")),
        worker_concurrency=int(env("LLMGW_WORKER_CONCURRENCY", "8")),
        worker_poll_s=float(env("LLMGW_WORKER_POLL_S", "1")),
        worker_id=env("LLMGW_WORKER_ID") or f"{socket.gethostname()}:{os.getpid()}",
        log_format=env("LLMGW_LOG_FORMAT", "console"),
        log_level=env("LLMGW_LOG_LEVEL", "info"),
        metrics_port=_opt_int(env("LLMGW_METRICS_PORT")),
        metrics_host=env("LLMGW_METRICS_HOST", "0.0.0.0"),
        loop_lag_interval_s=float(env("LLMGW_LOOP_LAG_INTERVAL_S", "0.5")),
        ledger_check_interval_s=float(env("LLMGW_LEDGER_CHECK_INTERVAL_S", "300")),
        shutdown_grace_s=float(env("LLMGW_SHUTDOWN_GRACE_S", "20")),
    )
