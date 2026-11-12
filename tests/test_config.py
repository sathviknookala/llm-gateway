"""Settings guards."""

import pytest

from llmgw.config import Settings

BASE = dict(database_url="postgresql://x", db_pool_min=1, db_pool_max=1)


def test_defaults_are_valid():
    s = Settings(**BASE)
    assert s.job_hold_grace_s == s.upstream_timeout_s + s.settle_timeout_s + s.job_lease_s + 60


@pytest.mark.parametrize(
    "bad",
    [
        {"job_heartbeat_s": 20, "job_lease_s": 30},  # heartbeat must be at most half the lease
        {"job_max_attempts": 0},
        {"job_lease_s": 0},
        {"worker_concurrency": 0},
        {"job_retry_base_s": 10, "job_retry_cap_s": 5},
        {"job_retry_base_s": -1},
        {"log_format": "xml"},
        {"loop_lag_interval_s": 0},
        {"shutdown_grace_s": -1},
    ],
)
def test_rejects_bad_job_settings(bad):
    with pytest.raises(ValueError):
        Settings(**BASE, **bad)
