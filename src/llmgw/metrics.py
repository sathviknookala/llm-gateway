"""Prometheus metrics, defined once on the default registry.

One process per container, so no multiprocess mode. The gateway serves them
at `GET /metrics`; the worker and sweeper start `serve(port)` (a daemon thread
that never touches the event loop) when `LLMGW_METRICS_PORT` is set.

Counters are bumped outside transactions: after a commit, or in the `except`
that gave up. Gauges named after database state (jobs by state, open holds,
retry loss, ledger violations) are set by the sweeper from `llmgw.stats`.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# --- every mode ---
EVENT_LOOP_LAG = Histogram(
    "llmgw_event_loop_lag_seconds",
    "How late the loop-lag monitor's sleep woke up",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

# --- gateway ---
HTTP_REQUESTS = Counter("llmgw_http_requests_total", "Gateway responses", ["route", "status"])
REFUSALS = Counter("llmgw_refusals_total", "Requests refused before a hold or enqueue", ["code"])
ADMISSION = Histogram(
    "llmgw_admission_seconds",
    "Chat request arrival to the upstream send: auth, parse, rate limits, hold",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
INFLIGHT = Gauge("llmgw_inflight_streams", "Chat streams in flight in this process")

# --- gateway and worker ---
HOLD_CLOSES = Counter("llmgw_hold_closes_total", "Holds closed", ["path", "op", "flagged"])
HOLD_CLOSE_FAILURES = Counter("llmgw_hold_close_failures_total", "Closes given up on (left to the sweeper)", ["path"])
UPSTREAM_ERRORS = Counter("llmgw_upstream_errors_total", "Provider calls that didn't finish normally", ["path", "kind"])
RATELIMIT_UNAVAILABLE = Counter(
    "llmgw_ratelimit_unavailable_total", "Redis calls that failed open", ["op"]
)

# --- worker ---
JOB_ATTEMPTS = Counter("llmgw_job_attempts_total", "Finished job attempts", ["outcome"])
JOB_FINISH_FAILURES = Counter("llmgw_job_finish_failures_total", "Finishes given up on (lease left to expire)")
JOB_QUEUE_WAIT = Histogram(
    "llmgw_job_queue_wait_seconds",
    "Ready (run_after) to leased, on the database clock",
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900),
)
LEASE_LOST = Counter("llmgw_lease_lost_total", "Attempts cut short because the lease was lost")

# --- sweeper ---
SWEEPER_ACTIONS = Counter("llmgw_sweeper_actions_total", "Rows the sweeper acted on", ["action"])
SWEEP_PASS_FAILURES = Counter("llmgw_sweep_pass_failures_total", "Sweeper passes that raised")
JOBS = Gauge("llmgw_jobs", "Jobs by state", ["state"])
JOB_OLDEST_READY = Gauge("llmgw_job_oldest_ready_seconds", "Age of the oldest job ready to run (0 if none)")
HOLDS_OPEN = Gauge("llmgw_holds_open", "Open holds", ["kind"])
HOLD_OLDEST_OPEN = Gauge("llmgw_hold_oldest_open_seconds", "Age of the oldest open hold (0 if none)", ["kind"])
RETRY_LOSS = Gauge("llmgw_retry_loss_microcredits", "Total booked to retry_loss accounts")
FLAGGED_POSTINGS = Gauge("llmgw_flagged_postings", "Postings flagged for reconciliation")
LEDGER_VIOLATIONS = Gauge("llmgw_ledger_violations", "Problems found by the last check_ledger run")


def serve(port: int, addr: str = "0.0.0.0") -> None:
    """Expose the default registry on a side port (worker, sweeper)."""
    start_http_server(port, addr=addr)
