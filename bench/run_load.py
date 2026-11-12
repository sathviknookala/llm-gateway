"""Stated load test against the local Compose stack. Test traffic against the mock provider.

    uv run python bench/run_load.py --rate 200 --duration 60s [--job-rate 100] [--workers 2]

1. Brings up the stack (`--profile stack`) with N workers and waits for /readyz.
2. Seeds an org with 4 teams.
3. Sync baseline: k6 straight at the mock provider. Sync: the same load through the gateway.
4. Jobs: k6 submits at --job-rate; waits for the queue to drain.
5. Runs `llmgw check-ledger`, reads p95/p99 from Prometheus, writes
   bench/results/<UTC time>-load.md (gitignored) and prints it.

Gateway-added latency = gateway p50/p95 minus baseline p50/p95 for the same
load; the gateway's own admission histogram is reported next to it as a
cross-check. Event-loop lag p99 is reported alongside, so a high p95 can be
told apart from a starved loop.
"""

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results"
COMPOSE = ["docker", "compose", "--profile", "stack", "--profile", "bench"]


def sh(*args: str, capture: bool = True, check: bool = True) -> str:
    r = subprocess.run(args, cwd=ROOT, capture_output=capture, text=True)
    if check and r.returncode != 0:
        sys.exit(f"failed: {' '.join(args)}\n{r.stdout}\n{r.stderr}")
    return r.stdout if capture else ""


def psql(sql: str) -> str:
    return sh(*COMPOSE, "exec", "-T", "postgres", "psql", "-U", "llmgw", "-tAc", sql).strip()


def prom(query: str) -> float | None:
    r = httpx.get("http://127.0.0.1:9090/api/v1/query", params={"query": query}, timeout=10).json()
    result = r["data"]["result"]
    if not result:
        return None
    value = float(result[0]["value"][1])
    return None if value != value else value  # NaN → None


def k6(script: str, tag: str, **env: str) -> dict:
    out = f"/bench/results/{tag}.json"
    args = [*COMPOSE, "run", "--rm", "--user", f"{os.getuid()}:{os.getgid()}", "k6", "run", "--quiet", "--summary-export", out]
    for k, v in env.items():
        args += ["-e", f"{k}={v}"]
    sh(*args, f"/bench/k6/{script}", capture=False, check=False)
    return json.loads((RESULTS / f"{tag}.json").read_text())


def trend(summary: dict, name: str, stat: str) -> float:
    return summary["metrics"][name][stat]


def wait_ready(port: int, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{port}/readyz", timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(1)
    sys.exit("gateway never became ready")


def wait_drained(timeout: float) -> float:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        live = int(psql("SELECT count(*) FROM jobs WHERE state IN ('queued', 'running')"))
        held = int(psql("SELECT count(*) FROM holds h WHERE state = 'held'"
                        " AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.hold_id = h.id)"))
        if live == 0 and held == 0:
            return time.monotonic()
        time.sleep(1)
    sys.exit("queue did not drain")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=200, help="sync requests/s")
    ap.add_argument("--job-rate", type=int, default=100, help="job submissions/s")
    ap.add_argument("--duration", default="60s")
    ap.add_argument("--tokens", type=int, default=64)
    ap.add_argument("--delay-ms", type=int, default=0)
    ap.add_argument("--workers", type=int, default=2)
    args = ap.parse_args()
    port = int(os.environ.get("LLMGW_GATEWAY_PORT", "8000"))
    seconds = int(args.duration.rstrip("s"))
    RESULTS.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    sh(*COMPOSE, "up", "-d", "--build", "--scale", f"worker={args.workers}", capture=False)
    wait_ready(port)
    seed = json.loads(sh(*COMPOSE, "exec", "-T", "gateway", "llmgw", "seed", "--org", f"load-{stamp}",
                         "--teams", "4", "--fund", str(10**13), "--grant", str(2 * 10**12)))
    keys = ",".join(t["key"] for t in seed["teams"])
    load = dict(RATE=str(args.rate), DURATION=args.duration, TOKENS=str(args.tokens), DELAY_MS=str(args.delay_ms))

    base = k6("sync.js", f"{stamp}-sync-baseline", TARGET="http://mock-provider:8100", **load)
    time.sleep(5)
    gw = k6("sync.js", f"{stamp}-sync-gateway", TARGET="http://gateway:8000", KEYS=keys, **load)
    time.sleep(10)  # one Prometheus scrape past the end
    window = f"{seconds + 10}s"
    admission_p95 = prom(f"histogram_quantile(0.95, sum by (le) (increase(llmgw_admission_seconds_bucket[{window}])))")
    gw_lag_p99 = prom("histogram_quantile(0.99, sum by (le) (increase("
                      f"llmgw_event_loop_lag_seconds_bucket{{job=\"gateway\"}}[{window}])))")
    wait_drained(120)

    succeeded0 = int(psql("SELECT count(*) FROM jobs WHERE state = 'succeeded'"))
    t0 = time.monotonic()
    jobs = k6("jobs.js", f"{stamp}-jobs", TARGET="http://gateway:8000", KEYS=keys,
              RATE=str(args.job_rate), DURATION=args.duration, TOKENS=str(args.tokens),
              DELAY_MS=str(args.delay_ms))
    t1 = wait_drained(600)
    time.sleep(10)
    jwindow = f"{int(t1 - t0) + 10}s"
    succeeded = int(psql("SELECT count(*) FROM jobs WHERE state = 'succeeded'")) - succeeded0
    not_ok = int(psql("SELECT count(*) FROM jobs WHERE state IN ('failed', 'dead')"))
    wait_p95 = prom(f"histogram_quantile(0.95, sum by (le) (increase(llmgw_job_queue_wait_seconds_bucket[{jwindow}])))")
    wait_p50 = prom(f"histogram_quantile(0.5, sum by (le) (increase(llmgw_job_queue_wait_seconds_bucket[{jwindow}])))")
    worker_lag_p99 = prom("histogram_quantile(0.99, sum by (le) (increase("
                          f"llmgw_event_loop_lag_seconds_bucket{{job=\"worker\"}}[{jwindow}])))")

    ledger = subprocess.run([*COMPOSE, "exec", "-T", "gateway", "llmgw", "check-ledger"], cwd=ROOT,
                            capture_output=True, text=True)

    def ms(v: float | None) -> str:
        return "n/a" if v is None else f"{v * 1000:.1f} ms"

    b50, b95 = trend(base, "http_req_duration", "med"), trend(base, "http_req_duration", "p(95)")
    g50, g95 = trend(gw, "http_req_duration", "med"), trend(gw, "http_req_duration", "p(95)")
    report = f"""# Load test {stamp}

Test traffic against the mock provider (local Compose stack), not a real provider.

## Setup
- Machine: {platform.processor() or platform.machine()}, {os.cpu_count()} CPUs; {platform.system()} {platform.release()}
- Python 3.13 in the image; gateway on uvloop, worker and sweeper on the default asyncio loop
- One process per container; 1 gateway, {args.workers} workers (concurrency 8 each), 1 sweeper
- Pools: gateway 20, worker 10, sweeper 4; rate limits not configured (no buckets)
- Mock reply: {args.tokens} tokens, {args.delay_ms} ms per token; k6 0.55.0 constant arrival rate, {args.duration}

## Synchronous path at {args.rate} req/s
| | p50 | p95 | p99 | failed checks |
|---|---|---|---|---|
| Baseline (k6 → mock) | {b50:.1f} ms | {b95:.1f} ms | {trend(base, "http_req_duration", "p(99)"):.1f} ms | {base["metrics"]["checks"]["fails"]} |
| Through the gateway | {g50:.1f} ms | {g95:.1f} ms | {trend(gw, "http_req_duration", "p(99)"):.1f} ms | {gw["metrics"]["checks"]["fails"]} |
| **Added by the gateway** | **{g50 - b50:.1f} ms** | **{g95 - b95:.1f} ms** | | |

- Requests: {int(gw["metrics"]["http_reqs"]["count"])} through the gateway ({gw["metrics"]["http_reqs"]["rate"]:.1f}/s achieved)
- Gateway admission p95 (auth, parse, hold commit; its own histogram): {ms(admission_p95)}
- Gateway event-loop lag p99 during the run: {ms(gw_lag_p99)}

## Asynchronous path at {args.job_rate} submissions/s
- Submitted: {int(jobs["metrics"]["http_reqs"]["count"])}, failed submit checks: {jobs["metrics"]["checks"]["fails"]}
- Succeeded: {succeeded} in {t1 - t0:.1f} s from first submit to drained ({succeeded / (t1 - t0):.1f} jobs/s); failed or dead: {not_ok}
- Queue wait (ready → leased): p50 {ms(wait_p50)}, p95 {ms(wait_p95)}
- Worker event-loop lag p99: {ms(worker_lag_p99)}

## Ledger
`llmgw check-ledger` after both runs: exit {ledger.returncode}, {ledger.stderr.strip()}
"""
    (RESULTS / f"{stamp}-load.md").write_text(report)
    print(report)
    sys.exit(ledger.returncode)


if __name__ == "__main__":
    main()
