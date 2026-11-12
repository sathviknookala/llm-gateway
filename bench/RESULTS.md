# Stated results

All numbers here are **test traffic against the mock provider** on one machine, not a real provider and not real users. They measure the gateway's own cost and its behaviour under faults; provider latency is taken out by design (the mock replies in about a millisecond).

Reproduce with `uv run python bench/run_load.py` and `uv run python bench/faults.py` (see the README). Raw output lands in `bench/results/` (gitignored).

## Setup

| | |
|---|---|
| Machine | AMD Ryzen 9 7950X (16 cores, 32 threads), 124 GB RAM, Linux 6.8; everything in Docker Compose on the same host |
| Runtime | Python 3.13 (`python:3.13-slim`); gateway on uvicorn with uvloop, worker and sweeper on the default asyncio loop |
| Processes | One process per container: 1 gateway, 2 workers (concurrency 8 each), 1 sweeper, 1 mock provider |
| Pools | Postgres connections: gateway 20, each worker 10, sweeper 4. Gateway `max_inflight` 256 |
| Store | Postgres 17, Redis 7 (rate limits configured but no buckets set in the load runs, so no Redis calls) |
| Load | k6 0.55.0, constant arrival rate; each reply is 64 tokens with no per-token delay |

## Load (2026-11-12)

### Stated run: 200 streams/s and 200 jobs/s, 60 s each

| Synchronous path | p50 | p95 | p99 |
|---|---|---|---|
| Baseline: k6 straight to the mock | 1.2 ms | 1.5 ms | 1.6 ms |
| Through the gateway | 3.4 ms | 4.0 ms | 6.6 ms |
| **Added by the gateway** | **2.2 ms** | **2.5 ms** | |

- 12,000 streams, 200.0/s achieved, 0 failed checks (every response was 200 and carried a usage block).
- The gateway's own admission histogram (auth, parse, hold commit, up to the upstream send) agrees: p95 2.4 ms.
- Gateway event-loop lag p99 during the run: 1.0 ms, so the p95 above measures the design, not a starved loop.

| Asynchronous path | |
|---|---|
| Submitted | 12,000 at 200/s, all accepted (202) |
| Finished | 12,000 succeeded, 0 failed or dead; drained 61.8 s after the first submit (194 jobs/s) |
| Queue wait, ready → leased | p50 479 ms, p95 948 ms |
| Worker event-loop lag p99 | 4.7 ms |

Queue wait is dominated by the worker's idle poll (`LLMGW_WORKER_POLL_S`, 1 s): a loop that finds nothing sleeps for a second, so a job arriving into an idle loop waits up to that long. Throughput kept pace with submissions.

`llmgw check-ledger` afterwards: **0 problems**.

### Where it bends

The same synchronous load at other rates (30–60 s runs, same setup):

| Rate | Gateway-added p50 | Gateway-added p95 | Admission p95 | Loop lag p99 | Refused |
|---|---|---|---|---|---|
| 100/s | 2.6 ms | 2.9 ms | 2.4 ms | 4.5 ms | 0 |
| 200/s | 2.2 ms | 2.5 ms | 2.4 ms | 1.0 ms | 0 |
| 300/s | 4.7 ms | 75.7 ms | 68.4 ms | 2.2 ms | 0 |
| 500/s | 394 ms | 1,512 ms | ≥ 1 s | 5.0 ms | 35% (429 `capacity`) |

Past about 250/s, admission time climbs while event-loop lag stays low, which points at waiting for one of the gateway's 20 Postgres connections (a stream uses the pool three times: key lookup, hold, close) rather than CPU. At 500/s the gateway reached `max_inflight` (256) and refused the excess with 429 `capacity` instead of queueing without limit, as designed; one `/readyz` probe timed out on the busy pool. `check-ledger` reported 0 problems after every run, including the overloaded one. Raising the pool, caching key lookups, or running more gateway processes would move this point; none of that is measured here.

## Faults (2026-11-12)

`bench/faults.py --seed 42 --duration 300` on the Compose stack with `docker-compose.faults.yml`: toxiproxy in front of Postgres, Redis and the provider; 1 gateway, 4 workers, 1 sweeper; hold TTL 40 s, upstream timeout 20 s, lease 6 s, sweep every 2 s.

**Traffic:** 16 streaming clients and 4 job clients for 300 s. Each request picks a mock behaviour: normal (50%), cut mid-stream (10%), no usage block (10%), provider 503 / 429 / 400 (5% each), a 4 s stream (15%), or a 30 s stream that outlasts the 20 s upstream timeout (3%).

**Injected faults** (21, seeded schedule every 5–12 s):

| Fault | Count |
|---|---|
| SIGKILL the gateway, then restart | 3 |
| SIGKILL every worker, then restart | 4 |
| SIGTERM the gateway (graceful), then restart | 3 |
| SIGTERM every worker (graceful), then restart | 3 |
| Redis unreachable for 10 s (limiter fails open) | 5 |
| Postgres unreachable for 2 s (every app connection dropped) | 3 |

**Result: pass.** After traffic stopped and faults healed, everything settled in 78 s, and:

- `check_ledger`: **0 problems** (balances match entries, every posting sums to zero, held balances match open holds, every job's hold matches its state).
- **2,749 streams read to the end with a usage block: all 2,749 charged exactly their reported usage**, unflagged. None needed the expired-hold allowance.
- Sync holds: 4,380 created for 4,379 admitted responses, plus 2,206 requests that got no response at all (gateway down or killed; most were clients retrying a dead port). 3,692 settled, 640 released, 48 expired by the sweeper (their close was lost: a gateway SIGKILL mid-stream, or a Postgres cut longer than the close's retries). None left open.
- Every provider error that must release (429, passthrough 4xx; 429 of them) was covered by released or expired holds; settled holds stayed within the 200s, timeouts and cut-offs that may settle.
- 1,020 jobs accepted: 729 succeeded, 50 failed (provider 400), 241 dead. The dead ones are the bodies that fail every attempt (always 503, always 429, always cut, always slower than the timeout), which run out of their 5 attempts. Every succeeded job's hold is settled; nothing was left queued or running.
- Billed failed attempts went to retry loss (306,942 micro-credits in total on this database, earlier runs included); 4,212 postings in this run were flagged for reconciliation.

**Not covered:** a provider that slows down mid-run through the network (a toxiproxy latency toxic added under live streams wedged toxiproxy 2.9.0's control API, so slow providers come from the mock instead), a SIGKILL timed exactly inside a commit (kills land at random points; the window is narrow), and anything multi-host.
