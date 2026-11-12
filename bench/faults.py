"""Fault test: traffic plus injected crashes and outages, then the books must still balance.

    uv run python bench/faults.py --seed 42 --duration 300

Runs against the Compose stack with the faults overlay (toxiproxy in front of
Postgres, Redis and the provider; short timeouts). Test traffic against the
mock provider. While sync streams and jobs run with mock faults mixed in
(cuts, missing usage, 503/429/400, 4 s streams, and 30 s streams that outlast
the 20 s upstream timeout), a seeded schedule injects:

  kill_worker / kill_gateway   SIGKILL, then start again
  term_worker / term_gateway   SIGTERM (graceful path), then start again
  redis_cut                    Redis proxy disabled for 10 s (limiter fails open)
  pg_cut                       Postgres proxy disabled for 2 s: every app
                               connection dropped, new ones refused

Toxiproxy is only switched on and off: a latency toxic added under live
streams wedged its control API (2.9.0), so slow providers come from the mock.

Then traffic stops, faults heal, and once nothing is held or running it checks:
  - check_ledger() == []
  - every job is terminal, and every succeeded job's hold is settled
  - a stream the client read to the end with a usage block was charged exactly
    that usage, unflagged; or its hold expired because the close was lost to a
    crash or outage (counted as under-charge awaiting reconciliation)
  - admitted ≤ sync holds created ≤ admitted + aborted + errored, where refused
    requests (gateway pre-hold codes) must not have placed a hold
  - every sync hold is closed, and released + expired covers every response that
    must have released (provider unreachable, provider 429, passthrough 4xx)
  - settled ≤ 200s + upstream_timeout + upstream_error + aborted + errored

Exit 1 on any failed check. Writes bench/results/<UTC time>-faults.md.
"""

import argparse
import asyncio
import json
import random
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import httpx
import orjson

from llmgw.ledger.checks import check_ledger
from llmgw.pricing import DEFAULT_PRICES, cost

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results"
COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.faults.yml", "--profile", "stack"]
DB = "postgresql://llmgw:llmgw@127.0.0.1:5432/llmgw"
TOXI = "http://127.0.0.1:8474"
PRICE = DEFAULT_PRICES["mock-small"]

# Gateway codes for errors before any hold.
REFUSED = {
    "invalid_api_key", "invalid_request", "invalid_json", "model_not_found", "stream_not_supported",
    "stream_required", "capacity", "request_exceeds_rate_limit", "team_rate_limited",
    "provider_budget_exhausted", "insufficient_credits",
}
MUST_RELEASE = {"upstream_unreachable", "upstream_rate_limited", "passthrough_4xx"}
FAULTS = ["kill_worker", "kill_gateway", "term_worker", "term_gateway", "redis_cut", "pg_cut"]
MOCKS = [  # (weight, mock body)
    (50, {"tokens": 24}),
    (10, {"tokens": 40, "cut_after": 10}),
    (10, {"tokens": 24, "no_usage": True}),
    (5, {"status": 503}),
    (5, {"status": 429}),
    (5, {"status": 400}),
    (15, {"tokens": 100, "delay_ms": 40}),  # 4 s streams: crashes land mid-stream
    (3, {"tokens": 30, "delay_ms": 1000}),  # 30 s: past the 20 s upstream timeout
]


def compose(*args: str, check: bool = True) -> str:
    r = subprocess.run([*COMPOSE, *args], cwd=ROOT, capture_output=True, text=True)
    if check and r.returncode != 0:
        sys.exit(f"failed: {' '.join(args)}\n{r.stdout}\n{r.stderr}")
    return r.stdout


@dataclass
class Tally:
    outcomes: Counter = field(default_factory=Counter)  # by classification
    complete: list[tuple[str, int, int]] = field(default_factory=list)  # (request_id, prompt, completion)
    jobs: list[int] = field(default_factory=list)
    job_submits: Counter = field(default_factory=Counter)
    injected: Counter = field(default_factory=Counter)


def classify(status: int, body: bytes) -> str:
    try:
        err = orjson.loads(body).get("error", {})
    except (orjson.JSONDecodeError, AttributeError):
        return "errored"  # the gateway's own 500 (e.g. Postgres down): hold unknown
    code = err.get("code") if isinstance(err, dict) else None
    if code:
        return code
    if 400 <= status < 500:
        return "passthrough_4xx"  # the mock's own error body
    return "errored"


async def sync_loop(rng: random.Random, client: httpx.AsyncClient, keys: list[str], stop: asyncio.Event,
                    t: Tally) -> None:
    weights, mocks = zip(*MOCKS)
    while not stop.is_set():
        mock = rng.choices(mocks, weights)[0]
        payload = {"model": "mock-small", "messages": [{"role": "user", "content": "fault test"}],
                   "stream": True, "max_tokens": 120, "mock": mock}
        try:
            async with client.stream("POST", "/v1/chat/completions", json=payload,
                                     headers={"authorization": f"Bearer {rng.choice(keys)}"}) as r:
                if r.status_code != 200:
                    t.outcomes[classify(r.status_code, await r.aread())] += 1
                    continue
                t.outcomes["ok_200"] += 1
                rid = r.headers["x-llmgw-request-id"]
                body = b""
                try:
                    async for chunk in r.aiter_raw():
                        body += chunk
                except httpx.HTTPError:
                    continue  # cut mid-stream (gateway or provider): admitted, not complete
                usage = _usage(body)
                if usage and b"data: [DONE]" in body:
                    t.complete.append((rid, *usage))
        except httpx.HTTPError:
            t.outcomes["aborted"] += 1  # no response at all (gateway down or killed)
            await asyncio.sleep(0.2)


def _usage(body: bytes) -> tuple[int, int] | None:
    for line in body.split(b"\n"):
        if line.startswith(b"data: {"):
            u = orjson.loads(line[6:]).get("usage")
            if u:
                return u["prompt_tokens"], u["completion_tokens"]
    return None


async def job_loop(rng: random.Random, client: httpx.AsyncClient, keys: list[str], stop: asyncio.Event,
                   t: Tally) -> None:
    weights, mocks = zip(*MOCKS)
    while not stop.is_set():
        mock = rng.choices(mocks, weights)[0]
        payload = {"model": "mock-small", "messages": [{"role": "user", "content": "fault job"}],
                   "max_tokens": 120, "mock": mock}
        try:
            r = await client.post("/v1/jobs", json=payload, headers={"authorization": f"Bearer {rng.choice(keys)}"})
            t.job_submits[str(r.status_code)] += 1
            if r.status_code == 202:
                t.jobs.append(r.json()["id"])
        except httpx.HTTPError:
            t.job_submits["aborted"] += 1
        await asyncio.sleep(1.0)


async def toxi(client: httpx.AsyncClient, method: str, path: str, body: dict | None = None) -> None:
    r = await client.request(method, f"{TOXI}{path}", json=body)
    r.raise_for_status()


async def heal(tc: httpx.AsyncClient) -> None:
    for name in ("postgres", "redis", "upstream"):
        await toxi(tc, "POST", f"/proxies/{name}", {"enabled": True})
        r = await tc.get(f"{TOXI}/proxies/{name}/toxics")
        for tox in r.json():
            await toxi(tc, "DELETE", f"/proxies/{name}/toxics/{tox['name']}")
    await asyncio.to_thread(compose, "up", "-d", "--no-recreate", "gateway", "worker", "sweeper")


async def inject(fault: str, tc: httpx.AsyncClient) -> None:
    if fault in ("kill_worker", "kill_gateway", "term_worker", "term_gateway"):
        how, svc = fault.split("_")
        sig = "KILL" if how == "kill" else "TERM"
        await asyncio.to_thread(compose, "kill", "-s", sig, svc)
        await asyncio.sleep(1 if how == "kill" else 8)  # TERM: give it time to drain
        await asyncio.to_thread(compose, "start", svc)
    elif fault == "redis_cut":
        await toxi(tc, "POST", "/proxies/redis", {"enabled": False})
        await asyncio.sleep(10)
        await toxi(tc, "POST", "/proxies/redis", {"enabled": True})
    elif fault == "pg_cut":
        await toxi(tc, "POST", "/proxies/postgres", {"enabled": False})
        await asyncio.sleep(2)
        await toxi(tc, "POST", "/proxies/postgres", {"enabled": True})


async def fault_loop(rng: random.Random, stop: asyncio.Event, t: Tally) -> None:
    # Toxiproxy can take a while to apply a toxic to links with traffic in flight.
    async with httpx.AsyncClient(timeout=60) as tc:
        await asyncio.sleep(10)  # let traffic settle first
        while not stop.is_set():
            fault = rng.choice(FAULTS)
            print(f"{datetime.now(UTC):%H:%M:%S} inject {fault}", flush=True)
            try:
                await inject(fault, tc)
                t.injected[fault] += 1
            except (httpx.HTTPError, SystemExit) as e:
                # Reported, not fatal: the run heals everything before checking.
                t.injected[f"{fault} (failed to inject)"] += 1
                print(f"inject {fault} failed: {e!r}", flush=True)
                await heal(tc)
            await asyncio.sleep(rng.uniform(5, 12))


async def wait_ready(port: int, timeout: float = 120) -> None:
    async with httpx.AsyncClient(timeout=2) as c:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if (await c.get(f"http://127.0.0.1:{port}/readyz")).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(1)
    sys.exit("gateway never became ready")


async def wait_quiet(conn: asyncpg.Connection, timeout: float) -> float:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        held = await conn.fetchval("SELECT count(*) FROM holds WHERE state = 'held'")
        live = await conn.fetchval("SELECT count(*) FROM jobs WHERE state IN ('queued', 'running')")
        if held == 0 and live == 0:
            return time.monotonic() - start
        await asyncio.sleep(2)
    raise TimeoutError(f"still {held} held holds and {live} live jobs after {timeout} s")


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--duration", type=int, default=300, help="seconds of traffic under faults")
    ap.add_argument("--streams", type=int, default=16, help="concurrent sync clients")
    ap.add_argument("--job-clients", type=int, default=4)
    ap.add_argument("--port", type=int, default=int(__import__("os").environ.get("LLMGW_GATEWAY_PORT", "8000")))
    args = ap.parse_args()
    rng = random.Random(args.seed)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    RESULTS.mkdir(parents=True, exist_ok=True)

    await asyncio.to_thread(compose, "up", "-d", "--build")
    async with httpx.AsyncClient(timeout=60) as tc:
        await heal(tc)  # a previous run may have stopped mid-fault
    await wait_ready(args.port)
    seed = json.loads(await asyncio.to_thread(
        compose, "exec", "-T", "gateway", "llmgw", "seed", "--org", f"faults-{stamp}", "--teams", "4",
        "--fund", str(10**13), "--grant", str(2 * 10**12)))
    keys = [t["key"] for t in seed["teams"]]

    conn = await asyncpg.connect(DB)
    window_start = await conn.fetchval("SELECT now()")
    t = Tally()
    stop = asyncio.Event()
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{args.port}",
                                 timeout=httpx.Timeout(60, connect=2)) as client:
        tasks = [asyncio.create_task(sync_loop(random.Random(rng.random()), client, keys, stop, t))
                 for _ in range(args.streams)]
        tasks += [asyncio.create_task(job_loop(random.Random(rng.random()), client, keys, stop, t))
                  for _ in range(args.job_clients)]
        faults = asyncio.create_task(fault_loop(random.Random(args.seed), stop, t))
        await asyncio.sleep(args.duration)
        stop.set()
        await asyncio.gather(*tasks)
        await faults  # finish the fault in progress, so nothing is left broken
    async with httpx.AsyncClient(timeout=60) as tc:
        await heal(tc)
    await wait_ready(args.port)
    settle_s = await wait_quiet(conn, 900)
    window_end = await conn.fetchval("SELECT now()")

    failures: list[str] = []
    problems = await check_ledger(conn)
    if problems:
        failures += [f"check_ledger: {p}" for p in problems]

    # Jobs.
    by_state = dict(await conn.fetch(
        "SELECT state::text, count(*) FROM jobs WHERE id = ANY($1::bigint[]) GROUP BY 1", t.jobs))
    bad_jobs = await conn.fetchval(
        "SELECT count(*) FROM jobs j JOIN holds h ON h.id = j.hold_id"
        " WHERE j.id = ANY($1::bigint[]) AND j.state = 'succeeded' AND h.state <> 'settled'", t.jobs)
    if set(by_state) - {"succeeded", "failed", "dead"}:
        failures.append(f"non-terminal jobs: {by_state}")
    if bad_jobs:
        failures.append(f"{bad_jobs} succeeded job(s) whose hold isn't settled")

    # Complete streams: charged exactly their usage, or expired (close lost).
    exact = expired_complete = 0
    for rid, prompt, completion in t.complete:
        row = await conn.fetchrow(
            "SELECT h.state::text, h.settled_amount, h.amount, c.flagged FROM holds h"
            " JOIN postings p ON p.id = h.hold_posting_id LEFT JOIN postings c ON c.id = h.close_posting_id"
            " WHERE p.idempotency_key = $1", f"hold:{rid}")
        want = min(cost(PRICE, prompt, completion), row["amount"]) if row else None
        if row and row["state"] == "settled" and not row["flagged"] and row["settled_amount"] == want:
            exact += 1
        elif row and row["state"] == "expired":
            expired_complete += 1
        else:
            failures.append(f"complete stream {rid}: hold {dict(row) if row else None}, expected settle at {want}")

    # Sync holds created in the window.
    holds = dict(await conn.fetch(
        "SELECT h.state::text, count(*) FROM holds h"
        " WHERE h.created_at BETWEEN $1 AND $2 AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.hold_id = h.id)"
        " GROUP BY 1", window_start, window_end))
    n_holds = sum(holds.values())
    o = t.outcomes
    refused = sum(n for code, n in o.items() if code in REFUSED)
    admitted = sum(n for code, n in o.items() if code not in REFUSED | {"aborted", "errored"})
    slack = o["aborted"] + o["errored"]
    if not admitted <= n_holds <= admitted + slack:
        failures.append(f"sync holds {n_holds} outside [{admitted}, {admitted + slack}]")
    if holds.get("held"):
        failures.append(f"{holds['held']} sync hold(s) still held")
    must_release = sum(o[c] for c in MUST_RELEASE)
    if holds.get("released", 0) + holds.get("expired", 0) < must_release:
        failures.append(f"released + expired {holds.get('released', 0) + holds.get('expired', 0)} < {must_release}")
    may_settle = o["ok_200"] + o["upstream_timeout"] + o["upstream_error"] + slack
    if holds.get("settled", 0) > may_settle:
        failures.append(f"settled {holds.get('settled', 0)} > {may_settle}")

    retry_loss = await conn.fetchval(
        "SELECT coalesce(sum(b.amount), 0) FROM accounts a JOIN balances b ON b.account_id = a.id"
        " WHERE a.kind = 'retry_loss'")
    flagged = await conn.fetchval(
        "SELECT count(*) FROM postings WHERE flagged AND created_at BETWEEN $1 AND $2", window_start, window_end)
    await conn.close()

    def table(c: Counter | dict) -> str:
        return "\n".join(f"| {k} | {v} |" for k, v in sorted(c.items())) or "| (none) | 0 |"

    report = f"""# Fault test {stamp}

Test traffic against the mock provider (local Compose stack with toxiproxy). Seed {args.seed},
{args.duration} s of traffic under faults ({args.streams} stream clients, {args.job_clients} job clients),
then {settle_s:.0f} s for the sweeper to finish.

**Result: {"PASS" if not failures else "FAIL"}**, {len(problems)} ledger problem(s).

## Faults injected ({sum(t.injected.values())})
| fault | count |
|---|---|
{table(t.injected)}

## Sync responses ({sum(o.values())})
| outcome | count |
|---|---|
{table(o)}

Refused before a hold: {refused}. Admitted: {admitted}. Aborted or errored (hold unknown): {slack}.

## Sync holds created ({n_holds})
| state | count |
|---|---|
{table(holds)}

- Complete streams (read to the end with usage): {len(t.complete)}; charged exactly their usage: {exact};
  expired because a crash or outage lost the close (under-charge awaiting reconciliation): {expired_complete}

## Jobs ({len(t.jobs)} accepted)
| state | count |
|---|---|
{table(by_state)}

Submissions: {dict(t.job_submits)}

## Money
- Retry loss booked (micro-credits, all runs): {retry_loss}
- Postings flagged for reconciliation in this run: {flagged}

## Failed checks
{chr(10).join(f"- {f}" for f in failures) or "None."}
"""
    (RESULTS / f"{stamp}-faults.md").write_text(report)
    print(report)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
