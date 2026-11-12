"""Entrypoint: `python -m llmgw <mode>` or `llmgw <mode>`.

Shutdown: `docker stop` sends SIGTERM. The worker and sweeper turn the first
SIGTERM or SIGINT into a cancel of their main task, so the worker drains its
pending finishes; the handler is one-shot, so a second signal can't interrupt
the drain (it gets the default action instead). The gateway leaves signals to
uvicorn, which gives streams `shutdown_grace_s` and then cancels them.
"""

import argparse
import asyncio
import contextlib
import json
import os
import signal
import sys
from collections.abc import Coroutine
from typing import Any

MODES = ("gateway", "mock-provider", "create-key", "seed", "check-ledger", "worker", "sweeper", "reconciler")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="llmgw")
    parser.add_argument("mode", choices=MODES)
    parser.add_argument("--team-id", type=int, help="create-key: the team that owns the key")
    parser.add_argument("--org", default="seed", help="seed: the org's name")
    parser.add_argument("--teams", type=int, default=1, help="seed: how many teams to create")
    parser.add_argument("--fund", type=int, default=100_000_000, help="seed: org funding, micro-credits")
    parser.add_argument("--grant", type=int, default=10_000_000, help="seed: grant per team, micro-credits")
    args = parser.parse_args(argv)

    if args.mode == "gateway":
        import uvicorn

        from llmgw import config, logs
        from llmgw.gateway.app import create_app

        settings = config.load()
        logs.configure(settings.log_format, settings.log_level)
        # uvicorn captures SIGTERM, shuts down, restores the previous handler and
        # re-raises the signal; with the default handler that ends the process
        # with -15. This one makes a clean shutdown exit 0.
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
        uvicorn.run(
            create_app(settings), host=settings.host, port=settings.port,
            # Keep our log handler instead of uvicorn's dictConfig.
            log_config=None,
            timeout_graceful_shutdown=settings.shutdown_grace_s,
        )
        return 0

    if args.mode == "mock-provider":
        import uvicorn

        from llmgw import logs
        from llmgw.mock_provider import create_app

        logs.configure(os.environ.get("LLMGW_LOG_FORMAT", "console"), os.environ.get("LLMGW_LOG_LEVEL", "info"))
        tpm, rpm = os.environ.get("LLMGW_MOCK_TPM"), os.environ.get("LLMGW_MOCK_RPM")
        uvicorn.run(
            create_app(tpm=int(tpm) if tpm else None, rpm=int(rpm) if rpm else None),
            host=os.environ.get("LLMGW_MOCK_HOST", "127.0.0.1"),
            port=int(os.environ.get("LLMGW_MOCK_PORT", "8100")),
            log_config=None,
        )
        return 0

    if args.mode == "create-key":
        if args.team_id is None:
            parser.error("create-key needs --team-id")
        print(asyncio.run(_create_key(args.team_id)))
        return 0

    if args.mode == "seed":
        if args.teams < 1 or args.grant <= 0 or args.fund <= 0:
            parser.error("seed needs --teams >= 1 and positive --fund and --grant")
        if args.fund < args.teams * args.grant:
            parser.error(f"--fund {args.fund} can't cover {args.teams} grants of {args.grant}")
        print(json.dumps(asyncio.run(_seed(args.org, args.teams, args.fund, args.grant))))
        return 0

    if args.mode == "check-ledger":
        problems = asyncio.run(_check_ledger())
        for p in problems:
            print(p)
        print(f"{len(problems)} problem(s)", file=sys.stderr)
        return 1 if problems else 0

    if args.mode == "sweeper":
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(_until_signal(_sweep()))
        return 0

    if args.mode == "worker":
        from llmgw import config, logs
        from llmgw.jobs import worker

        settings = config.load()
        logs.configure(settings.log_format, settings.log_level)
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(_until_signal(worker.serve(settings)))
        return 0

    print(f"llmgw: mode '{args.mode}' is not implemented yet", file=sys.stderr)
    return 2


async def _until_signal(coro: Coroutine[Any, Any, None]) -> None:
    """Run `coro` until it returns or the first SIGTERM/SIGINT cancels it."""
    loop = asyncio.get_running_loop()
    main = asyncio.ensure_future(coro)
    sigs = (signal.SIGTERM, signal.SIGINT)

    def stop() -> None:
        for sig in sigs:  # one-shot: a second signal mustn't cancel the drain
            loop.remove_signal_handler(sig)
        main.cancel()

    for sig in sigs:
        loop.add_signal_handler(sig, stop)
    # wait, not await: a cancel of this task is not swallowed, and only a
    # cancelled `main` (our signal) counts as a clean exit.
    await asyncio.wait([main])
    if not main.cancelled():
        main.result()


async def _seed(org_name: str, teams: int, fund: int, grant: int) -> dict[str, Any]:
    """One transaction: an org funded with `fund`, `teams` teams granted `grant` each, a key each."""
    import asyncpg

    from llmgw import auth, config
    from llmgw.ledger import accounts, postings

    conn = await asyncpg.connect(config.load().database_url)
    try:
        async with conn.transaction():
            org = await accounts.create_org(conn, org_name)
            await postings.fund_org(conn, org_id=org.org_id, amount=fund, idempotency_key=f"seed:{org.org_id}:fund")
            out = []
            for i in range(1, teams + 1):
                team = await accounts.create_team(conn, org.org_id, f"team-{i}")
                await postings.grant(
                    conn, team_id=team.team_id, amount=grant, idempotency_key=f"seed:{team.team_id}:grant"
                )
                key = await auth.create_api_key(conn, team.team_id)
                out.append({"team_id": team.team_id, "name": f"team-{i}", "key": key})
        return {"org_id": org.org_id, "teams": out}
    finally:
        await conn.close()


async def _check_ledger() -> list[str]:
    import asyncpg

    from llmgw import config
    from llmgw.ledger.checks import check_ledger

    conn = await asyncpg.connect(config.load().database_url)
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            return await check_ledger(conn)
    finally:
        await conn.close()


async def _create_key(team_id: int) -> str:
    import asyncpg

    from llmgw import auth, config

    conn = await asyncpg.connect(config.load().database_url)
    try:
        async with conn.transaction():
            return await auth.create_api_key(conn, team_id)
    finally:
        await conn.close()


async def _sweep() -> None:
    from llmgw import config, logs, looplag, metrics, sweeper
    from llmgw.db import create_pool

    settings = config.load()
    logs.configure(settings.log_format, settings.log_level)
    if settings.metrics_port is not None:
        metrics.serve(settings.metrics_port, settings.metrics_host)
    pool = await create_pool(settings)
    lag = asyncio.create_task(looplag.monitor(settings.loop_lag_interval_s))
    try:
        await sweeper.run(
            pool, interval_s=settings.sweep_interval_s, batch=settings.sweep_batch,
            retry_base_s=settings.job_retry_base_s, retry_cap_s=settings.job_retry_cap_s,
            ledger_check_interval_s=settings.ledger_check_interval_s,
        )
    finally:
        await looplag.stop(lag)
        await pool.close()


if __name__ == "__main__":
    sys.exit(main())
