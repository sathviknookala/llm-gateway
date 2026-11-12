"""Entrypoint: `python -m llmgw <mode>` or `llmgw <mode>`."""

import argparse
import asyncio
import contextlib
import os
import sys

MODES = ("gateway", "mock-provider", "create-key", "worker", "sweeper", "reconciler")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="llmgw")
    parser.add_argument("mode", choices=MODES)
    parser.add_argument("--team-id", type=int, help="create-key: the team that owns the key")
    args = parser.parse_args(argv)

    if args.mode == "gateway":
        import uvicorn

        from llmgw import config, logs
        from llmgw.gateway.app import create_app

        settings = config.load()
        logs.configure(settings.log_format, settings.log_level)
        # log_config=None: keep our handler instead of uvicorn's dictConfig.
        uvicorn.run(create_app(settings), host=settings.host, port=settings.port, log_config=None)
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

    if args.mode == "sweeper":
        asyncio.run(_sweep())
        return 0

    if args.mode == "worker":
        from llmgw import config, logs
        from llmgw.jobs import worker

        settings = config.load()
        logs.configure(settings.log_format, settings.log_level)
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(worker.serve(settings))
        return 0

    print(f"llmgw: mode '{args.mode}' is not implemented yet", file=sys.stderr)
    return 2


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
