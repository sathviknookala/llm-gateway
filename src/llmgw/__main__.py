"""Entrypoint: `python -m llmgw <mode>` or `llmgw <mode>`."""

import argparse
import asyncio
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

        from llmgw import config
        from llmgw.gateway.app import create_app

        settings = config.load()
        uvicorn.run(create_app(settings), host=settings.host, port=settings.port)
        return 0

    if args.mode == "mock-provider":
        import uvicorn

        from llmgw.mock_provider import create_app

        uvicorn.run(
            create_app(),
            host=os.environ.get("LLMGW_MOCK_HOST", "127.0.0.1"),
            port=int(os.environ.get("LLMGW_MOCK_PORT", "8100")),
        )
        return 0

    if args.mode == "create-key":
        if args.team_id is None:
            parser.error("create-key needs --team-id")
        print(asyncio.run(_create_key(args.team_id)))
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


if __name__ == "__main__":
    sys.exit(main())
