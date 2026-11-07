"""Entrypoint: `python -m llmgw <mode>` or `llmgw <mode>`."""

import argparse
import sys

MODES = ("gateway", "worker", "sweeper", "reconciler")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="llmgw")
    parser.add_argument("mode", choices=MODES)
    args = parser.parse_args(argv)
    print(f"llmgw: mode '{args.mode}' is not implemented yet", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
