"""Logging setup for the CLI modes: one stream, JSON or console.

structlog events and stdlib `logging` records (uvicorn's included) go through
the same `ProcessorFormatter`, so a container writes one JSON object per line.
Context bound with `structlog.contextvars` (request_id, job_id, ...) is merged
into every event logged in that task and the tasks it starts.

Called once at the top of each CLI mode, never at import: tests keep
structlog's defaults. uvicorn must be started with `log_config=None`, or its
dictConfig installs its own non-propagating handlers over this one.
"""

import logging
import sys

import structlog

FORMATS = ("console", "json")

_SHARED = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="iso", utc=True),
]

_handler: logging.Handler | None = None


class _StdoutHandler(logging.StreamHandler):
    """Writes to whatever `sys.stdout` is at emit time (tests swap it)."""

    @property
    def stream(self):  # type: ignore[override]
        return sys.stdout

    @stream.setter
    def stream(self, value) -> None:
        pass


def configure(fmt: str = "console", level: str = "info") -> None:
    global _handler
    if fmt not in FORMATS:
        raise ValueError(f"log format must be one of {FORMATS}, not {fmt!r}")
    if fmt == "json":
        # stdlib json returns str; orjson's bytes would break the formatter.
        tail = [structlog.processors.format_exc_info, structlog.processors.JSONRenderer()]
    else:
        tail = [structlog.dev.ConsoleRenderer()]

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *_SHARED,
            structlog.processors.StackInfoRenderer(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        # Not cached: a module-level logger must follow a later reconfigure.
        cache_logger_on_first_use=False,
    )

    handler = _StdoutHandler()
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=_SHARED,
            processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, *tail],
        )
    )
    root = logging.getLogger()
    if _handler is not None:
        root.removeHandler(_handler)
    root.addHandler(handler)
    root.setLevel(level.upper())
    _handler = handler
    logging.captureWarnings(True)


def reset() -> None:
    """Undo `configure` (tests)."""
    global _handler
    if _handler is not None:
        logging.getLogger().removeHandler(_handler)
        _handler = None
    logging.getLogger().setLevel(logging.WARNING)
    logging.captureWarnings(False)
    structlog.reset_defaults()
