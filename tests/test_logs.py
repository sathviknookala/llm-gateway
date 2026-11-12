"""Logging setup: JSON lines with bound context, stdlib records included."""

import logging

import orjson
import pytest
import structlog

from llmgw import logs


@pytest.fixture
def json_logs(capsys):
    logs.configure("json", "info")
    try:
        yield capsys
    finally:
        logs.reset()


def lines(capsys) -> list[dict]:
    return [orjson.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]


def test_structlog_event_is_json_with_bound_context(json_logs):
    log = structlog.get_logger("llmgw.test")
    with structlog.contextvars.bound_contextvars(request_id="r1"):
        log.info("thing_happened", n=3)
    log.debug("hidden")  # below the level
    [line] = lines(json_logs)
    assert line["event"] == "thing_happened"
    assert line["level"] == "info"
    assert line["request_id"] == "r1"
    assert line["n"] == 3
    assert line["logger"] == "llmgw.test"
    assert "timestamp" in line


def test_stdlib_records_are_json_too(json_logs):
    # Not a uvicorn logger: in-process uvicorn servers in other tests reconfigure those.
    logging.getLogger("some.library").info("GET /healthz 200")
    [line] = lines(json_logs)
    assert line["event"] == "GET /healthz 200"
    assert line["logger"] == "some.library"


def test_exceptions_are_rendered(json_logs):
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        structlog.get_logger("llmgw.test").exception("failed")
    [line] = lines(json_logs)
    assert "RuntimeError: boom" in line["exception"]


def test_bad_format_is_refused():
    with pytest.raises(ValueError):
        logs.configure("xml")
