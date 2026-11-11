"""StreamMeter: counting, and rebuilding a chat.completion with collect=True."""

import orjson

from llmgw.gateway.meter import StreamMeter


def sse(obj) -> bytes:
    return b"data: " + orjson.dumps(obj) + b"\n\n"


def chunk(delta, finish=None):
    return sse({"id": "c1", "object": "chat.completion.chunk", "created": 7, "model": "mock-small",
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]})


STREAM = (
    chunk({"role": "assistant", "content": ""})
    + chunk({"content": "tok0 "})
    + chunk({"content": "tok1 "})
    + chunk({}, "stop")
    + sse({"id": "c1", "choices": [], "usage": {"prompt_tokens": 9, "completion_tokens": 2}})
    + b"data: [DONE]\n\n"
)


def feed(meter, data, step):
    for i in range(0, len(data), step):
        meter.feed(data[i:i + step])
    return meter


def test_collect_rebuilds_the_completion_across_any_split():
    for step in (1, 7, len(STREAM)):
        m = feed(StreamMeter(collect=True), STREAM, step)
        assert m.result() == {
            "id": "c1", "object": "chat.completion", "created": 7, "model": "mock-small",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "tok0 tok1 "},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 9, "completion_tokens": 2, "total_tokens": 11},
        }
        assert m.done and m.content_chunks == 2


def test_default_meter_counts_but_keeps_no_content():
    m = feed(StreamMeter(), STREAM, 5)
    assert m.parts == [] and m.meta == {}
    assert (m.content_chunks, m.content_chars, m.finish_reason) == (2, 10, "stop")


def test_cut_stream_has_no_finish_reason():
    m = feed(StreamMeter(collect=True), chunk({"content": "tok0 "}), 4)
    assert m.finish_reason is None and m.usage is None
    assert "usage" not in m.result()
