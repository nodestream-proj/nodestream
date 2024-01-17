import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.transformers import ExpandJsonField

SIMPLE_INPUT = {"a": 1, "b": '{"hello": "world"}'}
SIMPLE_OUTPUT = {"a": 1, "b": {"hello": "world"}}

DEEP_INPUT = {"a": 1, "b": {"c": {"d": '{"hello": "world"}'}}}
DEEP_OUTPUT = {"a": 1, "b": {"c": {"d": {"hello": "world"}}}}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "input,output,path",
    [
        (SIMPLE_INPUT, SIMPLE_OUTPUT, "b"),
        (DEEP_INPUT, DEEP_OUTPUT, ["b", "c", "d"]),
        (DEEP_INPUT, DEEP_INPUT, ["b", "f"]),
        (DEEP_INPUT, DEEP_INPUT, ["b", "f", "d"]),
    ],
)
async def test_expand_json_fields(input, output, path):
    subject = ExpandJsonField.from_file_data(path)

    async def upstream():
        yield input

    results = [r async for r in subject.handle_async_record_stream(upstream())]
    assert_that(results, equal_to([output]))
