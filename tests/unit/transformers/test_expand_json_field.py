import pytest
from hamcrest import assert_that, equal_to

from nodestream.transformers import ExpandJsonField

SIMPLE_INPUT = {"a": 1, "b": '{"hello": "world"}'}
SIMPLE_OUTPUT = {"a": 1, "b": {"hello": "world"}}

DEEP_INPUT = {"a": 1, "b": {"c": {"d": '{"hello": "world"}'}}}
DEEP_OUTPUT = {"a": 1, "b": {"c": {"d": {"hello": "world"}}}}


@pytest.mark.asyncio
@pytest.mark.parametrize("input,output,path", [(SIMPLE_INPUT, SIMPLE_OUTPUT, "b")])
async def test_expand_json_fields(input, output, path):
    subject = ExpandJsonField.from_file_data(path)
    result = await subject.transform_record(input)
    assert_that(result, equal_to(output))
