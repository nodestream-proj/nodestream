import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.transformers import ValueProjection
from nodestream.pipeline.value_providers import JmespathValueProvider


@pytest.mark.asyncio
async def test_value_projection_transform_record():
    subject = ValueProjection(
        projection=JmespathValueProvider.from_string_expression("items[*]")
    )

    async def record():
        yield {"items": [1, 2, 3]}

    results = [r async for r in subject.handle_async_record_stream(record())]
    assert_that(results, equal_to([1, 2, 3]))
