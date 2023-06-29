import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.transformers import ValueProjection
from nodestream.interpreting.value_providers import JmespathValueProvider


@pytest.mark.asyncio
async def test_value_projection_transform_record():
    subject = ValueProjection(
        projection=JmespathValueProvider.from_string_expression("items[*]")
    )
    results = [r async for r in subject.transform_record(record={"items": [1, 2, 3]})]
    assert_that(results, equal_to([1, 2, 3]))
