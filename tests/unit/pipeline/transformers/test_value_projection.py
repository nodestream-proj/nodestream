import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.transformers import ValueProjection
from nodestream.pipeline.value_providers import JmespathValueProvider

TEST_DATA = {"metadata": "stuff", "items": [{"index": 1}, {"index": 2}, {"index": 3}]}


async def record():
    yield TEST_DATA


@pytest.mark.asyncio
async def test_value_projection_transform_record():
    subject = ValueProjection(
        projection=JmespathValueProvider.from_string_expression("items[*]")
    )
    results = [r async for r in subject.process_record(TEST_DATA, None)]
    assert_that(results, equal_to([{"index": 1}, {"index": 2}, {"index": 3}]))


@pytest.mark.asyncio
async def test_value_projection_with_additional_values():
    subject = ValueProjection(
        projection=JmespathValueProvider.from_string_expression("items[*]"),
        additional_values={
            "metadata": JmespathValueProvider.from_string_expression("metadata")
        },
    )

    results = [r async for r in subject.process_record(TEST_DATA, None)]
    assert_that(
        results,
        equal_to(
            [
                {"metadata": "stuff", "index": 1},
                {"metadata": "stuff", "index": 2},
                {"metadata": "stuff", "index": 3},
            ]
        ),
    )
