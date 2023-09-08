import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.filters import (
    ExcludeWhenValuesMatchPossibilities,
    Filter,
    ValuesMatchPossibilitiesFilter,
)

from ..stubs import StubbedValueProvider

PASSING_FILTER_CONFIGURATION = [
    {
        "value": StubbedValueProvider("test"),
        "possibilities": [StubbedValueProvider("A Miss"), StubbedValueProvider("test")],
    }
]

FAILING_FILTER_CONFIGURATION = [
    *PASSING_FILTER_CONFIGURATION,
    {
        "value": StubbedValueProvider("un-findable"),
        "possibilities": [StubbedValueProvider("not the right value")],
    },
]


@pytest.mark.asyncio
async def test_match_possibilities_successful():
    subject = ValuesMatchPossibilitiesFilter.from_file_data(
        fields=PASSING_FILTER_CONFIGURATION
    )
    result = await subject.filter_record({})
    assert_that(result, equal_to(False))


@pytest.mark.asyncio
async def test_match_possibilities_failing():
    subject = ValuesMatchPossibilitiesFilter.from_file_data(
        fields=FAILING_FILTER_CONFIGURATION
    )
    result = await subject.filter_record({})
    assert_that(result, equal_to(True))


@pytest.mark.asyncio
async def test_exclude_possibilities_successful():
    subject = ExcludeWhenValuesMatchPossibilities.from_file_data(
        fields=PASSING_FILTER_CONFIGURATION
    )
    result = await subject.filter_record({})
    assert_that(result, equal_to(True))


@pytest.mark.asyncio
async def test_exclude_possibilities__failing():
    subject = ExcludeWhenValuesMatchPossibilities.from_file_data(
        fields=FAILING_FILTER_CONFIGURATION
    )
    result = await subject.filter_record({})
    assert_that(result, equal_to(False))


@pytest.mark.asyncio
async def test_base_filter_filters_correctly():
    async def records():
        yield 1
        yield 2

    class TestFilter(Filter):
        def __init__(self, results) -> None:
            self.results = results

        async def filter_record(self, record):
            return self.results.pop(0)

    subject = TestFilter([False, True])
    results = [record async for record in subject.handle_async_record_stream(records())]
    assert_that(results, equal_to([1]))
