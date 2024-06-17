import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.filters import (
    ExcludeWhenValuesMatchPossibilities,
    Filter,
    ValueMatchesRegexFilter,
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
    class TestFilter(Filter):
        def __init__(self, results) -> None:
            self.results = results

        async def filter_record(self, record):
            return self.results.pop(0)

    subject = TestFilter([False, True])
    results = [record async for record in subject.process_record(1, None)]
    assert_that(results, equal_to([1]))
    results = [record async for record in subject.process_record(2, None)]
    assert_that(results, equal_to([]))


REGEX = ".*[\[\]\{\}\(\)\\\/~,]+"
REGEX_TEST_CASES = [
    {"value": "[test]", "include": True, "expect": False},
    {"value": "test", "include": True, "expect": True},
    {"value": "[test]", "include": False, "expect": True},
    {"value": "test", "include": False, "expect": False},
]


@pytest.mark.asyncio
async def test_match_regex():
    for test_case in REGEX_TEST_CASES:
        subject = ValueMatchesRegexFilter.from_file_data(
            value=test_case["value"], regex=REGEX, include=test_case["include"]
        )
        result = await subject.filter_record({})
        assert_that(result, equal_to(test_case["expect"]))
