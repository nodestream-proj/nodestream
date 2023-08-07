import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline import (
    ExcludeWhenValuesMatchPossibilities,
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
