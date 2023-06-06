import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline import ValuesMatchPossibilitiesFilter

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
        "value": StubbedValueProvider("unfindable"),
        "possibilities": [StubbedValueProvider("not the right value")],
    },
]


@pytest.mark.asyncio
async def test_match_possiblities_successful():
    subject = ValuesMatchPossibilitiesFilter.__declarative_init__(
        fields=PASSING_FILTER_CONFIGURATION
    )
    result = await subject.filter_record({})
    assert_that(result, equal_to(False))


@pytest.mark.asyncio
async def test_match_possiblities_failing():
    subject = ValuesMatchPossibilitiesFilter.__declarative_init__(
        fields=FAILING_FILTER_CONFIGURATION
    )
    result = await subject.filter_record({})
    assert_that(result, equal_to(True))
