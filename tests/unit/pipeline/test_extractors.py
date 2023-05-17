import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline import IterableExtractor


@pytest.mark.asyncio
async def test_iterable_extractor():
    expected_results = [1, 2, 3]
    subject = IterableExtractor(iterable=expected_results)
    results = [item async for item in subject.extract_records()]
    assert_that(results, equal_to(results))


@pytest.mark.asyncio
async def test_iterable_extractor_range_factory():
    expected_results = [1, 3, 5]
    subject = IterableExtractor.range(1, 6, 2)
    results = [item async for item in subject.extract_records()]
    assert_that(results, equal_to(expected_results))
