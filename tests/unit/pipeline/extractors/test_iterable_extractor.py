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
    expected_results = [{"index": 1}, {"index": 3}, {"index": 5}]
    subject = IterableExtractor.range(1, 6, 2)
    results = [item async for item in subject.extract_records()]
    assert_that(results, equal_to(expected_results))


@pytest.mark.asyncio
async def test_iterable_extractor_resume():
    expected_results = [{"index": 3}, {"index": 5}]
    subject = IterableExtractor.range(1, 6, 2)
    await subject.resume_from_checkpoint(1)
    results = [item async for item in subject.extract_records()]
    assert_that(results, equal_to(expected_results))


@pytest.mark.asyncio
async def test_iterable_extractor_checkpoint():
    subject = IterableExtractor([1, 2, 3])
    generator = subject.extract_records()
    await anext(generator)
    await anext(generator)
    checkpoint = await subject.make_checkpoint()
    assert_that(checkpoint, equal_to(1))
