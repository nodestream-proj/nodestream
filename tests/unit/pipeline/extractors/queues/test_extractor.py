import json

import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline import Flush
from nodestream.pipeline.extractors.queues.extractor import (
    JsonStreamRecordFormat,
    QueueExtractor,
)


@pytest.fixture
def extractor(mocker):
    return QueueExtractor(
        record_format=JsonStreamRecordFormat(),
        connector=mocker.AsyncMock(),
    )


@pytest.mark.asyncio
async def test_extractor_polls_until_error(extractor):
    input_batches = [['{"key": "test-value"}'] for _ in range(10)]
    expected_results = [json.loads(r) for batch in input_batches for r in batch]
    extractor.connector.poll.side_effect = [
        *input_batches,
        ValueError,
    ]
    result = [record async for record in extractor.extract_records()]
    assert_that(result, equal_to(expected_results))
    assert_that(extractor.connector.poll.call_count, equal_to(11))


@pytest.mark.asyncio
async def test_extractor_polls_empty_list(extractor):
    input_batches = []
    expected_results = [Flush]
    extractor.connector.poll.side_effect = [
        input_batches,
        ValueError,
    ]
    result = [record async for record in extractor.extract_records()]
    assert_that(result, equal_to(expected_results))
