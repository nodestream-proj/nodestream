import json

import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline import Flush
from nodestream.pipeline.extractors.queues.extractor import (
    QueueConnector,
    QueueExtractor,
)
from nodestream.pipeline.extractors.streams.extractor import JsonStreamRecordFormat


@pytest.fixture
def extractor(mocker):
    return QueueExtractor(
        record_format=JsonStreamRecordFormat(),
        connector=mocker.AsyncMock(),
    )


class MockQueueConnector(QueueConnector, alias="mock_connector"):
    def __init__(self, max_polls=1):
        self.poll_count = 0
        self.max_polls = max_polls

    async def poll(self):
        if self.poll_count < self.max_polls:
            self.poll_count += 1
            return ['{"key": "test-value"}']
        else:
            raise StopAsyncIteration


@pytest.mark.asyncio
async def test_queue_extractor_from_file_data():
    queue_extractor = QueueExtractor.from_file_data(
        connector="mock_connector",
        record_format="json",
    )

    assert isinstance(queue_extractor.record_format, JsonStreamRecordFormat)
    assert isinstance(queue_extractor.connector, MockQueueConnector)

    extracted_records = []
    try:
        async for record in queue_extractor.extract_records():
            extracted_records.append(record)
    except StopAsyncIteration:
        # Allow for a clean exit from the extract_records() infinite loop
        pass
    assert extracted_records == [{"key": "test-value"}]


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
