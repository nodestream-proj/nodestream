import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.extractors.streams.extractor import (
    JsonStreamRecordFormat,
    StreamExtractor,
)


@pytest.fixture
def extractor(mocker):
    return StreamExtractor(
        record_format=JsonStreamRecordFormat(),
        connector=mocker.AsyncMock(),
    )


@pytest.mark.asyncio
async def test_extract(extractor):
    extractor.connector.poll.side_effect = [['{"key": "test-value"}'], ValueError]
    result = [record async for record in extractor.extract_records()]
    assert_that(result, equal_to([{"key": "test-value"}]))
    extractor.connector.poll.assert_called_once_with()
    extractor.connector.connect.assert_called_once()
    extractor.connector.disconnect.assert_called_once()
