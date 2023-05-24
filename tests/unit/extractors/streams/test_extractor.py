import pytest

from nodestream.extractors.streams.extractor import (
    JsonStreamRecordFormat,
    StreamExtractor,
)


@pytest.fixture
def extractor(mocker):
    return StreamExtractor(
        timeout=1,
        max_records=1,
        record_format=JsonStreamRecordFormat(),
        connector=mocker.AsyncMock(),
    )


@pytest.mark.asyncio
async def test_extract(extractor):
    extractor.connector.poll.side_effect = [['{"key": "test-value"}'], ValueError]
    result = [record async for record in extractor.extract_records()]
    assert result == [{"key": "test-value"}]
    extractor.connector.poll.assert_called_once_with(timeout=1, max_records=1)
    extractor.connector.connect.assert_called_once()
    extractor.connector.disconnect.assert_called_once()
