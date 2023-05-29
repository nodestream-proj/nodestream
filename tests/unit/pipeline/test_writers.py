import pytest

from nodestream.pipeline.pipeline import empty_asnyc_generator
from nodestream.pipeline.writers import LoggerWriter


@pytest.mark.asyncio
async def test_write_item(mocker):
    writer = LoggerWriter()
    writer.logger = mocker.Mock()
    item = "test"
    await writer.write_record(item)
    writer.logger.log.assert_called_once_with(writer.level, item)


@pytest.mark.asyncio
async def test_finish_is_called(mocker):
    writer = LoggerWriter()
    writer.finish = mocker.AsyncMock()
    [item async for item in writer.handle_async_record_stream(empty_asnyc_generator())]
    writer.finish.assert_awaited_once()
