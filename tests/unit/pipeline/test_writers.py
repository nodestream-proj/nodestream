import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.writers import LoggerWriter, Flush


@pytest.mark.asyncio
async def test_write_item(mocker):
    writer = LoggerWriter()
    writer.logger = mocker.Mock()
    item = "test"
    await writer.write_record(item)
    writer.logger.log.assert_called_once_with(writer.level, item)


@pytest.mark.asyncio
async def test_writers_flush_on_write(mocker):
    writer = LoggerWriter()
    writer.flush = mocker.AsyncMock()
    writer.write_record = mocker.AsyncMock()

    async def input():
        yield 1
        yield Flush
        yield 2

    results = [r async for r in writer.handle_async_record_stream(input())]
    assert_that(results, equal_to([1, Flush, 2]))
    assert_that(writer.write_record.await_count, equal_to(2))
    assert_that(writer.flush.await_count, equal_to(1))
