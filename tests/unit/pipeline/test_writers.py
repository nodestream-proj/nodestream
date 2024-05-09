import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.writers import Flush, LoggerWriter


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

    not_flush = await anext(writer.process_record(1, None))
    assert_that(not_flush, equal_to(1))
    assert_that(writer.write_record.await_count, equal_to(1))
    assert_that(writer.flush.await_count, equal_to(0))

    a_flush = await anext(writer.process_record(Flush, None))
    assert_that(a_flush, equal_to(Flush))
    assert_that(writer.flush.await_count, equal_to(1))
