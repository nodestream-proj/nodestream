import pytest

from nodestream.pipeline.writers import LoggerWriter

        
@pytest.mark.asyncio
async def test_write_item(mocker):
    writer = LoggerWriter()
    writer.logger = mocker.Mock()
    item = 'test'
    await writer.write_record(item)
    writer.logger.log.assert_called_once_with(writer.level, item)
