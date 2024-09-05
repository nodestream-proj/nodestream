import pytest

from nodestream.databases import GraphDatabaseWriter


@pytest.fixture
def stubbed_writer(mocker):
    return GraphDatabaseWriter(10, mocker.AsyncMock())


@pytest.mark.asyncio
async def test_finish(stubbed_writer, mocker):
    stubbed_writer.ingest_strategy.finish = mocker.AsyncMock()
    await stubbed_writer.finish(None)
    stubbed_writer.ingest_strategy.finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush(stubbed_writer, mocker):
    stubbed_writer.flush = mocker.AsyncMock()
    await stubbed_writer.finish(None)
    stubbed_writer.flush.assert_awaited_once()
