import pytest
from hamcrest import assert_that, equal_to

from nodestream.databases import GraphDatabaseWriter
from nodestream.model import DesiredIngestion
from nodestream.pipeline import Flush


@pytest.fixture
def stubbed_writer(mocker):
    return GraphDatabaseWriter(10, mocker.AsyncMock())


@pytest.mark.asyncio
async def test_graph_database_writer_flushes_when_recieving_a_flush(stubbed_writer):
    await stubbed_writer.write_record(DesiredIngestion())
    await stubbed_writer.write_record(DesiredIngestion())
    await stubbed_writer.write_record(Flush)
    stubbed_writer.ingest_strategy.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_graph_database_writer_batches_appropriately(mocker):
    stubbed_writer = GraphDatabaseWriter(10, mocker.AsyncMock())
    for _ in range(100):
        await stubbed_writer.write_record(DesiredIngestion())
    times_flushed = stubbed_writer.ingest_strategy.flush.await_count
    assert_that(times_flushed, equal_to(10))
