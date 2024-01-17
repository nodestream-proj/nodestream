import pytest
from hamcrest import assert_that, equal_to

from nodestream.databases import GraphDatabaseWriter
from nodestream.model import DesiredIngestion


@pytest.fixture
def stubbed_writer(mocker):
    return GraphDatabaseWriter(10, mocker.AsyncMock())


@pytest.mark.asyncio
async def test_graph_database_writer_batches_appropriately(mocker):
    stubbed_writer = GraphDatabaseWriter(10, mocker.AsyncMock())
    for _ in range(100):
        await stubbed_writer.write_record(DesiredIngestion())
    times_flushed = stubbed_writer.ingest_strategy.flush.await_count
    assert_that(times_flushed, equal_to(10))
