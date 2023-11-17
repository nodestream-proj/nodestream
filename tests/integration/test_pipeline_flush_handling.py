import pytest

from nodestream.databases import GraphDatabaseWriter
from nodestream.interpreting import Interpreter
from nodestream.pipeline import Flush, IterableExtractor, Pipeline


@pytest.fixture
def writer(mocker):
    return GraphDatabaseWriter(3, mocker.AsyncMock())


@pytest.fixture
def interpreter():
    return Interpreter.from_file_data(interpretations=[])


@pytest.fixture
def test_extractor_with_flushes(mocker):
    return IterableExtractor(
        [
            Flush,
            1,
            2,
            Flush,
            3,
            4,
            5,
            6,
            Flush,
            7,
        ]
    )


@pytest.mark.asyncio
async def test_flush_handling(writer, interpreter, test_extractor_with_flushes):
    pipeline = Pipeline([test_extractor_with_flushes, interpreter, writer], 1000)
    await pipeline.run()
    assert writer.ingest_strategy.flush.call_count == 5
