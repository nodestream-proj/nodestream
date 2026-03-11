import asyncio
from collections import deque

import pytest

from nodestream.databases import ConcurrentGraphDatabaseWriter, GraphDatabaseWriter

# ---------------------------------------------------------------------------
# GraphDatabaseWriter (base class)
# ---------------------------------------------------------------------------


@pytest.fixture
def stubbed_writer(mocker):
    return GraphDatabaseWriter(10, mocker.AsyncMock())


@pytest.mark.asyncio
async def test_finish(stubbed_writer, mocker):
    # Write some records to accumulate pending state.
    ingestible = mocker.AsyncMock()
    await stubbed_writer.write_record(ingestible)
    assert stubbed_writer.pending_records == 1

    await stubbed_writer.finish(None)

    # After finish, the batch has been flushed and the strategy finalised.
    assert stubbed_writer.pending_records == 0


@pytest.mark.asyncio
async def test_flush_resets_pending_records(stubbed_writer, mocker):
    ingestible = mocker.AsyncMock()
    await stubbed_writer.write_record(ingestible)
    assert stubbed_writer.pending_records == 1

    await stubbed_writer.flush()
    assert stubbed_writer.pending_records == 0


# ---------------------------------------------------------------------------
# ConcurrentGraphDatabaseWriter
# ---------------------------------------------------------------------------


@pytest.fixture
def concurrent_writer(mocker):
    strategy = mocker.AsyncMock()
    factory = mocker.Mock(side_effect=lambda: mocker.AsyncMock())
    return ConcurrentGraphDatabaseWriter(
        batch_size=3,
        ingest_strategy=strategy,
        strategy_factory=factory,
        max_flush_lanes=2,
    )


@pytest.mark.asyncio
async def test_concurrent_writer_rotates_strategy_on_full_batch(
    concurrent_writer, mocker
):
    """When the batch is full, the writer should rotate to a fresh strategy."""
    original_strategy = concurrent_writer.ingest_strategy
    ingestible = mocker.AsyncMock()

    # Write batch_size records to trigger rotation.
    for _ in range(concurrent_writer.batch_size):
        await concurrent_writer.write_record(ingestible)

    # The strategy should have been swapped out and the counter reset.
    assert concurrent_writer.ingest_strategy is not original_strategy
    assert concurrent_writer.pending_records == 0

    # Let the background flush complete and verify the tasks are drained.
    await concurrent_writer._drain_pending_flushes()
    assert len(concurrent_writer.pending_flush_tasks) == 0


@pytest.mark.asyncio
async def test_concurrent_writer_flush_drains_pending_tasks(concurrent_writer, mocker):
    """flush() should drain all pending background flushes."""
    ingestible = mocker.AsyncMock()

    # Fill two batches to create two background flush tasks.
    for _ in range(concurrent_writer.batch_size * 2):
        await concurrent_writer.write_record(ingestible)

    assert len(concurrent_writer.pending_flush_tasks) > 0

    await concurrent_writer.flush()

    # All tasks should have been drained.
    assert len(concurrent_writer.pending_flush_tasks) == 0


@pytest.mark.asyncio
async def test_concurrent_writer_raises_background_flush_errors(
    concurrent_writer, mocker
):
    """Errors from background flushes should be re-raised on the next write_record."""
    failing_strategy = mocker.AsyncMock()
    failing_strategy.flush.side_effect = RuntimeError("db connection lost")
    concurrent_writer.strategy_factory = mocker.Mock(return_value=mocker.AsyncMock())

    # Replace the current strategy with the one that will fail on flush.
    concurrent_writer.ingest_strategy = failing_strategy
    ingestible = mocker.AsyncMock()

    # Fill the batch to trigger a background flush.
    for _ in range(concurrent_writer.batch_size):
        await concurrent_writer.write_record(ingestible)

    # Let the event loop process the background task.
    await asyncio.sleep(0)

    # The next write should raise the error from the background flush.
    with pytest.raises(RuntimeError, match="db connection lost"):
        await concurrent_writer.write_record(ingestible)


@pytest.mark.asyncio
async def test_concurrent_writer_finish_flushes_everything(concurrent_writer, mocker):
    """finish() should flush active batch and drain pending tasks."""
    ingestible = mocker.AsyncMock()

    # Write a partial batch (less than batch_size).
    await concurrent_writer.write_record(ingestible)
    assert concurrent_writer.pending_records == 1

    await concurrent_writer.finish(None)

    # After finish, everything should be flushed and drained.
    assert concurrent_writer.pending_records == 0
    assert len(concurrent_writer.pending_flush_tasks) == 0


def test_concurrent_writer_flush_errors_is_deque(concurrent_writer):
    """flush_errors should be a deque for O(1) popleft."""
    assert isinstance(concurrent_writer.flush_errors, deque)


@pytest.mark.asyncio
async def test_concurrent_writer_from_connector(mocker):
    """from_connector with flush_concurrency > 1 should return a ConcurrentGraphDatabaseWriter."""
    mock_connector = mocker.Mock()
    mock_executor = mocker.Mock()
    mock_connector.get_query_executor.return_value = mock_executor

    writer = GraphDatabaseWriter.from_connector(
        connector=mock_connector,
        flush_concurrency=4,
    )

    assert isinstance(writer, ConcurrentGraphDatabaseWriter)
    assert writer.max_flush_lanes == 4


@pytest.mark.asyncio
async def test_base_writer_from_connector(mocker):
    """from_connector with flush_concurrency=1 should return a plain GraphDatabaseWriter."""
    mock_connector = mocker.Mock()
    mock_executor = mocker.Mock()
    mock_connector.get_query_executor.return_value = mock_executor

    writer = GraphDatabaseWriter.from_connector(
        connector=mock_connector,
        flush_concurrency=1,
    )

    assert type(writer) is GraphDatabaseWriter


@pytest.mark.asyncio
async def test_from_file_data_returns_concurrent_writer(mocker):
    """from_file_data with flush_concurrency > 1 should return ConcurrentGraphDatabaseWriter."""
    mock_connector = mocker.Mock()
    mock_executor = mocker.Mock()
    mock_connector.get_query_executor.return_value = mock_executor
    mocker.patch(
        "nodestream.databases.writer.DatabaseConnector.from_database_args",
        return_value=mock_connector,
    )

    writer = GraphDatabaseWriter.from_file_data(
        database="null",
        flush_concurrency=2,
    )

    assert isinstance(writer, ConcurrentGraphDatabaseWriter)
    assert writer.max_flush_lanes == 2


@pytest.mark.asyncio
async def test_from_file_data_returns_base_writer(mocker):
    """from_file_data with flush_concurrency=1 should return plain GraphDatabaseWriter."""
    mock_connector = mocker.Mock()
    mock_executor = mocker.Mock()
    mock_connector.get_query_executor.return_value = mock_executor
    mocker.patch(
        "nodestream.databases.writer.DatabaseConnector.from_database_args",
        return_value=mock_connector,
    )

    writer = GraphDatabaseWriter.from_file_data(
        database="null",
        flush_concurrency=1,
    )

    assert type(writer) is GraphDatabaseWriter


@pytest.mark.asyncio
async def test_concurrent_writer_strategy_factory_creates_new_strategies(mocker):
    """The strategy_factory from from_connector should produce new strategy instances."""
    mock_connector = mocker.Mock()
    mock_executor = mocker.Mock()
    mock_connector.get_query_executor.return_value = mock_executor

    writer = GraphDatabaseWriter.from_connector(
        connector=mock_connector,
        flush_concurrency=2,
    )

    assert isinstance(writer, ConcurrentGraphDatabaseWriter)
    # The factory should produce new strategy objects each time.
    s1 = writer.strategy_factory()
    s2 = writer.strategy_factory()
    assert s1 is not s2
