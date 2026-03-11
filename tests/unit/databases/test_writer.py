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
    stubbed_writer.ingest_strategy.finish = mocker.AsyncMock()
    await stubbed_writer.finish(None)
    stubbed_writer.ingest_strategy.finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush(stubbed_writer, mocker):
    stubbed_writer.flush = mocker.AsyncMock()
    await stubbed_writer.finish(None)
    stubbed_writer.flush.assert_awaited_once()


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

    # The strategy should have been swapped out.
    assert concurrent_writer.ingest_strategy is not original_strategy
    assert concurrent_writer.strategy_factory.call_count == 1

    # Let the background flush task complete, then verify.
    await asyncio.sleep(0)
    assert original_strategy.flush.await_count == 1


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

    await concurrent_writer.finish(None)

    # The active strategy should have been flushed.
    assert concurrent_writer.ingest_strategy.flush.await_count >= 1
    assert concurrent_writer.ingest_strategy.finish.await_count == 1


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
