import pytest

from nodestream.pipeline.meta import start_context, get_context
from nodestream.databases.query_executor_with_statistics import (
    QueryExecutorWithStatistics,
    NODE_STAT,
    RELATIONSHIP_STAT,
    KEY_INDEX_STAT,
    FIELD_INDEX_STAT,
    TTL_STAT,
    HOOK_STAT,
)


@pytest.fixture
def query_executor_with_statistics(mocker):
    return QueryExecutorWithStatistics(mocker.AsyncMock())


@pytest.mark.asyncio
async def test_upsert_nodes_in_bulk_with_same_operation_increments_counter_by_size_of_list(
    query_executor_with_statistics,
):
    with start_context("test"):
        await query_executor_with_statistics.upsert_nodes_in_bulk_with_same_operation(
            "operation", ["node1", "node2"]
        )
        query_executor_with_statistics.inner.upsert_nodes_in_bulk_with_same_operation.assert_awaited_once_with(
            "operation",
            ["node1", "node2"],
        )
        assert get_context().stats[NODE_STAT] == 2


@pytest.mark.asyncio
async def test_upsert_relationships_in_bulk_of_same_operation_increments_counter_by_size_of_list(
    query_executor_with_statistics,
):
    with start_context("test"):
        await query_executor_with_statistics.upsert_relationships_in_bulk_of_same_operation(
            "operation", ["relationship1", "relationship2"]
        )
        query_executor_with_statistics.inner.upsert_relationships_in_bulk_of_same_operation.assert_awaited_once_with(
            "operation",
            ["relationship1", "relationship2"],
        )
        assert get_context().stats[RELATIONSHIP_STAT] == 2


@pytest.mark.asyncio
async def test_upsert_key_index_increments_counter_by_one(
    query_executor_with_statistics,
):
    with start_context("test"):
        await query_executor_with_statistics.upsert_key_index("index")
        query_executor_with_statistics.inner.upsert_key_index.assert_awaited_once_with(
            "index"
        )
        assert get_context().stats[KEY_INDEX_STAT] == 1


@pytest.mark.asyncio
async def test_upsert_field_index_increments_counter_by_one(
    query_executor_with_statistics,
):
    with start_context("test"):
        await query_executor_with_statistics.upsert_field_index("index")
        query_executor_with_statistics.inner.upsert_field_index.assert_awaited_once_with(
            "index"
        )
        assert get_context().stats[FIELD_INDEX_STAT] == 1


@pytest.mark.asyncio
async def test_perform_ttl_op_increments_counter_by_one(query_executor_with_statistics):
    with start_context("test"):
        await query_executor_with_statistics.perform_ttl_op("config")
        query_executor_with_statistics.inner.perform_ttl_op.assert_awaited_once_with(
            "config"
        )
        assert get_context().stats[TTL_STAT] == 1


@pytest.mark.asyncio
async def test_execute_hook_increments_counter_by_one(query_executor_with_statistics):
    with start_context("test"):
        await query_executor_with_statistics.execute_hook("hook")
        query_executor_with_statistics.inner.execute_hook.assert_awaited_once_with(
            "hook"
        )
        assert get_context().stats[HOOK_STAT] == 1
