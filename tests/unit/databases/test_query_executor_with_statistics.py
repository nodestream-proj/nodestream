from unittest.mock import call

import pytest

from nodestream.databases.query_executor_with_statistics import (
    QueryExecutorWithStatistics,
)
from nodestream.metrics import Metrics, NodestreamMetricRegistry
from nodestream.model import Node, Relationship, RelationshipWithNodes


@pytest.fixture
def query_executor_with_statistics(mocker):
    return QueryExecutorWithStatistics(mocker.AsyncMock())


@pytest.mark.asyncio
async def test_upsert_nodes_in_bulk_with_same_operation_increments_counter_by_size_of_list(
    query_executor_with_statistics, mocker
):
    with Metrics.capture() as metrics:
        metrics.increment = mocker.Mock()
        await query_executor_with_statistics.upsert_nodes_in_bulk_with_same_operation(
            "operation",
            [Node("node_type", "node1", "id1"), Node("node_type", "node2", "id2")],
        )
        query_executor_with_statistics.inner.upsert_nodes_in_bulk_with_same_operation.assert_awaited_once_with(
            "operation",
            [Node("node_type", "node1", "id1"), Node("node_type", "node2", "id2")],
        )

        assert "node_type" in query_executor_with_statistics.node_metric_by_type
        assert (
            call(query_executor_with_statistics.node_metric_by_type["node_type"], 2)
            in metrics.increment.call_args_list
        )
        assert (
            call(NodestreamMetricRegistry.NODES_UPSERTED, 2)
            in metrics.increment.call_args_list
        )


@pytest.mark.asyncio
async def test_upsert_relationships_in_bulk_of_same_operation_increments_counter_by_size_of_list(
    query_executor_with_statistics, mocker
):
    with Metrics.capture() as metrics:
        metrics.increment = mocker.Mock()
        await query_executor_with_statistics.upsert_relationships_in_bulk_of_same_operation(
            "operation",
            [
                RelationshipWithNodes(
                    "node1", "node2", Relationship("relationship_type")
                ),
                RelationshipWithNodes(
                    "node3", "node4", Relationship("relationship_type")
                ),
            ],
        )
        query_executor_with_statistics.inner.upsert_relationships_in_bulk_of_same_operation.assert_awaited_once_with(
            "operation",
            [
                RelationshipWithNodes(
                    "node1", "node2", Relationship("relationship_type")
                ),
                RelationshipWithNodes(
                    "node3", "node4", Relationship("relationship_type")
                ),
            ],
        )
        assert (
            "relationship_type"
            in query_executor_with_statistics.relationship_metric_by_relationship_type
        )
        assert (
            call(
                query_executor_with_statistics.relationship_metric_by_relationship_type[
                    "relationship_type"
                ],
                2,
            )
            in metrics.increment.call_args_list
        )
        assert (
            call(NodestreamMetricRegistry.RELATIONSHIPS_UPSERTED, 2)
            in metrics.increment.call_args_list
        )


@pytest.mark.asyncio
async def test_perform_ttl_op_increments_counter_by_one(
    query_executor_with_statistics, mocker
):
    with Metrics.capture() as metrics:
        metrics.increment = mocker.Mock()
        await query_executor_with_statistics.perform_ttl_op("config")
        query_executor_with_statistics.inner.perform_ttl_op.assert_awaited_once_with(
            "config"
        )
        metrics.increment.assert_called_once_with(
            NodestreamMetricRegistry.TIME_TO_LIVE_OPERATIONS
        )


@pytest.mark.asyncio
async def test_execute_hook_increments_counter_by_one(
    query_executor_with_statistics, mocker
):
    with Metrics.capture() as metrics:
        metrics.increment = mocker.Mock()
        await query_executor_with_statistics.execute_hook("hook")
        query_executor_with_statistics.inner.execute_hook.assert_awaited_once_with(
            "hook"
        )
        metrics.increment.assert_called_once_with(
            NodestreamMetricRegistry.INGEST_HOOKS_EXECUTED
        )


@pytest.mark.asyncio
async def test_finish(query_executor_with_statistics, mocker):
    query_executor_with_statistics.inner.finish = mocker.AsyncMock()
    await query_executor_with_statistics.finish()
    query_executor_with_statistics.inner.finish.assert_awaited_once()
