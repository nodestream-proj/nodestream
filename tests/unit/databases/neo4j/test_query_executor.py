import pytest

from nodestream.model import TimeToLiveConfiguration, GraphObjectType
from nodestream.databases.neo4j.query import Query, QueryBatch, COMMIT_QUERY
from nodestream.databases.neo4j import Neo4jQueryExecutor


@pytest.fixture
def some_query():
    return Query("MATCH (n) RETURN n LIMIT $limit", {"limit ": "10"})


@pytest.fixture
def some_query_batch(some_query):
    return QueryBatch(some_query.query_statement, [some_query.parameters] * 10)


@pytest.fixture
def query_executor(mocker):
    return Neo4jQueryExecutor(mocker.AsyncMock(), mocker.Mock(), mocker.Mock(), "test")


@pytest.mark.asyncio
async def test_upsert_nodes_in_bulk_of_same_operation(
    mocker, query_executor, some_query_batch
):
    query_executor.ingest_query_builder.generate_batch_update_node_operation_batch.return_value = (
        some_query_batch
    )
    query_executor.execute = mocker.AsyncMock()
    await query_executor.upsert_nodes_in_bulk_with_same_operation(None, None)
    query_executor.ingest_query_builder.generate_batch_update_node_operation_batch.assert_called_once_with(
        None, None
    )
    query_executor.execute.assert_called_once_with(
        some_query_batch.as_query(), log_result=True
    )
    query_executor.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_upsert_rel_in_bulk_of_same_shape(
    mocker, query_executor, some_query_batch
):
    query_executor.ingest_query_builder.generate_batch_update_relationship_query_batch.return_value = (
        some_query_batch
    )
    query_executor.execute = mocker.AsyncMock()
    await query_executor.upsert_relationships_in_bulk_of_same_operation(None, None)
    query_executor.ingest_query_builder.generate_batch_update_relationship_query_batch.assert_called_once_with(
        None, None
    )
    query_executor.execute.assert_called_once_with(
        some_query_batch.as_query(), log_result=True
    )
    query_executor.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_perform_ttl_op(query_executor, some_query):
    ttl_config = TimeToLiveConfiguration(GraphObjectType.NODE, "NodeType")
    query_generator = (
        query_executor.ingest_query_builder.generate_ttl_query_from_configuration
    )
    query_generator.return_value = some_query
    await query_executor.perform_ttl_op(ttl_config)
    query_generator.assert_called_once_with(ttl_config)
    query_executor.driver.execute_query.assert_called_once_with(
        some_query.query_statement, some_query.parameters
    )
