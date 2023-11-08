import pytest

from nodestream.databases.neo4j.query import Query, QueryBatch
from nodestream.databases.neo4j.query_executor import Neo4jQueryExecutor
from nodestream.model import TimeToLiveConfiguration
from nodestream.schema.schema import GraphObjectType


@pytest.fixture
def some_query():
    return Query("MATCH (n) RETURN n LIMIT $limit", {"limit ": "10"})


@pytest.fixture
def some_query_batch(some_query):
    return QueryBatch(some_query.query_statement, [some_query.parameters] * 10)


@pytest.fixture
def query_executor(mocker):
    ingest_query_builder_mock = mocker.Mock()
    ingest_query_builder_mock.apoc_iterate = True
    return Neo4jQueryExecutor(
        mocker.AsyncMock(), ingest_query_builder_mock, mocker.Mock(), "test"
    )


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
        some_query_batch.as_query(query_executor.ingest_query_builder.apoc_iterate),
        log_result=True,
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
        some_query_batch.as_query(query_executor.ingest_query_builder.apoc_iterate),
        log_result=True,
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
        some_query.query_statement, some_query.parameters, database_="test"
    )


@pytest.mark.asyncio
async def test_upsert_key_index(query_executor, some_query):
    query_generator = query_executor.index_query_builder.create_key_index_query
    query_generator.return_value = some_query
    await query_executor.upsert_key_index(None)
    query_generator.assert_called_once_with(None)
    query_executor.driver.execute_query.assert_called_once_with(
        some_query.query_statement, some_query.parameters, database_="test"
    )


@pytest.mark.asyncio
async def test_upsert_field_index(query_executor, some_query):
    query_generator = query_executor.index_query_builder.create_field_index_query
    query_generator.return_value = some_query
    await query_executor.upsert_field_index(None)
    query_generator.assert_called_once_with(None)
    query_executor.driver.execute_query.assert_called_once_with(
        some_query.query_statement, some_query.parameters, database_="test"
    )


@pytest.mark.asyncio
async def test_execute_hook(query_executor, some_query, mocker):
    hook = mocker.Mock()
    hook.as_cypher_query_and_parameters = mocker.Mock(
        return_value=(some_query.query_statement, some_query.parameters)
    )
    await query_executor.execute_hook(hook)
    hook.as_cypher_query_and_parameters.assert_called_once()
    query_executor.driver.execute_query.assert_called_once_with(
        some_query.query_statement, some_query.parameters, database_="test"
    )
