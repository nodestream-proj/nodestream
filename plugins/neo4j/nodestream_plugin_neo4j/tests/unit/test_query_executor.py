import pytest
from hamcrest import assert_that
from nodestream.model import TimeToLiveConfiguration
from nodestream.schema import GraphObjectType

from nodestream_plugin_neo4j.neo4j_database import Neo4jDatabaseConnection
from nodestream_plugin_neo4j.query import Query, QueryBatch
from nodestream_plugin_neo4j.query_executor import Neo4jQueryExecutor

from .matchers import ran_query
from .test_ingest_query_builder import DEFAULT_RETRIES_PER_CHUNK

TEST_PARAMS = {
    "retries_per_chunk": DEFAULT_RETRIES_PER_CHUNK,
}


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
    database_connection = mocker.AsyncMock(Neo4jDatabaseConnection)
    return Neo4jQueryExecutor(database_connection, ingest_query_builder_mock)


@pytest.mark.asyncio
async def test_execute_query_batch(query_executor, some_query_batch, mocker):
    expected_query = some_query_batch.as_query(
        query_executor.ingest_query_builder.apoc_iterate,
        chunk_size=query_executor.chunk_size,
        execute_chunks_in_parallel=query_executor.execute_chunks_in_parallel,
        retries_per_chunk=query_executor.retries_per_chunk,
    )

    await query_executor.execute_query_batch(some_query_batch)
    assert_that(query_executor, ran_query(expected_query))


@pytest.mark.asyncio
async def test_upsert_nodes_in_bulk_of_same_operation(query_executor, some_query_batch):
    query_executor.ingest_query_builder.generate_batch_update_node_operation_batch.return_value = (
        some_query_batch
    )
    await query_executor.upsert_nodes_in_bulk_with_same_operation(None, None)
    query_executor.ingest_query_builder.generate_batch_update_node_operation_batch.assert_called_once_with(
        None, None
    )

    expected_query = some_query_batch.as_query(
        query_executor.ingest_query_builder.apoc_iterate
    )
    assert_that(query_executor, ran_query(expected_query))


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

    expected_query = some_query_batch.as_query(
        query_executor.ingest_query_builder.apoc_iterate
    )
    assert_that(query_executor, ran_query(expected_query))


@pytest.mark.asyncio
async def test_perform_ttl_op(query_executor, some_query):
    ttl_config = TimeToLiveConfiguration(GraphObjectType.NODE, "NodeType")
    query_generator = (
        query_executor.ingest_query_builder.generate_ttl_query_from_configuration
    )
    query_generator.return_value = some_query
    await query_executor.perform_ttl_op(ttl_config)
    query_generator.assert_called_once_with(ttl_config, **TEST_PARAMS)
    assert_that(query_executor, ran_query(some_query))


@pytest.mark.asyncio
async def test_execute_hook(query_executor, some_query, mocker):
    hook = mocker.Mock()
    hook.as_cypher_query_and_parameters = mocker.Mock(
        return_value=(some_query.query_statement, some_query.parameters)
    )
    await query_executor.execute_hook(hook)
    hook.as_cypher_query_and_parameters.assert_called_once()
    assert_that(query_executor, ran_query(some_query))
