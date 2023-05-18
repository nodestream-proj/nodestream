import pytest

from nodestream.model import TimeToLiveConfiguration, GraphObjectType
from nodestream.databases.neo4j.query import Query
from nodestream.databases.neo4j import Neo4jQueryExecutor


@pytest.fixture
def some_query():
    return Query("MATCH (n) RETURN n", {"limit ": "10"})


@pytest.fixture
def query_executor(mocker):
    return Neo4jQueryExecutor(mocker.AsyncMock(), mocker.Mock(), mocker.Mock(), "test")


@pytest.mark.asyncio
async def test_upsert_nodes_in_bulk_of_same_shape(query_executor):
    pass


@pytest.mark.asyncio
async def test_upsert_rel_in_bulk_of_same_shape(query_executor):
    pass


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
