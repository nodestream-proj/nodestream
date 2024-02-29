import pytest
from hamcrest import assert_that, equal_to
from neo4j import AsyncDriver, RoutingControl

from nodestream_plugin_neo4j.neo4j_database import Neo4jDatabaseConnection
from nodestream_plugin_neo4j.query import Query


@pytest.fixture
def database_connection(mocker):
    return Neo4jDatabaseConnection(mocker.AsyncMock(AsyncDriver), "neo4j")


@pytest.mark.asyncio
async def test_execute(database_connection):
    query = Query("MATCH (n) RETURN n LIMIT $limit", {"limit": 2})
    records = [
        {"n": {"name": "foo"}},
        {"n": {"name": "bar"}},
    ]
    database_connection.driver.execute_query.return_value.records = records
    result = await database_connection.execute(query, log_result=True)
    assert_that(result, equal_to(records))
    database_connection.driver.execute_query.assert_called_once_with(
        query.query_statement,
        query.parameters,
        database_="neo4j",
        routing_=RoutingControl.WRITE,
    )


@pytest.mark.asyncio
async def test_session(database_connection):
    session = database_connection.session()
    assert_that(session, equal_to(database_connection.driver.session.return_value))
    database_connection.driver.session.assert_called_once_with(database="neo4j")
