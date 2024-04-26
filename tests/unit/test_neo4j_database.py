import pytest
from hamcrest import assert_that, equal_to
from neo4j import AsyncDriver, RoutingControl

from nodestream.file_io import LazyLoadedArgument

from nodestream_plugin_neo4j.neo4j_database import (
    Neo4jDatabaseConnection,
    auth_provider_factory,
)
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


@pytest.mark.asyncio
async def test_auth_provider_factory_with_dynamic_values(mocker):
    username = mocker.Mock(LazyLoadedArgument)
    password = mocker.Mock(LazyLoadedArgument)
    provider = auth_provider_factory(username, password)
    retrieved_username, retrieved_password = await provider()
    assert_that(retrieved_username, equal_to(username.get_value.return_value))
    assert_that(retrieved_password, equal_to(password.get_value.return_value))


@pytest.mark.asyncio
async def test_auth_provider_factory_with_static_values():
    username = "neo4j"
    password = "password"
    provider = auth_provider_factory(username, password)
    retrieved_username, retrieved_password = await provider()
    assert_that(retrieved_username, equal_to(username))
    assert_that(retrieved_password, equal_to(password))
