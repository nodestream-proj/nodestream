import pytest
from hamcrest import assert_that, equal_to

from nodestream_plugin_neo4j.extractor import Neo4jExtractor
from nodestream_plugin_neo4j.neo4j_database import Neo4jDatabaseConnection
from nodestream_plugin_neo4j.query import Query

from .matchers import ran_query


@pytest.mark.asyncio
async def test_extract_records(mocker):
    query = "MATCH (n:{test: $test}) RETURN n.name as name"
    connection = mocker.AsyncMock(Neo4jDatabaseConnection)
    extractor = Neo4jExtractor(
        query=query,
        database_connection=connection,
        parameters={"test": "test"},
        limit=2,
    )

    connection.execute.side_effect = [
        [{"name": "test1"}, {"name": "test2"}],
        [{"name": "test3"}],
        [],
    ]

    result = [item async for item in extractor.extract_records()]
    assert_that(
        result, equal_to([{"name": "test1"}, {"name": "test2"}, {"name": "test3"}])
    )

    expected_query = Query(query, {"test": "test", "limit": 2, "offset": 0})

    assert_that(extractor, ran_query(expected_query))
