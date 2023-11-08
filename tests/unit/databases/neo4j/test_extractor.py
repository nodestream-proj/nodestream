import pytest
from hamcrest import assert_that, equal_to

from nodestream.databases.neo4j.extractor import Neo4jExtractor


@pytest.mark.asyncio
async def test_extract_records(mocker):
    mock_connector = mocker.patch(
        "nodestream.databases.neo4j.extractor.Neo4jDatabaseConnector"
    )
    mock_connector.from_file_data.return_value = mock_connector
    mock_connector.driver = mocker.AsyncMock()
    mock_connector.database_name = "test"
    mock_connector.driver.execute_query.side_effect = [
        [[{"name": "test1"}, {"name": "test2"}], "SummaryObject", ["name"]],
        [[{"name": "test3"}], "SummaryObject", ["name"]],
        [[], "SummaryObject", ["name"]],
    ]

    extractor = Neo4jExtractor(
        query="MATCH (n) RETURN n.name as name",
        params={"test": "test"},
        limit=2,
        uri="bolt://localhost:7687",
        username="neo4j",
        password="test",
    )

    result = [item async for item in extractor.extract_records()]
    print(result)
    assert_that(
        result, equal_to([{"name": "test1"}, {"name": "test2"}, {"name": "test3"}])
    )
