from hamcrest import assert_that, equal_to, instance_of

from nodestream.databases.neo4j import Neo4jDatabaseConnector
from nodestream.databases.neo4j.index_query_builder import (
    Neo4jEnterpriseIndexQueryBuilder,
    Neo4jIndexQueryBuilder,
)


def test_make_query_executor(mocker):
    connector = Neo4jDatabaseConnector(
        driver=mocker.Mock(),
        index_query_builder=mocker.Mock(),
        ingest_query_builder=mocker.Mock(),
        database_name="neo4j",
    )
    executor = connector.make_query_executor()
    assert_that(executor.driver, equal_to(connector.driver))
    assert_that(executor.ingest_query_builder, equal_to(connector.ingest_query_builder))
    assert_that(executor.index_query_builder, equal_to(connector.index_query_builder))
    assert_that(executor.database_name, equal_to(connector.database_name))


def test_make_type_retriever(mocker):
    connector = Neo4jDatabaseConnector(
        driver=mocker.Mock(),
        index_query_builder=mocker.Mock(),
        ingest_query_builder=mocker.Mock(),
        database_name="neo4j",
    )
    retriever = connector.make_type_retriever()
    assert_that(retriever.connector, equal_to(connector))


def test_from_file_data_no_enterprise_features():
    connector = Neo4jDatabaseConnector.from_file_data(
        uri="bolt://localhost:7687",
        username="neo4j",
        password="password",
        database_name="neo4j",
        use_enterprise_features=False,
        use_apoc=False,
    )
    assert_that(connector.index_query_builder, instance_of(Neo4jIndexQueryBuilder))


def test_from_file_data_with_enterprise_features():
    connector = Neo4jDatabaseConnector.from_file_data(
        uri="bolt://localhost:7687",
        username="neo4j",
        password="password",
        database_name="neo4j",
        use_enterprise_features=True,
        use_apoc=False,
    )
    assert_that(
        connector.index_query_builder, instance_of(Neo4jEnterpriseIndexQueryBuilder)
    )
