from hamcrest import assert_that, equal_to

from nodestream_plugin_neo4j import Neo4jDatabaseConnector


def test_make_query_executor(mocker):
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    executor = connector.make_query_executor()
    assert_that(executor.database_connection, equal_to(connector.database_connection))
    assert_that(executor.ingest_query_builder.apoc_iterate, equal_to(True))


def test_make_type_retriever(mocker):
    connector = Neo4jDatabaseConnector(
        database_connection=mocker.Mock(),
        use_apoc=True,
        use_enterprise_features=True,
    )
    retriever = connector.make_type_retriever()
    assert_that(retriever.database_connection, equal_to(connector.database_connection))


def test_from_file_data_no_enterprise_features():
    connector = Neo4jDatabaseConnector.from_file_data(
        uri="bolt://localhost:7687",
        username="neo4j",
        password="password",
        database_name="neo4j",
        use_enterprise_features=False,
        use_apoc=False,
    )
    assert_that(connector.use_enterprise_features, equal_to(False))


def test_from_file_data_with_enterprise_features():
    connector = Neo4jDatabaseConnector.from_file_data(
        uri="bolt://localhost:7687",
        username="neo4j",
        password="password",
        database_name="neo4j",
        use_enterprise_features=True,
        use_apoc=False,
    )
    assert_that(connector.use_enterprise_features, equal_to(True))
