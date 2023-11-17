from neo4j import AsyncDriver, AsyncGraphDatabase

from ..copy import TypeRetriever
from ..database_connector import DatabaseConnector
from ..query_executor import QueryExecutor
from .index_query_builder import (
    Neo4jEnterpriseIndexQueryBuilder,
    Neo4jIndexQueryBuilder,
)
from .ingest_query_builder import Neo4jIngestQueryBuilder


class Neo4jDatabaseConnector(DatabaseConnector, alias="neo4j"):
    @classmethod
    def from_file_data(
        cls,
        uri: str,
        username: str,
        password: str,
        database_name: str = "neo4j",
        use_enterprise_features: bool = False,
        use_apoc: bool = True,
    ):
        driver = AsyncGraphDatabase.driver(uri, auth=(username, password))
        if use_enterprise_features:
            index_query_builder = Neo4jEnterpriseIndexQueryBuilder()
        else:
            index_query_builder = Neo4jIndexQueryBuilder()
        return cls(
            driver=driver,
            index_query_builder=index_query_builder,
            ingest_query_builder=Neo4jIngestQueryBuilder(use_apoc),
            database_name=database_name,
        )

    def __init__(
        self,
        driver: AsyncDriver,
        index_query_builder: Neo4jIndexQueryBuilder,
        ingest_query_builder: Neo4jIngestQueryBuilder,
        database_name: str,
    ) -> None:
        self.driver = driver
        self.index_query_builder = index_query_builder
        self.ingest_query_builder = ingest_query_builder
        self.database_name = database_name

    def make_query_executor(self) -> QueryExecutor:
        from .query_executor import Neo4jQueryExecutor

        return Neo4jQueryExecutor(
            self.driver,
            self.ingest_query_builder,
            self.index_query_builder,
            self.database_name,
        )

    def make_type_retriever(self) -> TypeRetriever:
        from .type_retriever import Neo4jTypeRetriever

        return Neo4jTypeRetriever(self)
