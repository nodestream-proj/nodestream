from neo4j import AsyncDriver, AsyncGraphDatabase
from neo4j.auth_management import AsyncAuthManagers

from ...pipeline.argument_resolvers import RefreshableArgument
from ..database_connector import DatabaseConnector
from ..query_executor import QueryExecutor
from .index_query_builder import (
    Neo4jEnterpriseIndexQueryBuilder,
    Neo4jIndexQueryBuilder,
)
from .ingest_query_builder import Neo4jIngestQueryBuilder
from .query_executor import Neo4jQueryExecutor


class Neo4jDatabaseConnector(DatabaseConnector, alias="neo4j"):
    @classmethod
    def from_file_data(
        cls,
        uri: str,
        username: str | RefreshableArgument,
        password: str | RefreshableArgument,
        database_name: str = "neo4j",
        use_enterprise_features: bool = False,
    ):
        async def get_credentials():
            usr, pwd = username, password
            if isinstance(usr, RefreshableArgument):
                usr = username.get_current_value()
            if isinstance(pwd, RefreshableArgument):
                usr = password.get_current_value()
            return username, password

        driver = AsyncGraphDatabase.driver(
            uri, auth=AsyncAuthManagers.basic(get_credentials)
        )
        if use_enterprise_features:
            index_query_builder = Neo4jEnterpriseIndexQueryBuilder()
        else:
            index_query_builder = Neo4jIndexQueryBuilder()
        return cls(
            driver=driver,
            index_query_builder=index_query_builder,
            ingest_query_builder=Neo4jIngestQueryBuilder(),
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
        return Neo4jQueryExecutor(
            self.driver,
            self.ingest_query_builder,
            self.index_query_builder,
            self.database_name,
        )
