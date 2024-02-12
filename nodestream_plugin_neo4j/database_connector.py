from nodestream.databases import DatabaseConnector, TypeRetriever
from nodestream.databases.query_executor import QueryExecutor
from nodestream.schema.migrations import Migrator

from .ingest_query_builder import Neo4jIngestQueryBuilder
from .migrator import Neo4jMigrator
from .neo4j_database import Neo4jDatabaseConnection
from .query_executor import Neo4jQueryExecutor
from .type_retriever import Neo4jTypeRetriever


class Neo4jDatabaseConnector(DatabaseConnector, alias="neo4j"):
    """A Connector for Neo4j Graph Databases.

    This class is responsible for creating the various components needed for
    nodestream to interact with a Neo4j database. It is also responsible
    for providing the configuration options for the Neo4j database.
    """

    @classmethod
    def from_file_data(
        cls,
        use_enterprise_features: bool = False,
        use_apoc: bool = True,
        **connection_args
    ):
        database_connection = Neo4jDatabaseConnection.from_configuration(
            **connection_args
        )
        return cls(
            database_connection=database_connection,
            use_enterprise_features=use_enterprise_features,
            use_apoc=use_apoc,
        )

    def __init__(
        self,
        database_connection: Neo4jDatabaseConnection,
        use_apoc: bool,
        use_enterprise_features: bool,
    ) -> None:
        self.use_enterprise_features = use_enterprise_features
        self.use_apoc = use_apoc
        self.database_connection = database_connection

    def make_query_executor(self) -> QueryExecutor:
        query_builder = Neo4jIngestQueryBuilder(self.use_apoc)
        return Neo4jQueryExecutor(self.database_connection, query_builder)

    def make_type_retriever(self) -> TypeRetriever:
        return Neo4jTypeRetriever(self.database_connection)

    def make_migrator(self) -> Migrator:
        return Neo4jMigrator(self.database_connection, self.use_enterprise_features)
