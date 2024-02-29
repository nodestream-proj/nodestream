from logging import getLogger
from typing import Iterable

from nodestream.databases.query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)
from nodestream.model import (
    IngestionHook,
    Node,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)

from .ingest_query_builder import Neo4jIngestQueryBuilder
from .neo4j_database import Neo4jDatabaseConnection
from .query import Query


class Neo4jQueryExecutor(QueryExecutor):
    def __init__(
        self,
        database_connection: Neo4jDatabaseConnection,
        ingest_query_builder: Neo4jIngestQueryBuilder,
    ) -> None:
        self.database_connection = database_connection
        self.ingest_query_builder = ingest_query_builder
        self.logger = getLogger(self.__class__.__name__)

    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        batched_query = (
            self.ingest_query_builder.generate_batch_update_node_operation_batch(
                operation, nodes
            )
        )
        await self.database_connection.execute(
            batched_query.as_query(self.ingest_query_builder.apoc_iterate),
            log_result=True,
        )

    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        batched_query = (
            self.ingest_query_builder.generate_batch_update_relationship_query_batch(
                shape, relationships
            )
        )
        await self.database_connection.execute(
            batched_query.as_query(self.ingest_query_builder.apoc_iterate),
            log_result=True,
        )

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        query = self.ingest_query_builder.generate_ttl_query_from_configuration(config)
        await self.database_connection.execute(query)

    async def execute_hook(self, hook: IngestionHook):
        query_string, params = hook.as_cypher_query_and_parameters()
        await self.database_connection.execute(Query(query_string, params))
