from logging import getLogger
from typing import Iterable

from neo4j import AsyncDriver
from neo4j.exceptions import ServiceUnavailable
from nodestream.model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from nodestream.schema.indexes import FieldIndex, KeyIndex
from nodestream.databases.query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)
from nodestream.databases.neo4j.index_query_builder import Neo4jIndexQueryBuilder
from nodestream.databases.neo4j.ingest_query_builder import Neo4jIngestQueryBuilder
from nodestream.databases.neo4j.query import Query
from time import sleep

MAX_ATTEMPTS = 10
BASE_BACKOFF_TIME = 1 # 1 second is arbitrary, backoff increases exponentially. 

class Neo4jQueryExecutor(QueryExecutor):
    def __init__(
        self,
        driver: AsyncDriver,
        ingest_query_builder: Neo4jIngestQueryBuilder,
        index_query_builder: Neo4jIndexQueryBuilder,
        database_name: str,
    ) -> None:
        self.driver = driver
        self.ingest_query_builder = ingest_query_builder
        self.index_query_builder = index_query_builder
        self.logger = getLogger(self.__class__.__name__)
        self.database_name = database_name

    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        batched_query = (
            self.ingest_query_builder.generate_batch_update_node_operation_batch(
                operation, nodes
            )
        )
        await self.execute(
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
        await self.execute(
            batched_query.as_query(self.ingest_query_builder.apoc_iterate),
            log_result=True,
        )

    async def upsert_key_index(self, index: KeyIndex):
        query = self.index_query_builder.create_key_index_query(index)
        await self.execute(query)

    async def upsert_field_index(self, index: FieldIndex):
        query = self.index_query_builder.create_field_index_query(index)
        await self.execute(query)

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        query = self.ingest_query_builder.generate_ttl_query_from_configuration(config)
        await self.execute(query)

    async def execute_hook(self, hook: IngestionHook):
        query_string, params = hook.as_cypher_query_and_parameters()
        await self.execute(Query(query_string, params))

    async def execute(self, query: Query, log_result: bool = False):
        self.logger.info(
            "Executing Cypher Query to Neo4j",
            extra={
                "query": query.query_statement,
                "uri": self.driver._pool.address.host,
            },
        )

        try:
            await self.driver.verify_connectivity()
        except ServiceUnavailable:
            self.logger.exception(
                f"Neo4j Session timed out while waiting for other steps to resolve. Trying one more time.",
                stack_info=True,
                extra={"class": self.__class__.__name__},
            )
            await self.driver.verify_connectivity()

        result = await self.driver.execute_query(
            query.query_statement,
            query.parameters,
            database_=self.database_name,
        )
        if log_result:
            for record in result.records:
                self.logger.info(
                    "Gathered Query Results",
                    extra=dict(**record, query=query.query_statement),
                )
