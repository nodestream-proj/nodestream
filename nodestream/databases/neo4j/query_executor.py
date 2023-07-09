from logging import getLogger
from typing import Iterable

from neo4j import AsyncDriver, AsyncGraphDatabase

from ...model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from ...schema.indexes import FieldIndex, KeyIndex
from ..query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)
from .index_query_builder import (
    Neo4jEnterpriseIndexQueryBuilder,
    Neo4jIndexQueryBuilder,
)
from .ingest_query_builder import Neo4jIngestQueryBuilder
from .query import Query


class Neo4jQueryExecutor(QueryExecutor, alias="neo4j"):
    @classmethod
    def from_file_data(
        cls,
        uri: str,
        username: str,
        password: str,
        database_name: str = "neo4j",
        use_enterprise_features: bool = False,
    ):
        driver = AsyncGraphDatabase.driver(uri, auth=(username, password))
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
        self.logger = getLogger(self.__class__.__name__)

    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        batched_query = (
            self.ingest_query_builder.generate_batch_update_node_operation_batch(
                operation, nodes
            )
        )
        await self.execute(batched_query.as_query(), log_result=True)

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
        await self.execute(batched_query.as_query(), log_result=True)

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
        self.logger.debug(
            "Executing Cypher Query to Neo4j",
            extra={
                "query": query.query_statement,
                "uri": self.driver._pool.address.host,
            },
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
