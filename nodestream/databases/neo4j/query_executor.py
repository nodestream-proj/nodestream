from typing import Iterable

from neo4j import AsyncDriver, AsyncSession

from ...model import (
    Node,
    RelationshipWithNodes,
    KeyIndex,
    FieldIndex,
    GraphObjectShape,
    TimeToLiveConfiguration,
    IngestionHook,
)
from .index_query_builder import Neo4jIndexQueryBuilder
from .ingest_query_builder import IngestQueryBuilder
from .query import Query

# 1.) Node + Relationship can create shape ignoring keys.
# 2.) Node + Relationship lists can be deduplicatable:
#       A.) Node is by key and type.
#       B.) Rel is by key and type and nodes its connecting.
# 3.) Ingest Query Builder operates basically on shape.

# ----> store_by_shape ----> debounce_by_keys+type ----> build_query_by_shape ---> bulk execut


# TODO: Move these to the model
class NodeIdentityShape:
    pass


class RelationshipIdentityShape:
    pass


class RelationshipWithNodesIdentityShape:
    # shapes of the three parts.
    # the match strategy.
    pass


class QueryExecutor:
    def __init__(
        self,
        driver: AsyncDriver,
        index_query_builder: Neo4jIndexQueryBuilder,
        ingest_query_builder: IngestQueryBuilder,
        database_name: str,
    ) -> None:
        self.driver = driver
        self.index_query_builder = index_query_builder
        self.ingest_query_builder = ingest_query_builder
        self.database_name = database_name

    async def upsert_nodes_in_bulk_of_same_shape(
        self, shape: NodeIdentityShape, nodes: Iterable[Node]
    ):
        raise NotImplementedError

    async def upsert_relationships_in_bulk_of_same_shape(
        self,
        shape: RelationshipWithNodesIdentityShape,
        rels: Iterable[RelationshipWithNodes],
    ):
        raise NotImplementedError

    async def upsert_key_index(self, index: KeyIndex):
        await self.execute(self.index_query_builder.create_key_index_query(index))

    async def upsert_field_index(self, index: FieldIndex):
        await self.execute(self.index_query_builder.create_field_index_query(index))

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        # TODO:
        raise NotImplementedError

    async def execute_hook(self, hook: IngestionHook):
        query, params = hook.as_cypher_query_and_parameters()
        await self.execute(Query(query, params))

    async def execute(self, query: Query):
        # TODO: Add some logging here.
        await self.driver.verify_connectivity()
        await self.driver.execute_query(query.query_statement, query.paramters)
