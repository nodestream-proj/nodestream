from typing import Iterable

from ..model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from ..pipeline.meta import get_context
from .query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)

NODE_STAT = "Node Upsert Operations"
RELATIONSHIP_STAT = "Relationship Upsert Operations"
TTL_STAT = "Time to Live Operations"
HOOK_STAT = "Ingest Hooks Executed"


class QueryExecutorWithStatistics(QueryExecutor):
    __slots__ = ("inner",)

    def __init__(self, inner: QueryExecutor) -> None:
        self.inner = inner

    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        await self.inner.upsert_nodes_in_bulk_with_same_operation(operation, nodes)
        get_context().increment_stat(NODE_STAT, len(nodes))

    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        await self.inner.upsert_relationships_in_bulk_of_same_operation(
            shape, relationships
        )
        get_context().increment_stat(RELATIONSHIP_STAT, len(relationships))

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        await self.inner.perform_ttl_op(config)
        get_context().increment_stat(TTL_STAT)

    async def execute_hook(self, hook: IngestionHook):
        await self.inner.execute_hook(hook)
        get_context().increment_stat(HOOK_STAT)
