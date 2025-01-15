from typing import Iterable

from ..metrics import Metric, Metrics
from ..model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from .query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)


class QueryExecutorWithStatistics(QueryExecutor):
    __slots__ = ("inner",)

    def __init__(self, inner: QueryExecutor) -> None:
        self.inner = inner

    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        await self.inner.upsert_nodes_in_bulk_with_same_operation(operation, nodes)
        Metrics.get().increment(Metric.NODES_UPSERTED, len(nodes))

    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        await self.inner.upsert_relationships_in_bulk_of_same_operation(
            shape, relationships
        )
        Metrics.get().increment(Metric.RELATIONSHIPS_UPSERTED, len(relationships))

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        await self.inner.perform_ttl_op(config)
        Metrics.get().increment(Metric.TIME_TO_LIVE_OPERATIONS)

    async def execute_hook(self, hook: IngestionHook):
        await self.inner.execute_hook(hook)
        Metrics.get().increment(Metric.INGEST_HOOKS_EXECUTED)

    async def finish(self):
        await self.inner.finish()
