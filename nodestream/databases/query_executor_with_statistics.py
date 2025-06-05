from typing import Iterable

from ..metrics import (
    INGEST_HOOKS_EXECUTED,
    NODES_UPSERTED,
    RELATIONSHIPS_UPSERTED,
    TIME_TO_LIVE_OPERATIONS,
    Metric,
    Metrics,
)
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
        self.node_metric_by_type: dict[str, Metric] = {}
        self.relationship_metric_by_relationship_type: dict[str, Metric] = {}

    def _get_or_create_node_metric(self, node_type: str) -> Metric:
        """Get or create a metric for a node type."""
        if node_type not in self.node_metric_by_type:
            self.node_metric_by_type[node_type] = Metric(
                f"nodes_upserted_by_type_{node_type}",
                f"Number of nodes upserted by type {node_type}",
            )
        return self.node_metric_by_type[node_type]

    def _get_or_create_relationship_metric(self, relationship_type: str) -> Metric:
        """Get or create a metric for a relationship type."""
        if relationship_type not in self.relationship_metric_by_relationship_type:
            self.relationship_metric_by_relationship_type[relationship_type] = Metric(
                f"relationships_upserted_by_relationship_type_{relationship_type}",
                f"Number of relationships upserted by relationship type {relationship_type}",
            )
        return self.relationship_metric_by_relationship_type[relationship_type]

    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        await self.inner.upsert_nodes_in_bulk_with_same_operation(operation, nodes)

        # Tally node types
        node_type_counts: dict[str, int] = {}
        total_nodes = 0
        for node in nodes:
            node_type_counts[node.type] = node_type_counts.get(node.type, 0) + 1
            total_nodes += 1

        # Increment metrics in bulk
        metrics = Metrics.get()
        metrics.increment(NODES_UPSERTED, total_nodes)
        for node_type, count in node_type_counts.items():
            metric = self._get_or_create_node_metric(node_type)
            metrics.increment(metric, count)

    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        await self.inner.upsert_relationships_in_bulk_of_same_operation(
            shape, relationships
        )

        # Tally relationship types
        relationship_type_counts: dict[str, int] = {}
        total_relationships = 0
        for relationship in relationships:
            rel_type = relationship.relationship.type
            relationship_type_counts[rel_type] = (
                relationship_type_counts.get(rel_type, 0) + 1
            )
            total_relationships += 1

        # Increment metrics in bulk
        metrics = Metrics.get()
        metrics.increment(RELATIONSHIPS_UPSERTED, total_relationships)
        for rel_type, count in relationship_type_counts.items():
            metric = self._get_or_create_relationship_metric(rel_type)
            metrics.increment(metric, count)

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        await self.inner.perform_ttl_op(config)
        Metrics.get().increment(TIME_TO_LIVE_OPERATIONS)

    async def execute_hook(self, hook: IngestionHook):
        await self.inner.execute_hook(hook)
        Metrics.get().increment(INGEST_HOOKS_EXECUTED)

    async def finish(self):
        await self.inner.finish()
