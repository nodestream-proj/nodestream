import asyncio
from abc import ABC, abstractmethod
from logging import getLogger
from typing import AsyncGenerator, Coroutine, Dict, List, Optional

from ..metrics import Metric, Metrics
from ..model import Node, RelationshipWithNodes
from ..pipeline import Extractor
from ..pipeline.channel import DoneObject
from ..pipeline.step import StepContext
from ..schema import Adjacency, Schema

ORCHESTRATOR_QUEUE = Metric(
    "orchestrator_queue", "Number of items in the orchestrator queue", accumulate=False
)
ACTIVE_QUERIES = Metric(
    "active_queries",
    "Number of active database queries in the copier",
    accumulate=False,
)


class TypeRetriever(ABC):
    @abstractmethod
    async def preview_relationship_count(self, relationship_type: str) -> int:
        raise NotImplementedError

    @abstractmethod
    async def preview_node_count(self, node_type: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_nodes_of_type(self, node_type: str) -> AsyncGenerator[Node, None]:
        raise NotImplementedError

    @abstractmethod
    def get_relationships_of_type_between(
        self, from_node_type: str, to_node_type: str, relationship_type: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        raise NotImplementedError


class Copier(Extractor):
    """Copies nodes and relationships sequentially, one type at a time."""

    def __init__(
        self,
        type_retriever: TypeRetriever,
        schema: Schema,
        node_types_to_copy: List[str],
        relationship_types_to_copy: List[str],
    ) -> None:
        self.type_retriever = type_retriever
        self.relationship_types = relationship_types_to_copy
        self.node_types = node_types_to_copy
        self.schema = schema
        self.logger = getLogger(__name__)
        self._node_counts: Dict[str, int] = {}
        self._relationship_counts: Dict[str, int] = {}

    @classmethod
    def create(
        cls,
        type_retriever: TypeRetriever,
        schema: Schema,
        node_types: List[str],
        relationship_types: List[str],
        concurrency_limit: int = 1,
        orchestrator_queue_size: Optional[int] = None,
    ) -> "Copier":
        if concurrency_limit > 1:
            return ConcurrentCopier(
                type_retriever,
                schema,
                node_types,
                relationship_types,
                concurrency_limit=concurrency_limit,
                orchestrator_queue_size=orchestrator_queue_size,
            )
        return cls(type_retriever, schema, node_types, relationship_types)

    async def start(self, context: StepContext):
        await super().start(context)

        # Preview counts and sort descending so largest types are copied first.
        for node_type in self.node_types:
            self._node_counts[node_type] = await self.type_retriever.preview_node_count(
                node_type
            )
        for relationship_type in self.relationship_types:
            self._relationship_counts[relationship_type] = (
                await self.type_retriever.preview_relationship_count(relationship_type)
            )

        self.node_types.sort(key=lambda t: self._node_counts[t], reverse=True)
        self.relationship_types.sort(
            key=lambda t: self._relationship_counts[t], reverse=True
        )

        self._log_histogram()

    def _log_histogram(self):
        self.logger.info("Node type histogram (descending):")
        for node_type in self.node_types:
            self.logger.info("  %s: %d", node_type, self._node_counts[node_type])
        self.logger.info("Relationship type histogram (descending):")
        for relationship_type in self.relationship_types:
            self.logger.info(
                "  %s: %d",
                relationship_type,
                self._relationship_counts[relationship_type],
            )
        self.logger.info(
            "Total nodes: %d, Total relationships: %d",
            sum(self._node_counts.values()),
            sum(self._relationship_counts.values()),
        )

    async def extract_records(self):
        for node_type in self.node_types:
            expected_count = self._node_counts.get(node_type, "?")
            self.logger.info(
                "Copying nodes of type %s (expected ~%s)", node_type, expected_count
            )
            async for node in self.type_retriever.get_nodes_of_type(node_type):
                yield self.convert_node_to_ingest(node)

        for relationship_type in self.relationship_types:
            expected_count = self._relationship_counts.get(relationship_type, "?")
            adjacencies = list(
                self.schema.get_adjacencies_by_relationship_type(relationship_type)
            )
            for adjacency in adjacencies:
                self.logger.info(
                    "Copying %s (%s -> %s, expected ~%s)",
                    adjacency.relationship_type,
                    adjacency.from_node_type,
                    adjacency.to_node_type,
                    expected_count,
                )
                async for (
                    relationship
                ) in self.type_retriever.get_relationships_of_type_between(
                    adjacency.from_node_type,
                    adjacency.to_node_type,
                    adjacency.relationship_type,
                ):
                    yield self.convert_relationship_to_ingest(relationship)

    def reorganize_node_key_properties(self, node: Node):
        """Move key fields from properties into key_values for ingestion.

        The type retriever returns all properties in a flat dict. The ingest
        pipeline expects key fields to be separated from regular properties so
        that the database connector can build the correct MERGE clause. We
        relocate them here rather than pushing that concern into every connector.
        """
        node_type_definition = self.schema.get_node_type_by_name(node.type)
        if node_type_definition is None:
            return
        for key_name in node_type_definition.keys:
            node.key_values[key_name] = node.properties[key_name]
            del node.properties[key_name]

    def convert_node_to_ingest(self, node: Node):
        self.reorganize_node_key_properties(node)
        return node.into_ingest()

    def convert_relationship_to_ingest(self, relationship: RelationshipWithNodes):
        self.reorganize_node_key_properties(relationship.from_node)
        self.reorganize_node_key_properties(relationship.to_node)
        return relationship.into_ingest()


class ConcurrentCopier(Copier):
    """Copier that runs concurrent fetch loops per type.

    All node types are fetched concurrently first, then all relationship types.
    The two groups are never mixed so slow relationship scans cannot starve
    fast node scans.  A semaphore bounds the number of concurrent producers.
    """

    def __init__(
        self,
        type_retriever: TypeRetriever,
        schema: Schema,
        node_types_to_copy: List[str],
        relationship_types_to_copy: List[str],
        concurrency_limit: int = 10,
        orchestrator_queue_size: Optional[int] = None,
    ) -> None:
        super().__init__(
            type_retriever, schema, node_types_to_copy, relationship_types_to_copy
        )
        self.concurrency_limit = max(1, concurrency_limit)
        self.orchestrator_queue_size = orchestrator_queue_size or 0

    async def extract_records(self):
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.orchestrator_queue_size)

        async def produce_nodes(node_type: str) -> None:
            expected_count = self._node_counts.get(node_type, "?")
            self.logger.info(
                "Copying nodes of type %s (expected ~%s)", node_type, expected_count
            )
            async for node in self.type_retriever.get_nodes_of_type(node_type):
                Metrics.get().increment(ORCHESTRATOR_QUEUE)
                await queue.put(self.convert_node_to_ingest(node))

        async def produce_relationships(adjacency: Adjacency) -> None:
            expected_count = self._relationship_counts.get(
                adjacency.relationship_type, "?"
            )
            self.logger.info(
                "Copying %s (%s -> %s, expected ~%s)",
                adjacency.relationship_type,
                adjacency.from_node_type,
                adjacency.to_node_type,
                expected_count,
            )
            async for (
                relationship
            ) in self.type_retriever.get_relationships_of_type_between(
                adjacency.from_node_type,
                adjacency.to_node_type,
                adjacency.relationship_type,
            ):
                Metrics.get().increment(ORCHESTRATOR_QUEUE)
                await queue.put(self.convert_relationship_to_ingest(relationship))

        async def run_bounded(coroutines: List[Coroutine]) -> None:
            semaphore = asyncio.Semaphore(self.concurrency_limit)

            async def run_with_limit(coroutine: Coroutine) -> None:
                async with semaphore:
                    Metrics.get().increment(ACTIVE_QUERIES)
                    try:
                        await coroutine
                    finally:
                        Metrics.get().decrement(ACTIVE_QUERIES)

            if coroutines:
                await asyncio.gather(*(run_with_limit(c) for c in coroutines))

        async def orchestrate() -> None:
            try:
                # All nodes first, then all relationships.
                await run_bounded(
                    [produce_nodes(node_type) for node_type in self.node_types]
                )

                all_adjacencies: List[Adjacency] = []
                for relationship_type in self.relationship_types:
                    all_adjacencies.extend(
                        self.schema.get_adjacencies_by_relationship_type(
                            relationship_type
                        )
                    )
                await run_bounded(
                    [produce_relationships(adjacency) for adjacency in all_adjacencies]
                )
            finally:
                # Always signal the consumer so it never hangs, even on error.
                await queue.put(DoneObject)

        orchestrator_task = asyncio.create_task(orchestrate())
        orchestrator_error = None
        try:
            while True:
                message = await queue.get()
                if message is DoneObject:
                    break
                Metrics.get().decrement(ORCHESTRATOR_QUEUE)
                yield message
        finally:
            # Wait for the orchestrator to finish and capture any error.
            try:
                await orchestrator_task
            except Exception as exc:
                orchestrator_error = exc

        # Re-raise the orchestrator error after the generator has cleaned up.
        if orchestrator_error is not None:
            raise orchestrator_error
