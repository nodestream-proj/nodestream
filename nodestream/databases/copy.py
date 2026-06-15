import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import AsyncGenerator, Dict, List

from ..metrics import Metric, Metrics
from ..model import Node, RelationshipWithNodes
from ..pipeline import Extractor
from ..pipeline.channel import DoneObject
from ..pipeline.step import StepContext
from ..schema import Schema

ORCHESTRATOR_REL_QUEUE = Metric(
    "orchestrator_rel_queue",
    "Number of relationships in the orchestrator relationship queue",
    accumulate=False,
)
ACTIVE_QUERIES = Metric(
    "active_queries",
    "Number of active database queries in the copier",
    accumulate=False,
)


@dataclass
class TypeHistogram:
    """Count estimates for all node and relationship types to be copied."""

    node_counts: Dict[str, int] = field(default_factory=dict)
    relationship_counts: Dict[str, int] = field(default_factory=dict)

    def sorted_node_types(self) -> List[str]:
        return sorted(self.node_counts, key=self.node_counts.__getitem__, reverse=True)

    def sorted_relationship_types(self) -> List[str]:
        return sorted(
            self.relationship_counts,
            key=self.relationship_counts.__getitem__,
            reverse=True,
        )

    def log(self, logger: Logger) -> None:
        logger.info("Node type histogram (descending):")
        for nodeType in self.sorted_node_types():
            logger.info("  %s: %d", nodeType, self.node_counts[nodeType])
        logger.info("Relationship type histogram (descending):")
        for relType in self.sorted_relationship_types():
            logger.info("  %s: %d", relType, self.relationship_counts[relType])
        logger.info(
            "Total nodes: %d, Total relationships: %d",
            sum(self.node_counts.values()),
            sum(self.relationship_counts.values()),
        )


class TypeRetriever(ABC):
    """Abstract base for retrieving graph objects from a source database.

    The retriever owns schema, type decomposition, shard splitting, and
    histogram computation. Schema is a constructor argument — the retriever
    uses it internally; callers never pass it at fetch time.

    node_only=True: fetch only nodes (fetchRelationships is never called).
    node_only=False (default): adjacency extraction — nodes then relationships.
    """

    def __init__(
        self,
        schema: Schema,
        concurrency_limit: int = 1,
        orchestrator_queue_size: int = 0,
        node_only: bool = False,
    ) -> None:
        self.schema = schema
        self.concurrency_limit = concurrency_limit
        self.orchestrator_queue_size = orchestrator_queue_size
        self.node_only = node_only

    @abstractmethod
    async def fetchNodes(self) -> AsyncGenerator[Node, None]:
        """Yield all nodes that should be copied, across all types."""
        raise NotImplementedError

    @abstractmethod
    async def fetchRelationships(
        self,
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        """Yield all relationships that should be copied, across all types."""
        raise NotImplementedError

    async def build_histogram(self) -> TypeHistogram:
        """Return count estimates for all types to be copied.

        Default returns an empty histogram. Subclasses that support counting
        should override this to issue real COUNT queries.
        """
        return TypeHistogram()


class Copier(Extractor):
    """Copies nodes and/or relationships from a source via a TypeRetriever.

    node_only=True  → yields nodes only (fetchRelationships never called).
    node_only=False → yields adjacencies via a producer/consumer queue;
                      RelationshipWithNodes carries both endpoints so no
                      separate node fetch is needed. The queue is unbounded
                      when orchestrator_queue_size=0, making it effectively
                      sequential for concurrency_limit=1.
    """

    def __init__(self, type_retriever: TypeRetriever) -> None:
        self.type_retriever = type_retriever
        self.logger = getLogger(__name__)

    async def start(self, context: StepContext):
        await super().start(context)
        histogram = await self.type_retriever.build_histogram()
        histogram.log(self.logger)

    async def extract_records(self):
        if self.type_retriever.node_only:
            async for node in self.type_retriever.fetchNodes():
                yield self.convert_node_to_ingest(node)
            return

        queueSize = self.type_retriever.orchestrator_queue_size

        async def fillQueue(queue) -> None:
            try:
                async for relationship in self.type_retriever.fetchRelationships():
                    Metrics.get().increment(ORCHESTRATOR_REL_QUEUE)
                    await queue.put(self.convert_relationship_to_ingest(relationship))
            finally:
                await queue.put(DoneObject)

        queue: asyncio.Queue = asyncio.Queue(maxsize=queueSize)
        task = asyncio.create_task(fillQueue(queue))
        taskError = None
        try:
            while True:
                record = await queue.get()
                if record is DoneObject:
                    break
                Metrics.get().decrement(ORCHESTRATOR_REL_QUEUE)
                yield record
        finally:
            try:
                await task
            except Exception as exc:
                taskError = exc
        if taskError is not None:
            raise taskError

    def reorganize_node_key_properties(self, node: Node):
        """Move key fields from properties into key_values for ingestion."""
        nodeTypeDefinition = self.type_retriever.schema.get_node_type_by_name(node.type)
        if nodeTypeDefinition is None:
            return
        for keyName in nodeTypeDefinition.keys:
            if keyName in node.properties:
                node.key_values[keyName] = node.properties.pop(keyName)

    def convert_node_to_ingest(self, node: Node):
        self.reorganize_node_key_properties(node)
        return node.into_ingest()

    def convert_relationship_to_ingest(self, relationship: RelationshipWithNodes):
        self.reorganize_node_key_properties(relationship.from_node)
        self.reorganize_node_key_properties(relationship.to_node)
        return relationship.into_ingest()


# Backwards-compatible alias
ConcurrentCopier = Copier
