import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import Any, AsyncGenerator, Callable, Dict, List

from ..metrics import Metric, Metrics
from ..model import Node, RelationshipWithNodes
from ..pipeline import Extractor
from ..pipeline.channel import DoneObject
from ..pipeline.step import StepContext
from ..schema import Schema

ORCHESTRATOR_NODE_QUEUE = Metric(
    "orchestrator_node_queue",
    "Number of nodes in the orchestrator node queue",
    accumulate=False,
)
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

    @classmethod
    def empty(cls) -> "TypeHistogram":
        return cls()

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
        return TypeHistogram.empty()


class ExtractionOrchestrator(ABC):
    """Drives fetchNodes and/or fetchRelationships for a single copy run.

    Two concrete modes, selected once at Copier construction time:
      NodeOnlyOrchestrator   — fetches nodes, never calls fetchRelationships.
      AdjacencyOrchestrator  — fetches nodes then relationships (default).
    Sequential vs concurrent is a sub-variant of AdjacencyOrchestrator.
    """

    @abstractmethod
    async def extract(
        self,
        retriever: TypeRetriever,
        convertNode: Callable[[Node], Any],
        convertRelationship: Callable[[RelationshipWithNodes], Any],
    ) -> AsyncGenerator[Any, None]: ...


class NodeOnlyOrchestrator(ExtractionOrchestrator):
    """Yields only nodes — fetchRelationships is never called."""

    async def extract(
        self,
        retriever: TypeRetriever,
        convertNode: Callable[[Node], Any],
        convertRelationship: Callable[[RelationshipWithNodes], Any],
    ) -> AsyncGenerator[Any, None]:
        async for node in retriever.fetchNodes():
            yield convertNode(node)


class SequentialAdjacencyOrchestrator(ExtractionOrchestrator):
    """Yields nodes then relationships, sequentially."""

    async def extract(
        self,
        retriever: TypeRetriever,
        convertNode: Callable[[Node], Any],
        convertRelationship: Callable[[RelationshipWithNodes], Any],
    ) -> AsyncGenerator[Any, None]:
        async for node in retriever.fetchNodes():
            yield convertNode(node)
        async for relationship in retriever.fetchRelationships():
            yield convertRelationship(relationship)


class ConcurrentAdjacencyOrchestrator(ExtractionOrchestrator):
    """Producer/consumer adjacency orchestrator with separate node and relationship queues.

    The node queue is fully produced and drained before the relationship queue
    starts, preserving write-side ordering. Metrics track queue depth per type.
    """

    async def extract(
        self,
        retriever: TypeRetriever,
        convertNode: Callable[[Node], Any],
        convertRelationship: Callable[[RelationshipWithNodes], Any],
    ) -> AsyncGenerator[Any, None]:
        queueSize = retriever.orchestrator_queue_size

        async def fillQueue(generator, queue, queueMetric) -> None:
            try:
                async for record in generator:
                    Metrics.get().increment(queueMetric)
                    await queue.put(record)
            finally:
                await queue.put(DoneObject)

        async def drainQueue(queue, queueMetric):
            while True:
                message = await queue.get()
                if message is DoneObject:
                    break
                Metrics.get().decrement(queueMetric)
                yield message

        nodeQueue: asyncio.Queue = asyncio.Queue(maxsize=queueSize)
        nodesGenerator = (convertNode(node) async for node in retriever.fetchNodes())
        nodeTask = asyncio.create_task(
            fillQueue(nodesGenerator, nodeQueue, ORCHESTRATOR_NODE_QUEUE)
        )
        nodeError = None
        try:
            async for record in drainQueue(nodeQueue, ORCHESTRATOR_NODE_QUEUE):
                yield record
        finally:
            try:
                await nodeTask
            except Exception as exception:
                nodeError = exception
        if nodeError is not None:
            raise nodeError

        relationshipQueue: asyncio.Queue = asyncio.Queue(maxsize=queueSize)
        relationshipsGenerator = (
            convertRelationship(relationship)
            async for relationship in retriever.fetchRelationships()
        )
        relationshipTask = asyncio.create_task(
            fillQueue(relationshipsGenerator, relationshipQueue, ORCHESTRATOR_REL_QUEUE)
        )
        relationshipError = None
        try:
            async for record in drainQueue(relationshipQueue, ORCHESTRATOR_REL_QUEUE):
                yield record
        finally:
            try:
                await relationshipTask
            except Exception as exception:
                relationshipError = exception
        if relationshipError is not None:
            raise relationshipError


def buildExtractionOrchestrator(retriever: TypeRetriever) -> ExtractionOrchestrator:
    if retriever.node_only:
        return NodeOnlyOrchestrator()
    if retriever.concurrency_limit > 1:
        return ConcurrentAdjacencyOrchestrator()
    return SequentialAdjacencyOrchestrator()


class Copier(Extractor):
    """Copies nodes and relationships from a source via a TypeRetriever.

    The fetch orchestration strategy (sequential vs concurrent, nodes vs
    relationships-only) is resolved once at construction time and stored as
    self.orchestrator — extract_records contains no branching.
    """

    def __init__(self, type_retriever: TypeRetriever) -> None:
        self.type_retriever = type_retriever
        self.logger = getLogger(__name__)
        self.orchestrator = buildExtractionOrchestrator(type_retriever)

    async def start(self, context: StepContext):
        await super().start(context)
        histogram = await self.type_retriever.build_histogram()
        histogram.log(self.logger)

    async def extract_records(self):
        async for record in self.orchestrator.extract(
            self.type_retriever,
            self.convert_node_to_ingest,
            self.convert_relationship_to_ingest,
        ):
            yield record

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
