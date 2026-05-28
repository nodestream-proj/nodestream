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
        for t in self.sorted_node_types():
            logger.info("  %s: %d", t, self.node_counts[t])
        logger.info("Relationship type histogram (descending):")
        for t in self.sorted_relationship_types():
            logger.info("  %s: %d", t, self.relationship_counts[t])
        logger.info(
            "Total nodes: %d, Total relationships: %d",
            sum(self.node_counts.values()),
            sum(self.relationship_counts.values()),
        )


class TypeRetriever(ABC):
    """Abstract base for retrieving graph objects from a source database.

    Implementors own type decomposition, shard splitting, histogram computation,
    and concurrency strategy. The copier's only interface to the retriever is
    fetch_nodes() and fetch_relationships() — both async generators.
    """

    def __init__(
        self,
        concurrency_limit: int = 1,
        orchestrator_queue_size: int = 0,
        relationships_only: bool = False,
    ) -> None:
        self.concurrency_limit = concurrency_limit
        self.orchestrator_queue_size = orchestrator_queue_size
        self.relationships_only = relationships_only

    @abstractmethod
    async def fetch_nodes(self, schema: Schema) -> AsyncGenerator[Node, None]:
        """Yield all nodes that should be copied, across all types."""
        raise NotImplementedError

    @abstractmethod
    async def fetch_relationships(
        self, schema: Schema
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        """Yield all relationships that should be copied, across all types."""
        raise NotImplementedError

    async def build_histogram(self, schema: Schema) -> TypeHistogram:
        """Return count estimates for all types to be copied.

        Default returns an empty histogram. Subclasses that support counting
        should override this to issue real COUNT queries.
        """
        return TypeHistogram.empty()


class FetchOrchestrator(ABC):
    """Controls how the Copier drives fetch_nodes and fetch_relationships.

    Selected once at Copier construction time based on the retriever's
    configuration — no runtime branching in extract_records.
    """

    @abstractmethod
    async def extract(
        self,
        retriever: TypeRetriever,
        schema: Schema,
        convert_node: Callable[[Node], Any],
        convert_relationship: Callable[[RelationshipWithNodes], Any],
    ) -> AsyncGenerator[Any, None]: ...


class SequentialFetchOrchestrator(FetchOrchestrator):
    def __init__(self, include_nodes: bool) -> None:
        self.include_nodes = include_nodes

    async def extract(
        self,
        retriever: TypeRetriever,
        schema: Schema,
        convert_node: Callable[[Node], Any],
        convert_relationship: Callable[[RelationshipWithNodes], Any],
    ) -> AsyncGenerator[Any, None]:
        if self.include_nodes:
            async for node in retriever.fetch_nodes(schema):
                yield convert_node(node)
        async for relationship in retriever.fetch_relationships(schema):
            yield convert_relationship(relationship)


class ConcurrentFetchOrchestrator(FetchOrchestrator):
    """Producer/consumer orchestrator with separate node and relationship queues.

    The node queue is fully produced and drained before the relationship queue
    starts, preserving write-side ordering. Metrics track queue depth per type.
    """

    def __init__(self, include_nodes: bool) -> None:
        self.include_nodes = include_nodes

    async def extract(
        self,
        retriever: TypeRetriever,
        schema: Schema,
        convert_node: Callable[[Node], Any],
        convert_relationship: Callable[[RelationshipWithNodes], Any],
    ) -> AsyncGenerator[Any, None]:
        queue_size = retriever.orchestrator_queue_size

        async def fill_queue(generator, queue, queue_metric) -> None:
            try:
                async for record in generator:
                    Metrics.get().increment(queue_metric)
                    await queue.put(record)
            finally:
                await queue.put(DoneObject)

        async def drain_queue(queue, queue_metric):
            while True:
                message = await queue.get()
                if message is DoneObject:
                    break
                Metrics.get().decrement(queue_metric)
                yield message

        if self.include_nodes:
            node_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
            nodes_gen = (convert_node(n) async for n in retriever.fetch_nodes(schema))
            node_task = asyncio.create_task(
                fill_queue(nodes_gen, node_queue, ORCHESTRATOR_NODE_QUEUE)
            )
            node_error = None
            try:
                async for record in drain_queue(node_queue, ORCHESTRATOR_NODE_QUEUE):
                    yield record
            finally:
                try:
                    await node_task
                except Exception as exc:
                    node_error = exc
            if node_error is not None:
                raise node_error

        rel_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        rels_gen = (
            convert_relationship(r) async for r in retriever.fetch_relationships(schema)
        )
        rel_task = asyncio.create_task(
            fill_queue(rels_gen, rel_queue, ORCHESTRATOR_REL_QUEUE)
        )
        rel_error = None
        try:
            async for record in drain_queue(rel_queue, ORCHESTRATOR_REL_QUEUE):
                yield record
        finally:
            try:
                await rel_task
            except Exception as exc:
                rel_error = exc
        if rel_error is not None:
            raise rel_error


def make_fetch_orchestrator(retriever: TypeRetriever) -> FetchOrchestrator:
    include_nodes = not retriever.relationships_only
    if retriever.concurrency_limit > 1:
        return ConcurrentFetchOrchestrator(include_nodes=include_nodes)
    return SequentialFetchOrchestrator(include_nodes=include_nodes)


class Copier(Extractor):
    """Copies nodes and relationships from a source via a TypeRetriever.

    The fetch orchestration strategy (sequential vs concurrent, nodes vs
    relationships-only) is resolved once at construction time and stored as
    self.orchestrator — extract_records contains no branching.
    """

    def __init__(
        self,
        type_retriever: TypeRetriever,
        schema: Schema,
    ) -> None:
        self.type_retriever = type_retriever
        self.schema = schema
        self.logger = getLogger(__name__)
        self.orchestrator = make_fetch_orchestrator(type_retriever)

    async def start(self, context: StepContext):
        await super().start(context)
        histogram = await self.type_retriever.build_histogram(self.schema)
        histogram.log(self.logger)

    async def extract_records(self):
        async for record in self.orchestrator.extract(
            self.type_retriever,
            self.schema,
            self.convert_node_to_ingest,
            self.convert_relationship_to_ingest,
        ):
            yield record

    def reorganize_node_key_properties(self, node: Node):
        """Move key fields from properties into key_values for ingestion."""
        node_type_definition = self.schema.get_node_type_by_name(node.type)
        if node_type_definition is None:
            return
        for key_name in node_type_definition.keys:
            if key_name in node.properties:
                node.key_values[key_name] = node.properties.pop(key_name)

    def convert_node_to_ingest(self, node: Node):
        self.reorganize_node_key_properties(node)
        return node.into_ingest()

    def convert_relationship_to_ingest(self, relationship: RelationshipWithNodes):
        self.reorganize_node_key_properties(relationship.from_node)
        self.reorganize_node_key_properties(relationship.to_node)
        return relationship.into_ingest()


# Backwards-compatible alias
ConcurrentCopier = Copier
