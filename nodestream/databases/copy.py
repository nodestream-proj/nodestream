import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import AsyncGenerator, Dict, List, Optional

from ..metrics import Metric, Metrics
from ..model import Node, RelationshipWithNodes
from ..pipeline import Extractor
from ..pipeline.channel import DoneObject
from ..pipeline.step import StepContext
from ..schema import Schema

ORCHESTRATOR_NODE_QUEUE = Metric(
    "orchestrator_node_queue", "Number of nodes in the orchestrator node queue", accumulate=False
)
ORCHESTRATOR_REL_QUEUE = Metric(
    "orchestrator_rel_queue", "Number of relationships in the orchestrator relationship queue", accumulate=False
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
    fetch_nodes() and fetch_relationships() — both async generators. If the
    retriever wants internal parallelism (shards, per-type coroutines) it runs
    its own concurrency machinery and yields results as they arrive. The copier
    never sees the difference.

    Concurrency knobs (concurrency_limit, orchestrator_queue_size) live here so
    that a single Copier pipeline can be wired with any retriever implementation,
    including one that is fully sequential (the default).
    """

    # Subclasses set these to opt into the concurrent consumer loop in Copier.
    concurrency_limit: int = 1
    orchestrator_queue_size: int = 0

    relationships_only: bool = False

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


class Copier(Extractor):
    """Copies nodes and relationships from a source via a TypeRetriever.

    Concurrency is controlled entirely by the retriever: if
    retriever.concurrency_limit > 1 the copier spins up a producer/consumer
    queue so the retriever's async generators can push records concurrently
    while the copier drains them. If concurrency_limit == 1 the copier iterates
    sequentially — no queue, no tasks.

    This means a single Copier(retriever, schema) works for any retriever
    regardless of its internal strategy (sequential, sharded, sampled, etc.).
    """

    def __init__(
        self,
        type_retriever: TypeRetriever,
        schema: Schema,
    ) -> None:
        self.type_retriever = type_retriever
        self.schema = schema
        self.logger = getLogger(__name__)

    async def start(self, context: StepContext):
        await super().start(context)
        histogram = await self.type_retriever.build_histogram(self.schema)
        histogram.log(self.logger)

    async def extract_records(self):
        if self.type_retriever.concurrency_limit > 1:
            async for record in self._extract_concurrent():
                yield record
        else:
            async for record in self._extract_sequential():
                yield record

    async def _extract_sequential(self):
        if not self.type_retriever.relationships_only:
            async for node in self.type_retriever.fetch_nodes(self.schema):
                yield self.convert_node_to_ingest(node)
        async for relationship in self.type_retriever.fetch_relationships(self.schema):
            yield self.convert_relationship_to_ingest(relationship)

    async def _extract_concurrent(self):
        """Producer/consumer loop with separate node and relationship queues.

        Two typed queues keep nodes and relationships distinct throughout the
        pipeline. The node queue is fully produced and drained before the
        relationship queue starts, preserving write-side ordering. Metrics are
        tracked independently so queue depth is visible per type.
        """
        concurrency_limit = self.type_retriever.concurrency_limit
        queue_size = self.type_retriever.orchestrator_queue_size

        node_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        rel_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)

        async def _fill_queue(generator, queue, queue_metric) -> None:
            try:
                async for record in generator:
                    Metrics.get().increment(queue_metric)
                    await queue.put(record)
            finally:
                await queue.put(DoneObject)

        async def _drain_queue(queue, queue_metric):
            while True:
                message = await queue.get()
                if message is DoneObject:
                    break
                Metrics.get().decrement(queue_metric)
                yield message

        # --- nodes first (skipped when relationships_only=True) ---
        if not self.type_retriever.relationships_only:
            self.logger.info("Node fetch started, concurrency_limit=%d", concurrency_limit)
            nodes_gen = (self.convert_node_to_ingest(n)
                         async for n in self.type_retriever.fetch_nodes(self.schema))
            node_task = asyncio.create_task(
                _fill_queue(nodes_gen, node_queue, ORCHESTRATOR_NODE_QUEUE)
            )
            node_error = None
            try:
                async for record in _drain_queue(node_queue, ORCHESTRATOR_NODE_QUEUE):
                    yield record
            finally:
                try:
                    await node_task
                except Exception as exc:
                    node_error = exc
            if node_error is not None:
                raise node_error

        # --- relationships (only reached after nodes fully drained, or immediately if relationships_only) ---
        self.logger.info(
            "Relationship fetch started, concurrency_limit=%d", concurrency_limit
        )
        rels_gen = (self.convert_relationship_to_ingest(r)
                    async for r in self.type_retriever.fetch_relationships(self.schema))
        rel_task = asyncio.create_task(
            _fill_queue(rels_gen, rel_queue, ORCHESTRATOR_REL_QUEUE)
        )
        rel_error = None
        try:
            async for record in _drain_queue(rel_queue, ORCHESTRATOR_REL_QUEUE):
                yield record
        finally:
            try:
                await rel_task
            except Exception as exc:
                rel_error = exc
        if rel_error is not None:
            raise rel_error

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
            if key_name in node.properties:
                node.key_values[key_name] = node.properties.pop(key_name)

    def convert_node_to_ingest(self, node: Node):
        self.reorganize_node_key_properties(node)
        return node.into_ingest()

    def convert_relationship_to_ingest(self, relationship: RelationshipWithNodes):
        self.reorganize_node_key_properties(relationship.from_node)
        self.reorganize_node_key_properties(relationship.to_node)
        return relationship.into_ingest()


# Backwards-compatible alias — callers that imported ConcurrentCopier directly
# still work; Copier now handles both modes based on retriever.concurrency_limit.
ConcurrentCopier = Copier
