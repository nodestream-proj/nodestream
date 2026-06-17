import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import AsyncGenerator, Dict, List

from ..metrics import Metric, Metrics
from ..pipeline import Extractor
from ..pipeline.channel import DoneObject
from ..pipeline.step import StepContext
from ..schema import Schema

ORCHESTRATOR_QUEUE = Metric(
    "orchestrator_queue",
    "Number of extractor jobs pending in the copier queue",
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
        for relationshipType in self.sorted_relationship_types():
            logger.info(
                "  %s: %d", relationshipType, self.relationship_counts[relationshipType]
            )
        logger.info(
            "Total nodes: %d, Total relationships: %d",
            sum(self.node_counts.values()),
            sum(self.relationship_counts.values()),
        )


class TypeRetriever(ABC):
    """Abstract base for retrieving graph objects from a source database.

    The retriever owns schema, type decomposition, shard splitting, histogram
    computation, and node_only policy. It exposes two async generators of
    Extractor objects — one for node shards, one for relationship shards.
    The Copier drives those extractors concurrently; concurrency parameters
    belong to the Copier, not here.
    """

    def __init__(self, schema: Schema) -> None:
        self.schema = schema

    @abstractmethod
    async def fetch_extractors(self) -> AsyncGenerator[Extractor, None]:
        """Yield all Extractor objects for this copy run.

        node_only=True  → yield node extractors only.
        node_only=False → yield relationship extractors only (RelationshipWithNodes
                          carries both endpoints, so a separate node pass is not needed).
        """
        raise NotImplementedError

    async def build_histogram(self) -> TypeHistogram:
        """Return count estimates for all types to be copied.

        Default returns an empty histogram. Subclasses that support counting
        should override this to issue real COUNT queries.
        """
        return TypeHistogram()


class Copier(Extractor):
    """Copies nodes and relationships from a source via a TypeRetriever.

    Pulls Extractor objects from fetch_extractors() and runs them concurrently
    up to concurrency_limit via a producer/consumer queue. No branching on
    types, shards, or node_only — all of that lives in the TypeRetriever.
    """

    def __init__(
        self,
        type_retriever: TypeRetriever,
        concurrency_limit: int = 1,
        queue_size: int = 0,
    ) -> None:
        self.type_retriever = type_retriever
        self.concurrency_limit = concurrency_limit
        self.queue_size = queue_size
        self.logger = getLogger(__name__)

    async def start(self, context: StepContext):
        await super().start(context)
        histogram = await self.type_retriever.build_histogram()
        histogram.log(self.logger)

    async def extract_records(self):
        semaphore = asyncio.Semaphore(self.concurrency_limit)
        recordQueue: asyncio.Queue = asyncio.Queue(maxsize=self.queue_size)

        async def runExtractor(extractor: Extractor) -> None:
            async with semaphore:
                Metrics.get().increment(ACTIVE_QUERIES)
                try:
                    async for record in extractor.extract_records():
                        await recordQueue.put(record)
                finally:
                    Metrics.get().decrement(ACTIVE_QUERIES)

        async def produceAll() -> None:
            tasks = []
            try:
                async for extractor in self.type_retriever.fetch_extractors():
                    tasks.append(asyncio.create_task(runExtractor(extractor)))
                await asyncio.gather(*tasks)
            finally:
                await recordQueue.put(DoneObject)

        producerTask = asyncio.create_task(produceAll())
        producerError = None
        try:
            while True:
                record = await recordQueue.get()
                if record is DoneObject:
                    break
                yield record
        finally:
            try:
                await producerTask
            except Exception as exc:
                producerError = exc
        if producerError is not None:
            raise producerError

    def reorganize_node_key_properties(self, node):
        """Move key fields from properties into key_values for ingestion."""
        nodeTypeDefinition = self.type_retriever.schema.get_node_type_by_name(node.type)
        if nodeTypeDefinition is None:
            return
        for keyName in nodeTypeDefinition.keys:
            if keyName in node.properties:
                node.key_values[keyName] = node.properties.pop(keyName)

    def convert_node_to_ingest(self, node):
        self.reorganize_node_key_properties(node)
        return node.into_ingest()

    def convert_relationship_to_ingest(self, relationship):
        self.reorganize_node_key_properties(relationship.from_node)
        self.reorganize_node_key_properties(relationship.to_node)
        return relationship.into_ingest()


ConcurrentCopier = Copier
