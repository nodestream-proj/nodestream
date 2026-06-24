import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import AsyncGenerator

from ..metrics import Metric, Metrics
from ..pipeline import Extractor
from ..pipeline.channel import DoneObject
from ..pipeline.step import StepContext
from ..schema import Schema

ACTIVE_QUERIES = Metric(
    "active_queries",
    "Number of active database queries in the copier",
    accumulate=False,
)


@dataclass
class TypeHistogram:
    """Count estimates for all node and relationship types to be copied."""

    node_counts: dict[str, int] = field(default_factory=dict)
    relationship_counts: dict[str, int] = field(default_factory=dict)

    @staticmethod
    def sorted_types_by_count(counts: dict[str, int]) -> list[str]:
        return sorted(counts, key=counts.__getitem__, reverse=True)

    def log(self, logger: Logger) -> None:
        logger.info("Node type histogram (descending):")
        for node_type in self.sorted_types_by_count(self.node_counts):
            logger.info("  %s: %d", node_type, self.node_counts[node_type])
        logger.info("Relationship type histogram (descending):")
        for relationship_type in self.sorted_types_by_count(self.relationship_counts):
            logger.info(
                "  %s: %d",
                relationship_type,
                self.relationship_counts[relationship_type],
            )
        logger.info(
            "Total nodes: %d, Total relationships: %d",
            sum(self.node_counts.values()),
            sum(self.relationship_counts.values()),
        )


class TypeRetriever(ABC):
    """Abstract base for retrieving graph objects from a source database.

    Owns the schema and is responsible for building a TypeHistogram and yielding
    Extractor objects for each type in the copy run. The Copier drives those
    extractors concurrently; concurrency parameters belong to the Copier, not here.

    Required call order: ``build_histogram()`` must be awaited before
    ``fetch_extractors()`` is iterated. Implementations should assert this
    invariant; the Copier guarantees it via ``start()``.
    """

    def __init__(self, schema: Schema) -> None:
        self.schema = schema

    @abstractmethod
    async def fetch_extractors(self) -> AsyncGenerator[Extractor, None]:
        """Yield all Extractor objects for this copy run in implementation-defined order.

        ``build_histogram()`` must be called before iterating this generator.
        """
        raise NotImplementedError

    @abstractmethod
    async def build_histogram(self) -> TypeHistogram:
        """Return count estimates for all types to be copied."""
        raise NotImplementedError


class Copier(Extractor):
    """Copies nodes and relationships from a source via a TypeRetriever.

    Pulls Extractor objects from fetch_extractors() and runs them concurrently
    up to concurrency_limit via a producer/consumer queue. No branching on
    types or shards — all of that lives in the TypeRetriever.

    Args:
        type_retriever: Supplies the extractors and histogram for this run.
        concurrency_limit: Maximum number of extractor tasks running at once.
        queue_size: Maximum records buffered between producer and consumer.
            0 means unbounded.
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
        record_queue: asyncio.Queue = asyncio.Queue(maxsize=self.queue_size)

        async def run_extractor(extractor: Extractor) -> None:
            async with semaphore:
                Metrics.get().increment(ACTIVE_QUERIES)
                try:
                    async for record in extractor.extract_records():
                        await record_queue.put(record)
                finally:
                    Metrics.get().decrement(ACTIVE_QUERIES)

        async def produce_all() -> None:
            # All extractor tasks are created upfront before any are awaited.
            # The semaphore enforces concurrency_limit, so tasks queue behind
            # it rather than running freely. Creating them all at once lets
            # asyncio schedule them as soon as a semaphore slot opens without
            # requiring the producer loop to still be running.
            tasks = []
            try:
                async for extractor in self.type_retriever.fetch_extractors():
                    tasks.append(asyncio.create_task(run_extractor(extractor)))
                await asyncio.gather(*tasks)
            finally:
                await record_queue.put(DoneObject)

        producer_task = asyncio.create_task(produce_all())
        producer_error = None
        try:
            while True:
                record = await record_queue.get()
                if record is DoneObject:
                    break
                yield record
        finally:
            # Cancel the producer first so it stops blocking on a full queue
            # if the consumer exited early (exception or generator abandoned).
            producer_task.cancel()
            try:
                await producer_task
            except (asyncio.CancelledError, Exception) as exc:
                # Store non-cancellation errors to re-raise after the finally
                # completes. CancelledError is expected when we cancelled above.
                if not isinstance(exc, asyncio.CancelledError):
                    producer_error = exc
        if producer_error is not None:
            raise producer_error


