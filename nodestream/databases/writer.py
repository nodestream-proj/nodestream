import asyncio
from collections import deque
from logging import getLogger
from typing import Callable, Set

from ..metrics import Metric, Metrics
from ..pipeline import Writer
from .database_connector import DatabaseConnector
from .debounced_ingest_strategy import DebouncedIngestStrategy
from .ingest_strategy import INGESTION_STRATEGY_REGISTRY, IngestionStrategy

WRITER_PENDING_RECORDS = Metric(
    "writer_pending_records",
    "Number of records pending in the writer",
    accumulate=False,
)
WRITER_ACTIVE_FLUSH_LANES = Metric(
    "writer_active_flush_lanes",
    "Number of active background flush tasks in the writer",
    accumulate=False,
)


class GraphDatabaseWriter(Writer):
    @classmethod
    def from_file_data(
        cls,
        database: str,
        ingest_strategy_name: str = INGESTION_STRATEGY_REGISTRY.name_for(
            DebouncedIngestStrategy
        ),
        collect_stats: bool = True,
        batch_size: int = 1000,
        flush_concurrency: int = 1,
        **database_args,
    ):
        connector = DatabaseConnector.from_database_args(
            database=database, **database_args
        )
        return cls.from_connector(
            connector=connector,
            ingest_strategy_name=ingest_strategy_name,
            collect_stats=collect_stats,
            batch_size=batch_size,
            flush_concurrency=flush_concurrency,
        )

    @classmethod
    def from_connector(
        cls,
        connector: DatabaseConnector,
        ingest_strategy_name: str = INGESTION_STRATEGY_REGISTRY.name_for(
            DebouncedIngestStrategy
        ),
        collect_stats: bool = True,
        batch_size: int = 1000,
        flush_concurrency: int = 1,
    ):
        executor = connector.get_query_executor(collect_stats=collect_stats)
        ingest_strategy_cls = INGESTION_STRATEGY_REGISTRY.get(ingest_strategy_name)
        ingest_strategy = ingest_strategy_cls(executor)

        if flush_concurrency > 1:

            def strategy_factory():
                return ingest_strategy_cls(executor)

            return ConcurrentGraphDatabaseWriter(
                batch_size=batch_size,
                ingest_strategy=ingest_strategy,
                strategy_factory=strategy_factory,
                max_flush_lanes=flush_concurrency,
            )

        return cls(
            batch_size=batch_size,
            ingest_strategy=ingest_strategy,
        )

    def __init__(self, batch_size: int, ingest_strategy: IngestionStrategy) -> None:
        self.batch_size = batch_size
        self.ingest_strategy = ingest_strategy
        self.pending_records = 0

    async def flush(self):
        await self.ingest_strategy.flush()
        self.pending_records = 0

    async def write_record(self, ingestible):
        await ingestible.ingest(self.ingest_strategy)
        self.pending_records += 1
        if self.pending_records >= self.batch_size:
            await self.flush()

    async def finish(self, _):
        """Close connector by calling finish method from Step"""
        await self.flush()
        await self.ingest_strategy.finish()


class ConcurrentGraphDatabaseWriter(GraphDatabaseWriter):
    """A writer that rotates ingest strategies and flushes batches concurrently.

    When a batch is full the writer swaps in a fresh strategy and launches a
    background task to flush the completed one.  A semaphore (``max_flush_lanes``)
    bounds how many background flushes can be in flight at once, providing
    back-pressure when the database cannot keep up.
    """

    def __init__(
        self,
        batch_size: int,
        ingest_strategy: IngestionStrategy,
        strategy_factory: Callable[[], IngestionStrategy],
        max_flush_lanes: int = 2,
    ) -> None:
        super().__init__(batch_size, ingest_strategy)
        self.strategy_factory = strategy_factory
        self.max_flush_lanes = max(1, max_flush_lanes)
        self.logger = getLogger(self.__class__.__name__)

        # Semaphore bounds how many flushes can run concurrently.
        self.flush_lane_semaphore = asyncio.Semaphore(self.max_flush_lanes)
        # Track in-flight flush tasks so we can drain them on finish.
        self.pending_flush_tasks: Set[asyncio.Task] = set()
        # Collect errors from background flushes to re-raise on the main path.
        self.flush_errors: deque = deque()

        Metrics.get().set_value(WRITER_ACTIVE_FLUSH_LANES, 0)

    async def write_record(self, ingestible):
        self._raise_if_flush_errors()
        await ingestible.ingest(self.ingest_strategy)
        self.pending_records += 1
        Metrics.get().increment(WRITER_PENDING_RECORDS)
        if self.pending_records >= self.batch_size:
            await self._rotate_and_flush()

    async def flush(self):
        """Drain all background flush tasks, then flush the active strategy."""
        await self._drain_pending_flushes()
        await self.ingest_strategy.flush()
        self.pending_records = 0

    async def _rotate_and_flush(self):
        """Swap the active strategy for a fresh one and flush the completed
        batch in a background task."""

        # Back-pressure: wait until a flush lane is available.
        await self.flush_lane_semaphore.acquire()
        self._report_active_lanes()  # report immediately on acquire, not after release
        self._raise_if_flush_errors()

        # Rotate: save the full strategy, give the pipeline a fresh one.
        completed_strategy = self.ingest_strategy
        self.ingest_strategy = self.strategy_factory()
        Metrics.get().set_value(WRITER_PENDING_RECORDS, 0)
        self.pending_records = 0

        # Flush the completed batch in the background.
        task = asyncio.create_task(self._flush_in_background(completed_strategy))
        self.pending_flush_tasks.add(task)
        task.add_done_callback(self.pending_flush_tasks.discard)

    async def _flush_in_background(self, strategy: IngestionStrategy):
        """Flush a completed strategy, then release the flush lane."""
        try:
            await strategy.flush()
        except Exception as exc:
            self.flush_errors.append(exc)
        finally:
            self.flush_lane_semaphore.release()
            self._report_active_lanes()

    async def _drain_pending_flushes(self):
        """Wait for all in-flight background flushes to complete."""
        if self.pending_flush_tasks:
            await asyncio.gather(*self.pending_flush_tasks, return_exceptions=True)
            self.pending_flush_tasks.clear()
        self._raise_if_flush_errors()

    def _raise_if_flush_errors(self):
        if self.flush_errors:
            raise self.flush_errors.popleft()

    def _report_active_lanes(self):
        occupied = self.max_flush_lanes - self.flush_lane_semaphore._value  # type: ignore[attr-defined]
        Metrics.get().set_value(WRITER_ACTIVE_FLUSH_LANES, occupied)
