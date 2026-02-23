import asyncio
import time
from logging import getLogger
from typing import Callable, Optional, Set

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
    """Batches incoming records and flushes them to the database.

    When ``flush_concurrency`` is 1 (the default), flushes are synchronous:
    the writer blocks until each batch is written before accumulating the next.

    When ``flush_concurrency`` > 1, ``from_connector`` / ``from_file_data``
    return a :class:`ConcurrentGraphDatabaseWriter` that rotates strategies
    and flushes full batches in background tasks bounded by a semaphore.
    """

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
        """Create a writer from an existing connector.

        Returns a :class:`ConcurrentGraphDatabaseWriter` when
        ``flush_concurrency > 1``, otherwise a plain
        :class:`GraphDatabaseWriter`.
        """
        executor = connector.get_query_executor(collect_stats=collect_stats)
        ingest_strategy_cls = INGESTION_STRATEGY_REGISTRY.get(ingest_strategy_name)

        def strategy_factory():
            return ingest_strategy_cls(executor)

        ingest_strategy = strategy_factory()

        if flush_concurrency > 1:
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

    def __init__(
        self,
        batch_size: int,
        ingest_strategy: IngestionStrategy,
    ) -> None:
        self.batch_size = batch_size
        self.ingest_strategy = ingest_strategy
        self.pending_records = 0
        self.logger = getLogger(self.__class__.__name__)
        self._accumulate_start: float = time.monotonic()

    async def write_record(self, ingestible):
        await ingestible.ingest(self.ingest_strategy)
        self.pending_records += 1
        Metrics.get().increment(WRITER_PENDING_RECORDS)
        if self.pending_records >= self.batch_size:
            await self.flush_batch()

    async def flush_batch(self):
        """Flush the current batch and reset counters."""
        accumulate_elapsed = time.monotonic() - self._accumulate_start
        flush_start = time.monotonic()
        await self.ingest_strategy.flush()
        flush_elapsed = time.monotonic() - flush_start
        self.logger.info(
            "Writer batch cycle",
            extra={
                "records_in_batch": self.pending_records,
                "accumulate_s": round(accumulate_elapsed, 3),
                "flush_s": round(flush_elapsed, 3),
            },
        )
        self.pending_records = 0
        Metrics.get().set_value(WRITER_PENDING_RECORDS, 0)
        self._accumulate_start = time.monotonic()

    async def flush(self):
        """Full barrier flush invoked by the pipeline on ``Flush`` sentinels."""
        await self.ingest_strategy.flush()
        self.pending_records = 0

    async def finish(self, _):
        """Flush remaining records and close the executor."""
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
        self.flush_lane_semaphore: Optional[asyncio.Semaphore] = None
        self.pending_flush_tasks: Set[asyncio.Task] = set()
        self.flush_errors: list = []
        Metrics.get().set_value(WRITER_ACTIVE_FLUSH_LANES, 0)

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Concurrent flush internals
    # ------------------------------------------------------------------

    async def _rotate_and_flush(self):
        """Swap the active strategy for a fresh one and flush the full one
        in a background task.  Blocks if all flush lanes are occupied."""
        if self.flush_lane_semaphore is None:
            self.flush_lane_semaphore = asyncio.Semaphore(self.max_flush_lanes)

        # Wait for a flush lane to free up (back-pressure).
        await self.flush_lane_semaphore.acquire()
        self._raise_if_flush_errors()
        self._report_active_lanes()

        # Rotate: stash the full strategy, create a fresh one.
        completed_strategy = self.ingest_strategy
        completed_batch_size = self.pending_records
        self.ingest_strategy = self.strategy_factory()
        Metrics.get().set_value(WRITER_PENDING_RECORDS, 0)
        self.pending_records = 0
        self._accumulate_start = time.monotonic()

        # Launch the background flush.
        task = asyncio.create_task(
            self._flush_in_background(completed_strategy, completed_batch_size)
        )
        self.pending_flush_tasks.add(task)
        task.add_done_callback(self.pending_flush_tasks.discard)

    async def _flush_in_background(
        self,
        strategy: IngestionStrategy,
        records_in_batch: int,
    ):
        """Flush a completed strategy, then release the flush lane."""
        try:
            flush_start = time.monotonic()
            await strategy.flush()
            flush_elapsed = time.monotonic() - flush_start
            self.logger.info(
                "Background flush completed",
                extra={
                    "records_in_batch": records_in_batch,
                    "flush_s": round(flush_elapsed, 3),
                },
            )
        except Exception as exc:
            self.flush_errors.append(exc)
        finally:
            if self.flush_lane_semaphore is not None:
                self.flush_lane_semaphore.release()
                self._report_active_lanes()

    async def _drain_pending_flushes(self):
        """Wait for every in-flight background flush to complete."""
        if self.pending_flush_tasks:
            await asyncio.gather(*self.pending_flush_tasks, return_exceptions=True)
            self.pending_flush_tasks.clear()
        self._raise_if_flush_errors()

    def _raise_if_flush_errors(self):
        """Re-raise the first captured background flush error."""
        if self.flush_errors:
            raise self.flush_errors.pop(0)

    def _report_active_lanes(self):
        """Push the current number of occupied flush lanes to metrics."""
        if self.flush_lane_semaphore is not None:
            occupied = self.max_flush_lanes - self.flush_lane_semaphore._value  # type: ignore[attr-defined]
            Metrics.get().set_value(WRITER_ACTIVE_FLUSH_LANES, occupied)
