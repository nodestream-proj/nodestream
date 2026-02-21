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

        When ``flush_concurrency > 1``, the writer will rotate strategies and
        flush full batches concurrently using multiple "buckets".
        """
        executor = connector.get_query_executor(collect_stats=collect_stats)
        ingest_strategy_cls = INGESTION_STRATEGY_REGISTRY.get(ingest_strategy_name)

        def strategy_factory():
            return ingest_strategy_cls(executor)

        ingest_strategy = strategy_factory()
        return cls(
            batch_size=batch_size,
            ingest_strategy=ingest_strategy,
            strategy_factory=strategy_factory,
            flush_concurrency=flush_concurrency,
        )

    def __init__(
        self,
        batch_size: int,
        ingest_strategy: IngestionStrategy,
        strategy_factory: Optional[Callable[[], IngestionStrategy]] = None,
        flush_concurrency: int = 1,
    ) -> None:
        self.batch_size = batch_size
        self.ingest_strategy = ingest_strategy
        self.pending_records = 0
        self._strategy_factory = strategy_factory
        self._flush_concurrency = max(1, flush_concurrency)
        self._flush_tasks: Set[asyncio.Task] = set()
        self._flush_semaphore: Optional[asyncio.Semaphore] = None
        self._flush_errors: list = []
        self.logger = getLogger(self.__class__.__name__)
        self._accumulate_start: float = time.monotonic()
        Metrics.get().set_value(WRITER_ACTIVE_FLUSH_LANES, 0)

    @property
    def _concurrent_flush_enabled(self) -> bool:
        return self._flush_concurrency > 1 and self._strategy_factory is not None

    async def write_record(self, ingestible):
        # Fail fast if a background flush encountered an error.
        self._raise_if_flush_errors()
        await ingestible.ingest(self.ingest_strategy)
        self.pending_records += 1
        Metrics.get().increment(WRITER_PENDING_RECORDS)
        if self.pending_records >= self.batch_size:
            if self._concurrent_flush_enabled:
                await self._rotate_and_flush()
            else:
                await self._sync_flush()

    async def _sync_flush(self):
        """Original synchronous flush behaviour (flush_concurrency=1)."""
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

    async def _rotate_and_flush(self):
        """Rotate the active strategy and flush the full one in the background.

        The writer immediately starts accumulating into a fresh strategy while
        the previous batch is flushed concurrently.  A semaphore bounds the
        number of in-flight flush tasks to ``flush_concurrency`` so that we
        only allow N concurrent writer buckets at a time.
        """
        if self._flush_semaphore is None:
            self._flush_semaphore = asyncio.Semaphore(self._flush_concurrency)

        # Block until a flush lane is available (backpressure). This ensures we
        # never have more than ``flush_concurrency`` background flushes in flight.
        await self._flush_semaphore.acquire()

        # Fail fast if any previous background flush failed now that a lane is free.
        self._raise_if_flush_errors()

        # Track that we have consumed a flush lane for metrics purposes.
        Metrics.get().set_value(
            WRITER_ACTIVE_FLUSH_LANES, self._flush_concurrency - self._flush_semaphore._value  # type: ignore[attr-defined]
        )

        # Swap: move the full strategy out, create a fresh one.
        full_strategy = self.ingest_strategy
        records_in_batch = self.pending_records
        self.ingest_strategy = self._strategy_factory()
        Metrics.get().set_value(WRITER_PENDING_RECORDS, 0)
        self.pending_records = 0

        self._accumulate_start = time.monotonic()

        # Launch the flush as a background task.
        task = asyncio.create_task(
            self._background_flush(full_strategy, records_in_batch)
        )
        self._flush_tasks.add(task)
        task.add_done_callback(self._flush_tasks.discard)

    async def _background_flush(
        self, strategy: IngestionStrategy, records_in_batch: int
    ):
        """Flush a strategy in the background, releasing the semaphore slot on
        completion (success or failure)."""
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
        except Exception as e:
            self._flush_errors.append(e)
        finally:
            if self._flush_semaphore is not None:
                self._flush_semaphore.release()
                # Update active lane count after releasing the semaphore.
                Metrics.get().set_value(
                    WRITER_ACTIVE_FLUSH_LANES,
                    self._flush_concurrency - self._flush_semaphore._value,  # type: ignore[attr-defined]
                )

    def _raise_if_flush_errors(self):
        """Raise the first captured background flush error, if any."""
        if self._flush_errors:
            raise self._flush_errors.pop(0)

    async def _drain_flush_tasks(self):
        """Wait for all in-flight background flushes to complete."""
        if self._flush_tasks:
            await asyncio.gather(*self._flush_tasks, return_exceptions=True)
            self._flush_tasks.clear()
        self._raise_if_flush_errors()

    async def flush(self):
        """Drain all background tasks, then flush the active strategy.

        This is the "full barrier" flush invoked by the pipeline on ``Flush``
        sentinels and during ``finish()``.
        """
        await self._drain_flush_tasks()
        await self.ingest_strategy.flush()
        self.pending_records = 0

    async def finish(self, _):
        """Drain backgrounds, flush remaining records, close the executor."""
        await self.flush()
        await self.ingest_strategy.finish()
