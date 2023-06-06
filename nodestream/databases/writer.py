from typing import Optional

from ..model.ingest_strategy import INGESTION_STRATEGY_REGISTRY, IngestionStrategy
from ..pipeline import Flush, Writer
from .debounced_ingest_strategy import DebouncedIngestStrategy
from .query_executor import QUERY_EXECUTOR_SUBCLASS_REGISTRY


class GraphDatabaseWriter(Writer):
    @classmethod
    def __declarative_init__(
        cls,
        batch_size: int,
        database: str,
        ingest_strategy_name: Optional[str] = None,
        **database_args
    ):
        ingest_strategy_name = (
            ingest_strategy_name
            or INGESTION_STRATEGY_REGISTRY.name_for(DebouncedIngestStrategy)
        )
        ingest_strategy = INGESTION_STRATEGY_REGISTRY.get(ingest_strategy_name)
        executor_class = QUERY_EXECUTOR_SUBCLASS_REGISTRY.get(database)
        executor = executor_class.from_file_arguments(**database_args)
        return cls(
            batch_size=batch_size,
            ingest_strategy=ingest_strategy(executor),
        )

    def __init__(self, batch_size: int, ingest_strategy: IngestionStrategy) -> None:
        self.batch_size = batch_size
        self.ingest_strategy = ingest_strategy
        self.pending_records = 0

    async def flush(self):
        await self.ingest_strategy.flush()
        self.pending_records = 0

    async def write_record(self, ingestible):
        if ingestible is Flush:
            await self.flush()
            return

        await ingestible.ingest(self.ingest_strategy)
        self.pending_records += 1
        if self.pending_records >= self.batch_size:
            await self.flush()

    async def finish(self):
        await self.flush()
