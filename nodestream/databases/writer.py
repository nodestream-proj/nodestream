from ..model.ingest_strategy import IngestionStrategy, INGESTION_STRATEGY_REGISTRY
from ..pipeline import Flush, Writer
from .query_executor import QUERY_EXECUTOR_SUBCLASS_REGISTRY


class GraphDatabaseWriter(Writer):
    @classmethod
    def __declarative_init__(
        cls, batch_size: int, ingest_strategy_name: str, database: str, **database_args
    ):
        ingest_strategy = INGESTION_STRATEGY_REGISTRY.get(ingest_strategy_name)
        executor = QUERY_EXECUTOR_SUBCLASS_REGISTRY.get(database)(**database_args)
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

    async def write_record(self, ingestable):
        if ingestable is Flush:
            await self.flush()
            return

        await ingestable.ingest(self.ingest_strategy)
        self.pending_records += 1
        if self.pending_records >= self.batch_size:
            await self.flush()
