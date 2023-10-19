from ..pipeline import Flush, Writer
from .database_connector import DatabaseConnector
from .debounced_ingest_strategy import DebouncedIngestStrategy
from .ingest_strategy import INGESTION_STRATEGY_REGISTRY, IngestionStrategy


class GraphDatabaseWriter(Writer):
    @classmethod
    def from_file_data(
        cls,
        batch_size: int,
        database: str,
        ingest_strategy_name: str = INGESTION_STRATEGY_REGISTRY.name_for(
            DebouncedIngestStrategy
        ),
        collect_stats: bool = True,
        **database_args
    ):
        # Import all query executors so that they can register themselves
        DatabaseConnector.import_all()
        connector = DatabaseConnector.from_database_args(
            database=database, **database_args
        )
        executor = connector.get_query_executor(collect_stats=collect_stats)
        ingest_strategy_cls = INGESTION_STRATEGY_REGISTRY.get(ingest_strategy_name)
        ingest_strategy = ingest_strategy_cls(executor)

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
        if ingestible is Flush:
            await self.flush()
            return

        await ingestible.ingest(self.ingest_strategy)
        self.pending_records += 1
        if self.pending_records >= self.batch_size:
            await self.flush()

    async def finish(self):
        await self.flush()
