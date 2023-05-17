from ..pipeline import Writer, Flush
from ..model import IngestionStrategy


class GraphDatabaseWriter(Writer):
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

        ingestable.ingest(self.ingest_strategy)
        self.pending_records += 1
        if self.pending_records >= self.batch_size:
            await self.flush()
