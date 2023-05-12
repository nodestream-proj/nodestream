from ..pipeline import Writer
from ..model import IngestionStrategy


class GraphDatabaseWriter(Writer):
    def __init__(self, batch_size: int, ingest_strategy: IngestionStrategy) -> None:
        self.batch_size = batch_size
        self.ingest_strategy = ingest_strategy

    # TODO: Make this ingestable notion a type.
    async def write_record(self, ingestable):
        # TODO: Handle Flush.
        ingestable.ingest(self.ingest_strategy)
