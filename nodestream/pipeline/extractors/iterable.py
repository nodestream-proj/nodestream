from logging import getLogger
from typing import Any, AsyncGenerator, Iterable

from .extractor import Extractor


class IterableExtractor(Extractor):
    """An extractor that produces records from an iterable."""

    @classmethod
    def range(cls, start=0, stop=100, step=1):
        return cls(iterable=({"index": i} for i in range(start, stop, step)))

    def __init__(self, iterable: Iterable[Any]) -> None:
        self.iterable = iterable
        self.index = 0
        self.logger = getLogger(self.__class__.__name__)

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        for index, record in enumerate(self.iterable):
            if index < self.index:
                continue
            self.index = index
            yield record

    async def make_checkpoint(self):
        return self.index

    async def resume_from_checkpoint(self, checkpoint):
        if isinstance(checkpoint, int):
            self.index = checkpoint
            self.logger.info(f"Resuming from checkpoint {checkpoint}")
