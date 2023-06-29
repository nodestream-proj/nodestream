from typing import Any, AsyncGenerator, Iterable

from .extractor import Extractor


class IterableExtractor(Extractor):
    """An extractor that produces records from an iterable."""

    @classmethod
    def range(cls, start=0, stop=100, step=1):
        return cls(iterable=({"index": i} for i in range(start, stop, step)))

    def __init__(self, iterable: Iterable[Any]) -> None:
        self.iterable = iterable

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        for record in self.iterable:
            yield record
