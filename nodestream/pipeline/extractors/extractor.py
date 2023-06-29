from abc import abstractmethod
from typing import Any, AsyncGenerator

from ..step import Step


class Extractor(Step):
    """Extractors represent the source of a set of records.

    They are like any other step. However, they ignore the incoming record stream and instead produce their own
    stream of records. For this reason they generally should only be set at the beginning of a pipeline.
    """

    def handle_async_record_stream(
        self, _: AsyncGenerator[Any, Any]
    ) -> AsyncGenerator[Any, Any]:
        return self.extract_records()

    @abstractmethod
    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        raise NotImplementedError
