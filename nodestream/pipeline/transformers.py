from abc import abstractmethod
from typing import Any, AsyncGenerator

from .step import Step


class Transformer(Step):
    """A `Transformer` takes a given record and mutates into a new record.

    `Transformer` steps generally make up the middle of an ETL pipeline and are responsible
    for reshaping an object so its more ingestable by the downstream sink.
    """

    async def handle_async_record_stream(
        self, record_stream: AsyncGenerator[Any, Any]
    ) -> AsyncGenerator[Any, Any]:
        async for record in record_stream:
            yield await self.transform_record(record)

    @abstractmethod
    async def transform_record(self, record: Any) -> Any:
        raise NotImplementedError
