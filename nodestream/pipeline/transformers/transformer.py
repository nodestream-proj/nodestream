from abc import abstractmethod
from typing import Any, AsyncGenerator, Generator

from ..flush import Flush
from ..step import Step


class Transformer(Step):
    """A `Transformer` takes a given record and mutates into a new record.

    `Transformer` steps generally make up the middle of an ETL pipeline and are responsible
    for reshaping an object so its more ingestible by the downstream sink.
    """

    async def handle_async_record_stream(
        self, record_stream: AsyncGenerator[Any, Any]
    ) -> AsyncGenerator[Any, Any]:
        async for record in record_stream:
            if record is Flush:
                yield record
            else:
                val_or_gen = await self.transform_record(record)
                if isinstance(val_or_gen, Generator):
                    for result in val_or_gen:
                        yield result
                else:
                    yield val_or_gen

    @abstractmethod
    async def transform_record(self, record: Any) -> Any:
        raise NotImplementedError
