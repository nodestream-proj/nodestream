from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator


class Step(ABC):
    """A `Step` represents a phase of an ETl pipeline."""

    @classmethod
    def __declarative_init__(cls, **kwargs):
        return cls(**kwargs)

    @abstractmethod
    async def handle_async_record_stream(
        self, record_stream: AsyncGenerator[Any, Any]
    ) -> AsyncGenerator[Any, Any]:
        raise NotImplementedError


class PassStep(Step):
    """A `PassStep` is a step that does nothing."""

    async def handle_async_record_stream(
        self, record_stream: AsyncGenerator[Any, Any]
    ) -> AsyncGenerator[Any, Any]:
        async for record in record_stream:
            yield record
