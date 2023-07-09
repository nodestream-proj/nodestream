import asyncio
from functools import reduce
from typing import Any, AsyncGenerator, Iterable, List

from ..schema.schema import (
    AggregatedIntrospectiveIngestionComponent,
    IntrospectiveIngestionComponent,
)
from .flush import Flush
from .step import Step


async def empty_async_generator():
    for item in []:
        yield item


class Pipeline(AggregatedIntrospectiveIngestionComponent):
    """A pipeline is a series of steps that are executed in order."""

    __slots__ = ("steps",)

    def __init__(self, steps: List[Step]) -> None:
        self.steps = steps

    async def run(self) -> AsyncGenerator[Any, Any]:
        record_stream_over_all_steps = reduce(
            lambda stream, step: step.handle_async_record_stream(stream),
            self.steps,
            empty_async_generator(),
        )
        async for record in record_stream_over_all_steps:
            if record is not Flush:
                yield record

        await self.finish()

    async def finish(self):
        await asyncio.gather(*(step.finish() for step in self.steps))

    def all_subordinate_components(self) -> Iterable[IntrospectiveIngestionComponent]:
        return (s for s in self.steps if isinstance(s, IntrospectiveIngestionComponent))
