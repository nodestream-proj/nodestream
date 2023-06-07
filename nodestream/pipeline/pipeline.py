from functools import reduce
from typing import Any, AsyncGenerator, Iterable, List

from ..model import AggregatedIntrospectionMixin, IntrospectiveIngestionComponent
from .step import Step


async def empty_async_generator():
    for item in []:
        yield item


class Pipeline(AggregatedIntrospectionMixin, IntrospectiveIngestionComponent):
    """A pipeline is a series of steps that are executed in order."""

    __slots__ = ("steps",)

    def __init__(self, steps: List[Step]) -> None:
        self.steps = steps

    def run(self) -> AsyncGenerator[Any, Any]:
        record_stream_over_all_steps = reduce(
            lambda stream, step: step.handle_async_record_stream(stream),
            self.steps,
            empty_async_generator(),
        )
        return record_stream_over_all_steps

    def all_subordinate_components(self) -> Iterable[IntrospectiveIngestionComponent]:
        return (s for s in self.steps if isinstance(s, IntrospectiveIngestionComponent))
