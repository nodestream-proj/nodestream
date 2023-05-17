from functools import reduce
from typing import Any, AsyncGenerator, List

from .step import Step


async def empty_asnyc_generator():
    for item in []:
        yield item


class Pipeline:
    """A pipeline is a series of steps that are executed in order."""

    __slots__ = ("steps",)

    def __init__(self, steps: List[Step]) -> None:
        self.steps = steps

    def run(self) -> AsyncGenerator[Any, Any]:
        record_stream_over_all_steps = reduce(
            lambda stream, step: step.handle_async_record_stream(stream),
            self.steps,
            empty_asnyc_generator(),
        )
        return record_stream_over_all_steps
