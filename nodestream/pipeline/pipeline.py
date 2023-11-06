import asyncio
from typing import Any, AsyncGenerator, Iterable, List, Optional

from ..schema.schema import (
    AggregatedIntrospectiveIngestionComponent,
    IntrospectiveIngestionComponent,
)
from .meta import get_context
from .progress_reporter import PipelineProgressReporter
from .step import Step


async def empty_async_generator():
    for item in []:
        yield item


async def enumerate_async(iterable):
    count = 0

    async for item in iterable:
        yield count, item
        count += 1


class DoneObject:
    pass


class StepExecutor:
    def __init__(
        self,
        upstream: Optional["StepExecutor"],
        step: Step,
        outbox_size: int = 0,
        progress_reporter: Optional[PipelineProgressReporter] = None,
    ) -> None:
        self.outbox = asyncio.Queue(maxsize=outbox_size)
        self.upstream = upstream
        self.done = False
        self.step = step
        self.progress_reporter = progress_reporter
        self.end_of_line = False

    async def outbox_generator(self):
        while not self.done or not self.outbox.empty():
            if (value := await self.outbox.get()) is not DoneObject:
                yield value
            self.outbox.task_done()

    def start(self):
        if self.progress_reporter:
            self.progress_reporter.on_start_callback()

    def set_end_of_line(self, progress_reporter: Optional[PipelineProgressReporter]):
        self.progress_reporter = progress_reporter
        self.end_of_line = True

    async def stop(self):
        self.done = True
        await self.outbox.put(DoneObject)
        await self.step.finish()

        if self.progress_reporter:
            self.progress_reporter.on_finish_callback(get_context())

    async def work_body(self):
        if self.upstream is None:
            upstream = empty_async_generator()
        else:
            upstream = self.upstream.outbox_generator()

        results = self.step.handle_async_record_stream(upstream)
        async for index, record in enumerate_async(results):
            if not self.end_of_line:
                await self.outbox.put(record)
            if self.progress_reporter:
                self.progress_reporter.report(index, record)

    async def work_loop(self):
        self.start()
        await self.work_body()
        await self.stop()


class Pipeline(AggregatedIntrospectiveIngestionComponent):
    """A pipeline is a series of steps that are executed in order."""

    __slots__ = ("steps",)

    def __init__(self, steps: List[Step], step_outbox_size: int) -> None:
        self.steps = steps
        self.step_outbox_size = step_outbox_size

    async def run(
        self, progress_reporter: Optional[PipelineProgressReporter] = None
    ) -> AsyncGenerator[Any, Any]:
        current_executor = None
        tasks = []

        for step in self.steps:
            current_executor = StepExecutor(
                upstream=current_executor, step=step, outbox_size=self.step_outbox_size
            )
            tasks.append(asyncio.create_task(current_executor.work_loop()))

        current_executor.set_end_of_line(progress_reporter)

        await asyncio.gather(*tasks)

    def all_subordinate_components(self) -> Iterable[IntrospectiveIngestionComponent]:
        return (s for s in self.steps if isinstance(s, IntrospectiveIngestionComponent))
