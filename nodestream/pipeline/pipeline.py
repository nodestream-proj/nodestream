import asyncio
import traceback
from logging import getLogger
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


class StepException(Exception):
    """
    Exception Format:
        Exceptions in StepExecutor {exec_num} ({step_name})
            Exception in Start
                Stack
            Exception in Work Body
                Stack
            Exception in Stop
                Stack
    """

    def __init__(self, errors: dict[str, Exception], identifier: Optional[str]):
        self.exceptions = errors
        self.identifier = identifier
        super().__init__(self.build_message())

    def build_message(self):
        message = ""
        for section, exception in self.exceptions.items():
            sub_message = "".join(traceback.format_tb(exception.__traceback__))
            # Break it apart, add a tab, bring it back together.
            message += f"\t{section}\n"
            for sentence in [
                "\t\t" + sentence + "\n" for sentence in sub_message.split("\n")
            ]:
                message += sentence
        return f"Exceptions in {self.identifier}:\n" + message


class PipelineException(Exception):
    """
    Exception in Pipeline:
        Output from StepException 0
        Output from StepException 1
    """

    def __init__(self, errors: list[Exception]):
        self.errors = errors
        super().__init__(self.build_message())

    def build_message(self):
        message = ""
        sub_message = ""
        for error in self.errors:
            sub_message += error.build_message()

        # Break it apart, add a tab, bring it back together.
        for sentence in [
            "\t" + sentence + "\n" for sentence in sub_message.split("\n")
        ]:
            message += sentence

        return "Exceptions in Pipeline:\n" + message


class StepExecutor:
    def __init__(
        self,
        upstream: Optional["StepExecutor"],
        step: Step,
        outbox_size: int = 0,
        step_index: Optional[int] = 0,
        progress_reporter: Optional[PipelineProgressReporter] = None,
    ) -> None:
        self.outbox = asyncio.Queue(maxsize=outbox_size)
        self.upstream = upstream
        self.done = False
        self.step = step
        self.step_index = step_index
        self.progress_reporter = progress_reporter
        self.end_of_line = False
        self.identifier = f"{self.__class__.__name__} {self.step_index} ({self.step.__class__.__name__})"
        self.logger = getLogger(name=self.identifier)
        self.exceptions = {}

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
        # Start only calls some callbacks on the reporter.
        # If this fails, we can just log the error and move on.
        try:
            self.start()
        except Exception as start_exception:
            self.logger.exception(
                f"Exception during start for step {self.step.__class__.__name__}. This will not be considered fatal.",
                stack_info=True,
                extra={"step": self.step.__class__.__name__},
            )
            self.exceptions["Exception in Start Process:"] = start_exception

        # During the main exection of the code, we want to catch any
        # exceptions that happen and log them.
        # Since this step can no longer do any work, we will raise the
        # exception so that the pipeline can safely come to a stop.
        try:
            await self.work_body()
        except Exception as work_body_exception:
            self.logger.exception(
                f"Exception during the work body for step {self.step.__class__.__name__}. This will not be considered fatal.",
                stack_info=True,
                extra={"step": self.step.__class__.__name__},
            )
            self.exceptions["Exception in Work Body:"] = work_body_exception

        # When we're done, we need to try to call stop on the step.
        # This is because the step may have some cleanup to do.
        # If this fails, we are near the end of the exection of the pipeline
        # so we can just log the error and move on.
        finally:
            try:
                await self.stop()
            except Exception as stop_exception:
                self.logger.exception(
                    f"Exception during stop for step {self.step.__class__.__name__}. This will not be considered fatal.",
                    stack_info=True,
                    extra={"step": self.step.__class__.__name__},
                )
                self.exceptions["Exception in Stop Process:"] = stop_exception
                raise StepException(errors=self.exceptions, identifier=self.identifier)

            if self.exceptions:
                raise StepException(errors=self.exceptions, identifier=self.identifier)


class Pipeline(AggregatedIntrospectiveIngestionComponent):
    """A pipeline is a series of steps that are executed in order."""

    __slots__ = ("steps",)

    def __init__(self, steps: List[Step], step_outbox_size: int) -> None:
        self.steps = steps
        self.step_outbox_size = step_outbox_size
        self.logger = getLogger(self.__class__.__name__)
        self.errors = []

    async def run(
        self, progress_reporter: Optional[PipelineProgressReporter] = None
    ) -> AsyncGenerator[Any, Any]:
        current_executor = None
        tasks = []

        for step_index, step in enumerate(self.steps):
            current_executor = StepExecutor(
                upstream=current_executor,
                step=step,
                outbox_size=self.step_outbox_size,
                step_index=step_index,
            )
            tasks.append(asyncio.create_task(current_executor.work_loop()))

        current_executor.set_end_of_line(progress_reporter)
        return_states = await asyncio.gather(*tasks, return_exceptions=True)
        for return_state in return_states:
            if return_state:
                self.errors.append(return_state)

        # Raise the error and log it.
        if self.errors:
            try:
                raise PipelineException(self.errors)
            except PipelineException:
                self.logger.exception(
                    "Exception during execution of the pipeline.",
                )
                raise 

    def all_subordinate_components(self) -> Iterable[IntrospectiveIngestionComponent]:
        return (s for s in self.steps if isinstance(s, IntrospectiveIngestionComponent))
