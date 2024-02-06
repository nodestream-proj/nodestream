import asyncio
import traceback
from logging import getLogger
from typing import Any, AsyncGenerator, Iterable, List, Optional

from ..schema import ExpandsSchema, ExpandsSchemaFromChildren
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


START_EXCEPTION = "Exception in Start Process:"
WORK_BODY_EXCEPTION = "Exception in Work Body:"
STOP_EXCEPTION = "Exception in Stop Process:"
OUTBOX_POLL_TIME = 0.1
PRECHECK_MESSAGE = "Detected fatal error in pipeline while attempting to process record, this step can no longer continue."
TIMEOUT_MESSAGE = "Unable to place record into outbox because the pipeline has failed, this step can no longer continue."


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


class ForwardProgressHalted(Exception):
    pass


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


class PipelineState:
    def __init__(self) -> None:
        self.has_fatal_error = False

    def signal_fatal_error(self):
        self.has_fatal_error = True


class StepExecutor:
    def __init__(
        self,
        pipeline_state: PipelineState,
        upstream: Optional["StepExecutor"],
        step: Step,
        outbox_size: int = 0,
        step_index: Optional[int] = 0,
        progress_reporter: Optional[PipelineProgressReporter] = None,
    ) -> None:
        self.pipeline_state = pipeline_state
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

    def pipeline_has_died(self) -> bool:
        return self.pipeline_state.has_fatal_error

    async def submit_object_or_die_trying(self, obj):
        should_continue = True
        while should_continue:
            if self.pipeline_has_died() and not self.done:
                raise ForwardProgressHalted(PRECHECK_MESSAGE)
            try:
                await asyncio.wait_for(self.outbox.put(obj), timeout=OUTBOX_POLL_TIME)
                should_continue = False
            except asyncio.TimeoutError:
                # We timed out, now we need to check if the pipeline has died and if so, raise an exception.
                if self.pipeline_has_died():
                    raise ForwardProgressHalted(TIMEOUT_MESSAGE)

    async def stop(self):
        self.done = True
        await self.submit_object_or_die_trying(DoneObject)
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
                await self.submit_object_or_die_trying(record)
            if self.progress_reporter:
                self.progress_reporter.report(index, record)

    def try_start(self):
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
            self.exceptions[START_EXCEPTION] = start_exception

    async def try_work_body(self):
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
            self.exceptions[WORK_BODY_EXCEPTION] = work_body_exception

    async def try_stop(self):
        # When we're done, we need to try to call stop on the step.
        # This is because the step may have some cleanup to do.
        # If this fails, we are near the end of the exection of the pipeline
        # so we can just log the error and move on.
        try:
            await self.stop()
        except Exception as stop_exception:
            self.logger.exception(
                f"Exception during stop for step {self.step.__class__.__name__}. This will not be considered fatal.",
                stack_info=True,
                extra={"step": self.step.__class__.__name__},
            )
            self.exceptions[STOP_EXCEPTION] = stop_exception

    def raise_encountered_exceptions(self):
        if self.exceptions:
            self.pipeline_state.signal_fatal_error()
            raise StepException(errors=self.exceptions, identifier=self.identifier)

    async def work_loop(self):
        self.try_start()
        await self.try_work_body()
        await self.try_stop()
        self.raise_encountered_exceptions()


class Pipeline(ExpandsSchemaFromChildren):
    """A pipeline is a series of steps that are executed in order."""

    __slots__ = ("steps",)

    def __init__(self, steps: List[Step], step_outbox_size: int) -> None:
        self.steps = steps
        self.step_outbox_size = step_outbox_size
        self.logger = getLogger(self.__class__.__name__)
        self.errors = []

    def build_steps_into_tasks(
        self, progress_reporter: Optional[PipelineProgressReporter] = None
    ):
        current_executor = None
        tasks = []
        state = PipelineState()
        for step_index, step in enumerate(self.steps):
            current_executor = StepExecutor(
                pipeline_state=state,
                upstream=current_executor,
                step=step,
                outbox_size=self.step_outbox_size,
                step_index=step_index,
            )
            tasks.append(asyncio.create_task(current_executor.work_loop()))
        current_executor.set_end_of_line(progress_reporter)
        return tasks

    def propogate_errors_from_return_states(self, return_states):
        for return_state in return_states:
            if return_state:
                self.errors.append(return_state)

        # Raise the error and log it.
        if self.errors:
            raise PipelineException(self.errors)

    async def run(
        self, progress_reporter: Optional[PipelineProgressReporter] = None
    ) -> AsyncGenerator[Any, Any]:
        tasks = self.build_steps_into_tasks(progress_reporter)
        return_states = await asyncio.gather(*tasks, return_exceptions=True)
        self.propogate_errors_from_return_states(return_states)

    def get_child_expanders(self) -> Iterable[ExpandsSchema]:
        return (s for s in self.steps if isinstance(s, ExpandsSchema))
