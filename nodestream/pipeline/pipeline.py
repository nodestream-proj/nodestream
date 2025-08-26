from asyncio import create_task, gather
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, Coroutine, Iterable, List, Optional, Tuple

from ..metrics import (
    RECORDS,
    STEPS_RUNNING,
    Metrics,
)
from ..schema import ExpandsSchema, ExpandsSchemaFromChildren
from .channel import StepInput, StepOutput, channel
from .object_storage import ObjectStore
from .progress_reporter import PipelineProgressReporter
from .step import Step, StepContext


async def no_op(_):
    pass


@dataclass(slots=True)
class Record:
    """A `Record` is a unit of data that is processed by a pipeline."""

    data: Any
    callback_token: Any = None
    callback: Optional[Coroutine[None, None, None]] = field(default=no_op)
    originated_from: Optional["Record"] = field(default=None)
    child_record_count: int = field(default=0)

    async def child_dropped(self):
        # If we have no children after this child has reported itself as
        # having been dropped, then we can consider ourselves dropped as
        # well since this must mean that we are not responsible for any more
        # work and are not a resultant record in the pipeline and instead
        # had to have been created as an intermediate step so our usefulness
        # is simply if our children are useful.
        self.child_record_count -= 1
        if self.child_record_count == 0:
            await self.drop()

    async def drop(self):
        # If we are being told to drop, then we need to run our callback so
        # that the step that created us can clean up any resources it has
        # allocated for this record.
        if self.callback is not None:
            await self.callback(self.callback_token)

        # If _we_ are being dropped, then there is a chance that our parent is
        # done as well. So we can propagate the drop up the chain and ensure that
        # all records are properly cleaned up.
        if self.originated_from is not None:
            await self.originated_from.child_dropped()


class StepExecutor:
    """`StepExecutor` is a utility that is used to run a step in a pipeline.

    The `StepExecutor` is responsible for starting, stopping, and running a
    step in a pipeline. It is used to execute a step by passing records
    between the input and output channels of the step.
    """

    __slots__ = ("step", "input", "output", "context")

    def __init__(
        self,
        step: Step,
        input: StepInput,
        output: StepOutput,
        context: StepContext,
    ) -> None:
        self.step = step
        self.input = input
        self.output = output
        self.context = context

    async def start_step(self):
        try:
            Metrics.get().increment(STEPS_RUNNING)
            await self.step.start(self.context)
        except Exception as e:
            self.context.report_error("Error starting step", e)

    async def stop_step(self):
        try:
            Metrics.get().decrement(STEPS_RUNNING)
            await self.step.finish(self.context)
        except Exception as e:
            self.context.report_error("Error stopping step", e)

    async def emit_record(self, record):
        can_continue = await self.output.put(record)
        if not can_continue:
            self.context.debug(
                "Downstream is not accepting more records. Gracefully stopping."
            )

        return can_continue

    async def wrap_generator_entry(
        self,
        originating_record: Optional[Record],
        entry: Tuple[Any, Any] | Any,
    ) -> Record:
        data = entry
        token = None
        callback = None

        if isinstance(entry, tuple):
            data, token = entry
            callback = self.step.finalize_record

        return Record(
            data=data,
            callback_token=token,
            callback=callback,
            originated_from=originating_record,
        )

    async def emit_generator(
        self,
        generator,
        originating_record: Optional[Record] = None,
    ):
        produced_something = False
        async for entry in generator:
            record = await self.wrap_generator_entry(originating_record, entry)
            if not await self.emit_record(record):
                return
            produced_something = True

        # If a record did not result in any new records, then we can consider
        # it to be dropped since it did not produce any new work and will leave
        # scope here.
        if not produced_something and originating_record is not None:
            await originating_record.drop()

    async def drive_step(self):
        try:
            while (next_record := await self.input.get()) is not None:
                results = self.step.process_record(next_record.data, self.context)
                await self.emit_generator(results, next_record)

            outstanding = self.step.emit_outstanding_records(self.context)
            await self.emit_generator(outstanding)

            self.context.debug("Step finished emitting")
        except Exception as e:
            self.context.report_error("Error running step", e, fatal=True)

    async def run(self):
        self.context.debug("Starting step")
        await self.start_step()
        await self.drive_step()
        await self.output.done()
        self.input.done()
        await self.stop_step()
        self.context.debug("Finished step")


class PipelineOutput:
    """`PipelineOutput` is an output channel for a pipeline.

    A `PipelineOutput` is used to consume records from the last step in a
    pipeline and report the progress of the pipeline.
    """

    __slots__ = ("input", "reporter", "observe_results")

    def __init__(self, input: StepInput, reporter: PipelineProgressReporter):
        self.input = input
        self.reporter = reporter

    def call_handling_errors(self, f, *args):
        try:
            f(*args)
        except Exception:
            self.reporter.logger.exception(f"Error running {f.__name__}")

    async def run(self):
        """Run the pipeline output.

        This method is used to run the pipeline output. It will consume records
        from the last step in the pipeline and report the progress of the
        pipeline using the `PipelineProgressReporter`. The pipeline output will
        block until all records have been consumed from the last step in the
        pipeline.
        """
        metrics = Metrics.get()
        self.call_handling_errors(self.reporter.on_start_callback)

        index = 0
        while (record := await self.input.get()) is not None:
            metrics.increment(RECORDS)
            self.call_handling_errors(self.reporter.report, index, metrics)
            self.call_handling_errors(self.reporter.observe, record)
            await record.drop()
            index += 1

        self.call_handling_errors(self.reporter.on_finish_callback, metrics)


class Pipeline(ExpandsSchemaFromChildren):
    """`Pipeline` is a collection of steps that are executed in sequence.

    A `Pipeline` is a collection of steps that are executed in sequence. Each
    step processes records and emits new records that are passed to the next
    step in the pipeline. The pipeline is responsible for starting, stopping,
    and running the steps in the pipeline.
    """

    __slots__ = ("steps", "step_outbox_size", "logger", "object_store")

    def __init__(
        self,
        steps: Tuple[Step, ...],
        step_outbox_size: int,
        object_store: ObjectStore,
    ) -> None:
        self.steps = steps
        self.step_outbox_size = step_outbox_size
        self.logger = getLogger(self.__class__.__name__)
        self.object_store = object_store

    def get_child_expanders(self) -> Iterable[ExpandsSchema]:
        return (s for s in self.steps if isinstance(s, ExpandsSchema))

    async def run(self, reporter: PipelineProgressReporter):
        """Run the pipeline.

        This method is used to run the pipeline. It will start, stop, and run
        the steps in the pipeline in sequence. It will pass records between the
        steps in the pipeline using channels. The pipeline will report on the
        progress of the pipeline using the `PipelineProgressReporter`. The
        pipeline will run in an asynchronous context. The pipeline will block
        until all steps in the pipeline are finished.

        This method does not return anything. If an error occurs during the
        processing of the pipeline, it will be reported using the
        `PipelineProgressReporter`.

        Args:
            reporter: The `PipelineProgressReporter` used to report on the
                progress of the pipeline.
        """
        # Create the input and output channels for the pipeline. The input
        # channel is used to pass records from the previous step to the current
        # step. The output channel is used to pass records from the current
        # step to the next step. The channels are used to pass records between
        # the steps in the pipeline. The channels have a fixed size to control
        # the flow of records between the steps.
        executors: List[StepExecutor] = []
        current_input_name = None
        current_output_name = self.steps[-1].__class__.__name__ + f"_{len(self.steps)}"

        current_input, current_output = channel(
            self.step_outbox_size, current_output_name, current_input_name
        )
        pipeline_output = PipelineOutput(current_input, reporter)

        # Create the executors for the steps in the pipeline. The executors
        # will be used to run the steps concurrently. The steps are created in
        # reverse order so that the output of each step is connected to the
        # input of the next step.
        for reversed_index, step in reversed(list(enumerate(self.steps))):
            index = len(self.steps) - reversed_index - 1
            storage = self.object_store.namespaced(str(index))
            context = StepContext(step.__class__.__name__, index, reporter, storage)
            current_output_name = (
                self.steps[reversed_index - 1].__class__.__name__
                + f"_{reversed_index - 1}"
                if reversed_index - 1 >= 0
                else None
            )
            current_input_name = step.__class__.__name__ + f"_{reversed_index}"
            current_input, next_output = channel(
                self.step_outbox_size, current_output_name, current_input_name
            )
            exec = StepExecutor(step, current_input, current_output, context)
            current_output = next_output
            executors.append(exec)

        # There is a "leftover" input channel that is not connected to any
        # step. This channel is connected to the first step in the pipeline
        # so we can mark it as done since we are not going to produce anything
        # onto it.
        await current_output.done()

        # Run the pipeline by running all the steps and the pipeline output
        # concurrently. This will block until all steps are finished.
        running_steps = (create_task(executor.run()) for executor in executors)

        await gather(*running_steps, create_task(pipeline_output.run()))
