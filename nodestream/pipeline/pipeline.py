from abc import ABC, abstractmethod
from asyncio import create_task, gather
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Iterable, List, Optional, Tuple, Type

from ..metrics import RECORDS, STEPS_RUNNING, Metrics
from ..schema import ExpandsSchema, ExpandsSchemaFromChildren
from .channel import StepInput, StepOutput, channel
from .object_storage import ObjectStore
from .progress_reporter import PipelineProgressReporter
from .step import Step, StepContext


async def no_op(_):
    pass


@dataclass(slots=True)
class RecordContext:
    """A `Record` is a unit of data that is processed by a pipeline."""

    record: Any
    originating_step: Step
    callback_token: Any
    originated_from: Optional["RecordContext"] = field(default=None)
    child_record_count: int = field(default=0)

    @staticmethod
    def from_step_emission(
        step: Step,
        emission: Any,
        originated_from: Optional["RecordContext"] = None,
    ):
        """Create a record from a step's emission of data.

        The `emission` can either be a single value or a tuple of two values.
        If it is a single value, then it is assumed to be the data for the
        record. If it is a tuple of two values, then the first value is
        assumed to be the data for the record and the second value is assumed
        to be the callback token for the record. If any other value is
        provided, the data and callback token are both set to the value
        provided.

        Args:
            step (Step): The step that emitted the record.
            emission (Any): The emission from the step.
            originated_from (Optional[Record], optional): The record that
                this record was emitted from. Defaults to None.

        Returns:
            Record: The record created from the emission.
        """
        record = callback_token = emission
        if isinstance(emission, tuple):
            record, callback_token = emission

        return RecordContext(record, step, callback_token, originated_from)

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
        # allocated for this record if it opts into the feature
        # (by implementing the `finalize_record` method).
        await self.originating_step.finalize_record(self.callback_token)

        # If _we_ are being dropped, then there is a chance that our parent is
        # done as well. So we can propagate the drop up the chain and ensure
        # that all records are properly cleaned up.
        if self.originated_from is not None:
            await self.originated_from.child_dropped()


class ExecutionState(ABC):
    @abstractmethod
    async def execute_until_state_change(self) -> Optional["ExecutionState"]:
        pass


class EmitResult(Enum):
    EMITTED_RECORDS = auto()
    CLOSED_DOWNSTREAM = auto()
    NO_OP = auto()

    @property
    def should_continue(self) -> bool:
        return self != EmitResult.CLOSED_DOWNSTREAM

    @property
    def did_emit_records(self) -> bool:
        return self == EmitResult.EMITTED_RECORDS


class StepExecutionState(ExecutionState):
    """State that a step is in when it is executing.

    This is the base class for all states that a step can be in. It provides
    the basic functionality for executing a step and transitioning between
    states. It also provides the basic functionality for emitting records
    and handling errors.
    """

    def __init__(
        self,
        step: Step,
        context: StepContext,
        input: StepInput,
        output: StepOutput,
    ):
        self.step = step
        self.context = context
        self.input = input
        self.output = output

    def make_state(self, next_state: Type["StepExecutionState"]):
        """Make the next state for the step.

        This method is used to create the next state for the step. It is
        responsible for creating the next state and returning it. The next
        state is created with the same step, context, input, and output as the
        current state.
        """
        return next_state(self.step, self.context, self.input, self.output)

    async def emit_record(self, record: RecordContext) -> EmitResult:
        """Emit a record to the output channel.

        This method is used to emit a record to the output channel. It will
        block until the record is put in the channel. If the channel is full,
        it will block until there is space in the channel unless the channel is
        closed on the other end.

        Returns:
            EmitResult: The result of the emit operation. If the downstream is
            not accepting more records, it will return
            `EmitResult.CLOSED_DOWNSTREAM`. Otherwise, it will return
            `EmitResult.EMITTED_RECORDS`.
        """
        if not await self.output.put(record):
            self.context.debug("Downstream is not accepting records. Stopping")
            return EmitResult.CLOSED_DOWNSTREAM

        return EmitResult.EMITTED_RECORDS

    async def emit_from_generator(
        self,
        generator,
        origin: Optional[RecordContext] = None,
    ) -> EmitResult:
        """Emit records from a generator.

        This method is used to emit records from a generator. It will block
        until the record is put in the channel. If the channel is full, it will
        block until there is space in the channel unless the channel is closed
        on the other end.

        Returns:
            EmitResult: The result of the emit operation. If the downstream
            is not accepting more records, it will return
            `EmitResult.CLOSED_DOWNSTREAM`. If no records were emitted, it
            will return `EmitResult.NO_OP`. Otherwise, it will return
            `EmitResult.EMITTED_RECORDS`.
        """
        emitted = False
        async for emission in generator:
            # We can create a record for this data and attempt to submit it
            # downstream. If we _failed_ to emit a record, then we need to
            # stop processing any more records and we will report this to
            # whatever state is calling us. Depending on that state, it can
            # decide what to do.
            record = RecordContext.from_step_emission(self.step, emission, origin)
            result = await self.emit_record(record)
            if result == EmitResult.CLOSED_DOWNSTREAM:
                return result

            # If we got here, then we successfully emitted at least 1 record.
            emitted = True

        # If we succesfully left the loop, then we either emitted something or
        # or the generator was empty. Depending that, we can return the correct
        # status to the caller.
        return EmitResult.EMITTED_RECORDS if emitted else EmitResult.NO_OP


class StartStepState(StepExecutionState):
    """State that a step is in when it is starting.

    This is the first state that a step is in when it is executed. The step
    is in this state when it is first created and before it has started
    processing records. Once the step has started, it will transition to the
    `ProcessRecordsState`. If the step fails to start, it will transition to
    the `StopStepExecution` state.
    """

    async def execute_until_state_change(self) -> Optional[StepExecutionState]:
        try:
            Metrics.get().increment(STEPS_RUNNING)
            await self.step.start(self.context)
            return self.make_state(ProcessRecordsState)
        except Exception as e:
            self.context.report_error("Error starting step", e, fatal=True)
            return None


class ProcessRecordsState(StepExecutionState):
    """State that a step is in when it is processing records.

    This is the state that a step is in when it is actively processing records.
    The step will remain in this state until it has processed all of its input
    records and emitted all of its output records. Once the step has finished
    processing records, it will transition to the
    `EmitOutstandingRecordsState`.

    If the step fails to process a record, it will transition to the
    `StopStepExecution` state. If the step downstream is not accepting more
    records, it will transition to the `StopStepExecution` state.
    """

    async def execute_until_state_change(self) -> Optional[StepExecutionState]:
        try:
            while (next := await self.input.get()) is not None:
                # Process the record and emit any resulting records downstream.
                emissions = self.step.process_record(next.record, self.context)
                result = await self.emit_from_generator(emissions, next)

                # If we didn't emit any records by processing this record,
                # then we need to drop the record since it is not
                # going to participate in the pipeline any further.
                if not result.did_emit_records:
                    await next.drop()

                # If the downstream is not accepting more records, then we need
                # to stop processing records by transitioning to a stop state.
                if not result.should_continue:
                    return self.make_state(StopStepExecution)

        # If we get an exception, we need to stop processing records by
        # transitioning to the stop state. Because this is part of the core
        # execution of the step, we consider this a fatal error.
        except Exception as e:
            self.context.report_error("Error processing record", e, fatal=True)
            return self.make_state(StopStepExecution)

        # If we have gotten here, then we have processed all of our input
        # records and we need to transition to the next state which is to emit
        # any outstanding records.
        return self.make_state(EmitOutstandingRecordsState)


class EmitOutstandingRecordsState(StepExecutionState):
    """State that a step is in when it is emitting outstanding records.

    This is the state that a step is in when it is emitting any outstanding
    records. This is done after all records have been processed. Regardless of
    success or failure, the step will transition to the `StopStepExecution`
    state.
    """

    async def execute_until_state_change(self) -> Optional[StepExecutionState]:
        try:
            # Emit any outstanding records. If we get an error, we will still
            # transition to the stop state. Unlike the processing state, this
            # state does not really care about the result of the emit
            # operation because we are transitioning to the stop state
            # regardless of success or failure and there is no originating
            # record to drop.
            #
            # NOTE: This is not quite true, as in theory records can be
            # outstanding that were some how originated from a record.
            # However, there is no nice way I've thought of to account for
            # this without breaking the step interface to track the
            # originating record for each outstanding record. For now, we
            # will just leave it out of scope.
            outstanding = self.step.emit_outstanding_records(self.context)
            await self.emit_from_generator(outstanding)

        # If we get an exception, we will report it as fatal as steps
        # processing outstanding records is part of the core execution
        # of the step.
        except Exception as e:
            self.context.report_error(
                "Error emitting outstanding records",
                e,
                fatal=True,
            )

        # We are doing to transition to the stop state regardless of what
        # happened to get us here.
        return self.make_state(StopStepExecution)


class StopStepExecution(StepExecutionState):
    """State that a step is in when it is stopping.

    This is the state that a step is in when it is stopping. This is the final
    state that a step will be in. Once it transitions to this state, it will
    not transition to any other state and end execution.
    """

    async def execute_until_state_change(self) -> Optional[StepExecutionState]:
        try:
            # Closing the output channel will signal to any downstream steps
            # that we are done processing records and that there nothing left
            # to wait for. Similarly, we mark the input as done to signal
            # to any upstream steps that we are done processing records and
            # that producing more records is futile.
            await self.output.done()
            self.input.done()

            # Steps may need to do some finalization work when they are done.
            await self.step.finish(self.context)

        # In the event of a failure closing out a step, we will report it as a
        # non-fatal error because all core work has been accomplished. Resource
        # cleanup, while messy, is not fatal to the pipeline as a whole.
        except Exception as e:
            self.context.report_error("Error stopping step", e)

        # We are done regarless of what happens. There is no next state.
        Metrics.get().decrement(STEPS_RUNNING)
        return None


class PipelineOutputState(ExecutionState):
    __slots__ = ("input", "reporter", "metrics")

    def __init__(
        self,
        input: StepInput,
        reporter: PipelineProgressReporter,
        metrics: Metrics,
    ):
        self.input = input
        self.reporter = reporter
        self.metrics = metrics

    def make_state(self, next_state: Type["ExecutionState"]):
        return next_state(self.input, self.reporter, self.metrics)

    def call_ignoring_errors(self, f, *args):
        try:
            f(*args)
        except Exception:
            self.reporter.logger.exception(f"Error running {f.__name__}")


class PipelineOutputStartState(PipelineOutputState):
    """State that the pipeline output is in when it is starting.

    This is the first state that the pipeline output is in when it is
    executed. The pipeline output is in this state when it is first created
    and before it has started processing records. Once the pipeline output
    has started, it will transition to the `PipelineOutputProcessRecordsState`.
    """

    async def execute_until_state_change(self) -> Optional[ExecutionState]:
        self.call_ignoring_errors(self.reporter.on_start_callback)
        return self.make_state(PipelineOutputProcessRecordsState)


class PipelineOutputProcessRecordsState(PipelineOutputState):
    """State that the pipeline output is in when it is processing records.

    This is the state that the pipeline output is in when it is processing
    records. The pipeline output is in this state after it has started and
    before it has finished processing all records. Once the pipeline output
    has finished processing all records, it will transition to the
    `PipelineOutputStopState`.
    """

    async def execute_until_state_change(self) -> Optional[ExecutionState]:
        index = 0
        while (next := await self.input.get()) is not None:
            self.metrics.increment(RECORDS)
            self.call_ignoring_errors(self.reporter.report, index, self.metrics)
            self.call_ignoring_errors(self.reporter.observe, next.record)
            await next.drop()
            index += 1
        return self.make_state(PipelineOutputStopState)


class PipelineOutputStopState(PipelineOutputState):
    """State that the pipeline output is in when it is stopping.

    This is the state that the pipeline output is in when it is stopping. This
    is the final state that the pipeline output will be in. Once it transitions
    to this state, it will not transition to any other state and end execution.
    """

    async def execute_until_state_change(self) -> Optional[ExecutionState]:
        # For cases where the reporter _wants_ to have the exception thrown
        # (e.g to have a status code in the CLI) we need to make sure we call
        # on_finish_callback without swallowing exceptions (because thats the point).
        self.reporter.on_finish_callback(self.metrics)
        return None


class Executor:
    __slots__ = ("state",)

    def __init__(self, state: ExecutionState) -> None:
        self.state = state

    @classmethod
    def for_step(
        cls,
        step: Step,
        input: StepInput,
        output: StepOutput,
        context: StepContext,
    ) -> "Executor":
        return cls(StartStepState(step, context, input, output))

    @classmethod
    def pipeline_output(
        cls, input: StepInput, reporter: PipelineProgressReporter
    ) -> "Executor":
        return cls(PipelineOutputStartState(input, reporter, Metrics.get()))

    async def run(self):
        while self.state is not None:
            self.state = await self.state.execute_until_state_change()


class Pipeline(ExpandsSchemaFromChildren):
    """`Pipeline` is a collection of steps that are executed in sequence.

    A `Pipeline` is a collection of steps that are executed in sequence. Each
    step processes records and emits new records that are passed to the next
    step in the pipeline. The pipeline is responsible for starting, stopping,
    and running the steps in the pipeline.
    """

    __slots__ = ("steps", "step_outbox_size", "object_store")

    def __init__(
        self,
        steps: Tuple[Step, ...],
        step_outbox_size: int,
        object_store: ObjectStore,
    ) -> None:
        self.steps = steps
        self.step_outbox_size = step_outbox_size
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
        executors: List[Executor] = []
        current_input_name = None
        current_output_name = self.steps[-1].__class__.__name__ + f"_{len(self.steps)}"

        # Here lies a footgun. DO NOT MOVE this executor from the first
        # position in the list that makes its way to the gather call. It will
        # break the pipeline because we need to ensure the on_start_callback
        # is called before any operations occur in the actual steps in the
        # pipeline. To do this, we need to make sure that the first coroutine
        # scheduled is the one that calls the on_start_callback.
        current_input, current_output = channel(
            self.step_outbox_size, current_output_name, current_input_name
        )
        executors.append(Executor.pipeline_output(current_input, reporter))

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
            exec = Executor.for_step(step, current_input, current_output, context)
            current_output = next_output
            executors.append(exec)

        # There is a "leftover" input channel that is not connected to any
        # step. This channel is connected to the first step in the pipeline
        # so we can mark it as done since we are not going to produce anything
        # onto it.
        await current_output.done()

        # Run the pipeline by running all the steps and the pipeline output
        # concurrently. This will block until all steps are finished.
        # Wait for all the executors to finish.
        await gather(*(create_task(executor.run()) for executor in executors))
