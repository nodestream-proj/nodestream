from typing import AsyncGenerator, Optional

from ..metrics import (
    FATAL_ERRORS,
    NON_FATAL_ERRORS,
    Metrics,
)
from .object_storage import ObjectStore
from .progress_reporter import PipelineProgressReporter


class StepContext:
    """`StepContext` is a context object that is passed to steps in a pipeline.

    The `StepContext` provides a way for steps to interact with the pipeline
    and report and perist information about the state of the pipeline.
    """

    __slots__ = ("reporter", "index", "name", "object_store")

    def __init__(
        self,
        name: str,
        index: int,
        reporter: PipelineProgressReporter,
        object_store: ObjectStore,
    ) -> None:
        self.name = name
        self.reporter = reporter
        self.index = index
        self.object_store = object_store

    @property
    def pipeline_encountered_fatal_error(self) -> bool:
        """Whether the pipeline has encountered a fatal error."""
        return self.reporter.encountered_fatal_error

    def report_error(
        self,
        message: str,
        exception: Optional[Exception] = None,
        fatal: bool = False,
        **extras,
    ):
        """Report an error.

        This method is used to report an error that occurred during the
        processing of a step. It can be used to log the error and take
        appropriate action based on the error.

        Args:
            message: The error message.
            exception: The exception that caused the error.
            fatal: Whether the error is fatal.
        """
        self.reporter.logger.error(
            message,
            extra=dict(index=self.index, fatal=fatal, step_name=self.name, **extras),
            exc_info=exception,
            stack_info=True,
        )
        if fatal:
            self.reporter.on_fatal_error(exception)
            Metrics.get().increment(FATAL_ERRORS)
        else:
            Metrics.get().increment(NON_FATAL_ERRORS)

    def debug(self, message: str, **extras):
        """Log a debug message.

        This method is used to log a debug message. It can be used to log
        information about the state of the pipeline and the steps in the
        pipeline.
        """
        self.reporter.logger.debug(
            message, extra=dict(index=self.index, step_name=self.name, **extras)
        )

    def info(self, message: str, **extras):
        """Log an info message.

        This method is used to log an info message. It can be used to log
        information about the state of the pipeline and the steps in the
        pipeline.
        """
        self.reporter.logger.info(
            message, extra=dict(index=self.index, step_name=self.name, **extras)
        )

    def warning(self, message: str, **extras):
        """Log a warning message.

        This method is used to log a warning message. It can be used to log
        information about the state of the pipeline and the steps in the
        pipeline.
        """
        self.reporter.logger.warning(
            message, extra=dict(index=self.index, step_name=self.name, **extras)
        )


class Step:
    """A `Step` is a unit of work that can be executed in a pipeline.

    Steps are the building blocks of a pipeline. They are responsible for
    processing records and emitting new records. Steps can be chained together
    to form a pipeline.

    Steps are asynchronous and can be started, stopped, and run in an
    asynchronous context. They can process records and emit new records.
    """

    async def start(self, context: StepContext):
        """Start the step.

        This method is called when the step is started. It is responsible for
        setting up the step and preparing it for processing records. This
        method is called once before any records are processed.
        """
        pass

    async def process_record(
        self, record, context: StepContext
    ) -> AsyncGenerator[object, None]:
        """Process a record.

        This method is called for each record that is passed to the step. It
        should process the record and emit new records.
        """
        yield record

    async def emit_outstanding_records(self, context: StepContext):
        """Emit any outstanding records.

        This method is called after all records have been processed. It is
        responsible for emitting any outstanding records that were not emitted
        during the processing of records.
        """
        for record in ():
            yield record  # pragma: no cover

    async def finish(self, context: StepContext):
        """Finish the step.

        This method is called when the step is finished. It is responsible for
        cleaning up the step and releasing any resources that were acquired
        during the processing of records. This method is called once after all
        records have been processed.
        """
        pass


class PassStep(Step):
    """A `PassStep` passes records through."""

    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)
