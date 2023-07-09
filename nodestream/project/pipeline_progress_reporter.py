import platform
import resource
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import Any, Callable

from ..pipeline import Pipeline
from ..pipeline.meta import PipelineContext, get_context


def no_op(*_, **__):
    """A no-op function that does nothing."""
    pass


def get_max_mem_mb():
    """Get the maximum memory used by the current process in MB.

    Returns:
        int: The maximum memory used by the current process in MB.
    """
    max_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    max_mem /= 1024
    if platform.system() == "Darwin":
        max_mem /= 1024
    return int(max_mem)


async def enumerate_async(iterable):
    """Asynchronously enumerate the given iterable.

    This function is similar to the built-in `enumerate` function, but it is
    asynchronous. It will yield a tuple of the index and the item from the
    iterable.

    Args:
        iterable: The iterable to enumerate.
    """
    count = 0

    async for item in iterable:
        yield count, item
        count += 1


@dataclass
class PipelineProgressReporter:
    """A `PipelineProgressReporter` is a utility that can be used to report on the progress of a pipeline."""

    reporting_frequency: int = 1000
    logger: Logger = field(default_factory=getLogger)
    callback: Callable[[int, Any], None] = field(default=no_op)
    on_start_callback: Callable[[], None] = field(default=no_op)
    on_finish_callback: Callable[[PipelineContext], None] = field(default=no_op)

    async def execute_with_reporting(self, pipeline: Pipeline):
        """Execute the given pipeline with progress reporting.

        This method will execute the given pipeline asynchronously, reporting on the
        progress of the pipeline every `reporting_frequency` records. The progress
        reporting will be done via the `report` method.

        Before and after the pipeline is executed, the `on_start_callback` and
        `on_finish_callback` will be called, respectively. The `on_finish_callback`
        will be passed the `PipelineContext` that was used to execute the pipeline.

        Args:
            pipeline: The pipeline to execute.
        """
        self.on_start_callback()
        async for index, record in enumerate_async(pipeline.run()):
            if index % self.reporting_frequency == 0:
                self.report(index, record)
        self.on_finish_callback(get_context())

    def report(self, index, record):
        self.logger.info(
            "Records Processed", extra={"index": index, "max_memory": get_max_mem_mb()}
        )
        self.callback(index, record)
