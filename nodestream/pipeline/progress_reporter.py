import platform
import resource
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import Any, Callable

from .meta import PipelineContext


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


@dataclass
class PipelineProgressReporter:
    """A `PipelineProgressReporter` is a utility that can be used to report on the progress of a pipeline."""

    reporting_frequency: int = 1000
    logger: Logger = field(default_factory=getLogger)
    callback: Callable[[int, Any], None] = field(default=no_op)
    on_start_callback: Callable[[], None] = field(default=no_op)
    on_finish_callback: Callable[[PipelineContext], None] = field(default=no_op)

    def report(self, index, record):
        if index % self.reporting_frequency == 0:
            self.logger.info(
                "Records Processed",
                extra={"index": index, "max_memory": get_max_mem_mb()},
            )
            self.callback(index, record)
