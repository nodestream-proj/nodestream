import os
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import Any, Callable

from psutil import Process

from .meta import PipelineContext


def no_op(*_, **__):
    """A no-op function that does nothing."""
    pass


def get_max_mem_mb():
    """Get the maximum memory used by the current process in MB.

    Returns:
        int: The maximum memory used by the current process in MB.
    """
    process = Process(os.getpid())
    return process.memory_info().rss


@dataclass
class PipelineProgressReporter:
    """A `PipelineProgressReporter` is a utility that can be used to report on the progress of a pipeline."""

    reporting_frequency: int = 1000
    logger: Logger = field(default_factory=getLogger)
    callback: Callable[[int, Any], None] = field(default=no_op)
    on_start_callback: Callable[[], None] = field(default=no_op)
    on_finish_callback: Callable[[PipelineContext], None] = field(default=no_op)

    @classmethod
    def for_testing(cls, results_list: list) -> "PipelineProgressReporter":
        """Create a `PipelineProgressReporter` for testing.

        This method is intended to be used for testing purposes only. It will create a
        `PipelineProgressReporter` with the default values for testing.

        Args:
            results_list: The list to append results to.

        Returns:
            PipelineProgressReporter: A `PipelineProgressReporter` for testing.
        """
        return cls(
            reporting_frequency=1,
            logger=getLogger("test"),
            callback=lambda _, record: results_list.append(record),
            on_start_callback=no_op,
            on_finish_callback=no_op,
        )

    def report(self, index, record):
        if index % self.reporting_frequency == 0:
            self.logger.info(
                "Records Processed",
                extra={"index": index, "max_memory": get_max_mem_mb()},
            )
            self.callback(index, record)
