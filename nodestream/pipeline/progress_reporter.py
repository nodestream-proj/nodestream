import os
import time
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import Any, Callable, Optional

from psutil import Process

from ..metrics import Metrics


def no_op(*_, **__):
    """A no-op function that does nothing."""
    pass


def raise_exception(ex):
    raise ex


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

    reporting_frequency: int = 10000
    metrics_interval_in_seconds: Optional[float] = None
    logger: Logger = field(default_factory=getLogger)
    callback: Callable[[int, Metrics], None] = field(default=no_op)
    on_start_callback: Callable[[], None] = field(default=no_op)
    on_finish_callback: Callable[[Metrics], None] = field(default=no_op)
    on_fatal_error_callback: Callable[[Exception], None] = field(default=no_op)
    encountered_fatal_error: bool = field(default=False)
    observability_callback: Callable[[Any], None] = field(default=no_op)
    last_report_time: float = field(default=0)

    def on_fatal_error(self, exception: Exception):
        self.encountered_fatal_error = True
        self.on_fatal_error_callback(exception)

    @classmethod
    def for_testing(cls, results_list: list) -> "PipelineProgressReporter":
        """Create a `PipelineProgressReporter` for testing.

        This method is intended to be used for testing purposes only. It will
        create a `PipelineProgressReporter` with the default values for
        testing.

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
            on_fatal_error_callback=raise_exception,
        )

    def report(self, index, metrics: Metrics):
        if self.metrics_interval_in_seconds is not None:
            current_time = time.time()
            if current_time - self.last_report_time >= self.metrics_interval_in_seconds:
                self.callback(index, metrics)
                self.last_report_time = current_time
        elif index % self.reporting_frequency == 0:
            self.callback(index, metrics)

    def observe(self, record: Any):
        self.observability_callback(record)
