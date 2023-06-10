from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import Any, Callable

from ..pipeline import Pipeline
from ..pipeline.meta import PipelineContext, get_context
from ..utilities import enumerate_async, get_max_mem_mb


def no_op(*_, **__):
    pass


@dataclass
class PipelineProgressReporter:
    """A `PipelineProgressReporter` is a utility that can be used to report on the progress of a pipeline."""

    reporting_frequency: int = 1000
    logger: Logger = field(default_factory=getLogger)
    callback: Callable[[int, Any], None] = field(default=no_op)
    on_start_callback: Callable[[], None] = field(default=no_op)
    on_finish_callback: Callable[[PipelineContext], None] = field(default=no_op)

    async def execute_with_reporting(self, pipeline: Pipeline):
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
