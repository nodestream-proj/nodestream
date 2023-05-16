from dataclasses import dataclass, field
from logging import getLogger, Logger
from typing import Callable, Any

from ..utilities import enumerate_async, get_max_mem_mb
from ..pipeline import Pipeline


@dataclass
class PipelineProgressReporter:
    reporting_frequency: int = 1000
    logger: Logger = field(default_factory=getLogger)
    callback: Callable[[int, Any], None] = field(default_factory=lambda: None)

    async def execute_with_reporting(self, pipeline: Pipeline):
        async for index, record in enumerate_async(pipeline.run()):
            if index % self.reporting_frequency == 0:
                self.report(index, record)

    def report(self, index, record):
        self.logger.info(
            "Records Processed", extra={"index": index, "max_memory": get_max_mem_mb()}
        )
        self.callback(index, record)
