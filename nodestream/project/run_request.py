from dataclasses import dataclass

from .pipeline_definition import PipelineDefinition
from .pipeline_progress_reporter import PipelineProgressReporter
from ..pipeline import PipelineInitializationArguments


@dataclass
class RunRequest:
    pipeline_name: str
    initialization_arguments: PipelineInitializationArguments
    progress_reporter: PipelineProgressReporter

    async def execute_with_definition(self, definition: PipelineDefinition):
        pipeline = definition.initialize(self.initialization_arguments)
        await self.progress_reporter.execute_with_reporting(pipeline)
