from dataclasses import dataclass

from ..pipeline import PipelineInitializationArguments, set_pipeline_name
from .pipeline_definition import PipelineDefinition
from .pipeline_progress_reporter import PipelineProgressReporter


@dataclass
class RunRequest:
    """A `RunRequest` represents a request to run a pipeline."""

    pipeline_name: str
    initialization_arguments: PipelineInitializationArguments
    progress_reporter: PipelineProgressReporter

    async def execute_with_definition(self, definition: PipelineDefinition):
        with set_pipeline_name(self.pipeline_name):
            pipeline = definition.initialize(self.initialization_arguments)
            await self.progress_reporter.execute_with_reporting(pipeline)
