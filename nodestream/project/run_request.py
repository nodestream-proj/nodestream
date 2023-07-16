from dataclasses import dataclass

from ..pipeline import PipelineInitializationArguments
from ..pipeline.meta import start_context
from ..pipeline.progress_reporter import PipelineProgressReporter
from .pipeline_definition import PipelineDefinition


@dataclass
class RunRequest:
    """A `RunRequest` represents a request to run a pipeline."""

    pipeline_name: str
    initialization_arguments: PipelineInitializationArguments
    progress_reporter: PipelineProgressReporter

    async def execute_with_definition(self, definition: PipelineDefinition):
        with start_context(self.pipeline_name):
            pipeline = definition.initialize(self.initialization_arguments)
            await pipeline.run(self.progress_reporter)
