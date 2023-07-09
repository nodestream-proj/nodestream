from dataclasses import dataclass

from ..pipeline import PipelineInitializationArguments
from ..pipeline.meta import start_context
from .pipeline_definition import PipelineDefinition
from .pipeline_progress_reporter import PipelineProgressReporter


@dataclass
class RunRequest:
    """A `RunRequest` represents a request to run a pipeline.

    `RunRequest` objects should be submitted to a `Project` object to be executed
    via its `run_request` method. The `run_request` method will execute the run request
    on the appopriate pipeline if it exists. Otherwise, it will be a no-op.
    """

    pipeline_name: str
    initialization_arguments: PipelineInitializationArguments
    progress_reporter: PipelineProgressReporter

    async def execute_with_definition(self, definition: PipelineDefinition):
        """Execute this run request with the given pipeline definition.

        This method is intended to be called by `PipelineScope` and should not be called
        directly without good reason. It will execute the run request asynchronously
        with the given pipeline definition. The run request will be executed within a
        context manager that sets the current pipeline name to the name of the pipeline
        being executed.

        Args:
            definition: The pipeline definition to execute this run request with.
        """
        with start_context(self.pipeline_name):
            pipeline = definition.initialize(self.initialization_arguments)
            await self.progress_reporter.execute_with_reporting(pipeline)
