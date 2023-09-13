from dataclasses import dataclass

from ..pipeline import PipelineInitializationArguments
from ..pipeline.meta import start_context
from ..pipeline.progress_reporter import PipelineProgressReporter
from ..pipeline.scope_config import ScopeConfig
from .pipeline_definition import PipelineDefinition


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

    @classmethod
    def for_testing(cls, pipeline_name: str, results_list: list) -> "RunRequest":
        """Create a `RunRequest` for testing.

        This method is intended to be used for testing purposes only. It will create a
        run request with the given pipeline name and `PipelineInitializationArguments`
        for testing.

        Args:
            pipeline_name: The name of the pipeline to run.
            results_list: The list to append results to.

        Returns:
            RunRequest: A `RunRequest` for testing.
        """
        return cls(
            pipeline_name,
            PipelineInitializationArguments.for_testing(),
            PipelineProgressReporter.for_testing(results_list),
        )

    async def execute_with_definition(
        self, definition: PipelineDefinition, config: ScopeConfig
    ):
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
            pipeline = definition.initialize(self.initialization_arguments, config)
            await pipeline.run(self.progress_reporter)
