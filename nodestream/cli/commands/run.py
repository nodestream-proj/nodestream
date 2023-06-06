from cleo.helpers import option

from ..operations import InitializeLogger, InitializeProject, RunPipeline
from .nodestream_command import NodestreamCommand
from .shared_options import JSON_OPTION, PIPELINE_ARGUMENT, PROJECT_FILE_OPTION


class Run(NodestreamCommand):
    name = "run"
    description = "run a pipeline in the current project"
    arguments = [PIPELINE_ARGUMENT]
    options = [
        PROJECT_FILE_OPTION,
        JSON_OPTION,
        option(
            "annotations",
            "a",
            "An annotation to apply to the pipeline during initialization. Steps without one of these annotations will be skipped.",
            multiple=True,
            flag=False,
        ),
        option(
            "reporting-frequency",
            "r",
            "How often to report progress",
            default=1000,
            flag=False,
        ),
    ]

    async def handle_async(self):
        await self.run_operation(InitializeLogger())
        project = await self.run_operation(InitializeProject())
        await self.run_operation(RunPipeline(project))
