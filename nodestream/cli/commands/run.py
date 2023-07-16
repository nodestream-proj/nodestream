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
        option("prometheus", None, "Weather or not to expose a prometheus server"),
        option(
            "prometheus-server-addr",
            None,
            "What address to listen on for the prometheus server",
            flag=False,
            default="0.0.0.0",
        ),
        option(
            "prometheus-server-port",
            None,
            "What port to listen on for the prometheus server",
            flag=False,
            default=8080,
        ),
    ]

    async def handle_async(self):
        await self.run_operation(InitializeLogger())
        project = await self.run_operation(InitializeProject())
        await self.run_operation(RunPipeline(project))
