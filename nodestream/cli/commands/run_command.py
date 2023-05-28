from cleo.helpers import argument, option

from ..operations import FindAndInitializeProject, RunPipeline, InitializeLogger
from .async_command import AsyncCommand


class Run(AsyncCommand):
    name = "run"
    description = "run a pipleine in the current project"
    arguments = [
        argument("pipeline", "the name of the pipeline to run"),
    ]
    options = [
        option(
            "project", "p", "The project file (nodestream.yaml) to load.", flag=False
        ),
        option(
            "annotations",
            "a",
            "an annotation to apply to the pipeline during initalization. Steps without one of these annotations will be skipped.",
            multiple=True,
            flag=False,
        ),
        option(
            "reporting-frequency",
            "r",
            "how often to report progress",
            default=1000,
            flag=False,
        ),
        option("json-logging", "j", "Log output in JSON", flag=True),
    ]

    async def handle_async(self):
        await self.run_operation(InitializeLogger())
        project = await self.run_operation(FindAndInitializeProject())
        await self.run_operation(RunPipeline(project))
