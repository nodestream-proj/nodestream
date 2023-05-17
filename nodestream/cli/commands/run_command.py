from pathlib import Path

from cleo.commands.command import Command
from cleo.helpers import argument, option

from ...project import Project, RunRequest, PipelineProgressReporter
from ...pipeline import PipelineInitializationArguments
from .async_command import AsyncCommand


class RunCommand(AsyncCommand):
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

    @property
    def use_json_logging(self) -> bool:
        return self.option("json-logging")

    async def handle_async(self):
        self.configure_logging()
        await self.run_pipeline()

    def configure_logging(self):
        if not self.use_json_logging:
            pass

        # TODO: Configure JSON Logging

    def get_project(self) -> Project:
        path = self.option("project")
        if path is not None:
            path = Path(path)
        return Project.from_file(path)

    async def run_pipeline(self):
        pipeline_name = self.argument("pipeline")
        self.progress = self.progress_indicator()
        self.progress.start(f"Running pipeline: '{pipeline_name}'")
        await self.get_project().run(self.make_run_request())
        self.progress.finish(f"Finished running pipeline: '{pipeline_name}'")

    def make_run_request(self) -> RunRequest:
        return RunRequest(
            pipeline_name=self.argument("pipeline"),
            initialization_arguments=self.create_pipeline_init_args(),
            progress_reporter=self.create_progress_reporter(),
        )

    def create_pipeline_init_args(self) -> PipelineInitializationArguments:
        return PipelineInitializationArguments(
            annotations=self.option("annotations"),
        )

    def create_progress_reporter(self) -> PipelineProgressReporter:
        return PipelineProgressReporter(
            reporting_frequency=int(self.option("reporting-frequency")),
            callback=self.progress_callback,
        )

    def progress_callback(self, index, _):
        if not self.use_json_logging:
            self.progress.set_message(
                f"Currently processing record at index: <info>{index}</info>"
            )
