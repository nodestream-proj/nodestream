from pathlib import Path

from cleo.helpers import argument, option

from ...logging import configure_logging_with_json_defaults
from ...pipeline import PipelineInitializationArguments
from ...project import PipelineProgressReporter, Project, RunRequest
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
        self.pipeline_progress_indicator = self.get_progress_indicator()
        self.pipeline_progress_indicator.on_start()
        self.progress_callback = self.pipeline_progress_indicator.progress_callback
        project = self.get_project()
        project.ensure_modules_are_imported()
        await project.run(self.make_run_request())
        self.pipeline_progress_indicator.on_finish()

    def get_project(self) -> Project:
        path = self.option("project")
        if path is not None:
            path = Path(path)
        return Project.from_file(path)

    def make_run_request(self) -> RunRequest:
        return RunRequest(
            pipeline_name=self.argument("pipeline"),
            initialization_arguments=self.create_pipeline_init_args(),
            progress_reporter=self.create_progress_reporter(),
        )

    def get_progress_indicator(self) -> "ProgressIndicator":
        if self.use_json_logging:
            configure_logging_with_json_defaults()
            return ProgressIndicator(self)

        return SpinnerProgressIndicator(self)

    def create_pipeline_init_args(self) -> PipelineInitializationArguments:
        return PipelineInitializationArguments(
            annotations=self.option("annotations"),
        )

    def create_progress_reporter(
        self,
    ) -> PipelineProgressReporter:
        return PipelineProgressReporter(
            reporting_frequency=int(self.option("reporting-frequency")),
            callback=self.progress_callback,
        )


class ProgressIndicator:
    def __init__(self, command: RunCommand) -> None:
        self.command = command

    def on_start(self):
        pass

    def progress_callback(self, index, record):
        pass

    def on_finish(self):
        pass

    @property
    def pipeline_name(self) -> str:
        return self.command.argument("pipeline")


class SpinnerProgressIndicator(ProgressIndicator):
    def on_start(self):
        self.progress = self.command.progress_indicator()
        self.progress.start(f"Running pipeline: '{self.pipeline_name}'")

    def progress_callback(self, index, _):
        self.progress.set_message(
            f"Currently processing record at index: <info>{index}</info>"
        )

    def on_finish(self):
        self.progress.finish(f"Finished running pipeline: '{self.pipeline_name}'")
