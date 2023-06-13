from ...pipeline import PipelineInitializationArguments
from ...pipeline.meta import PipelineContext
from ...project import PipelineProgressReporter, Project, RunRequest
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation

STATS_TABLE_COLS = ["Statistic", "Value"]


class RunPipeline(Operation):
    def __init__(self, project: Project) -> None:
        self.project = project

    async def perform(self, command: NodestreamCommand):
        await self.project.run(self.make_run_request(command))

    def make_run_request(self, command: NodestreamCommand) -> RunRequest:
        return RunRequest(
            pipeline_name=command.argument("pipeline"),
            initialization_arguments=PipelineInitializationArguments(
                annotations=command.option("annotations"),
            ),
            progress_reporter=self.create_progress_reporter(command),
        )

    def get_progress_indicator(self, command: NodestreamCommand) -> "ProgressIndicator":
        if command.has_json_logging_set:
            return ProgressIndicator(command)

        return SpinnerProgressIndicator(command)

    def create_progress_reporter(
        self, command: NodestreamCommand
    ) -> PipelineProgressReporter:
        indicator = self.get_progress_indicator(command)
        return PipelineProgressReporter(
            reporting_frequency=int(command.option("reporting-frequency")),
            callback=indicator.progress_callback,
            on_start_callback=indicator.on_start,
            on_finish_callback=indicator.on_finish,
        )


class ProgressIndicator:
    def __init__(self, command: NodestreamCommand) -> None:
        self.command = command

    def on_start(self):
        pass

    def progress_callback(self, _, __):
        pass

    def on_finish(self, context: PipelineContext):
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

    def on_finish(self, context: PipelineContext):
        self.progress.finish(f"Finished running pipeline: '{self.pipeline_name}'")

        stats = ((k, str(v)) for k, v in context.stats.items())
        table = self.command.table(STATS_TABLE_COLS, stats)
        table.render()
