from ...pipeline import PipelineInitializationArguments
from ...pipeline.meta import PipelineContext, listen, STAT_INCREMENTED
from ...project import PipelineProgressReporter, Project, RunRequest
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation

from prometheus_client import start_http_server, Summary, REGISTRY

STATS_TABLE_COLS = ["Statistic", "Value"]


class RunPipeline(Operation):
    def __init__(self, project: Project) -> None:
        self.project = project
        self.metric_summaries = {}

    async def perform(self, command: NodestreamCommand):
        self.init_prometheus_server_if_needed(command)
        await self.project.run(self.make_run_request(command))

    def init_prometheus_server_if_needed(self, command: NodestreamCommand):
        if not command.option("prometheus"):
            return

        port = int(command.option("prometheus-server-port"))
        addr = command.option("prometheus-server-addr")
        start_http_server(port, addr)

        @listen(STAT_INCREMENTED)
        def _(metric_name, increment):
            if metric_name not in self.metric_summaries:
                name = metric_name.replace(" ", "_").lower()
                self.metric_summaries[metric_name] = Summary(name, metric_name)

            self.metric_summaries[metric_name].observe(increment)

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
