from typing import Iterable

from yaml import safe_dump

from ...pipeline import PipelineInitializationArguments, PipelineProgressReporter
from ...pipeline.meta import PipelineContext
from ...project import Project, RunRequest
from ...project.pipeline_definition import PipelineDefinition
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation

STATS_TABLE_COLS = ["Statistic", "Value"]

ERROR_NO_PIPELINES_FOUND = "<error>No pipelines with the provided name were found in your project. If you didn't provide a name, you have no pipelines.</error>"
HINT_CHECK_PIPELINE_NAME = "<info>HINT: Check that the pipelines you are trying to run are named correctly and in the registry.</info>"
HINT_USE_NODESTREAM_SHOW = "<info>HINT: You can view your project's pipelines by running 'nodestream show'. </info>"


class RunPipeline(Operation):
    def __init__(self, project: Project) -> None:
        self.project = project

    def get_pipelines_to_run(
        self, command: NodestreamCommand
    ) -> Iterable[PipelineDefinition]:
        supplied_commands = command.argument("pipelines")
        if supplied_commands:
            return [
                self.project.get_pipeline_by_name(name) for name in supplied_commands
            ]
        return self.project.get_all_pipelines()

    async def perform(self, command: NodestreamCommand):
        pipelines_ran = 0
        for pipeline in self.get_pipelines_to_run(command):
            request = self.make_run_request(command, pipeline)
            pipelines_ran += await self.project.run(request)

        if pipelines_ran == 0:
            command.line(ERROR_NO_PIPELINES_FOUND)
            command.line(HINT_CHECK_PIPELINE_NAME)
            command.line(HINT_USE_NODESTREAM_SHOW)

    def get_writer_steps_for_specified_targets(
        self, command: NodestreamCommand, pipeline: PipelineDefinition
    ):
        targets_names = self.combine_targets_from_command_and_pipeline(
            command, pipeline
        )
        for target_name in targets_names:
            target = self.project.get_target_by_name(target_name)
            if target:
                yield target.make_writer()
            else:
                command.line(
                    f"<error>Target '{target_name}' not found in project. Ignoring.</error>"
                )

    def combine_targets_from_command_and_pipeline(
        self, command: NodestreamCommand, pipeline: PipelineDefinition
    ):
        from_cli = set(command.option("target") or {})
        from_pipeline = set(pipeline.configuration.effective_targets or {})
        return from_cli.union(from_pipeline)

    def make_run_request(
        self, command: NodestreamCommand, pipeline: PipelineDefinition
    ) -> RunRequest:
        def print_effective_config(config):
            if command.is_very_verbose:
                command.line("<info>Effective configuration:</info>")
                command.line(f"<info>{safe_dump(config)}</info>")

        return RunRequest(
            pipeline_name=pipeline.name,
            initialization_arguments=PipelineInitializationArguments(
                annotations=command.option("annotations"),
                step_outbox_size=int(command.option("step-outbox-size")),
                on_effective_configuration_resolved=print_effective_config,
                extra_steps=list(
                    self.get_writer_steps_for_specified_targets(command, pipeline)
                ),
            ),
            progress_reporter=self.create_progress_reporter(command, pipeline.name),
        )

    def get_progress_indicator(
        self, command: NodestreamCommand, pipeline_name: str
    ) -> "ProgressIndicator":
        if command.has_json_logging_set:
            return ProgressIndicator(command, pipeline_name)

        return SpinnerProgressIndicator(command, pipeline_name)

    def create_progress_reporter(
        self, command: NodestreamCommand, pipeline_name: str
    ) -> PipelineProgressReporter:
        indicator = self.get_progress_indicator(command, pipeline_name)
        return PipelineProgressReporter(
            reporting_frequency=int(command.option("reporting-frequency")),
            callback=indicator.progress_callback,
            on_start_callback=indicator.on_start,
            on_finish_callback=indicator.on_finish,
        )


class ProgressIndicator:
    def __init__(self, command: NodestreamCommand, pipeline_name: str) -> None:
        self.command = command
        self.pipeline_name = pipeline_name

    def on_start(self):
        pass

    def progress_callback(self, _, __):
        pass

    def on_finish(self, context: PipelineContext):
        pass


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
