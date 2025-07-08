from logging import getLogger
from typing import Iterable, Optional

from yaml import safe_dump

from ...metrics import Metrics
from ...pipeline import PipelineInitializationArguments, PipelineProgressReporter
from ...project import Project, RunRequest
from ...project.pipeline_definition import PipelineDefinition
from ...utils import StringSuggester
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation

STATS_TABLE_COLS = ["Statistic", "Value"]

ERROR_NO_PIPELINES_FOUND = "<error>No pipelines with the provided name were found in your project. If you didn't provide a name, you have no pipelines.</error>"
HINT_CHECK_PIPELINE_NAME = "<info>HINT: Check that the pipelines you are trying to run are named correctly and in the registry.</info>"
HINT_USE_NODESTREAM_SHOW = "<info>HINT: You can view your project's pipelines by running 'nodestream show'. </info>"
WARNING_NO_TARGETS_PROVIDED = "<error>No targets provided. Running pipeline without writing to any targets.</error>"


class RunPipeline(Operation):
    def __init__(self, project: Project) -> None:
        self.project = project

    def get_pipeline_to_run_or_suggest_to_user(
        self, command: NodestreamCommand, pipeline_name: str
    ) -> Optional[PipelineDefinition]:
        if pipeline_definition := self.project.get_pipeline_by_name(pipeline_name):
            return pipeline_definition

        all_names = {ppl.name for ppl in self.project.get_all_pipelines()}
        suggester = StringSuggester(all_names)
        suggested_name = suggester.suggest_closest(pipeline_name)
        command.line(
            f"<error>No pipeline with the name '{pipeline_name}' found in your project</error>"
        )
        command.line(f"<info>HINT: Did you mean '{suggested_name}'?</info>")

    def get_pipelines_to_run(
        self, command: NodestreamCommand
    ) -> Iterable[PipelineDefinition]:
        supplied_commands = command.argument("pipelines")
        if supplied_commands:
            return [
                ppl
                for name in supplied_commands
                if (ppl := self.get_pipeline_to_run_or_suggest_to_user(command, name))
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
        effective_target_names = from_cli.union(from_pipeline)
        if not effective_target_names:
            command.line(WARNING_NO_TARGETS_PROVIDED)
        return effective_target_names

    def make_run_request(
        self, command: NodestreamCommand, pipeline: PipelineDefinition
    ) -> RunRequest:
        def print_effective_config(config):
            if command.is_very_verbose:
                command.line("<info>Effective configuration:</info>")
                command.line(f"<info>{safe_dump(config)}</info>")

        storage_name = command.option("storage-backend")
        object_store = self.project.get_object_storage_by_name(storage_name)

        return RunRequest(
            pipeline_name=pipeline.name,
            initialization_arguments=PipelineInitializationArguments(
                annotations=command.option("annotations"),
                step_outbox_size=int(command.option("step-outbox-size")),
                on_effective_configuration_resolved=print_effective_config,
                extra_steps=list(
                    self.get_writer_steps_for_specified_targets(command, pipeline)
                ),
                object_store=object_store,
            ),
            progress_reporter=self.create_progress_reporter(command, pipeline.name),
        )

    def get_progress_indicator(
        self, command: NodestreamCommand, pipeline_name: str
    ) -> "ProgressIndicator":
        if command.has_json_logging_set:
            return JsonProgressIndicator(command, pipeline_name)

        return SpinnerProgressIndicator(command, pipeline_name)

    def create_progress_reporter(
        self, command: NodestreamCommand, pipeline_name: str
    ) -> PipelineProgressReporter:
        indicator = self.get_progress_indicator(command, pipeline_name)
        metrics_interval_in_seconds = (
            float(command.option("metrics-interval-in-seconds"))
            if command.option("metrics-interval-in-seconds")
            else None
        )
        return PipelineProgressReporter(
            reporting_frequency=int(command.option("reporting-frequency")),
            metrics_interval_in_seconds=metrics_interval_in_seconds,
            callback=indicator.progress_callback,
            on_start_callback=indicator.on_start,
            on_finish_callback=indicator.on_finish,
            on_fatal_error_callback=indicator.on_fatal_error,
        )


class ProgressIndicator:
    def __init__(self, command: NodestreamCommand, pipeline_name: str) -> None:
        self.command = command
        self.pipeline_name = pipeline_name

    def on_start(self):
        pass

    def progress_callback(self, _, __):
        pass

    def on_finish(self, metrics: Metrics):
        pass

    def on_fatal_error(self, exception: Exception):
        self.command.line(
            "<error>Encountered a fatal error while running pipeline</error>"
        )
        self.command.line(f"<error>{exception}</error>")


class SpinnerProgressIndicator(ProgressIndicator):
    def __init__(self, command: NodestreamCommand, pipeline_name: str) -> None:
        super().__init__(command, pipeline_name)
        self.exception = None

    def on_start(self):
        self.progress = self.command.progress_indicator()
        self.progress.start(f"Running pipeline: '{self.pipeline_name}'")

    def progress_callback(self, index, metrics: Metrics):
        self.progress.set_message(
            f"Currently processing record at index: <info>{index}</info>"
        )
        metrics.tick()

    def on_finish(self, metrics: Metrics):
        self.progress.finish(f"Finished running pipeline: '{self.pipeline_name}'")
        metrics.tick()
        if self.exception:
            raise self.exception

    def on_fatal_error(self, exception: Exception):
        self.progress.set_message(
            "<error>Encountered a fatal error while running pipeline</error>"
        )
        self.exception = exception


class JsonProgressIndicator(ProgressIndicator):
    def __init__(self, command: NodestreamCommand, pipeline_name: str) -> None:
        super().__init__(command, pipeline_name)
        self.logger = getLogger()
        self.exception = None

    def on_start(self):
        self.logger.info("Starting Pipeline")

    def progress_callback(self, index, metrics: Metrics):
        self.logger.info("Processing Record", extra={"index": index})
        metrics.tick()

    def on_finish(self, metrics: Metrics):
        self.logger.info("Pipeline Completed")
        metrics.tick()
        if self.exception:
            raise self.exception

    def on_fatal_error(self, exception: Exception):
        self.logger.error("Pipeline Failed", exc_info=exception)
        self.exception = exception
