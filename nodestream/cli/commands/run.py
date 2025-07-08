from cleo.helpers import option

from ...metrics import Metrics
from ..operations import (
    ExecuteMigrations,
    InitializeLogger,
    InitializeMetricsHandler,
    InitializeProject,
    RunPipeline,
)
from .nodestream_command import NodestreamCommand
from .shared_options import (
    JSON_OPTION,
    MANY_PIPELINES_ARGUMENT,
    PROJECT_FILE_OPTION,
    PROMETHEUS_OPTIONS,
    TARGETS_OPTION,
)


class Run(NodestreamCommand):
    name = "run"
    description = "run a pipeline in the current project"
    arguments = [MANY_PIPELINES_ARGUMENT]
    options = [
        PROJECT_FILE_OPTION,
        JSON_OPTION,
        TARGETS_OPTION,
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
        option(
            "step-outbox-size",
            "s",
            "How many records to buffer in each step's outbox before blocking",
            default=1000,
            flag=False,
        ),
        option(
            "auto-migrate",
            description="Ensure all specified targets are migrated before running specified pipelines",
            flag=True,
        ),
        option(
            "storage-backend",
            description="Storage backend to use for checkpointing",
            flag=False,
        ),
        option(
            "metrics-interval-in-seconds",
            description="Time interval to report metrics in seconds",
            default=None,
            flag=False,
        ),
        *PROMETHEUS_OPTIONS,
    ]

    async def auto_migrate_targets_if_needed(self, project):
        if not self.option("auto-migrate"):
            return

        targets = self.option("targets")
        migrations = self.get_migrations()
        for target_name in targets:
            target = project.get_target_by_name(target_name)
            await self.run_operation(ExecuteMigrations(migrations, target))

    async def handle_async(self):
        with Metrics.capture():
            await self.run_operation(InitializeLogger())
            await self.run_operation(InitializeMetricsHandler())
            project = await self.run_operation(InitializeProject())
            await self.auto_migrate_targets_if_needed(project)
            await self.run_operation(RunPipeline(project))
