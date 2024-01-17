from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION, TARGETS_OPTION
from ..operations import InitializeProject, ExecuteMigrations


class RunMigrations(NodestreamCommand):
    name = "migrations run"
    description = "Execute pending migrations on the specified target."
    options = [PROJECT_FILE_OPTION, TARGETS_OPTION]

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        targets = self.option("target")
        for target in targets:
            await self.run_operation(ExecuteMigrations(project, target))
        else:
            self.info("No targets specified, nothing to do.")
