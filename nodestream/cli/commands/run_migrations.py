from ..operations import ExecuteMigrations
from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION, TARGETS_OPTION


class RunMigrations(NodestreamCommand):
    name = "migrations run"
    description = "Execute pending migrations on the specified target."
    options = [PROJECT_FILE_OPTION, TARGETS_OPTION]

    async def handle_async(self):
        project = self.get_project()
        migrations = self.get_migrations()
        targets = self.option("target")

        if len(targets) == 0:
            self.info("No targets specified, nothing to do.")
            return

        for target_name in targets:
            target = project.get_target_by_name(target_name)
            await self.run_operation(ExecuteMigrations(migrations, target))
