from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION
from ..operations import GenerateMigration
from ...schema.migrations import ProjectMigrations


class MakeMigration(NodestreamCommand):
    name = "migrations make"
    description = "Generate a migration for the current project."
    options = [PROJECT_FILE_OPTION]

    async def handle_async(self):
        project = self.get_project()
        schema = project.get_schema()
        migrations = ProjectMigrations.from_directory(self.get_migrations_path())
        await self.run_operation(GenerateMigration(migrations, schema))
