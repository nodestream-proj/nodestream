from cleo.helpers import option

from ..operations import GenerateMigration
from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION


class MakeMigration(NodestreamCommand):
    name = "migrations make"
    description = "Generate a migration for the current project."
    options = [
        PROJECT_FILE_OPTION,
        option(
            "dry-run",
            description="Don't generate the migration file, just output the detected changes.",
            flag=True,
        ),
    ]

    async def handle_async(self):
        project = self.get_project()
        schema = project.get_schema()
        migrations = self.get_migrations()
        dry_run = self.option("dry-run")
        await self.run_operation(GenerateMigration(migrations, schema, dry_run))
