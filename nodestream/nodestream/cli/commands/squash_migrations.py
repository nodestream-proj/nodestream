from cleo.helpers import option

from ..operations import GenerateSquashedMigration
from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION


class SquashMigration(NodestreamCommand):
    name = "migrations squash"
    description = "Generate a migration for the current project."
    options = [
        PROJECT_FILE_OPTION,
        option(
            "from",
            description="The name of the migration to squash from.",
            value_required=True,
            flag=False,
        ),
        option("to", description="The name of the migration to squash to.", flag=False),
    ]

    async def handle_async(self):
        from_migration_name = self.option("from")
        to_migration_name = self.option("to")
        migrations = self.get_migrations()
        operation = GenerateSquashedMigration(
            migrations,
            from_migration_name,
            to_migration_name,
        )
        await self.run_operation(operation)
