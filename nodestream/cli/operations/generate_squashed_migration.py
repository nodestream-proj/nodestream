from ...schema.migrations import ProjectMigrations
from .operation import NodestreamCommand, Operation


class GenerateSquashedMigration(Operation):
    def __init__(
        self,
        migrations: ProjectMigrations,
        from_migration_name: str,
        to_migration_name: str,
    ) -> None:
        self.migrations = migrations
        self.from_migration_name = from_migration_name
        self.to_migration_name = to_migration_name

    async def perform(self, command: NodestreamCommand):
        from_migration = self.migrations.graph.get_migration(self.from_migration_name)
        to_migration = (
            self.migrations.graph.get_migration(self.to_migration_name)
            if self.to_migration_name
            else None
        )
        migration, path = self.migrations.create_squash_between(
            from_migration, to_migration
        )
        command.line(f"Generated squashed migration {migration.name}.")
        command.line(
            f"The migration contains {len(migration.operations)} schema changes."
        )
        for operation in migration.operations:
            command.line(f"  - {operation.describe()}")
        command.line(f"Migration written to {path}")
        command.line("Run `nodestream migrations run` to apply the migration.")
