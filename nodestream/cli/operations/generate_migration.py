from ...schema import Schema
from ...schema.migrations import Migration, MigratorInput, ProjectMigrations
from .operation import NodestreamCommand, Operation


class CleoMigrationInput(MigratorInput):
    def __init__(self, command: NodestreamCommand):
        self.command = command

    def ask_yes_no(self, question: str) -> bool:
        return self.command.confirm(question, default=False)


class GenerateMigration(Operation):
    def __init__(
        self,
        migrations: ProjectMigrations,
        current_state: Schema,
        dry_run: bool = False,
    ):
        self.current_state = current_state
        self.migrations = migrations
        self.dry_run = dry_run

    def describe_migration(self, command: NodestreamCommand, migration: Migration):
        num_changes = len(migration.operations)
        command.line(f"Generated migration {migration.name}.")
        command.line(f"The migration contains {num_changes} schema changes.")

        for operation in migration.operations:
            command.line(f"  - {operation.describe()}")

    async def generate_migration(self, input: CleoMigrationInput):
        migration = path = None

        # If we are dry running, just get the changes as a migration.
        # If we are doing this for real, we can get the path and the migration.
        if self.dry_run:
            migration = await self.migrations.detect_changes(input, self.current_state)
        else:
            result = self.migrations.create_migration_from_changes(
                input, self.current_state
            )
            if (result := await result) is not None:
                migration, path = result

        return migration, path

    async def perform(self, command: NodestreamCommand):
        input = CleoMigrationInput(command)
        migration, path = await self.generate_migration(input)

        # Describe any changes that were made if they were made.
        schema_has_changes = migration is not None
        if schema_has_changes:
            self.describe_migration(command, migration)
        else:
            command.line("No changes to migrate")

        # Output the path if the migration was commmited to disk.
        if path is not None:
            command.line(f"Migration written to {path}")
            command.line("Run `nodestream migrations run` to apply the migration.")
