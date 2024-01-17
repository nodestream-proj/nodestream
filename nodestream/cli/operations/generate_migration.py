from .operation import Operation, NodestreamCommand
from ...schema import Schema
from ...schema.migrations import MigratorInput, ProjectMigrations


class CleoMigrationInput(MigratorInput):
    def __init__(self, command: NodestreamCommand):
        self.command = command

    def ask_yes_no(self, question: str) -> bool:
        return self.command.confirm(question, default=False)


class GenerateMigration(Operation):
    def __init__(self, migrations: ProjectMigrations, current_state: Schema):
        self.current_state = current_state
        self.migrations = migrations

    async def perform(self, command: NodestreamCommand):
        input = CleoMigrationInput(command)
        result = await self.migrations.generate_migration_from_changes(
            input, self.current_state
        )
        if result is None:
            command.line("No changes to migrate")
        else:
            migration, path = result
            num_changes = len(migration.operations)
            command.line(f"Generated migration {migration.name} at {path}")
            command.line("Run `nodestream migrations run` to apply the migration.")
            command.line(f"The migration contains {num_changes} schema changes.")
