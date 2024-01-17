from .operation import Operation, NodestreamCommand
from ...project import Project
from ...schema.migrations import MigratorInput


class CleoMigrationInput(MigratorInput):
    def __init__(self, command: NodestreamCommand):
        self.command = command

    def ask_yes_no(self, question: str) -> bool:
        return self.command.confirm(question, default=False)


class GenerateMigration(Operation):
    def __init__(self, project: Project):
        self.project = project

    async def perform(self, command: NodestreamCommand):
        input = CleoMigrationInput(command)
        result = await self.project.generate_migration_from_changes(input)
        if result is None:
            command.line("No changes to apply")
        else:
            migration, path = result
            num_changes = len(migration.operations)
            command.line(f"Generated migration {migration.name} at {path}")
            command.line("Run `nodestream migrations run` to apply the migration.")
            command.line(f"The migration contains {num_changes} schema changes.")
