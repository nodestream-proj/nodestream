from .operation import Operation, NodestreamCommand
from ...project import Project


class ExecuteMigrations(Operation):
    def __init__(self, project: Project, target: str):
        self.project = project
        self.target = target

    async def perform(self, command: NodestreamCommand):
        async with command.spin(
            f"Executing migrations on target {self.target}...",
            f"Migrations executed on target {self.target}.",
        ):
            async for migration in self.project.migrate_target(self.target):
                command.info(f"Migration {migration.name} executed successfully.")
