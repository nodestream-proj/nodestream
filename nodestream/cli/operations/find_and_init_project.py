from pathlib import Path

from cleo.commands.command import Command

from ...project import Project
from .operation import Operation


class FindAndInitializeProject(Operation):
    async def perform(self, command: Command) -> Project:
        project = self.get_project(command)
        project.ensure_modules_are_imported()
        return project

    def get_project(self, command: Command) -> Project:
        path = command.option("project")
        if path is not None:
            path = Path(path)
        return Project.from_file(path)
