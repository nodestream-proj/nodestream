from ...project import Project
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


class InitializeProject(Operation):
    async def perform(self, command: NodestreamCommand) -> Project:
        project = command.get_project()
        project.ensure_modules_are_imported()
        return project
