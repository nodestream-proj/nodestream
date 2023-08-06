from ...project import Project
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


class InitializeProject(Operation):
    async def perform(self, command: NodestreamCommand) -> Project:
        return command.get_project()
