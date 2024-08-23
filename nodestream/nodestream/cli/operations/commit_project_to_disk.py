from pathlib import Path

from ...project import Project
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


class CommitProjectToDisk(Operation):
    def __init__(self, project: Project, project_path: Path) -> None:
        self.project = project
        self.project_path = project_path

    async def perform(self, _: NodestreamCommand):
        self.project.write_to_file(self.project_path)
