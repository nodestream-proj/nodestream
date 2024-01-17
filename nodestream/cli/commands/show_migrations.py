from typing import List

from ...project import Project
from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION, TARGETS_OPTION
from ..operations import InitializeProject, GenerateMigration


class ShowMigration(NodestreamCommand):
    name = "migration show"
    description = (
        "List all migrations for the current project and their state on each target."
    )
    options = [PROJECT_FILE_OPTION, TARGETS_OPTION]

    def get_target_names(self, project: Project) -> List[str]:
        return self.option("target") or project.targets.keys()

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        targets = self.get_target_names(project)
