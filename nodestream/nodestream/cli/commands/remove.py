from cleo.helpers import argument

from ..operations import (
    CommitProjectToDisk,
    InitializeProject,
    RemovePipelineFromProject,
)
from .nodestream_command import NodestreamCommand
from .shared_options import PIPELINE_ARGUMENT, PROJECT_FILE_OPTION


class Remove(NodestreamCommand):
    name = "remove"
    description = "Remove a Pipeline from Your Nodestream Project"
    arguments = [
        argument("scope", "the name of the scope the pipeline is in"),
        PIPELINE_ARGUMENT,
    ]
    options = [PROJECT_FILE_OPTION]

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        remove = RemovePipelineFromProject(
            project, self.argument("scope"), self.argument("pipeline")
        )
        await self.run_operation(remove)
        await self.run_operation(CommitProjectToDisk(project, self.get_project_path()))
