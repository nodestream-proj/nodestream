from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION
from ..operations import InitializeProject, GenerateMigration


class MakeMigration(NodestreamCommand):
    name = "migrations make"
    description = "Generate a migration for the current project."
    options = [PROJECT_FILE_OPTION]

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        await self.run_operation(GenerateMigration(project))
