from ..operations import InitializeProject, ShowPipelines
from .nodestream_command import NodestreamCommand
from .shared_options import JSON_OPTION, PROJECT_FILE_OPTION, SCOPE_NAME_OPTION


class Show(NodestreamCommand):
    name = "show"
    description = "Show the Pipelines in your Nodestream Project"
    options = [PROJECT_FILE_OPTION, SCOPE_NAME_OPTION, JSON_OPTION]

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        use_json = self.has_json_logging_set
        await self.run_operation(ShowPipelines(project, self.scope, use_json))
