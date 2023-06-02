from ...project import Project
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


class RemovePipelineFromProject(Operation):
    def __init__(
        self,
        project: Project,
        scope_name: str,
        pipeline_name: str,
    ) -> None:
        self.project = project
        self.pipeline_name = pipeline_name
        self.scope_name = scope_name

    async def perform(self, _: NodestreamCommand):
        self.project.delete_pipeline(self.scope_name, self.pipeline_name)
