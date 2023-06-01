from pathlib import Path
from typing import Optional

from ...project import PipelineDefinition, PipelineScope, Project
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation

DEFAULT_SCOPE_NAME = "default"


class AddPipelineToProject(Operation):
    def __init__(
        self, project: Project, pipeline_path: Path, scope_name: Optional[str] = None
    ) -> None:
        self.project = project
        self.pipeline_path = pipeline_path
        self.scope_name = scope_name or DEFAULT_SCOPE_NAME

    async def perform(self, _: NodestreamCommand):
        scope = self.get_scope()
        scope.add_pipeline_definition(PipelineDefinition.from_path(self.pipeline_path))

    def get_scope(self) -> PipelineScope:
        if self.scope_name in self.project.scopes_by_name:
            return self.project.scopes_by_name[self.scope_name]

        scope = PipelineScope(self.scope_name, [])
        self.project.add_scope(scope)
        return scope
