from typing import Optional

from ...project import Project
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


class ExplainProjectSchema(Operation):
    def __init__(
        self,
        project: Project,
        kind: str,
        type_name: str,
        scope: Optional[str] = None,
    ) -> None:
        self.project = project
        self.kind = kind
        self.type_name = type_name
        self.scope = scope

    async def perform(self, command: NodestreamCommand):
        if self.kind == "node":
            pipelines = self.project.explain_node_type(
                self.type_name,
                scope_name=self.scope,
            )
        else:
            pipelines = self.project.explain_relationship_type(
                self.type_name,
                scope_name=self.scope,
            )

        if not pipelines:
            scope_fragment = f" in scope '{self.scope}'" if self.scope else ""
            command.line(
                f"No pipelines found for {self.kind} type '{self.type_name}'"
                f"{scope_fragment}."
            )
            return

        scope_fragment = (
            f" in scope '{self.scope}'" if self.scope else " across all scopes"
        )
        command.line(
            f"Pipelines contributing to {self.kind} type '{self.type_name}'"
            f"{scope_fragment}:"
        )

        for name in pipelines:
            command.line(name)
