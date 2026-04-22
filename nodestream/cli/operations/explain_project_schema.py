from typing import Iterable, Optional, Tuple

from ...project import PipelineDefinition, Project
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation
from .show_pipelines import TableOutputFormat


class ExplainProjectSchema(Operation):
    def __init__(
        self,
        project: Project,
        node_type_name: Optional[str] = None,
        relationship_type_name: Optional[str] = None,
        scope: Optional[str] = None,
    ) -> None:
        self.project = project
        self.node_type_name = node_type_name
        self.relationship_type_name = relationship_type_name
        self.scope = scope

    def _get_matching_pipelines(self) -> Iterable[Tuple[str, PipelineDefinition]]:
        if self.scope:
            scope = self.project.scopes_by_name.get(self.scope)
            scopes = [scope] if scope else []
        else:
            scopes = self.project.scopes_by_name.values()

        for scope in scopes:
            for pipeline in scope.pipelines_by_name.values():
                yield scope.name, pipeline

    def _get_filtered_pipelines(self) -> Iterable[Tuple[str, PipelineDefinition]]:
        matching = list(self._get_matching_pipelines())

        if not self.node_type_name and not self.relationship_type_name:
            return matching

        node_pipelines = (
            set(self.project.explain_node_type(self.node_type_name, scope_name=self.scope))
            if self.node_type_name
            else None
        )
        relationship_pipelines = (
            set(self.project.explain_relationship_type(self.relationship_type_name, scope_name=self.scope))
            if self.relationship_type_name
            else None
        )

        if self.node_type_name and self.relationship_type_name:
            adjacencies = self.project.explain_relationship_adjacencies(
                self.relationship_type_name, scope_name=self.scope
            )
            endpoint_node_types = {
                node_type
                for adjacency in adjacencies
                for node_type in (adjacency.from_node_type, adjacency.to_node_type)
            }
            if self.node_type_name not in endpoint_node_types:
                return []

        return [
            (scope_name, pipeline)
            for scope_name, pipeline in matching
            if (node_pipelines is None or pipeline.name in node_pipelines)
            and (relationship_pipelines is None or pipeline.name in relationship_pipelines)
        ]

    def _scope_fragment(self) -> str:
        """Return a human-friendly clause describing the active scope filter."""
        return f" in scope '{self.scope}'" if self.scope else " across all scopes"

    def _no_pipelines_message(self) -> str:
        """Return the message to use when no matching pipelines are found."""
        scope_fragment = self._scope_fragment()

        if self.node_type_name and self.relationship_type_name:
            return (
                "No pipelines found that contribute to both "
                f"node type '{self.node_type_name}' and "
                f"relationship type '{self.relationship_type_name}'"
                + scope_fragment
                + "."
            )

        if self.node_type_name:
            return (
                f"No pipelines found for node type '{self.node_type_name}'"
                + scope_fragment
                + "."
            )

        if self.relationship_type_name:
            return (
                "No pipelines found for relationship type "
                f"'{self.relationship_type_name}'" + scope_fragment + "."
            )

        return "No pipelines found." + scope_fragment + "."

    def _header_message(self) -> str:
        """Return the header line describing what the table will show."""
        headers_context = []
        if self.node_type_name:
            headers_context.append(f"node type '{self.node_type_name}'")
        if self.relationship_type_name:
            headers_context.append(f"relationship type '{self.relationship_type_name}'")

        if headers_context:
            types_fragment = " and ".join(headers_context)
        else:
            types_fragment = "all types"

        scope_fragment = self._scope_fragment()
        return f"Pipelines contributing to {types_fragment}{scope_fragment}:"

    async def perform(self, command: NodestreamCommand):
        matching = list(self._get_filtered_pipelines())

        if not matching:
            command.line(self._no_pipelines_message())
            return

        command.line(self._header_message())
        TableOutputFormat(command).output(matching)
