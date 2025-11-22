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
            scopes = [self.project.scopes_by_name.get(self.scope)]
        else:
            scopes = self.project.scopes_by_name.values()

        for scope in scopes:
            if scope is None:
                continue

            for pipeline in scope.pipelines_by_name.values():
                yield scope.name, pipeline

    def _get_filtered_pipelines(self) -> Iterable[Tuple[str, PipelineDefinition]]:
        matching = list(self._get_matching_pipelines())

        if not self.node_type_name and not self.relationship_type_name:
            return matching

        if not matching:
            return []

        node_pipelines = None
        relationship_pipelines = None

        if self.node_type_name:
            node_pipelines = set(
                self.project.explain_node_type(
                    self.node_type_name,
                    scope_name=self.scope,
                )
            )

        if self.relationship_type_name:
            relationship_pipelines = set(
                self.project.explain_relationship_type(
                    self.relationship_type_name,
                    scope_name=self.scope,
                )
            )

        # If both a node and relationship type are provided, additionally ensure
        # that the requested node type is actually one of the endpoints of the
        # requested relationship type in the expanded schema. This uses
        # relationship adjacencies derived from pipeline interpretations such as
        # ``source_node`` and ``relationship``.
        endpoint_node_types = None
        if self.node_type_name and self.relationship_type_name:
            adjacencies = self.project.explain_relationship_adjacencies(
                self.relationship_type_name,
                scope_name=self.scope,
            )
            endpoint_node_types = {
                node_type
                for adjacency in adjacencies
                for node_type in (adjacency.from_node_type, adjacency.to_node_type)
            }

            if self.node_type_name not in endpoint_node_types:
                # The requested node type is not connected by this relationship in
                # the schema, so there can be no matching pipelines.
                return []

        def _include(scope_name: str, pipeline: PipelineDefinition) -> bool:
            name = pipeline.name

            if node_pipelines is not None and name not in node_pipelines:
                return False

            if (
                relationship_pipelines is not None
                and name not in relationship_pipelines
            ):
                return False

            return True

        return [
            (scope_name, pipeline)
            for scope_name, pipeline in matching
            if _include(scope_name, pipeline)
        ]

    async def perform(self, command: NodestreamCommand):
        matching = list(self._get_filtered_pipelines())

        if not matching:
            scope_fragment = (
                f" in scope '{self.scope}'" if self.scope else " across all scopes"
            )

            if self.node_type_name and self.relationship_type_name:
                command.line(
                    "No pipelines found that contribute to both "
                    f"node type '{self.node_type_name}' and "
                    f"relationship type '{self.relationship_type_name}'"
                    + scope_fragment
                    + "."
                )
            elif self.node_type_name:
                command.line(
                    f"No pipelines found for node type '{self.node_type_name}'"
                    + scope_fragment
                    + "."
                )
            elif self.relationship_type_name:
                command.line(
                    "No pipelines found for relationship type "
                    f"'{self.relationship_type_name}'" + scope_fragment + "."
                )
            else:
                command.line("No pipelines found." + scope_fragment + ".")

            return

        headers_context = []
        if self.node_type_name:
            headers_context.append(f"node type '{self.node_type_name}'")
        if self.relationship_type_name:
            headers_context.append(f"relationship type '{self.relationship_type_name}'")

        if headers_context:
            types_fragment = " and ".join(headers_context)
        else:
            types_fragment = "all types"

        scope_fragment = (
            f" in scope '{self.scope}'" if self.scope else " across all scopes"
        )
        command.line(f"Pipelines contributing to {types_fragment}{scope_fragment}:")

        TableOutputFormat(command).output(matching)
