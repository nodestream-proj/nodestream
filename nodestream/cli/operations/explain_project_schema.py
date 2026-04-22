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
        """Yield (scope_name, pipeline) pairs matching the requested scope.

        When ``self.scope`` is ``None``, all scopes are considered; otherwise we
        only consider the named scope (if present).
        """
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
        """Return pipelines filtered by node/relationship provenance.

        The filtering honours the optional ``node_type_name`` and
        ``relationship_type_name`` attributes, and when both are provided it also
        enforces that the requested node type is an endpoint of the requested
        relationship type in the expanded schema.
        """
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
