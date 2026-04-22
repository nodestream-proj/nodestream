from typing import ClassVar, Optional

from cleo.helpers import argument, option

from ..operations import ExplainProjectSchema, InitializeProject
from .nodestream_command import NodestreamCommand
from .shared_options import PROJECT_FILE_OPTION


class ExplainSchema(NodestreamCommand):
    name = "explain schema"
    description = "Explain which pipelines contribute to a given graph type."
    arguments: ClassVar[list[argument]] = [
        argument(
            "kind",
            "The kind of type to explain ('node' or 'relationship').",
            optional=True,
        ),
        argument(
            "name",
            ("The node or relationship type name when using positional " "KIND/NAME."),
            optional=True,
        ),
    ]
    options = [
        PROJECT_FILE_OPTION,
        option(
            "scope",
            "s",
            "Limit the explanation to a single scope (defaults to all scopes).",
            flag=False,
            default=None,
        ),
        option(
            "node",
            description=(
                "Explain pipelines contributing to the given node type. "
                "May be combined with --relationship to show only pipelines "
                "that contribute to both."
            ),
            flag=False,
        ),
        option(
            "relationship",
            description=(
                "Explain pipelines contributing to the given relationship type. "
                "May be combined with --node to show only pipelines that "
                "contribute to both."
            ),
            flag=False,
        ),
    ]

    def _get_positional_kind_and_name(self) -> Optional[tuple[str, str]]:
        """Return (kind, name) when using the positional KIND/NAME interface.

        This mode requires both positional arguments to be provided and no
        --node/--relationship options to be set.
        """

        kind = self.argument("kind")
        type_name = self.argument("name")

        if kind is None or type_name is None:
            return None

        if self.option("node") is not None or self.option("relationship") is not None:
            return None

        return kind, type_name

    def _resolve_type_args(
        self,
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """Resolve node/relationship arguments from positional or options.

        Returns:
            A tuple of (node_type_name, relationship_type_name, error_message).
            If error_message is not None, the caller should report it and
            return a non-zero exit code.
        """

        kind_and_name = self._get_positional_kind_and_name()
        if kind_and_name is not None:
            kind, type_name = kind_and_name

            if kind not in {"node", "relationship"}:
                return (
                    None,
                    None,
                    "Kind must be either 'node' or 'relationship'.",
                )

            if kind == "node":
                return type_name, None, None

            return None, type_name, None

        node_type_name = self.option("node")
        relationship_type_name = self.option("relationship")

        if not node_type_name and not relationship_type_name:
            return (
                None,
                None,
                (
                    "You must specify either positional KIND/NAME or "
                    "at least one of --node/--relationship."
                ),
            )

        return node_type_name, relationship_type_name, None

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        scope = self.option("scope")

        node_type_name, relationship_type_name, error = self._resolve_type_args()
        if error is not None:
            self.line_error(error)
            return 1

        await self.run_operation(
            ExplainProjectSchema(
                project=project,
                node_type_name=node_type_name,
                relationship_type_name=relationship_type_name,
                scope=scope,
            )
        )
        return 0
