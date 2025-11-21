from typing import ClassVar

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
        ),
        argument("name", "The node or relationship type name."),
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
    ]

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        kind = self.argument("kind")
        type_name = self.argument("name")
        scope = self.option("scope")

        if kind not in {"node", "relationship"}:
            self.line_error("Kind must be either 'node' or 'relationship'.")
            return 1

        await self.run_operation(
            ExplainProjectSchema(
                project=project,
                kind=kind,
                type_name=type_name,
                scope=scope,
            )
        )
        return 0
