from typing import List

from cleo.helpers import option

from ...project import Project, Target
from ...schema import GraphObjectSchema
from ..operations import InitializeProject, RunCopy
from .nodestream_command import NodestreamCommand
from .shared_options import JSON_OPTION, PROJECT_FILE_OPTION


class UnknownTargetError(Exception):
    pass


class Copy(NodestreamCommand):
    name = "copy"
    description = "Copy Data from one target to another"
    options = [
        PROJECT_FILE_OPTION,
        JSON_OPTION,
        option("from", "f", "The target to copy from", flag=False),
        option("to", "t", "The target to copy to", flag=False),
        option("all", "a", "Copy all node and relationship types", flag=True),
        option(
            "node",
            description="Specify a node type to copy",
            flag=False,
            multiple=True,
        ),
        option(
            "relationship",
            description="Specify a relationship type to copy",
            flag=False,
            multiple=True,
        ),
    ]

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())

        try:
            from_target = self.get_taget_from_user(project, "from")
            to_target = self.get_taget_from_user(project, "to")
            schema = project.get_schema()
            all_node_types = schema.nodes
            all_rel_types = schema.relationships
            node_types = self.get_type_selection_from_user(all_node_types, "node")
            rel_types = self.get_type_selection_from_user(all_rel_types, "relationship")
        except UnknownTargetError:
            return

        self.line("Starting to Copy:")
        self.line(f"<info>From: {from_target.name}</info>")
        self.line(f"<info>To: {to_target.name}</info>")
        self.line(f"<info>Node Types: {', '.join(node_types)}</info>")
        self.line(f"<info>Relationship Types: {', '.join(rel_types)}</info>")
        await self.run_operation(
            RunCopy(from_target, to_target, project, node_types, rel_types)
        )

    def get_taget_from_user(self, project: Project, action: str) -> Target:
        # If the user has specified the target in the options, we don't need to prompt
        # them for anything. We can just use the target they specified.
        if (choice := self.option(action)) is None:
            prompt = f"Which target would you like to copy {action}?"
            choices = [t for t in project.targets_by_name.keys()]
            choice = self.choice(prompt, choices)

        # If the target they specified is unknown, we should error out.
        try:
            return project.get_target_by_name(choice)
        except ValueError:
            self.line_error(f"Unknown target: {choice}")
            raise UnknownTargetError

    def get_type_selection_from_user(
        self, types: List[GraphObjectSchema], type_name: str
    ) -> List[str]:
        choices = [str(t.name) for t in types]

        # If the user has specified the --all flag, we don't need to prompt them for
        # anything. We can just return all the types.
        if self.option("all"):
            return choices

        # If the user has specified type(s) in the options, we don't need to prompt
        # them for anything. We can just return the type they specified. If they
        # specified an unknown type, we should error out.
        selections_from_options = self.option(type_name)
        if selections_from_options:
            for selection in selections_from_options:
                if selection not in choices:
                    self.line_error(
                        f"Unknown {type_name} type: {selection}. "
                        f"Valid options are: {', '.join(choices)}"
                    )
                    raise UnknownTargetError
            return selections_from_options

        # If the user has not specified the type(s) in the options, we need to prompt
        # them for the type(s). We can just return the type they specified.
        return self.choice(
            f"Which {type_name} types would you like to copy? (You can select multiple by separating them with a comma)",
            choices,
            multiple=True,
        )
