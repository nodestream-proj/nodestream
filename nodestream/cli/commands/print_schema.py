from typing import ClassVar

from cleo.helpers import argument, option

from ..operations import InitializeProject, PrintProjectSchema
from .nodestream_command import NodestreamCommand
from .shared_options import MANY_PIPELINES_ARGUMENT, PROJECT_FILE_OPTION


class PrintSchema(NodestreamCommand):
    name = "print schema"
    description = "Print the schema for the current project"
    arguments: ClassVar[list[argument]] = [MANY_PIPELINES_ARGUMENT]
    options = [
        PROJECT_FILE_OPTION,
        option(
            "format", "f", "Format to print the schema in", default="plain", flag=False
        ),
        option(
            "overrides",
            "O",
            "Path to a file containing type overrides",
            default=None,
            flag=False,
        ),
        option(
            "out",
            "o",
            "Path to a file to write the schema to",
            default=None,
            flag=False,
        ),
    ]

    async def handle_async(self):
        project = await self.run_operation(InitializeProject())
        await self.run_operation(
            PrintProjectSchema(
                project=project,
                format_string=self.option("format"),
                type_overrides_file=self.option("overrides"),
                output_file=self.option("out"),
                pipeline_names=self.argument("pipelines"),
            )
        )
