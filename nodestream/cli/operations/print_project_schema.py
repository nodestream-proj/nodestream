from pathlib import Path
from typing import Optional

from ...project import Project
from ...schema.printers import SchemaPrinter
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


class PrintProjectSchema(Operation):
    def __init__(
        self,
        project: Project,
        format_string: str,
        type_overrides_file: Optional[str] = None,
        output_file: Optional[str] = None,
    ) -> None:
        self.project = project
        self.format_string = format_string
        self.output_file = output_file
        self.type_overrides_file = type_overrides_file

    async def perform(self, command: NodestreamCommand):
        type_overrides_file = (
            Path(self.type_overrides_file) if self.type_overrides_file else None
        )
        schema = self.project.get_schema(type_overrides_file=type_overrides_file)

        # Import all schema printers so that they can register themselves
        SchemaPrinter.import_all()
        printer = SchemaPrinter.from_name(self.format_string)
        if self.output_file:
            printer.print_schema_to_file(schema, Path(self.output_file))
        else:
            printer.print_schema_to_stdout(schema, print_fn=command.write)
