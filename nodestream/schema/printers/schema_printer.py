from pathlib import Path

from ...pluggable import Pluggable
from ...subclass_registry import SubclassRegistry
from ..schema import GraphSchema

SCHEMA_PRINTER_SUBCLASS_REGISTRY = SubclassRegistry()


@SCHEMA_PRINTER_SUBCLASS_REGISTRY.connect_baseclass
class SchemaPrinter(Pluggable):
    entrypoint_name = "schema_printers"

    def print_schema_to_file(self, schema: GraphSchema, file_path: Path):
        with open(file_path, "w") as f:
            f.write(self.print_schema_to_string(schema))

    def print_schema_to_stdout(self, schema: GraphSchema, print_fn=print):
        print_fn(self.print_schema_to_string(schema))

    def print_schema_to_string(self, schema: GraphSchema) -> str:
        return str(schema)
