from pathlib import Path

from ...model import GraphSchema
from ...subclass_registry import SubclassRegistry

SCHEMA_PRINTER_SUBCLASS_REGISTRY = SubclassRegistry()


@SCHEMA_PRINTER_SUBCLASS_REGISTRY.connect_baseclass
class SchemaPrinter:
    def print_schema_to_file(self, schema: GraphSchema, file_path: Path):
        with open(file_path, "w") as f:
            f.write(self.print_schema_to_string(schema))

    def print_schema_to_stdout(self, schema: GraphSchema, print_fn=print):
        print_fn(self.print_schema_to_string(schema))

    def print_schema_to_string(self, schema: GraphSchema) -> str:
        return str(schema)
