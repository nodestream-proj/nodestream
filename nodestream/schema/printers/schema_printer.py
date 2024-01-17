from pathlib import Path

from ...pluggable import Pluggable
from ...subclass_registry import SubclassRegistry
from ..state import Schema

SCHEMA_PRINTER_SUBCLASS_REGISTRY = SubclassRegistry()


@SCHEMA_PRINTER_SUBCLASS_REGISTRY.connect_baseclass
class SchemaPrinter(Pluggable):
    entrypoint_name = "schema_printers"

    def print_schema_to_file(self, schema: Schema, file_path: Path):
        with file_path.open("w") as f:
            f.write(self.print_schema_to_string(schema))

    def print_schema_to_stdout(self, schema: Schema, print_fn=print):
        print_fn(self.print_schema_to_string(schema))

    def print_schema_to_string(self, schema: Schema) -> str:
        return str(schema)

    @classmethod
    def from_name(cls, name: str) -> "SchemaPrinter":
        return SCHEMA_PRINTER_SUBCLASS_REGISTRY.get(name)()
