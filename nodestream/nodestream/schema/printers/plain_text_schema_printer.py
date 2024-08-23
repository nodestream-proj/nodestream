from ..state import Schema
from .schema_printer import SchemaPrinter


class PlainTestSchemaPrinter(SchemaPrinter, alias="plain"):
    def print_schema_to_string(self, schema: Schema):
        return f"{schema}\n"
