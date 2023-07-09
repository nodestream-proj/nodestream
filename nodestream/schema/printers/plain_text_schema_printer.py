from .schema_printer import SchemaPrinter


class PlainTestSchemaPrinter(SchemaPrinter, alias="plain"):
    def print_schema_to_string(self, schema):
        schema_str = "\n".join(str(s) for s in schema.object_shapes if s.has_known_type)
        return f"{schema_str}\n"
