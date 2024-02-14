from collections import defaultdict

from ..state import Schema
from .schema_printer import SchemaPrinter


class LargeLanguageModelSchemaPrinter(SchemaPrinter, alias="genaillm"):
    def return_nodes_props(self, schema: Schema):
        return {
            node_shape.name: list(node_shape.properties.keys())
            for node_shape in schema.nodes
        }

    def return_rels_props(self, schema: Schema):
        return {
            rel_shape.name: list(rel_shape.properties.keys())
            for rel_shape in schema.relationships
        }

    def return_rels(self, schema: Schema):
        rels = defaultdict(list)
        for elem in schema.adjacencies:
            from_node = elem.from_node_type
            to_node = elem.to_node_type
            rel_name = elem.relationship_type
            rels[str(from_node)] = defaultdict(list)
            rels[str(from_node)][str(rel_name)].append(str(to_node))
        return rels

    def print_schema_to_string(self, schema: Schema) -> str:
        representation = str(self.return_nodes_props(schema))
        representation += ". "
        representation += str(self.return_rels_props(schema))
        representation += ". "
        representation += str(self.return_rels(schema))
        return representation
