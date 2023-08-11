from collections import defaultdict

from ..schema import GraphSchema
from .schema_printer import SchemaPrinter


class LargeLanguageModelSchemaPrinter(SchemaPrinter, alias="genaillm"):
    def return_nodes_props(self, schema):
        return {
            str(node_shape.object_type): node_shape.property_names()
            for node_shape in schema.known_node_types()
        }

    def return_rels_props(self, schema):
        return {
            str(rel_shape.object_type): rel_shape.property_names()
            for rel_shape in schema.known_relationship_types()
        }

    def return_rels(self, schema):
        rels = defaultdict(list)
        for elem in schema.relationships:
            from_node = elem.from_object_type
            to_node = elem.to_object_type
            rel_name = elem.relationship_type
            rels[str(from_node)] = defaultdict(list)
            rels[str(from_node)][str(rel_name)].append(str(to_node))
        return rels

    def print_schema_to_string(self, schema: GraphSchema) -> str:
        representation = str(self.return_nodes_props(schema))
        representation += ". "
        representation += str(self.return_rels_props(schema))
        representation += ". "
        representation += str(self.return_rels(schema))
        return representation
