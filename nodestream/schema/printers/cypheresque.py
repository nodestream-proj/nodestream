from ..state import Adjacency, GraphObjectSchema, Schema
from .schema_printer import SchemaPrinter

OUTPUT_FORMAT = """Node Types:
{node_types}
Relationship Types:
{relationship_types}
Adjancies:
{adjancies}
"""

TYPE_FORMAT = "{name}: {properties}"
PROPERTY_FORMAT = "{name}: {type}"
ADJACENCY_FORMAT = "(:{from_node_type})-[:{relationship_type}]->(:{to_node_type})"


class CypherEsquePrinter(SchemaPrinter, alias="cypheresque"):
    def print_type_schema(self, node_type: GraphObjectSchema) -> str:
        formatted_properties = (
            PROPERTY_FORMAT.format(name=prop, type=meta.type.value)
            for prop, meta in node_type.properties.items()
        )

        return TYPE_FORMAT.format(
            name=node_type.name, properties=", ".join(formatted_properties)
        )

    def join_line_seperated(self, lines: list) -> str:
        return "\n".join(lines)

    def print_adjanceny(self, adjancency: Adjacency) -> str:
        return ADJACENCY_FORMAT.format(
            from_node_type=adjancency.from_node_type,
            relationship_type=adjancency.relationship_type,
            to_node_type=adjancency.to_node_type,
        )

    def print_node_types(self, schema: Schema) -> str:
        return self.join_line_seperated(
            self.print_type_schema(node_type) for node_type in schema.nodes
        )

    def print_relationship_types(self, schema: Schema) -> str:
        return self.join_line_seperated(
            self.print_type_schema(relationship_type)
            for relationship_type in schema.relationships
        )

    def print_adjancies(self, schema: Schema) -> str:
        return self.join_line_seperated(
            self.print_adjanceny(adjancency) for adjancency in schema.adjacencies
        )

    def print_schema_to_string(self, schema: Schema) -> str:
        return OUTPUT_FORMAT.format(
            node_types=self.print_node_types(schema),
            relationship_types=self.print_relationship_types(schema),
            adjancies=self.print_adjancies(schema),
        )
