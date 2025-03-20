import re

from jinja2 import Environment

from ..state import Adjacency, Cardinality, GraphObjectSchema, PropertyType, Schema
from .schema_printer import SchemaPrinter

NODE_TYPE_DEF_TEMPLATE = """
{% set node_type = ensure_camel_case(shape.name) %}
type {{ node_type }} @exclude(operations: [CREATE, DELETE, UPDATE]) @queryOptions(limit: {default: 10}) {% if node_type != shape.name %} @node(labels: ["{{ shape.name }}"]) {% endif %} {
    # Node Properties
{% for property in shape.properties.items() %}
    {{ property[0] }}: {{ field_mappings[property[1].type] }}
{% endfor %}
    # Inbound Relationships
{% for rel in inbound_relationships %}
    {% if rel[1].to_side_cardinality == Cardinality.MANY %}
    {{ in_relationship_to_camel(rel[0].relationship_type, rel[0].from_node_type) }}: [{{ ensure_camel_case(rel[0].from_node_type) }}!]!
        @relationship(type: "{{ rel[0].relationship_type }}", direction: IN, properties: "{{ unique_rel_type_name(rel[0]) }}")
    {% else %}
    {{ in_relationship_to_camel(rel[0].relationship_type, rel[0].from_node_type) }}: {{ ensure_camel_case(rel[0].from_node_type) }}
        @relationship(type: "{{ rel[0].relationship_type }}", direction: IN, properties: "{{ unique_rel_type_name(rel[0]) }}")
    {% endif %}
{% endfor %}
    # Outbound Relationships
{% for rel in outbound_relationships %}
    {% if rel[1].from_side_cardinality == Cardinality.MANY %}
    {{ out_relationship_to_camel(rel[0].relationship_type, rel[0].to_node_type) }}: [{{ensure_camel_case( rel[0].to_node_type) }}!]!
        @relationship(type: "{{ rel[0].relationship_type }}", direction: OUT, properties: "{{ unique_rel_type_name(rel[0]) }}")
    {% else %}
    {{ out_relationship_to_camel(rel[0].relationship_type, rel[0].to_node_type) }}: {{ensure_camel_case( rel[0].to_node_type) }}
        @relationship(type: "{{ rel[0].relationship_type }}", direction: OUT, properties: "{{ unique_rel_type_name(rel[0]) }}")
    {% endif %}
{% endfor %}
}
"""

RELATIONSHIP_DEF_TEMPLATE = """
interface {{ unique_rel_type_name_from_shape(rel) }} @relationshipProperties {
{% for property in rel.properties.items() %}
    {{ property[0] }}: {{ field_mappings[property[1].type] }}
{% endfor %}
}
"""


SNAKE_CASE_REGEX = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")

PROPERTY_TYPE_TO_GRAPQHL_TYPES = {
    PropertyType.STRING: "String",
    PropertyType.DATETIME: "DateTime",
    PropertyType.INTEGER: "BigInt",
    PropertyType.BOOLEAN: "Boolean",
    PropertyType.FLOAT: "Float",
    PropertyType.LIST_OF_BOOLEANS: "[Boolean]",
    PropertyType.LIST_OF_STRINGS: "[String]",
    PropertyType.LIST_OF_INTEGERS: "[BigInt]",
    PropertyType.LIST_OF_FLOATS: "[Float]",
    PropertyType.LIST_OF_DATETIMES: "[DateTime]",
}


def unique_rel_type_name_from_shape(rel: GraphObjectSchema):
    return "".join(p.title() for p in rel.name.lower().split("_"))


def unique_rel_type_name(rel: Adjacency):
    rel_type = ensure_camel_case(rel.relationship_type)
    return "".join(p.title() for p in rel_type.lower().split("_"))


def ensure_camel_case(possible_snake_case: str):
    if SNAKE_CASE_REGEX.match(possible_snake_case):
        splits = possible_snake_case.split("_")
        return "".join(t.title() for t in splits)
    return possible_snake_case


def in_relationship_to_camel(relationship: str, other_node: str):
    splits = relationship.lower().split("_")
    other_node = ensure_camel_case(other_node)
    return other_node[0].lower() + other_node[1:] + "".join((t.title() for t in splits))


def out_relationship_to_camel(relationship: str, other_node: str):
    splits = relationship.lower().split("_")
    other_node = ensure_camel_case(other_node)
    return splits[0] + "".join((t.title() for t in splits[1:])) + other_node


class GraphQLSchemaPrinter(SchemaPrinter, alias="graphql"):
    def __init__(self) -> None:
        environment = Environment()
        environment.globals.update(
            {
                item.__name__: item
                for item in (
                    Cardinality,
                    in_relationship_to_camel,
                    out_relationship_to_camel,
                    ensure_camel_case,
                    unique_rel_type_name,
                    unique_rel_type_name_from_shape,
                )
            }
        )

        self.node_template = environment.from_string(NODE_TYPE_DEF_TEMPLATE)
        self.rel_template = environment.from_string(RELATIONSHIP_DEF_TEMPLATE)

    def render_node_schema(self, schema: Schema) -> str:
        representation = ""
        for node_shape in sorted(schema.nodes, key=lambda n: n.name):
            all_inbound_rels = [
                (adj, card)
                for adj, card in schema.cardinalities.items()
                if adj.to_node_type == node_shape.name
            ]
            all_outbound_rels = [
                (adj, card)
                for adj, card in schema.cardinalities.items()
                if adj.from_node_type == node_shape.name
            ]
            representation += self.node_template.render(
                shape=node_shape,
                field_mappings=PROPERTY_TYPE_TO_GRAPQHL_TYPES,
                inbound_relationships=all_inbound_rels,
                outbound_relationships=all_outbound_rels,
            )
        return representation

    def render_relationship_schema(self, schema: Schema) -> str:
        return "".join(
            self.rel_template.render(
                rel=rel, field_mappings=PROPERTY_TYPE_TO_GRAPQHL_TYPES
            )
            for rel in schema.relationships
        )

    def print_schema_to_string(self, schema: Schema) -> str:
        representation = self.render_node_schema(schema)
        representation += self.render_relationship_schema(schema)
        return representation + "\n"
