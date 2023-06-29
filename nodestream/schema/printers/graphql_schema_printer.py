import re

from jinja2 import Environment

from ..schema import (
    Cardinality,
    GraphObjectShape,
    GraphSchema,
    PresentRelationship,
    PropertyType,
)
from .schema_printer import SchemaPrinter

NODE_TYPE_DEF_TEMPLATE = """
{% set node_type = ensure_camel_case(shape.object_type.type) %}
type {{ node_type }} @exclude(operations: [CREATE, DELETE, UPDATE]) @pageOptions{limit: {default: 10}} {% if node_type != shape.object_type.type %} @node(labels: ["{{ shape.object_type.type }}"]) {% endif %} {
    # Node Properties
{% for property in shape.properties.properties.values() %}
    {{ property.name }}: {{ field_mappings[property.type] }}
{% endfor %}
    # Inbound Relationships
{% for rel in inbound_relationships %}
    {% if rel.to_side_cardinality == Cardinality.MANY %}
    {{ in_relationship_to_camel(rel.relationship_type, rel.from_object_type) }}: [{{ ensure_camel_case(rel.from_object_type.type) }}!]!
        @relationship(type: "{{ rel.relationship_type }}", direction: IN, properties: "{{ unique_rel_type_name(rel) }}")
    {% else %}
    {{ in_relationship_to_camel(rel.relationship_type, rel.from_object_type) }}: {{ ensure_camel_case(rel.from_object_type.type) }}
        @relationship(type: "{{ rel.relationship_type }}", direction: IN, properties: "{{ unique_rel_type_name(rel) }}")
    {% endif %}
{% endfor %}
    # Outbound Relationships
{% for rel in outbound_relationships %}
    {% if rel.from_side_cardinality == Cardinality.MANY %}
    {{ out_relationship_to_camel(rel.relationship_type, rel.to_object_type) }}: [{{ensure_camel_case( rel.to_object_type.type) }}!]!
        @relationship(type: "{{ rel.relationship_type }}", direction: OUT, properties: "{{ unique_rel_type_name(rel) }}")
    {% else %}
    {{ out_relationship_to_camel(rel.relationship_type, rel.to_object_type) }}: {{ensure_camel_case( rel.to_object_type.type) }}
        @relationship(type: "{{ rel.relationship_type }}", direction: OUT, properties: "{{ unique_rel_type_name(rel) }}")
    {% endif %}
{% endfor %}
}
"""

RELATIONSHIP_DEF_TEMPLATE = """
interface {{ unique_rel_type_name_from_shape(rel) }} @relationshipProperties {
{% for property in rel.properties.properties.values() %}
    {{ property.name }}: {{ field_mappings[property.type] }}
{% endfor %}
}
"""


SNAKE_CASE_REGEX = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")

PROPERTY_TYPE_TO_GRAPQHL_TYPES = {
    PropertyType.STRING: "String",
    PropertyType.DATETIME: "LocalDateTime",
    PropertyType.INTEGER: "BigInt",
    PropertyType.BOOLEAN: "Boolean",
    PropertyType.FLOAT: "Float",
}


def unique_rel_type_name_from_shape(rel: GraphObjectShape):
    rel_type = rel.object_type.type
    return "".join(p.title() for p in rel_type.lower().split("_"))


def unique_rel_type_name(rel: PresentRelationship):
    rel_type = ensure_camel_case(rel.relationship_type.type)
    return "".join(p.title() for p in rel_type.lower().split("_"))


def ensure_camel_case(possible_snake_case):
    if SNAKE_CASE_REGEX.match(possible_snake_case):
        splits = possible_snake_case.split("_")
        return "".join(t.title() for t in splits)
    return possible_snake_case


def in_relationship_to_camel(relationship, other_node):
    splits = str(relationship).lower().split("_")
    other_node = ensure_camel_case(str(other_node))
    return other_node[0].lower() + other_node[1:] + "".join((t.title() for t in splits))


def out_relationship_to_camel(relationship, other_node):
    splits = str(relationship).lower().split("_")
    other_node = ensure_camel_case(str(other_node))
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

    def render_node_schema(self, schema: GraphSchema) -> str:
        representation = ""
        for node_shape in sorted(
            schema.known_node_types(), key=lambda k: k.object_type.type
        ):
            all_inbound_rels = [
                r
                for r in schema.relationships
                if r.to_object_type == node_shape.object_type
            ]
            all_outbound_rels = [
                r
                for r in schema.relationships
                if r.from_object_type == node_shape.object_type
            ]
            representation += self.node_template.render(
                shape=node_shape,
                field_mappings=PROPERTY_TYPE_TO_GRAPQHL_TYPES,
                inbound_relationships=all_inbound_rels,
                outbound_relationships=all_outbound_rels,
            )
        return representation

    def render_relationship_schema(self, schema: GraphSchema) -> str:
        return "".join(
            self.rel_template.render(
                rel=rel, field_mappings=PROPERTY_TYPE_TO_GRAPQHL_TYPES
            )
            for rel in schema.known_relationship_types()
        )

    def print_schema_to_string(self, schema: GraphSchema) -> str:
        representation = self.render_node_schema(schema)
        representation += self.render_relationship_schema(schema)
        return representation + "\n"
