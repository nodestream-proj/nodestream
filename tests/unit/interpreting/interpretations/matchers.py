from typing import Iterable

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.description import Description

from nodestream.schema import GraphObjectType, SchemaExpansionCoordinator, Cardinality
from nodestream.schema.state import UnboundAdjacency


class HasObjectKeyDefined(BaseMatcher):
    def __init__(
        self,
        graph_type: GraphObjectType,
        object_type: str,
        keys: Iterable[str],
    ) -> None:
        self.graph_type = graph_type
        self.object_type = object_type
        self.keys = keys

    def _matches(self, item: SchemaExpansionCoordinator):
        if self.object_type in item.unbound_aliases:
            object_definition = item.unbound_aliases[self.object_type]
        else:
            object_definition = item.schema.get_by_type_and_object_type(
                self.graph_type, self.object_type
            )
        for key in self.keys:
            if key not in object_definition.keys:
                return False

        return True

    def describe_to(self, description: Description) -> None:
        description.append_text(f"has keys {self.keys} defined for {self.object_type}")


class HasObjectPropertiesDefined(BaseMatcher):
    def __init__(
        self,
        graph_type: GraphObjectType,
        object_type: str,
        properties: Iterable[str],
    ) -> None:
        self.graph_type = graph_type
        self.object_type = object_type
        self.properties = properties

    def _matches(self, item: SchemaExpansionCoordinator):
        if self.object_type in item.unbound_aliases:
            object_definition = item.unbound_aliases[self.object_type]
        else:
            object_definition = item.schema.get_by_type_and_object_type(
                self.graph_type, self.object_type
            )
        for property in self.properties:
            if property not in object_definition.properties:
                return False

        return True

    def describe_to(self, description: Description) -> None:
        description.append_text(
            f"has properties {self.properties} defined for {self.object_type}"
        )


class HasObjectIndexesDefined(BaseMatcher):
    def __init__(
        self,
        graph_type: GraphObjectType,
        object_type: str,
        indexes: Iterable[str],
    ) -> None:
        self.graph_type = graph_type
        self.object_type = object_type
        self.indexes = indexes

    def _matches(self, item: SchemaExpansionCoordinator):
        if self.object_type in item.unbound_aliases:
            object_definition = item.unbound_aliases[self.object_type]
        else:
            object_definition = item.schema.get_by_type_and_object_type(
                self.graph_type, self.object_type
            )
        for index in self.indexes:
            if index not in object_definition.indexed_properties:
                return False

        return True

    def describe_to(self, description: Description) -> None:
        description.append_text(
            f"has indexes {self.indexes} defined for {self.object_type}"
        )


class HasUnboundAdjacencyDefined(BaseMatcher):
    def __init__(
        self,
        from_node_type_or_alias,
        to_node_type_or_alias,
        relationship_type,
        from_node_cardinality,
        to_node_cardinality,
    ) -> None:
        self.unbound_adjacency = UnboundAdjacency(
            from_node_type_or_alias,
            to_node_type_or_alias,
            relationship_type,
            from_node_cardinality,
            to_node_cardinality,
        )

    def _matches(self, item: SchemaExpansionCoordinator):
        return self.unbound_adjacency in item.unbound_adjacencies

    def describe_to(self, description: Description) -> None:
        description.append_text(
            f"has node types {self.unbound_adjacency} inside unbound adjacency list."
        )


class DefinedNodeTypes(BaseMatcher):
    def __init__(self, node_types: Iterable[str]) -> None:
        self.node_types = node_types

    def _matches(self, item: SchemaExpansionCoordinator):
        return set(self.node_types).issubset(item.schema.nodes_by_name)

    def describe_to(self, description: Description) -> None:
        description.append_text(f"has node types {self.node_types} defined")


class DefinedRelationshipTypes(BaseMatcher):
    def __init__(self, relationship_types: Iterable[str]) -> None:
        self.relationship_types = relationship_types

    def _matches(self, item: SchemaExpansionCoordinator):
        return set(self.relationship_types).issubset(item.schema.relationships_by_name)

    def describe_to(self, description: Description) -> None:
        description.append_text(
            f"has relationship types {self.relationship_types} defined"
        )


def has_node_keys(node_type: str, keys: Iterable[str]) -> HasObjectKeyDefined:
    return HasObjectKeyDefined(GraphObjectType.NODE, node_type, keys)


def has_node_properties(
    node_type: str, properties: Iterable[str]
) -> HasObjectPropertiesDefined:
    return HasObjectPropertiesDefined(GraphObjectType.NODE, node_type, properties)


def has_relationship_keys(
    relationship_type: str, keys: Iterable[str]
) -> HasObjectKeyDefined:
    return HasObjectKeyDefined(GraphObjectType.RELATIONSHIP, relationship_type, keys)


def has_relationship_properties(
    relationship_type: str, properties: Iterable[str]
) -> HasObjectPropertiesDefined:
    return HasObjectPropertiesDefined(
        GraphObjectType.RELATIONSHIP, relationship_type, properties
    )


def has_relationship_indexes(
    relationship_type: str, indexes: Iterable[str]
) -> HasObjectIndexesDefined:
    return HasObjectIndexesDefined(
        GraphObjectType.RELATIONSHIP, relationship_type, indexes
    )


def has_node_indexes(node_type: str, indexes: Iterable[str]) -> HasObjectIndexesDefined:
    return HasObjectIndexesDefined(GraphObjectType.NODE, node_type, indexes)


def has_no_defined_nodes() -> DefinedNodeTypes:
    return DefinedNodeTypes([])


def has_defined_nodes(node_types: Iterable[str]) -> DefinedNodeTypes:
    return DefinedNodeTypes(node_types)


def has_no_defined_relationships() -> DefinedRelationshipTypes:
    return DefinedRelationshipTypes([])


def has_defined_relationships(
    relationship_types: Iterable[str],
) -> DefinedRelationshipTypes:
    return DefinedRelationshipTypes(relationship_types)


def has_unbound_adjacency(
    from_node_type_or_alias: str,
    to_node_type_or_alias: str,
    relationship_type: str,
    from_node_cardinality: Cardinality,
    to_node_cardinality: Cardinality,
):
    return HasUnboundAdjacencyDefined(
        from_node_type_or_alias,
        to_node_type_or_alias,
        relationship_type,
        from_node_cardinality,
        to_node_cardinality,
    )
