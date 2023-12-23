from dataclasses import dataclass, field
from typing import Dict, Set


@dataclass(frozen=True, slots=True)
class FieldIndex:
    """An index on a field.

    Attributes:
        field_name: The name of the field.
    """

    field_name: str


# TODO: Attach property metadata to properties and keys (e.g. data type, etc.).


@dataclass(slots=True)
class GraphObjectSchema:
    name: str
    keys: Set[str]
    properties: Set[str]
    indexes: Set[FieldIndex]

    def rename_property(self, old_property_name: str, new_property_name: str):
        """Rename a property.

        Args:
            old_property_name: The old property name.
            new_property_name: The new property name.
        """
        if old_property_name not in self.properties:
            raise ValueError(
                f"Property {old_property_name} does not exist on node type {self.name}."
            )
        self.properties.remove(old_property_name)
        self.properties.add(new_property_name)

    def rename_key(self, old_key: str, new_key: str):
        """Rename a key.

        Args:
            old_key: The old key.
            new_key: The new key.
        """
        if old_key not in self.keys:
            raise ValueError(f"Key {old_key} does not exist on node type {self.name}.")
        self.keys.remove(old_key)
        self.keys.add(new_key)

    def add_index(self, index: FieldIndex):
        """Add an index.

        Args:
            index: The index to add.
        """
        self.indexes.add(index)

    def drop_index(self, index: FieldIndex):
        """Drop an index.

        Args:
            index: The index to drop.
        """
        self.indexes.remove(index)

    def get_index_by_field_name(self, field_name: str) -> FieldIndex:
        """Get an index by field name.

        Args:
            field_name: The name of the field.

        Returns:
            The index.
        """
        for index in self.indexes:
            if index.field_name == field_name:
                return index
        raise ValueError(f"No index on field {field_name} exists.")

    def has_matching_keys(self, other: "GraphObjectSchema") -> bool:
        """Check if two node types have matching keys.

        Args:
            other: The other node type.

        Returns:
            True if the node types have matching keys.
        """
        return self.keys == other.keys

    def has_matching_properties(self, other: "GraphObjectSchema") -> bool:
        """Check if two node types have matching properties.

        Args:
            other: The other node type.

        Returns:
            True if the node types have matching properties.
        """
        return self.properties == other.properties

    def add_property(self, property_name: str):
        """Add a property.

        Args:
            property_name: The property name.
        """
        self.properties.add(property_name)

    def drop_property(self, property_name: str):
        """Drop a property.

        Args:
            property_name: The property name.
        """
        if property_name not in self.properties:
            raise ValueError(
                f"Property {property_name} does not exist on node type {self.name}."
            )
        self.properties.remove(property_name)

    def add_key(self, key: str):
        """Add a key.

        Args:
            key: The key.
        """
        self.keys.add(key)


@dataclass(slots=True)
class NodeSchema(GraphObjectSchema):
    """A node type in a schema."""

    pass


@dataclass(slots=True)
class RelationshipSchema(GraphObjectSchema):
    """A relationship type in a schema."""

    pass


@dataclass(slots=True, frozen=True)
class Adjacency:
    """An adjacency between two node types."""

    from_node_type: str
    to_node_type: str
    relationship_type: str

    # TODO: Set cardinalities here.


@dataclass(slots=True)
class Schema:
    """A schema for a database."""

    nodes: Dict[str, NodeSchema] = field(default_factory=dict)
    relationships: Dict[str, RelationshipSchema] = field(default_factory=dict)
    adjacencies: Set[Adjacency] = field(default_factory=set)

    def put_node_type(self, node_type: NodeSchema):
        """Add a node type to the schema.

        Args:
            node_type: The node type to add.

        Returns:
            A new schema with the node type added.
        """
        self.nodes[node_type.name] = node_type

    def put_relationship_type(self, relationship_type: RelationshipSchema):
        """Add a relationship type to the schema.

        Args:
            relationship_type: The relationship type to add.

        Returns:
            A new schema with the relationship type added.
        """
        self.relationships[relationship_type.name] = relationship_type

    def drop_node_type_by_name(self, node_type_name: str):
        """Drop a node type from the schema.

        Args:
            node_type_name: The name of the node type to drop.

        Returns:
            A new schema with the node type dropped.
        """
        del self.nodes[node_type_name]

    def drop_relationship_type_by_name(self, relationship_type_name: str):
        """Drop a relationship type from the schema.

        Args:
            relationship_type_name: The name of the relationship type to drop.

        Returns:
            A new schema with the relationship type dropped.
        """
        del self.relationships[relationship_type_name]

    def get_node_type_by_name(self, node_type_name: str) -> NodeSchema:
        """Get a node type by name.

        Args:
            node_type_name: The name of the node type to get.

        Returns:
            The node type.
        """
        return self.nodes[node_type_name]

    def get_relationship_type_by_name(
        self, relationship_type_name: str
    ) -> RelationshipSchema:
        """Get a relationship type by name.

        Args:
            relationship_type_name: The name of the relationship type to get.

        Returns:
            The relationship type.
        """
        return self.relationships[relationship_type_name]

    def add_adjacency(self, adjacency: Adjacency):
        """Add an adjacency.

        Args:
            adjacency: The adjacency to add.
        """
        self.adjacencies.add(adjacency)

    def drop_adjacency(self, adjacency: Adjacency):
        """Drop an adjacency.

        Args:
            adjacency: The adjacency to drop.
        """
        self.adjacencies.remove(adjacency)
