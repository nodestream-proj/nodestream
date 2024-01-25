from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, Iterable, Optional, Set, Tuple

from ..file_io import LoadsFromYaml, LoadsFromYamlFile, SavesToYaml, SavesToYamlFile


class GraphObjectType(str, Enum):
    """Represents the type of a graph object."""

    NODE = "NODE"
    RELATIONSHIP = "RELATIONSHIP"


class Cardinality(str, Enum):
    """Represents the cardinality of an adjacency."""

    SINGLE = "SINGLE"
    MANY = "MANY"


class PropertyType(str, Enum):
    """Represents the type of a property."""

    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    DATETIME = "DATETIME"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"


@dataclass(slots=True)
class PropertyMetadata(LoadsFromYaml, SavesToYaml):
    """Metadata for a property.

    Attributes:
        property_type: The type of the property.
    """

    type: PropertyType = PropertyType.STRING
    is_key: bool = False
    is_indexed: bool = False

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            {
                "type": str,
                Optional("is_key"): bool,
                Optional("is_indexed"): bool,
            }
        )

    @classmethod
    def from_file_data(cls, yaml_data):
        return cls(
            type=PropertyType(yaml_data["type"]),
            is_key=yaml_data.get("is_key", False),
            is_indexed=yaml_data.get("is_indexed", False),
        )

    def to_file_data(self):
        return {
            "type": self.type.value,
            "is_key": self.is_key,
            "is_indexed": self.is_indexed,
        }

    def merge(self, other: "PropertyMetadata"):
        """Merge another property metadata into this property metadata.

        Args:
            other: The other property metadata.
        """
        self.type = other.type
        self.is_key |= other.is_key
        self.is_indexed |= other.is_indexed


@dataclass(slots=True)
class GraphObjectSchema(LoadsFromYaml, SavesToYaml):
    """A graph object type in a schema."""

    name: str
    properties: Dict[str, PropertyMetadata] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.name}:\n" + "\n".join(
            f"  {property_name}: {property_metadata.type.value}"
            for property_name, property_metadata in self.properties.items()
        )

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Schema

        return Schema(
            {
                "name": str,
                "properties": {str: PropertyMetadata.describe_yaml_schema()},
            }
        )

    @classmethod
    def from_file_data(cls, yaml_data):
        return cls(
            name=yaml_data["name"],
            properties={
                key: PropertyMetadata.from_file_data(value)
                for key, value in yaml_data["properties"].items()
            },
        )

    def to_file_data(self):
        return {
            "name": self.name,
            "properties": {
                key: value.to_file_data() for key, value in self.properties.items()
            },
        }

    @property
    def keys(self) -> Set[str]:
        return {
            property_name
            for property_name, metadata in self.properties.items()
            if metadata.is_key
        }

    @property
    def non_key_properties(self) -> Set[str]:
        return set(self.properties) - self.keys

    @property
    def indexed_properties(self) -> Set[str]:
        return {
            property_name
            for property_name, metadata in self.properties.items()
            if metadata.is_indexed
        }

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

        self.properties[new_property_name] = self.properties.pop(old_property_name)

    def rename_key(self, old_key: str, new_key: str):
        """Rename a key.

        Args:
            old_key: The old key.
            new_key: The new key.
        """
        if old_key not in self.keys:
            raise ValueError(f"Key {old_key} does not exist on node type {self.name}.")
        self.rename_property(old_key, new_key)

    def add_index(self, property: str):
        """Add an index.

        Args:
            index: The index to add.
        """
        self.properties[property].is_indexed = True

    def drop_index(self, property: str):
        """Drop an index.

        Args:
            index: The index to drop.
        """
        self.properties[property].is_indexed = False

    def has_matching_keys(
        self, other: "GraphObjectSchema", allow_undefined: bool = False
    ) -> bool:
        """Check if two node types have matching keys.

        Args:
            other: The other node type.
            allow_undefined: Whether or not to allow undefined keys. If this is
                True, then the node types will be considered to have matching
                keys if one of the node types has no keys.

        Returns:
            True if the node types have matching keys.
        """
        us = self.keys
        them = other.keys
        return us == them or (allow_undefined and len(us) == 0 or len(them) == 0)

    def has_matching_properties(self, other: "GraphObjectSchema") -> bool:
        """Check if two node types have matching properties.

        Args:
            other: The other node type.

        Returns:
            True if the node types have matching properties.
        """
        return self.properties == other.properties

    def add_property(
        self, property_name: str, metadata: Optional[PropertyMetadata] = None
    ):
        """Add a property.

        If the property already exists, then the property will be replaced.
        If no metadata is provided, then the property will be added with the
        default metadata (string type, not key, not indexed).

        Args:
            property_name: The property name.
        """
        self.properties[property_name] = metadata or PropertyMetadata()

    def drop_property(self, property_name: str):
        """Drop a property.

        Args:
            property_name: The property name.
        """
        del self.properties[property_name]

    def add_key(self, key: str):
        """Add a key.

        Args:
            key: The key.
        """
        metadata = PropertyMetadata(PropertyType.STRING, is_key=True)
        self.add_property(key, metadata)

    def add_keys(self, keys: Iterable[str]):
        """Add keys.

        Args:
            keys: The keys.
        """
        if self.keys and self.keys != set(keys):
            raise ValueError(
                f"Cannot add keys {keys} to node type {self.name} because it already has keys {self.keys}."
            )

        for key in keys:
            self.add_key(key)

    def add_properties(self, properties: Iterable[str]):
        """Add properties.

        Args:
            properties: The properties.
        """
        for property in properties:
            self.add_property(property)

    def add_indexes(self, indexes: Iterable[str]):
        """Add indexes to the properties of this node type.

        Args:
            indexes: The properties to index.
        """
        for property in indexes:
            self.add_index(property)

    def add_indexed_timestamp(self):
        """Add a timestamp index."""
        property_name = "last_ingested_at"
        metadata = PropertyMetadata(PropertyType.DATETIME)
        self.add_property(property_name, metadata)
        self.add_index(property_name)

    def merge(self, other: "GraphObjectSchema"):
        """Merge another node type into this node type.

        Args:
            other: The other node type.
        """
        if self.name != other.name:
            raise ValueError(
                f"Cannot merge node type {other.name} into node type {self.name}."
            )

        if not self.has_matching_keys(other, allow_undefined=True):
            raise ValueError(
                f"Cannot merge node type {other.name} into node type {self.name} because the keys do not match."
            )

        for property_name, metadata in other.properties.items():
            if property_name not in self.properties:
                self.properties[property_name] = metadata
            else:
                self.properties[property_name].merge(metadata)


@dataclass(slots=True, frozen=True)
class Adjacency:
    """An adjacency between two node types."""

    from_node_type: str
    to_node_type: str
    relationship_type: str


@dataclass(slots=True, frozen=True)
class AdjacencyCardinality:
    """An adjacency cardinality."""

    from_side_cardinality: Cardinality = Cardinality.SINGLE
    to_side_cardinality: Cardinality = Cardinality.SINGLE


@dataclass(slots=True, frozen=True)
class Schema(SavesToYamlFile, LoadsFromYamlFile):
    """A schema for a database."""

    type_schemas: Dict[Tuple[GraphObjectType, str], GraphObjectSchema] = field(
        default_factory=dict
    )

    cardinalities: Dict[Adjacency, AdjacencyCardinality] = field(default_factory=dict)

    def __str__(self):
        return "\n".join(
            f"[{type.value}] {str(s)}" for (type, _), s in self.type_schemas.items()
        )

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            {
                Optional("nodes"): [GraphObjectSchema.describe_yaml_schema()],
                Optional("relationships"): [GraphObjectSchema.describe_yaml_schema()],
            }
        )

    @classmethod
    def from_file_data(cls, yaml_data):
        instance = cls()

        for node_data in yaml_data.get("nodes", []):
            node = GraphObjectSchema.from_file_data(node_data)
            instance.put_node_type(node)
        for relationship_data in yaml_data.get("relationships", []):
            relationship = GraphObjectSchema.from_file_data(relationship_data)
            instance.put_relationship_type(relationship)

        return instance

    def to_file_data(self):
        return {
            "nodes": [node.to_file_data() for node in self.nodes],
            "relationships": [
                relationship.to_file_data() for relationship in self.relationships
            ],
        }

    @property
    def nodes_by_name(self) -> Dict[str, GraphObjectSchema]:
        return {node.name: node for node in self.nodes}

    @property
    def relationships_by_name(self) -> Dict[str, GraphObjectSchema]:
        return {relationship.name: relationship for relationship in self.relationships}

    @property
    def nodes(self) -> Iterable[GraphObjectSchema]:
        return (
            schema
            for (type, _), schema in self.type_schemas.items()
            if type == GraphObjectType.NODE
        )

    @property
    def relationships(self) -> Iterable[GraphObjectSchema]:
        return (
            schema
            for (type, _), schema in self.type_schemas.items()
            if type == GraphObjectType.RELATIONSHIP
        )

    @property
    def adjacencies(self) -> Iterable[Adjacency]:
        return self.cardinalities.keys()

    def put_node_type(self, node_type: GraphObjectSchema):
        """Add a node type to the schema.

        Args:
            node_type: The node type to add.

        Returns:
            A new schema with the node type added.
        """
        self.type_schemas[(GraphObjectType.NODE, node_type.name)] = node_type

    def put_relationship_type(self, relationship_type: GraphObjectSchema):
        """Add a relationship type to the schema.

        Args:
            relationship_type: The relationship type to add.

        Returns:
            A new schema with the relationship type added.
        """
        key = (GraphObjectType.RELATIONSHIP, relationship_type.name)
        self.type_schemas[key] = relationship_type

    def drop_node_type_by_name(self, node_type_name: str):
        """Drop a node type from the schema.

        Args:
            node_type_name: The name of the node type to drop.

        Returns:
            A new schema with the node type dropped.
        """
        del self.type_schemas[(GraphObjectType.NODE, node_type_name)]

    def drop_relationship_type_by_name(self, relationship_type_name: str):
        """Drop a relationship type from the schema.

        Args:
            relationship_type_name: The name of the relationship type to drop.

        Returns:
            A new schema with the relationship type dropped.
        """
        del self.type_schemas[(GraphObjectType.RELATIONSHIP, relationship_type_name)]

    def get_by_type_and_object_type(
        self, object_type: GraphObjectType, name: str
    ) -> GraphObjectSchema:
        key = (object_type, name)
        if key not in self.type_schemas:
            self.type_schemas[key] = GraphObjectSchema(name)
        return self.type_schemas[key]

    def get_node_type_by_name(self, node_type_name: str) -> GraphObjectSchema:
        """Get a node type by name.

        If the node type does not exist, a new node type will be created.

        Args:
            node_type_name: The name of the node type to get.

        Returns:
            The node type.
        """
        return self.get_by_type_and_object_type(GraphObjectType.NODE, node_type_name)

    def get_relationship_type_by_name(
        self, relationship_type_name: str
    ) -> GraphObjectSchema:
        """Get a relationship type by name.

        If the relationship type does not exist, a new relationship type will be created.

        Args:
            relationship_type_name: The name of the relationship type to get.

        Returns:
            The relationship type.
        """
        return self.get_by_type_and_object_type(
            GraphObjectType.RELATIONSHIP, relationship_type_name
        )

    def add_adjacency(
        self, adjacency: Adjacency, cardinality: Optional[AdjacencyCardinality] = None
    ):
        """Add an adjacency.

        Args:
            adjacency: The adjacency to add.
        """
        self.cardinalities[adjacency] = cardinality

    def drop_adjacency(self, adjacency: Adjacency):
        """Drop an adjacency.

        Args:
            adjacency: The adjacency to drop.
        """
        del self.cardinalities[adjacency]

    def get_adjacency_cardinality(self, adjacency: Adjacency) -> AdjacencyCardinality:
        """Get an adjacency cardinality.

        Args:
            adjacency: The adjacency.

        Returns:
            The adjacency cardinality.
        """
        return self.cardinalities[adjacency]

    def merge(self, other: "Schema"):
        """Merge another schema into this schema.

        Merges the node types and relationship types from the other schema into
        this schema. If a node type or relationship type already exists in this
        schema, then the node type or relationship type will be merged such that
        the properties of the node type or relationship type in the other schema
        take precedence over the properties of the node type or relationship type
        in this schema.

        Args:
            other: The other schema.
        """
        self.cardinalities.update(other.cardinalities)
        all_types = set(self.type_schemas).union(set(other.type_schemas))
        for obj_type, type in all_types:
            us = self.get_by_type_and_object_type(obj_type, type)
            them = other.get_by_type_and_object_type(obj_type, type)
            us.merge(them)

    def has_node_of_type(self, node_type_name: str) -> bool:
        """Check if a node type exists in the schema.

        Args:
            node_type_name: The name of the node type.

        Returns:
            True if the node type exists in the schema.
        """
        return (GraphObjectType.NODE, node_type_name) in self.type_schemas

    def has_relationship_of_type(self, relationship_type_name: str) -> bool:
        """Check if a relationship type exists in the schema.

        Args:
            relationship_type_name: The name of the relationship type.

        Returns:
            True if the relationship type exists in the schema.
        """
        return (
            GraphObjectType.RELATIONSHIP,
            relationship_type_name,
        ) in self.type_schemas


@dataclass(slots=True, frozen=True)
class SchemaExpansionCoordinator:
    """A coordinator for expanding a schema."""

    schema: Schema
    aliases: Dict[str, str] = field(default_factory=dict)
    unbound_aliases: Dict[str, GraphObjectSchema] = field(default_factory=dict)

    def on_node_schema(
        self,
        fn: Callable[[GraphObjectSchema], None],
        node_type: Optional[str] = None,
        alias: Optional[str] = None,
    ):
        """Calls a Function on each type of node in the schema aliased.

        Depending on the context, there may be zero or more node schemas that
        match the given alias (if provided). If no alias is provided, then the
        function will be called on just the schema for the node type name.

        If an alias is provided, then the function will be called on all node
        schemas that match the alias.

        Args:
            fn: The function to call.
            node_type_name: The node type name.
            alias: The alias.
        """
        # If both the node_type_name and alias are provided, we are "concreting"
        # the node type name. This means that we are binding the alias(if it exists)
        # to the node type name.
        if node_type and alias:
            node_schema = self.schema.get_node_type_by_name(node_type)
            unbound = self.unbound_aliases.pop(alias, GraphObjectSchema(node_type))
            unbound.name = node_type
            node_schema.merge(unbound)
            self.aliases[alias] = node_type
            fn(node_schema)

        # If only the alias is provided, we are "abstracting" the node type name
        # because we don't know the node type name yet. We will bind the alias to
        # the node type name when we find it. Note that we will need to "hold on" to the
        # unbound alias until we find the node type name.
        elif alias:
            unbound = self.unbound_aliases.get(alias, GraphObjectSchema(alias))
            self.unbound_aliases[alias] = unbound
            fn(unbound)

        # If only the node_type_name is provided, we are not messing with the alias at all.
        # We are just calling the function on the node type name.
        elif node_type:
            node_schema = self.schema.get_node_type_by_name(node_type)
            fn(node_schema)

    def on_relationship_schema(
        self,
        fn: Callable[[GraphObjectSchema], None],
        relationship_type: str,
    ):
        """Calls a Function to modify the specified relationship schema."""
        fn(self.schema.get_relationship_type_by_name(relationship_type))

    def connect(
        self,
        from_type_or_alias: str,
        to_type_or_alias: str,
        relationship_type: str,
        from_cardinality: Cardinality,
        to_cardinality: Cardinality,
    ):
        """Connect two node types via a relationship type.

        When connecting two node types, the node types may be aliased. If the
        node types are aliased, then the aliases will be used to connect the
        node types by looking up the real node type names. If the node types
        are not aliased, then the node type names will be used directly.

        Args:
            from_type_or_alias: The from node type or alias.
            to_type_or_alias: The to node type or alias.
            relationship_type: The relationship type.
            from_cardinality: The from cardinality.
            to_cardinality: The to cardinality.
        """
        from_side_type = self.aliases.get(from_type_or_alias, from_type_or_alias)
        to_side_type = self.aliases.get(to_type_or_alias, to_type_or_alias)
        adjacency = Adjacency(from_side_type, to_side_type, relationship_type)
        cardinality = AdjacencyCardinality(from_cardinality, to_cardinality)
        self.schema.add_adjacency(adjacency, cardinality)

    def clear_aliases(self):
        self.aliases.clear()


class ExpandsSchema:
    """An interface for an object that expands a schema."""

    def expand_schema(self, coordinator: SchemaExpansionCoordinator):
        """Expand a schema.

        Args:
            schema: The schema to expand.

        Returns:
            The expanded schema.
        """
        pass

    def make_schema(self) -> Schema:
        """Generates a new schema.

        Returns:
            The new schema.
        """
        coordinator = SchemaExpansionCoordinator(schema := Schema())
        self.expand_schema(coordinator)
        return schema


class ExpandsSchemaFromChildren(ExpandsSchema, ABC):
    """An interface for an object that expands a schema from its children."""

    @abstractmethod
    def get_child_expanders(self) -> Iterable[ExpandsSchema]:
        """Expand a schema from its children.

        Args:
            schema: The schema to expand.

        Returns:
            The expanded schema.
        """
        pass

    def expand_schema(self, coordinator: SchemaExpansionCoordinator):
        """Expand a schema.

        Args:
            schema: The schema to expand.

        Returns:
            The expanded schema.
        """
        for child_expander in self.get_child_expanders():
            child_expander.expand_schema(coordinator)
