from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import yaml

SOURCE_ALIAS = "__SOURCE_NODE__"


class GraphObjectType(str, Enum):
    RELATIONSHIP = "RELATIONSHIP"
    NODE = "NODE"

    def __str__(self) -> str:
        return self.value


class Cardinality(str, Enum):
    SINGLE = "SINGLE"
    MANY = "MANY"


class PropertyType(str, Enum):
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    DATETIME = "DATETIME"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"


@dataclass(slots=True)
class PropertyMetadata:
    name: str
    type: PropertyType = PropertyType.STRING

    def __str__(self) -> str:
        return f"{self.name}: {self.type.value}"


@dataclass(slots=True)
class PropertyMetadataSet:
    properties: Dict[str, PropertyMetadata]

    def update(self, other: "PropertyMetadataSet"):
        for property_name, property_meta in other.properties.items():
            self.properties[property_name] = property_meta

    @classmethod
    def from_names(
        cls, *name_iterables: Iterable[str], include_timestamps: bool = True
    ):
        properties = {
            name: PropertyMetadata(name=name)
            for name_iterable in name_iterables
            for name in name_iterable
        }

        if include_timestamps:
            timestamp_name = "last_ingested_at"
            properties[timestamp_name] = PropertyMetadata(
                name=timestamp_name, type=PropertyType.DATETIME
            )

        return cls(properties=properties)

    def __str__(self) -> str:
        return "\n\t".join(str(property) for property in self.properties.values())


class TypeMarker(ABC):
    @abstractmethod
    def resolve_type(self, other_shapes: Iterable["GraphObjectShape"]) -> Optional[str]:
        raise NotImplementedError

    @abstractmethod
    def fulfills_alias(self, desired_alias: str) -> bool:
        raise NotImplementedError


class KnownTypeMarker(TypeMarker):
    """Represents a type that is known within the context of its creation and may be referenced as something else in the future."""

    def __init__(self, type: str, fulfills_alias: Optional[str] = None) -> None:
        self.type = type
        self.alias = fulfills_alias

    def resolve_type(
        self, other_shapes: Iterable["GraphObjectShape"]
    ) -> Optional[TypeMarker]:
        return self

    def fulfills_alias(self, desired_alias: str) -> bool:
        return self.alias == desired_alias

    def __eq__(self, o: object) -> bool:
        return isinstance(o, KnownTypeMarker) and o.type == self.type

    @classmethod
    def fulfilling_source_node(cls, type: str) -> "KnownTypeMarker":
        return cls(type=type, fulfills_alias=SOURCE_ALIAS)

    def __str__(self) -> str:
        return self.type


class UnknownTypeMarker(TypeMarker):
    """Represents a type that is not known currently and requires context from others to resolve.

    In other words, we know what role this type is, but not what the exact type is.
    """

    def __init__(self, alias: str) -> None:
        self.alias = alias

    def resolve_type(
        self, other_shapes: Iterable["GraphObjectShape"]
    ) -> Optional[TypeMarker]:
        for shape in other_shapes:
            if shape.object_type.fulfills_alias(self.alias):
                return shape.object_type

    def fulfills_alias(self, desired_alias: str) -> bool:
        return False

    def __eq__(self, o: object) -> bool:
        return isinstance(o, UnknownTypeMarker) and o.alias == self.alias

    def __str__(self) -> str:
        return f"[Uknown] Alias({self.alias}))"

    @classmethod
    def source_node(cls):
        return cls(alias=SOURCE_ALIAS)


@dataclass(slots=True)
class GraphObjectShape:
    """Defines the shape of an object in the graph.

    The shape of an object consists of three parts:

    1.) Whether its a node or a relationship.
    2.) What properties it has on it.
    3.) What the type of the node/relationship is.
    """

    graph_object_type: GraphObjectType
    object_type: TypeMarker
    properties: PropertyMetadataSet

    def overlaps_with(self, other: "GraphObjectShape") -> bool:
        return (
            self.graph_object_type == other.graph_object_type
            and self.object_type == other.object_type
        )

    def include(self, other: "GraphObjectShape"):
        if not self.overlaps_with(other):
            raise ValueError("Cannot include when the shape is not overlapping")

        self.properties.update(other.properties)

    def resolve_types(self, shapes: Iterable["GraphObjectShape"]):
        if object_type := self.object_type.resolve_type(shapes):
            self.object_type = object_type

    @property
    def is_node(self) -> bool:
        return self.graph_object_type == GraphObjectType.NODE

    @property
    def is_relationship(self) -> bool:
        return self.graph_object_type == GraphObjectType.RELATIONSHIP

    @property
    def has_known_type(self) -> bool:
        return isinstance(self.object_type, KnownTypeMarker)

    def __str__(self) -> str:
        return f"[{self.graph_object_type}] {self.object_type}:\n\t{self.properties}"


@dataclass(slots=True)
class PresentRelationship:
    """Indicates that a relationship of a specifc type exissts between two nodes with a certain cardinality.

    The shape of the relationship itself as well as the shapes of the node types are stored as `GraphObjectShape`.
    """

    from_object_type: TypeMarker
    to_object_type: TypeMarker
    relationship_type: TypeMarker
    from_side_cardinality: Cardinality
    to_side_cardinality: Cardinality

    def overlaps_with(self, other: "PresentRelationship") -> bool:
        return (
            self.from_object_type == other.from_object_type
            and self.to_object_type == other.to_object_type
            and self.relationship_type == other.relationship_type
        )

    def include(self, other: "PresentRelationship"):
        if not self.overlaps_with(other):
            raise ValueError(
                "Cannot include when other relationship is not overlapping"
            )

        # If multiple relationships come in, we do not have enough information to assume that
        # the relationship is anything other than MANY->MANY. For any side that is "single", we
        # do not know that the each one of the sources of the relationship will not all create a "single"
        # sided relationship off of the same nodes.
        self.from_side_cardinality = Cardinality.MANY
        self.to_side_cardinality = Cardinality.MANY

    def resolve_types(self, shapes: Iterable["GraphObjectShape"]):
        if object_type := self.from_object_type.resolve_type(shapes):
            self.from_object_type = object_type
        if object_type := self.to_object_type.resolve_type(shapes):
            self.to_object_type = object_type

        self.relationship_type = self.relationship_type.resolve_type(shapes)


@dataclass(frozen=True, slots=True)
class GraphSchema:
    """Defines the Schema of a graph by specifiying the node and relationship types and how they interconnect.

    See `GraphObjectShape` and `PresentRelationship` for more details.
    """

    object_shapes: List[GraphObjectShape]
    relationships: List[PresentRelationship]

    @classmethod
    def empty(cls):
        return cls(object_shapes=[], relationships=[])

    def merge(self, other: "GraphSchema") -> "GraphSchema":
        all_shapes = self.object_shapes + other.object_shapes
        all_rels = self.relationships + other.relationships
        return GraphSchema(
            object_shapes=_merge_overlapping_items(all_shapes, all_shapes),
            relationships=_merge_overlapping_items(all_rels, all_shapes),
        )

    def known_node_types(self) -> Iterable[GraphObjectShape]:
        for shape in self.object_shapes:
            if shape.is_node and shape.has_known_type:
                yield shape

    def known_relationship_types(self) -> Iterable[GraphObjectShape]:
        for shape in self.object_shapes:
            if shape.is_relationship and shape.has_known_type:
                yield shape

    def apply_type_overrides_from_file(self, file_path: Path):
        overrides = GraphSchemaOverrides.from_file(file_path)
        self.apply_overrides(overrides)

    def apply_overrides(self, overrides: "GraphSchemaOverrides"):
        overrides.apply_to(self)


@dataclass(frozen=True, slots=True)
class PropertyOverride:
    type: Optional[PropertyType]

    @classmethod
    def from_file_data(cls, file_data) -> "GraphSchemaOverrides":
        type_str = file_data.get("type")
        type = None if type_str is None else PropertyType(type_str)
        return cls(type=type)

    def apply_to(self, property_metadata: PropertyMetadata):
        if self.type:
            property_metadata.type = self.type


@dataclass(frozen=True, slots=True)
class PropertyOverrides:
    properties: Dict[str, PropertyOverride]

    @classmethod
    def from_file_data(cls, file_data) -> "GraphSchemaOverrides":
        properties = {
            key: PropertyOverride.from_file_data(value)
            for key, value in file_data.items()
        }
        return cls(properties=properties)

    def apply_to(self, graph_object_shape: GraphObjectShape):
        for property_name, override in self.properties.items():
            if property_name in graph_object_shape.properties.properties:
                override.apply_to(
                    graph_object_shape.properties.properties[property_name]
                )


@dataclass(frozen=True, slots=True)
class GraphSchemaOverrides:
    property_overrides: Dict[str, PropertyOverrides]

    @classmethod
    def from_file(cls, file_path: Path) -> "GraphSchemaOverrides":
        with open(file_path) as f:
            overrides_data = yaml.safe_load(f)
            property_overrides = {
                key: PropertyOverrides.from_file_data(value)
                for key, value in overrides_data.get("properties", {}).items()
            }
            return cls(property_overrides=property_overrides)

    def apply_to(self, schema: GraphSchema):
        for type_name, overrides in self.property_overrides.items():
            for shape in schema.known_node_types():
                if shape.object_type.type == type_name:
                    overrides.apply_to(shape)


def _merge_overlapping_items(unmerged_items, shapes):
    """Merges items that refer to the same inner types together and resolves ambiguities in types that are not known between components."""

    distinct_items = []

    for new_item in unmerged_items:
        new_item.resolve_types(shapes)
        for current_item in distinct_items:
            if new_item.overlaps_with(current_item):
                current_item.include(new_item)
                break
        else:
            distinct_items.append(new_item)

    return distinct_items


class IntrospectiveIngestionComponent(ABC):
    """An IntrospectableIngestionComponent is a componet that can be interrogated what it contributes the Graph Database Schema.

    Nearly all components in the transformation part of the ingestion stack (Interpreters, Interpretations, and Pipelines) are
    `IntrospectableIngestionComponent`s. Leaf components like Interpretations provide a relatively local scope as to what it knows
    about the schema. As is the hierarchy grows, more and more data is is combined and aggregated together until an entire schema
    is produced. For more information on aggregation, see `AggregatedIntrospectiveIngestionComponent`.
    """

    @abstractmethod
    def gather_used_indexes(self):
        raise NotImplementedError

    @abstractmethod
    def gather_object_shapes(self):
        raise NotImplementedError

    @abstractmethod
    def gather_present_relationships(self):
        raise NotImplementedError

    def generate_graph_schema(self) -> GraphSchema:
        shapes = self.gather_object_shapes()
        relationships = self.gather_present_relationships()
        return GraphSchema(
            object_shapes=_merge_overlapping_items(shapes, shapes),
            relationships=_merge_overlapping_items(relationships, shapes),
        )


class AggregatedIntrospectiveIngestionComponent(IntrospectiveIngestionComponent):
    """A mixin to provide utilities for `IntrospectableIngestionComponent`s that are aggregations of others.

    For many types in our ingestion hierarchy, they are aggregations of subordinate components that each
    on their own provide part of the make up for a graph schema. When we look at higher order components such
    as a Interpreter, they need to merge and resolve ambiguity amongst its child ingestion components (Interpretations).

    This mixin provides that higher order aggregation logic to merge and resolve ambiguity amongst such a hierarchy.
    """

    @abstractmethod
    def all_subordinate_components(self) -> Iterable[IntrospectiveIngestionComponent]:
        raise NotImplementedError

    def gather_used_indexes(self):
        return {
            index
            for component in self.all_subordinate_components()
            for index in component.gather_used_indexes()
        }

    def gather_object_shapes(self):
        shapes = [
            shape
            for component in self.all_subordinate_components()
            for shape in component.gather_object_shapes()
        ]
        return _merge_overlapping_items(shapes, shapes)

    def gather_present_relationships(self):
        shapes = self.gather_object_shapes()
        rels = [
            rel
            for component in self.all_subordinate_components()
            for rel in component.gather_present_relationships()
        ]
        return _merge_overlapping_items(rels, shapes)
