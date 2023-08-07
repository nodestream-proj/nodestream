from abc import ABC
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from .match_strategy import MatchStrategy

if TYPE_CHECKING:
    from ..interpreting.context import ProviderContext
    from ..interpreting.value_providers import ValueProvider


class DeduplicatableObject(ABC):
    def get_dedup_key(self) -> tuple:
        raise NotImplementedError

    def update(self, other: "DeduplicatableObject"):
        raise NotImplementedError


class PropertySet(dict):
    def set_property(self, property_key: str, property_value: Any):
        self[property_key] = property_value

    @classmethod
    def default_properties(cls) -> "PropertySet":
        from pandas import Timestamp

        from ..pipeline.meta import get_context

        """Returns a default set of properties which set values.

        These default values indicate when the current pipeline touched the object the properties are for.
        """
        pipeline_name = get_context().name
        now = Timestamp.utcnow()
        return cls(
            {
                "last_ingested_at": now,
                f"last_ingested_by_{pipeline_name}_at": now,
                f"was_ingested_by_{pipeline_name}": True,
            }
        )

    @classmethod
    def empty(cls) -> "PropertySet":
        """Returns an empty property set."""
        return PropertySet()

    def apply_providers(
        self,
        context: "ProviderContext",
        provider_map: "Dict[str, ValueProvider]",
        **norm_args,
    ):
        """For every `(key, provider)` pair provided, sets the property to the values provided.

        This method can take arbitrary keyword arguments which are passed to `ValueProvider` as
        arguments for value normalization.
        """
        for key, provider in provider_map.items():
            v = provider.normalize_single_value(context, **norm_args)
            self.set_property(key, v)


@dataclass(slots=True)
class Node(DeduplicatableObject):
    """A `Node` is a entity that has a distinct identity.

    Each `Node` represents an entity (a person, place, thing, category or other piece of data) that has a distinct
    identity. Nodestream assumes the underlying graph database layer is a Labeled Property Graph. The identity
    of a node is defined by a root `type` (sometimes referred to as a label) as well as set of property name, value pairs
    representing the primary key of that node. In a relational database, this would be the combination of the table name
    as well as the primary key columns.

    The node class also can store additional property key value pairs that are not considered part of the identity of the
    node but rather additional data. In a relational database, these would be the non-primary key columns.

    A `Node` can also store additional types (labels) that can apply additional categorization in the database. This has
    no direct analogy in a relational database.
    """

    type: Optional[str] = None
    key_values: PropertySet = field(default_factory=PropertySet.empty)
    properties: PropertySet = field(default_factory=PropertySet.default_properties)
    additional_types: Tuple[str] = field(default_factory=tuple)

    @property
    def has_valid_id(self) -> bool:
        # Return that some of the ID values are defined.
        return not all(value is None for value in self.key_values.values())

    @property
    def is_valid(self) -> bool:
        return self.has_valid_id and self.type is not None

    @property
    def identity_shape(self) -> "NodeIdentityShape":
        return NodeIdentityShape(
            type=self.type,
            keys=tuple(self.key_values.keys()),
            additional_types=self.additional_types,
        )

    def has_same_key(self, other: "Node") -> bool:
        return self.key_values == other.key_values

    def update(self, other: "Relationship"):
        self.properties.update(other.properties)

    def get_dedup_key(self) -> tuple:
        return tuple(sorted(self.key_values.values()))


@dataclass(slots=True, frozen=True)
class Relationship(DeduplicatableObject):
    """A `Relationship` represents an inherent connection between two `Node`s.

    Each `Relationship` follows a relatively similar model to a `Node`. There is a _single_ type for the relationship.
    Relationships can store properties on the relationship itself (This would be similar to a jump table in a relational database).

    A key for a `Relationship` can also be provided. By default, `nodestream` will assume that there should be one
    relationship of the same type between two nodes. By providing keys, `nodestream` will create multiple relationships between
    two nodes and will discriminate based on the key values.

    This model represents the relationship itself and DOES NOT include a reference of the nodes that are stored.
    """

    type: str
    key_values: PropertySet = field(default_factory=PropertySet.empty)
    properties: PropertySet = field(default_factory=PropertySet.default_properties)

    @property
    def identity_shape(self) -> "RelationshipIdentityShape":
        return RelationshipIdentityShape(
            type=self.type, keys=tuple(self.key_values.keys())
        )

    def has_same_key(self, other: "Node") -> bool:
        return self.key_values == other.key_values

    def update(self, other: "Relationship"):
        self.properties.update(other.properties)

    def get_dedup_key(self) -> tuple:
        return tuple(sorted(self.key_values.values()))


@dataclass(slots=True)
class RelationshipWithNodes(DeduplicatableObject):
    """Stores information about the related node and the relationship itself."""

    from_node: Node
    to_node: Node
    relationship: Relationship

    to_side_match_strategy: MatchStrategy = MatchStrategy.EAGER
    from_side_match_strategy: MatchStrategy = MatchStrategy.EAGER

    def has_same_keys(self, other: "RelationshipWithNodes") -> bool:
        return (
            self.to_node.has_same_key(other.to_node)
            and self.from_node.has_same_key(other.from_node)
            and self.relationship.has_same_key(other.relationship)
        )

    def update(self, other: "RelationshipWithNodes"):
        self.to_node.properties.update(other.to_node.properties)
        self.from_node.properties.update(other.from_node.properties)
        self.relationship.properties.update(other.relationship.properties)

    def get_dedup_key(self) -> tuple:
        return (
            self.to_node.get_dedup_key(),
            self.from_node.get_dedup_key(),
            self.relationship.get_dedup_key(),
        )


@dataclass(slots=True, frozen=True)
class NodeIdentityShape:
    type: str
    keys: Tuple[str]
    additional_types: Tuple[str] = field(default_factory=tuple)


@dataclass(slots=True, frozen=True)
class RelationshipIdentityShape:
    type: str
    keys: Tuple[str]
