import re
from dataclasses import asdict, dataclass
from typing import Any, List, Optional, Set

from ...file_io import LoadsFromYaml, SavesToYaml
from ...subclass_registry import SubclassRegistry
from ..state import GraphObjectSchema

# TODO: Future enhancements w/ new operations or changing old ones:
#
#   1.) Dropping a part of a key to a node or relationship:
#
#       This is hard because of the case (which is fairly likely) that
#       the removal of the key takes a node or relationship from being
#       unique to no longer being unique. How do we cope with that?
#       Do we merge the nodes together? Do we make the user drop the data
#       and reingest it with these new rules? There are multiple
#       possibilities here that require both design and consideration
#       that we want to defer for now.
#
#   2.) Providing defaults dynamically:
#
#       In some ways related to (1), this is hard because we don't have
#       a formal of a data model coming back from the database.
#       This ins't an ORM and it seems  that to be able to do this in
#       both a database-agnostic and intuitive way, we'd need to almost
#       build that in such a way.
#

# We need to ignore overrides because we want to be able to
# use `dataclass` to define the operations and then register
# them with the registry. If we don't ignore overrides, then
# we'll get an error when we try to register the same class
# twice (Which happens when we use `dataclass` as a decorator)
OPERATION_SUBCLASS_REGISTRY = SubclassRegistry(ignore_overrides=True)
CAMEL_TO_SNAKE_REGEX = re.compile(r"(?<!^)(?=[A-Z])")


def camel_case_to_snake_case(camel_case: str) -> str:
    """Convert a string from camel case to snake case.

    Args:
        camel_case: The camel case string.

    Returns:
        The snake case string.
    """
    return CAMEL_TO_SNAKE_REGEX.sub("_", camel_case).lower()


@OPERATION_SUBCLASS_REGISTRY.connect_baseclass
class Operation(LoadsFromYaml, SavesToYaml):
    """Base class for all operations.

    Operations are used to migrate the database schema from one
    version to another. Each type of operation is responsible for
    migrating a specific change to the schema.
    """

    def type_as_snake_case(self) -> str:
        operation_type_name = type(self).__name__
        return camel_case_to_snake_case(operation_type_name)

    def suggest_migration_name_slug(self) -> str:
        """Suggest a migration name slug for this operation.

        Returns:
            A migration name slug.
        """
        return "_".join(self.describe().lower().split(" "))

    def describe(self) -> str:
        return self.type_as_snake_case()

    @classmethod
    def from_file_data(cls, data):
        op_type = data["operation"]
        arguments = data["arguments"]
        operation_cls = OPERATION_SUBCLASS_REGISTRY.get(op_type)
        return operation_cls(**arguments)

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Schema

        return Schema({"operation": str, "arguments": dict})

    def to_file_data(self):
        return {
            "operation": OPERATION_SUBCLASS_REGISTRY.name_for(self.__class__),
            "arguments": asdict(self),
        }

    def reduce(self, other: "Operation") -> List["Operation"]:
        """Reduce this operation with another operation.

        This method should be implemented by subclasses to reduce this
        operation with another operation. The result of this operation should
        be a list of operations that are equivalent to applying this operation
        and then the other operation. If the operations cannot be reduced, then
        this method should return both operations in a list.
        """
        return [self] if self == other else [self, other]

    @classmethod
    def optimize(cls, operations: List["Operation"]) -> List["Operation"]:
        """Optimize a list of operations.

        This method should be implemented by subclasses to optimize a list of
        operations. The result of this operation should be a list of operations
        that are equivalent to applying all of the operations in the input list.
        """
        return OperationReducer(operations).reduce()


class OperationReducer:
    """A class that can reduce a list of operations."""

    def __init__(self, operations: List[Operation]):
        self.operations = operations

    def reduce(self) -> List[Operation]:
        """Reduce the list of operations.

        Returns:
            A list of operations that are equivalent to applying all of the
            operations in the input list.
        """
        current = self.operations
        while True:
            reduced = self.reduce_once(current)
            if reduced == current:
                break
            current = reduced
        return current

    def reduce_once(self, operations: List[Operation]) -> List[Operation]:
        # We are going to go from back to front and
        # reduce exactly one operation per iteration.
        for i, operation in enumerate(reversed(operations)):
            others = operations[: len(operations) - i - 1]
            for other in reversed(others):
                reduced = operation.reduce(other)
                if len(reduced) < 2:
                    unchanged = [o for o in operations if o not in (operation, other)]
                    return unchanged + reduced

        return operations


@dataclass(frozen=True, slots=True)
class CreateNodeType(Operation):
    """Create a node of a given type.

    The underlying database connector should use the keys to
    create a uniqueness constraint or index. The keys are used
    to determine if a node already exists.

    Attributes:
        name: The name of the node type.
        keys: The keys of the node type.
        properties: The properties of the node type.
    """

    name: str
    keys: List[str]
    properties: List[str]

    @property
    def proposed_index_name(self) -> str:
        return f"{self.name}_node_key"

    def describe(self) -> str:
        return f"Create node type {self.name}"

    def as_node_type(self) -> GraphObjectSchema:
        schema = GraphObjectSchema(self.name)
        schema.add_keys(self.keys)
        schema.add_properties(self.properties)
        return schema

    def reduce(self, other: Operation) -> List[Operation]:
        if isinstance(other, DropNodeType) and other.name == self.name:
            return []
        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class CreateRelationshipType(Operation):
    """Create a relationship of a given type.

    The underlying database connector should use the keys to
    create a uniqueness constraint or index. The keys are used
    to determine if a relationship already exists (between two nodes).

    Attributes:
        name: The name of the relationship type.
        keys: The keys of the relationship type.
        properties: The properties of the relationship type.
    """

    name: str
    keys: Set[str]
    properties: Set[str]

    @property
    def proposed_index_name(self) -> str:
        return f"{self.name}_relationship_key"

    def describe(self) -> str:
        return f"Create relationship type {self.name}"

    def as_relationship_type(self) -> GraphObjectSchema:
        schema = GraphObjectSchema(self.name)
        schema.add_keys(self.keys)
        schema.add_properties(self.properties)
        return schema

    def reduce(self, other: Operation) -> List[Operation]:
        if isinstance(other, DropRelationshipType) and other.name == self.name:
            return []
        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class DropNodeType(Operation):
    """Drop a node type.

    The underlying database connector should drop the node type
    and all nodes of that type. The database connector should
    also drop any relationships that are connected to the nodes
    of that type. The database connector should also drop any
    related indexes or constraints.

    Attributes:
        name: The name of the node type.
    """

    name: str

    @property
    def proposed_index_name(self) -> str:
        return f"{self.name}_node_key"

    def describe(self) -> str:
        return f"Drop node type {self.name}"

    def reduce(self, other: Operation) -> List[Operation]:
        if isinstance(other, CreateNodeType) and other.name == self.name:
            return []

        if (
            isinstance(other, (AddNodeProperty, DropNodeProperty, RenameNodeProperty))
            and other.node_type == self.name
        ):
            return [self]

        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class DropRelationshipType(Operation):
    """Drop a relationship type.

    The underlying database connector should drop the relationship
    type and all relationships of that type. The database connector
    should also drop any related indexes or constraints.

    Attributes:
        name: The name of the relationship type.
    """

    name: str

    def describe(self) -> str:
        return f"Drop relationship {self.name}"

    def reduce(self, other: Operation) -> List[Operation]:
        if isinstance(other, CreateRelationshipType) and other.name == self.name:
            return []

        if (
            isinstance(
                other,
                (
                    AddRelationshipProperty,
                    DropRelationshipProperty,
                    RenameRelationshipProperty,
                ),
            )
            and other.relationship_type == self.name
        ):
            return [self]

        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class RenameNodeProperty(Operation):
    """Rename a node property.

    The underlying database connector should rename the property on all nodes
    of the given type. The database connector should also rename any related
    indexes or constraints.

    Attributes:
        node_type: The type of the node.
        old_property_name: The old name of the property that is being renamed.
        new_property_name: The new name of the property that is being renamed.
    """

    node_type: str
    old_property_name: str
    new_property_name: str

    def describe(self) -> str:
        return f"Rename node property {self.old_property_name} to {self.new_property_name} on node type {self.node_type}"

    def reduce(self, other: Operation) -> List[Operation]:
        # If we add and then rename, we can just add with the new name.
        # if we rename and then drop, we can just drop the old name.
        if (
            isinstance(other, AddNodeProperty)
            and other.node_type == self.node_type
            and other.property_name == self.old_property_name
        ):
            return [
                AddNodeProperty(self.node_type, self.new_property_name, other.default)
            ]
        if (
            isinstance(other, DropNodeProperty)
            and other.node_type == self.node_type
            and other.property_name == self.old_property_name
        ):
            return [DropNodeProperty(self.node_type, self.old_property_name)]

        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class RenameRelationshipProperty(Operation):
    """Rename a relationship property.

    The underlying database connector should rename the property on all
    relationships of the given type. The database connector should also
    rename any related indexes or constraints.

    Attributes:
        relationship_type: The type of the relationship.
        old_property_name: The old name of the property that is being renamed.
        new_property_name: The new name of the property that is being renamed.
    """

    relationship_type: str
    old_property_name: str
    new_property_name: str

    def describe(self) -> str:
        return f"Rename relationship property {self.old_property_name} to {self.new_property_name} on relationship type {self.relationship_type}"

    def reduce(self, other: Operation) -> List[Operation]:
        # If we add and then rename, we can just add with the new name.
        # if we rename and then drop, we can just drop the old name.
        if (
            isinstance(other, AddRelationshipProperty)
            and other.relationship_type == self.relationship_type
            and other.property_name == self.old_property_name
        ):
            return [
                AddRelationshipProperty(
                    self.relationship_type, self.new_property_name, other.default
                )
            ]

        if (
            isinstance(other, DropRelationshipProperty)
            and other.relationship_type == self.relationship_type
            and other.property_name == self.old_property_name
        ):
            return [
                DropRelationshipProperty(self.relationship_type, self.old_property_name)
            ]

        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class RenameNodeType(Operation):
    """Rename a node type.

    The underlying database connector should rename the node type and all nodes
    of that type. The database connector should also rename any related indexes
    or constraints.

    Attributes:
        old_type: The old name of the node type.
        new_type: The new name of the node type.
    """

    old_type: str
    new_type: str

    @property
    def old_proposed_index_name(self) -> str:
        return f"{self.old_type}_node_key"

    @property
    def new_proposed_index_name(self) -> str:
        return f"{self.new_type}_node_key"

    def describe(self) -> str:
        return f"Rename node type {self.old_type} to {self.new_type}"

    def reduce(self, other: Operation) -> List[Operation]:
        # If we create and then rename, we can just create with the new name.
        # If we rename and then drop, we can just drop the old name.
        if isinstance(other, CreateNodeType) and other.name == self.old_type:
            return [CreateNodeType(self.new_type, other.keys, other.properties)]

        if isinstance(other, DropNodeType) and other.name == self.new_type:
            return [DropNodeType(self.old_type)]

        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class RenameRelationshipType(Operation):
    """Rename a relationship type.

    The underlying database connector should rename the relationship type and
    all relationships of that type. The database connector should also rename
    any related indexes or constraints.

    Attributes:
        old_type: The old name of the relationship type.
        new_type: The new name of the relationship type.
    """

    old_type: str
    new_type: str

    def describe(self) -> str:
        return f"Rename relationship type {self.old_type} to {self.new_type}"

    def reduce(self, other: Operation) -> List[Operation]:
        # If we create and then rename, we can just create with the new name.
        # If we rename and then drop, we can just drop the old name.
        if isinstance(other, CreateRelationshipType) and other.name == self.old_type:
            return [CreateRelationshipType(self.new_type, other.keys, other.properties)]

        if isinstance(other, DropRelationshipType) and other.name == self.new_type:
            return [DropRelationshipType(self.old_type)]

        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class AddAdditionalNodePropertyIndex(Operation):
    """Add a supplemental field index.

    The underlying database connector should add an index for the given field.

    Attributes:
        node_type: The type of the node.
        field_name: The name of the field that should be indexed.
    """

    node_type: str
    field_name: str

    @property
    def proposed_index_name(self) -> str:
        return f"{self.node_type}_{self.field_name}_additional_index"

    def describe(self) -> str:
        return f"Add additional index for node type {self.node_type} on field {self.field_name}"


@dataclass(frozen=True, slots=True)
class DropAdditionalNodePropertyIndex(Operation):
    """Drop a supplemental field index.

    The underlying database connector should drop an index for the given field.

    Attributes:
        node_type: The type of the node.
        field_name: The name of the field that shouldn't be indexed.
    """

    node_type: str
    field_name: str

    @property
    def proposed_index_name(self) -> str:
        return f"{self.node_type}_{self.field_name}_additional_index"

    def describe(self) -> str:
        return f"Drop additional index for node type {self.node_type} on field {self.field_name}"


@dataclass(frozen=True, slots=True)
class AddAdditionalRelationshipPropertyIndex(Operation):
    """Add a supplemental field index.

    The underlying database connector should add an index for the given field.

    Attributes:
        relationship_type: The type of the relationship.
        field_name: The name of the field that should be indexed.
    """

    relationship_type: str
    field_name: str

    @property
    def proposed_index_name(self) -> str:
        return f"{self.relationship_type}_{self.field_name}_additional_index"

    def describe(self) -> str:
        return f"Add additional index for relationship type {self.relationship_type} on field {self.field_name}"


@dataclass(frozen=True, slots=True)
class DropAdditionalRelationshipPropertyIndex(Operation):
    """Drop a supplemental field index.

    The underlying database connector should drop an index for the given field.

    Attributes:
        relationship_type: The type of the relationship.
        field_name: The name of the field that shouldn't be indexed.
    """

    relationship_type: str
    field_name: str

    @property
    def proposed_index_name(self) -> str:
        return f"{self.relationship_type}_{self.field_name}_additional_index"

    def describe(self) -> str:
        return f"Drop additional index for relationship type {self.relationship_type} on field {self.field_name}"


@dataclass(frozen=True, slots=True)
class AddNodeProperty(Operation):
    """Add a property to a node type.

    The underlying database connector should add the property to all nodes of
    the given type. If a default is provided, the database connector should
    populate the property with the default value for all nodes of the
    given type.

    Attributes:
        node_type: The type of the node.
        property_name: The name of the property that is being added.
        default: The default value for the property.
    """

    node_type: str
    property_name: str
    default: Optional[Any] = None

    def describe(self) -> str:
        return f"Add property {self.property_name} to node type {self.node_type}"

    def reduce(self, other: Operation) -> List[Operation]:
        # If we add and then rename, we can just add with the new name.
        # If we add and then drop, we can just no-op.
        if (
            isinstance(other, RenameNodeProperty)
            and other.node_type == self.node_type
            and other.old_property_name == self.property_name
        ):
            return [
                AddNodeProperty(self.node_type, other.new_property_name, self.default)
            ]

        if (
            isinstance(other, DropNodeProperty)
            and other.node_type == self.node_type
            and other.property_name == self.property_name
        ):
            return []

        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class AddRelationshipProperty(Operation):
    """Add a property to a relationship type.

    The underlying database connector should add the property to all
    relationships of the given type. If a default is provided, the database
    connector should populate the property with the default value for all
    relationships of the given type.

    Attributes:
        relationship_type: The type of the relationship.
        property_name: The name of the property that is being added.
        default: The default value for the property.
    """

    relationship_type: str
    property_name: str
    default: Optional[Any] = None

    def describe(self) -> str:
        return f"Add property {self.property_name} to relationship type {self.relationship_type}"

    def reduce(self, other: Operation) -> List[Operation]:
        # If we add and then rename, we can just add with the new name.
        # If we add and then drop, we can just no-op.
        if (
            isinstance(other, RenameRelationshipProperty)
            and other.relationship_type == self.relationship_type
            and other.old_property_name == self.property_name
        ):
            return [
                AddRelationshipProperty(
                    self.relationship_type, other.new_property_name, self.default
                )
            ]

        if (
            isinstance(other, DropRelationshipProperty)
            and other.relationship_type == self.relationship_type
            and other.property_name == self.property_name
        ):
            return []

        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class DropNodeProperty(Operation):
    """Drop a property from a node type.

    The underlying database connector should drop the property from all nodes
    of the given type.

    Attributes:
        node_type: The type of the node.
        property_name: The name of the property that is being dropped.
    """

    node_type: str
    property_name: str

    def describe(self) -> str:
        return f"Drop property {self.property_name} from node type {self.node_type}"

    def reduce(self, other: Operation) -> List[Operation]:
        # If we add and then drop, we can just no-op.
        # If we rename and then drop, we can just drop the original name.
        if (
            isinstance(other, AddNodeProperty)
            and other.node_type == self.node_type
            and other.property_name == self.property_name
        ):
            return []

        if (
            isinstance(other, RenameNodeProperty)
            and other.node_type == self.node_type
            and other.old_property_name == self.property_name
        ):
            return [self]

        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class DropRelationshipProperty(Operation):
    """Drop a property from a relationship type.

    The underlying database connector should drop the property from all
    relationships of the given type.

    Attributes:
        relationship_type: The type of the relationship.
        property_name: The name of the property that is being dropped.
    """

    relationship_type: str
    property_name: str

    def describe(self) -> str:
        return f"Drop property {self.property_name} from relationship type {self.relationship_type}"

    def reduce(self, other: Operation) -> List[Operation]:
        # If we add and then drop, we can just no-op.
        # If we rename and then drop, we can just drop the original name.
        if (
            isinstance(other, AddRelationshipProperty)
            and other.relationship_type == self.relationship_type
            and other.property_name == self.property_name
        ):
            return []

        if (
            isinstance(other, RenameRelationshipProperty)
            and other.relationship_type == self.relationship_type
            and other.old_property_name == self.property_name
        ):
            return [self]

        return Operation.reduce(self, other)


@dataclass(frozen=True, slots=True)
class NodeKeyExtended(Operation):
    """Add a element to a node key.

    The underlying database connector should add the key to all nodes of the
    given type. Adefault will be provided, so the database connector should
    populate the key with the default value for all nodes of the given type.

    Attributes:
        node_type: The type of the node.
        added_key_property: The name of the property that is being added.
        default: The default value for the property.
    """

    node_type: str
    added_key_property: str
    default: Any

    @property
    def proposed_index_name(self) -> str:
        return f"{self.node_type}_node_key"

    def describe(self) -> str:
        return f"Extend key {self.added_key_property} on node type {self.node_type}"


@dataclass(frozen=True, slots=True)
class RelationshipKeyExtended(Operation):
    """Add a element to a relationship key.

    The underlying database connector should add the key to all relationships
    of the given type. A default will be provided, so the database connector
    should populate the key with the default value for all relationships of
    the given type.

    Attributes:
        relationship_type: The type of the relationship.
        added_key_property: The name of the property that is being added.
        default: The default value for the property.
    """

    relationship_type: str
    added_key_property: str
    default: Any

    def describe(self) -> str:
        return f"Extend key {self.added_key_property} on relationship type {self.relationship_type}"


@dataclass(frozen=True, slots=True)
class NodeKeyPartRenamed(Operation):
    """Rename a part of a node key.

    The underlying database connector should rename the key part on all nodes
    of the given type. The database connector should also rename any related
    indexes or constraints.

    Attributes:
        node_type: The type of the node.
        old_key_part_name: The old name of the property that is being renamed.
        new_key_part_name: The new name of the property that is being renamed.
    """

    node_type: str
    old_key_part_name: str
    new_key_part_name: str

    @property
    def proposed_index_name(self) -> str:
        return f"{self.node_type}_node_key"

    def describe(self) -> str:
        return f"Rename key part {self.old_key_part_name} to {self.new_key_part_name} on node type {self.node_type}"


@dataclass(frozen=True, slots=True)
class RelationshipKeyPartRenamed(Operation):
    """Rename a part of a relationship key.

    The underlying database connector should rename the key part on all
    relationships of the given type. The database connector should also rename
    any related indexes or constraints.

    Attributes:
        relationship_type: The type of the relationship.
        old_key_part_name: The old name of the property that is being renamed.
        new_key_part_name: The new name of the property that is being renamed.
    """

    relationship_type: str
    old_key_part_name: str
    new_key_part_name: str

    def describe(self) -> str:
        return f"Rename key part {self.old_key_part_name} to {self.new_key_part_name} on relationship type {self.relationship_type}"
