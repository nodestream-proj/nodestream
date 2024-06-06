from typing import Dict, Iterable, List, Optional, Set, Tuple

from ..state import GraphObjectSchema
from .operations import (
    AddAdditionalNodePropertyIndex,
    AddAdditionalRelationshipPropertyIndex,
    AddNodeProperty,
    AddRelationshipProperty,
    CreateNodeType,
    CreateRelationshipType,
    DropAdditionalNodePropertyIndex,
    DropAdditionalRelationshipPropertyIndex,
    DropNodeProperty,
    DropNodeType,
    DropRelationshipProperty,
    DropRelationshipType,
    NodeKeyExtended,
    NodeKeyPartRenamed,
    Operation,
    RelationshipKeyExtended,
    RelationshipKeyPartRenamed,
    RenameNodeProperty,
    RenameNodeType,
    RenameRelationshipProperty,
    RenameRelationshipType,
)
from .state_providers import StateProvider


class MigratorInput:
    """Asks questions as the schema change detector works.

    This class is used by the schema change detector to ask questions to the
    user as it works. The schema change detector will ask questions to the
    user when it is not sure about the answer.

    The default implementation of this class will always answer no to all
    questions.
    """

    def ask_yes_no(self, question: str) -> bool:
        """Ask a yes/no question.

        Args:
            question: The question to ask.

        Returns:
            True if the user answered yes, False if the user answered no.
        """
        return False

    def format_ask_type_renamed(self, old_type: str, new_type: str) -> str:
        """Format a question about a type being renamed.

        Args:
            old_type: The old type name.
            new_type: The new type name.

        Returns:
            The formatted question.
        """
        return f"Did you rename {old_type} to {new_type}?"

    def ask_type_renamed(self, old_type: str, new_type: str) -> bool:
        """Ask if a type was renamed.

        Args:
            old_type: The old type name.
            new_type: The new type name.

        Returns:
            True if the user answered yes, False if the user answered no.
        """
        question = self.format_ask_type_renamed(old_type, new_type)
        return self.ask_yes_no(question)

    def format_ask_property_renamed(
        self, object_type: str, old_property_name: str, new_property_name: str
    ) -> str:
        """Format a question about a property being renamed.

        Args:
            object_type: The type of the object that the property belongs to.
            old_property_name: The old property name.
            new_property_name: The new property name.

        Returns:
            The formatted question.
        """
        return f"Did you rename {old_property_name} to {new_property_name} on {object_type}?"

    def ask_property_renamed(
        self, object_type: str, old_property_name: str, new_property_name: str
    ) -> bool:
        """Ask if a property was renamed.

        Args:
            object_type: The type of the object that the property belongs to.
            old_property_name: The old property name.
            new_property_name: The new property name.

        Returns:
            True if the user answered yes, False if the user answered no.
        """
        question = self.format_ask_property_renamed(
            object_type, old_property_name, new_property_name
        )
        return self.ask_yes_no(question)

    def determine_renamed_types(
        self, presumed_added_types: Set[str], presumed_deleted_types: Set[str]
    ) -> Set[Tuple[str, str]]:
        """Determine which types have been renamed.

        Args:
            presumed_added_types: The types that are presumed to have been
                added which may have been renamed.
            presumed_deleted_types: The types that are presumed to have been
                deleted which may have been renamed.

        Returns:
            A set of tuples containing the old and new type names.
        """
        renamed = set()

        for new_type_name in presumed_added_types:
            for prev_type_name in presumed_deleted_types:
                if self.ask_type_renamed(prev_type_name, new_type_name):
                    renamed.add((prev_type_name, new_type_name))
                    break

        return renamed

    def determine_renamed_properties(
        self,
        object_type: str,
        presumed_added_properties: Set[str],
        presumed_deleted_properties: Set[str],
    ) -> Set[Tuple[str, str]]:
        """Determine which properties have been renamed.

        Args:
            presumed_added_properties: The properties that are presumed to have been
                added which may have been renamed.
            presumed_deleted_properties: The properties that are presumed to have been
                deleted which may have been renamed.

        Returns:
            A set of tuples containing the old and new property names.
        """
        renamed = set()

        for new_property_name in presumed_added_properties:
            for prev_property_name in presumed_deleted_properties:
                if self.ask_property_renamed(
                    object_type, prev_property_name, new_property_name
                ):
                    renamed.add((prev_property_name, new_property_name))
                    break

        return renamed


class TypePairing:
    """A pair of types from two different states.

    This class represents a pair of types from two different states. It
    is used to compare the two types and determine what has changed.
    """

    __slots__ = ("from_type", "to_type")

    def __init__(
        self,
        from_type: Optional[GraphObjectSchema],
        to_type: Optional[GraphObjectSchema],
    ) -> None:
        """Initialize the type bridge.

        Args:
            from_type: The name of the type before the migration.
            to_type: The name of the type after the migration.
        """
        self.from_type = from_type or GraphObjectSchema(name="")
        self.to_type = to_type or GraphObjectSchema(name="")

    def get_property_drift(
        self, input: MigratorInput
    ) -> Tuple[Set[str], Set[str], Set[Tuple[str, str]]]:
        """Get the property drift between the two types.

        Returns:
            A tuple containing the set of properties that were deleted, the
            set of properties that were added, and the set of properties that
            were renamed.
        """
        deleted = self.from_type.non_key_properties - self.to_type.non_key_properties
        added = self.to_type.non_key_properties - self.from_type.non_key_properties
        renamed = input.determine_renamed_properties(self.to_type.name, added, deleted)
        deleted = deleted - {item[0] for item in renamed}
        added = added - {item[1] for item in renamed}
        return deleted, added, renamed

    def get_key_drift(
        self, input: MigratorInput
    ) -> Tuple[Set[str], Set[str], Set[Tuple[str, str]]]:
        """Get the key drift between the two types.

        Returns:
            A tuple containing the set of keys that were deleted, the
            set of keys that were added, and the set of keys that
            were renamed.
        """
        deleted = self.from_type.keys - self.to_type.keys
        added = self.to_type.keys - self.from_type.keys
        renamed = input.determine_renamed_properties(self.to_type.name, added, deleted)
        deleted = deleted - {item[0] for item in renamed}
        added = added - {item[1] for item in renamed}
        return deleted, added, renamed

    def get_index_drift(self) -> Tuple[Set[str], Set[str]]:
        """Get the index drift between the two types.

        Returns:
            A tuple containing the set of indexed properties that were deleted,
            the set of indexed properties that were added, and the set of indexed
            properties that were renamed.
        """
        deleted = self.from_type.indexed_properties - self.to_type.indexed_properties
        added = self.to_type.indexed_properties - self.from_type.indexed_properties
        return deleted, added


class AutoChangeDetector:
    """Detects changes between two schemas.

    Detects changes between two schemas and produces a migration that
    can be used to migrate the database from the first schema to the
    second schema.
    """

    __slots__ = (
        "input",
        "from_state_provider",
        "to_state_provider",
        "from_state",
        "to_state",
        "new_node_types",
        "renamed_node_types",
        "deleted_node_types",
        "new_relationship_types",
        "renamed_relationship_types",
        "deleted_relationship_types",
        "deleted_relationship_properties",
        "renamed_relationship_properties",
        "added_relationship_properties",
        "deleted_node_properties",
        "renamed_node_properties",
        "added_node_properties",
        "deleted_relationship_keys",
        "renamed_relationship_keys",
        "added_relationship_keys",
        "deleted_node_keys",
        "renamed_node_keys",
        "added_node_keys",
        "added_node_property_indexes",
        "deleted_node_property_indexes",
        "added_relationship_property_indexes",
        "deleted_relationship_property_indexes",
    )

    def __init__(
        self,
        input: MigratorInput,
        from_state: StateProvider,
        to_state: StateProvider,
    ) -> None:
        """Initialize the autodetector.

        Args:
            input: Input for asking questions when detecting ambiguous changes.
            from_state: The state provider for the state before the migration.
            to_state: The state provider for the state after the migration.
        """
        self.input = input
        self.from_state_provider = from_state
        self.to_state_provider = to_state
        # Node Types
        self.new_node_types = set()
        self.renamed_node_types = set()
        self.deleted_node_types = set()
        # Relationship Types
        self.new_relationship_types = set()
        self.renamed_relationship_types = set()
        self.deleted_relationship_types = set()
        # Relationship Properties
        self.deleted_relationship_properties = set()
        self.renamed_relationship_properties = set()
        self.added_relationship_properties = set()
        # Node Properties
        self.deleted_node_properties = set()
        self.renamed_node_properties = set()
        self.added_node_properties = set()
        # Relationship Keys
        self.deleted_relationship_keys = set()
        self.renamed_relationship_keys = set()
        self.added_relationship_keys = set()
        # Node Keys
        self.deleted_node_keys = set()
        self.renamed_node_keys = set()
        self.added_node_keys = set()
        # Node Indexes
        self.added_node_property_indexes = set()
        self.deleted_node_property_indexes = set()
        # Relationship Indexes
        self.added_relationship_property_indexes = set()
        self.deleted_relationship_property_indexes = set()

    async def get_to_and_from_state(self):
        """Get the to and from state from the state providers."""
        self.from_state = await self.from_state_provider.get_schema()
        self.to_state = await self.to_state_provider.get_schema()

    async def detect_changes(self) -> List[Operation]:
        """Detect changes between the two states and return operations.

        Returns:
            A list of operations that can be used to migrate the database
            from the from_state to the to_state.
        """
        await self.get_to_and_from_state()
        self.detect_node_type_changes()
        self.detect_relationship_type_changes()
        self.detect_node_property_changes()
        self.detect_node_key_changes()
        self.detect_relationship_property_changes()
        self.detect_node_index_changes()
        self.detect_relationship_index_changes()
        self.detect_relationship_key_changes()

        operations = []
        # Update Nodes
        operations.extend(self.make_node_type_change_operations())
        operations.extend(self.make_node_property_change_operations())
        operations.extend(self.make_node_index_change_operations())
        operations.extend(self.make_node_key_change_operations())

        # Update Relationships
        operations.extend(self.make_relationship_type_change_operations())
        operations.extend(self.make_relationship_property_change_operations())
        operations.extend(self.make_relationship_index_change_operations())
        operations.extend(self.make_relationship_key_change_operations())

        return operations

    def make_node_key_change_operations(self) -> Iterable[Operation]:
        if self.deleted_node_keys:
            raise NotImplementedError("Deleting node keys is not supported yet.")

        for type, old_field, new_field in self.renamed_node_keys:
            yield NodeKeyPartRenamed(type, old_field, new_field)

        for type, new_field in self.added_node_keys:
            yield NodeKeyExtended(type, new_field, None)

    def make_relationship_key_change_operations(self) -> Iterable[Operation]:
        if self.deleted_relationship_keys:
            raise NotImplementedError(
                "Deleting relationship keys is not supported yet."
            )

        for type, old_field, new_field in self.renamed_relationship_keys:
            yield RelationshipKeyPartRenamed(type, old_field, new_field)

        for type, new_field in self.added_relationship_keys:
            yield RelationshipKeyExtended(type, new_field, None)

    def make_node_property_change_operations(self) -> Iterable[Operation]:
        for type, deleted_field in self.deleted_node_properties:
            if (type, deleted_field) in self.added_node_keys:
                continue
            yield DropNodeProperty(type, deleted_field)

        for type, old_field, new_field in self.renamed_node_properties:
            yield RenameNodeProperty(type, old_field, new_field)

        for type, new_field in self.added_node_properties:
            yield AddNodeProperty(type, new_field)

    def make_relationship_property_change_operations(self) -> Iterable[Operation]:
        for type, deleted_field in self.deleted_relationship_properties:
            if (type, deleted_field) in self.added_relationship_keys:
                continue
            yield DropRelationshipProperty(type, deleted_field)

        for type, old_field, new_field in self.renamed_relationship_properties:
            yield RenameRelationshipProperty(type, old_field, new_field)

        for type, new_field in self.added_relationship_properties:
            yield AddRelationshipProperty(type, new_field)

    def make_node_type_change_operations(self) -> Iterable[Operation]:
        for deleted_type in self.deleted_node_types:
            yield DropNodeType(deleted_type)

        for old_type, new_type in self.renamed_node_types:
            yield RenameNodeType(old_type, new_type)

        for new_type in self.new_node_types:
            type_def = self.to_state.get_node_type_by_name(new_type)
            yield CreateNodeType(
                name=new_type,
                keys=type_def.keys,
                properties=type_def.non_key_properties,
            )

    def make_relationship_type_change_operations(self) -> Iterable[Operation]:
        for deleted_type in self.deleted_relationship_types:
            yield DropRelationshipType(deleted_type)

        for old_type, new_type in self.renamed_relationship_types:
            yield RenameRelationshipType(old_type, new_type)

        for new_type in self.new_relationship_types:
            type_def = self.to_state.get_relationship_type_by_name(new_type)
            yield CreateRelationshipType(
                name=new_type,
                keys=type_def.keys,
                properties=type_def.non_key_properties,
            )

    def make_relationship_index_change_operations(self) -> Iterable[Operation]:
        for deleted_type, deleted_field in self.deleted_relationship_property_indexes:
            yield DropAdditionalRelationshipPropertyIndex(deleted_type, deleted_field)

        for new_type, new_field in self.added_relationship_property_indexes:
            yield AddAdditionalRelationshipPropertyIndex(new_type, new_field)

    def make_node_index_change_operations(self) -> Iterable[Operation]:
        for deleted_type, deleted_field in self.deleted_node_property_indexes:
            yield DropAdditionalNodePropertyIndex(deleted_type, deleted_field)

        for new_type, new_field in self.added_node_property_indexes:
            yield AddAdditionalNodePropertyIndex(new_type, new_field)

    def detect_node_key_changes(self):
        for pair in self.get_node_pairs():
            deleted_keys, added_keys, renamed_keys = pair.get_key_drift(self.input)
            for key in deleted_keys:
                self.deleted_node_keys.add((pair.to_type.name, key))
            for old, new in renamed_keys:
                self.renamed_node_keys.add((pair.to_type.name, old, new))
            for key in added_keys:
                self.added_node_keys.add((pair.to_type.name, key))

    def detect_relationship_key_changes(self):
        for pair in self.get_relationship_pairs():
            deleted_keys, added_keys, renamed_keys = pair.get_key_drift(self.input)
            for key in deleted_keys:
                self.deleted_relationship_keys.add((pair.to_type.name, key))
            for old, new in renamed_keys:
                self.renamed_relationship_keys.add((pair.to_type.name, old, new))
            for key in added_keys:
                self.added_relationship_keys.add((pair.to_type.name, key))

    def detect_node_type_changes(self):
        self.deleted_node_types, self.new_node_types = self.from_state.diff_node_types(
            self.to_state
        )

        self.renamed_node_types = self.input.determine_renamed_types(
            self.new_node_types, self.deleted_node_types
        )

        for previous, new in self.renamed_node_types:
            self.deleted_node_types.remove(previous)
            self.new_node_types.remove(new)

    def detect_relationship_type_changes(self):
        (
            self.deleted_relationship_types,
            self.new_relationship_types,
        ) = self.from_state.diff_relationship_types(self.to_state)

        self.renamed_relationship_types = self.input.determine_renamed_types(
            self.new_relationship_types, self.deleted_relationship_types
        )

        for previous, new in self.renamed_relationship_types:
            self.deleted_relationship_types.remove(previous)
            self.new_relationship_types.remove(new)

    def detect_node_property_changes(self):
        for pair in self.get_node_pairs():
            (
                deleted_properties,
                added_properties,
                renamed_properties,
            ) = pair.get_property_drift(self.input)
            for prop in deleted_properties:
                self.deleted_node_properties.add((pair.to_type.name, prop))
            for prop in added_properties:
                self.added_node_properties.add((pair.to_type.name, prop))
            for old, new in renamed_properties:
                self.renamed_node_properties.add((pair.to_type.name, old, new))

    def detect_relationship_property_changes(self):
        for pair in self.get_relationship_pairs():
            (
                deleted_properties,
                added_properties,
                renamed_properties,
            ) = pair.get_property_drift(self.input)
            for prop in deleted_properties:
                self.deleted_relationship_properties.add((pair.to_type.name, prop))
            for prop in added_properties:
                self.added_relationship_properties.add((pair.to_type.name, prop))
            for old, new in renamed_properties:
                self.renamed_relationship_properties.add((pair.to_type.name, old, new))

    def detect_node_index_changes(self):
        for pair in self.get_node_pairs(ignore_created_and_deleted=False):
            deleted_indexes, added_indexes = pair.get_index_drift()
            self.deleted_node_property_indexes.update(
                (pair.to_type.name, idx) for idx in deleted_indexes
            )
            self.added_node_property_indexes.update(
                (pair.to_type.name, idx) for idx in added_indexes
            )

    def detect_relationship_index_changes(self):
        for pair in self.get_relationship_pairs(ignore_created_and_deleted=False):
            deleted_indexes, added_indexes = pair.get_index_drift()
            self.deleted_relationship_property_indexes.update(
                (pair.to_type.name, idx) for idx in deleted_indexes
            )
            self.added_relationship_property_indexes.update(
                (pair.to_type.name, idx) for idx in added_indexes
            )

    def get_node_pairs(
        self, ignore_created_and_deleted: bool = True
    ) -> Iterable[TypePairing]:
        return self.compare_types_pairwise(
            self.from_state.nodes_by_name,
            self.to_state.nodes_by_name,
            self.deleted_node_types,
            self.new_node_types,
            self.renamed_node_types,
            ignore_created_and_deleted,
        )

    def get_relationship_pairs(
        self, ignore_created_and_deleted: bool = True
    ) -> Iterable[TypePairing]:
        return self.compare_types_pairwise(
            self.from_state.relationships_by_name,
            self.to_state.relationships_by_name,
            self.deleted_relationship_types,
            self.new_relationship_types,
            self.renamed_relationship_types,
            ignore_created_and_deleted,
        )

    def compare_types_pairwise(
        self,
        from_population: Dict[str, GraphObjectSchema],
        to_population: Dict[str, GraphObjectSchema],
        deleted_types: Set[str],
        new_types: Set[str],
        renamed_types: Set[Tuple[str, str]],
        ignore_created_and_deleted: bool,
    ) -> Iterable[TypePairing]:
        all_types = set(from_population).union(to_population)
        for type in all_types:
            # If the type is new or deleted, we can ignore it.
            new_or_removed = type in new_types or type in deleted_types
            if new_or_removed and ignore_created_and_deleted:
                continue

            # If the type has been renamed, we can compare the old an
            # new type definitions. We only want to consider the type once,
            # so we'll ignore it if type is the old value.
            renamed_from = next(
                (old for old, new in renamed_types if new == type),
                None,
            )
            renamed_to = next(
                (new for old, new in renamed_types if old == type),
                None,
            )
            if renamed_to:
                continue

            # If the type was renamed, we can inform the property
            # diffing logic the two types. If it wasn't, it can be the
            # same name.
            if renamed_from:
                yield TypePairing(
                    from_type=from_population.get(renamed_from),
                    to_type=to_population.get(type),
                )
            else:
                yield TypePairing(
                    from_type=from_population.get(type),
                    to_type=to_population.get(type),
                )
