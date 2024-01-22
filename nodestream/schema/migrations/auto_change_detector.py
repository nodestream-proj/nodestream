from typing import Dict, Iterable, List, Set, Tuple

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
            yield DropNodeProperty(type, deleted_field)

        for type, old_field, new_field in self.renamed_node_properties:
            yield RenameNodeProperty(type, old_field, new_field)

        for type, new_field in self.added_node_properties:
            yield AddNodeProperty(type, new_field)

    def make_relationship_property_change_operations(self) -> Iterable[Operation]:
        for type, deleted_field in self.deleted_relationship_properties:
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
        self.detect_property_changes(
            from_population=self.from_state.nodes_by_name,
            to_population=self.to_state.nodes_by_name,
            deleted_property_location=self.deleted_node_keys,
            renamed_property_location=self.renamed_node_keys,
            added_property_loction=self.added_node_keys,
            deleted_types_location=self.deleted_node_types,
            renamed_types_location=self.renamed_node_types,
            new_types_location=self.new_node_types,
            properties_attr="keys",
        )

    def detect_relationship_key_changes(self):
        self.detect_property_changes(
            from_population=self.from_state.relationships_by_name,
            to_population=self.to_state.relationships_by_name,
            deleted_property_location=self.deleted_relationship_keys,
            renamed_property_location=self.renamed_relationship_keys,
            added_property_loction=self.added_relationship_keys,
            deleted_types_location=self.deleted_relationship_types,
            renamed_types_location=self.renamed_relationship_types,
            new_types_location=self.new_relationship_types,
            properties_attr="keys",
        )

    def detect_node_type_changes(self):
        self.detect_type_changes(
            from_population=self.from_state.nodes_by_name,
            to_population=self.to_state.nodes_by_name,
            deleted_types_location=self.deleted_node_types,
            renamed_types_location=self.renamed_node_types,
            new_types_location=self.new_node_types,
        )

    def detect_relationship_type_changes(self):
        self.detect_type_changes(
            from_population=self.from_state.relationships_by_name,
            to_population=self.to_state.relationships_by_name,
            deleted_types_location=self.deleted_relationship_types,
            renamed_types_location=self.renamed_relationship_types,
            new_types_location=self.new_relationship_types,
        )

    def detect_node_property_changes(self):
        self.detect_property_changes(
            from_population=self.from_state.nodes_by_name,
            to_population=self.to_state.nodes_by_name,
            deleted_property_location=self.deleted_node_properties,
            renamed_property_location=self.renamed_node_properties,
            added_property_loction=self.added_node_properties,
            deleted_types_location=self.deleted_node_types,
            renamed_types_location=self.renamed_node_types,
            new_types_location=self.new_node_types,
        )

    def detect_relationship_property_changes(self):
        self.detect_property_changes(
            from_population=self.from_state.relationships_by_name,
            to_population=self.to_state.relationships_by_name,
            deleted_property_location=self.deleted_relationship_properties,
            renamed_property_location=self.renamed_relationship_properties,
            added_property_loction=self.added_relationship_properties,
            deleted_types_location=self.deleted_relationship_types,
            renamed_types_location=self.renamed_relationship_types,
            new_types_location=self.new_relationship_types,
        )

    def detect_node_index_changes(self):
        self.detect_index_changes(
            from_population=self.from_state.nodes_by_name,
            to_population=self.to_state.nodes_by_name,
            added_index_location=self.added_node_property_indexes,
            deleted_index_location=self.deleted_node_property_indexes,
            deleted_types_location=self.deleted_node_types,
            renamed_types_location=self.renamed_node_types,
            new_types_location=self.new_node_types,
        )

    def detect_relationship_index_changes(self):
        self.detect_index_changes(
            from_population=self.from_state.relationships_by_name,
            to_population=self.to_state.relationships_by_name,
            added_index_location=self.added_relationship_property_indexes,
            deleted_index_location=self.deleted_relationship_property_indexes,
            deleted_types_location=self.deleted_relationship_types,
            renamed_types_location=self.renamed_relationship_types,
            new_types_location=self.new_relationship_types,
        )

    def compare_types_pairwise(
        self,
        from_population: Dict[str, GraphObjectSchema],
        to_population: Dict[str, GraphObjectSchema],
        deleted_types_location: Set[str],
        new_types_location: Set[str],
        renamed_types_location: Set[Tuple[str, str]],
        ignore_created_and_deleted: bool = True,
    ) -> Iterable[Tuple[str, str]]:
        all_types = set(from_population).union(to_population)
        for type in all_types:
            # If the type is new or deleted, we can ignore it.
            new_or_removed = (
                type in new_types_location or type in deleted_types_location
            )
            if new_or_removed and ignore_created_and_deleted:
                continue

            # If the type has been renamed, we can compare the old an
            # new type definitions. We only want to consider the type once,
            # so we'll ignore it if type is the old value.
            renamed_from = next(
                (old for old, new in renamed_types_location if new == type),
                None,
            )
            renamed_to = next(
                (new for old, new in renamed_types_location if old == type),
                None,
            )
            if renamed_to:
                continue

            # If the type was renamed, we can inform the property
            # diffing logic the two types. If it wasn't, it can be the
            # same name.
            if renamed_from:
                from_type_name, to_type_name = renamed_from, type
            else:
                from_type_name = to_type_name = type

            yield from_type_name, to_type_name

    def detect_index_changes(
        self,
        from_population: Dict[str, GraphObjectSchema],
        to_population: Dict[str, GraphObjectSchema],
        added_index_location: Set[Tuple[str, str]],
        deleted_index_location: Set[Tuple[str, str]],
        deleted_types_location: Set[str],
        renamed_types_location: Set[str],
        new_types_location: Set[Tuple[str, str]],
    ):
        def get_indexed_properties_or_empty_set(
            type_name, population: Dict[str, GraphObjectSchema]
        ):
            if type_name in population:
                return population[type_name].indexed_properties
            else:
                return set()

        def detect_changes_between_types(from_type_name, to_type_name):
            from_type_indexes = get_indexed_properties_or_empty_set(
                from_type_name, from_population
            )
            to_type_indexes = get_indexed_properties_or_empty_set(
                to_type_name, to_population
            )
            added_indexes = to_type_indexes - from_type_indexes
            deleted_indexes = from_type_indexes - to_type_indexes
            added_index_location.update((to_type_name, idx) for idx in added_indexes)
            deleted_index_location.update(
                (to_type_name, idx) for idx in deleted_indexes
            )

        for from_type_name, to_type_name in self.compare_types_pairwise(
            from_population,
            to_population,
            deleted_types_location,
            new_types_location,
            renamed_types_location,
            ignore_created_and_deleted=False,
        ):
            detect_changes_between_types(from_type_name, to_type_name)

    def detect_property_changes(
        self,
        from_population: Dict[str, GraphObjectSchema],
        to_population: Dict[str, GraphObjectSchema],
        deleted_property_location: Set[Tuple[str, str]],
        renamed_property_location: Set[Tuple[str, str, str]],
        added_property_loction: Set[Tuple[str, str]],
        deleted_types_location: Set[str],
        renamed_types_location: Set[str],
        new_types_location: Set[Tuple[str, str]],
        properties_attr: str = "non_key_properties",
    ):
        def detect_changes_between_types(from_type_name, to_type_name):
            from_type_props = getattr(from_population[from_type_name], properties_attr)
            to_type_props = getattr(to_population[to_type_name], properties_attr)
            # To start, we assume that all properties missing from one side or
            # the other were created or deleted.
            deleted = from_type_props - to_type_props
            added = to_type_props - from_type_props
            renamed = set()

            # We don't have any smart way to ascertain what is probably
            # a rename, so we are just going to have to ask about every
            # combination of properties
            for added_property in added:
                for deleted_property in deleted:
                    if self.input.ask_property_renamed(
                        to_type_name, deleted_property, added_property
                    ):
                        renamed.add((to_type_name, deleted_property, added_property))

            # For renamed properties, we can remove them from the
            # added and removed sets and add it to to the final renamed set.
            for item in renamed:
                _, deleted_property, added_property = item
                deleted.remove(deleted_property)
                added.remove(added_property)
                renamed_property_location.add(item)

            # We can add the type and the property to the final set once
            # we have removed renamed properties.
            for property in added:
                added_property_loction.add((to_type_name, property))
            for property in deleted:
                deleted_property_location.add((to_type_name, property))

        for from_type_name, to_type_name in self.compare_types_pairwise(
            from_population,
            to_population,
            deleted_types_location,
            new_types_location,
            renamed_types_location,
        ):
            detect_changes_between_types(from_type_name, to_type_name)

    def detect_type_changes(
        self,
        from_population: Dict[str, GraphObjectSchema],
        to_population: Dict[str, GraphObjectSchema],
        deleted_types_location: Set[str],
        renamed_types_location: Set[Tuple[str, str]],
        new_types_location: Set[str],
    ):
        # To start, we assume that all types missing from one side or
        # the other were created or deleted.
        current_types = set(to_population)
        previous_types = set(from_population)
        deleted_types_location.update(previous_types - current_types)
        new_types_location.update(current_types - previous_types)

        # After that, check for types that appear to have been renamed
        # where the type names are different but the keys are the same.
        for new_type_name in new_types_location:
            for prev_type_name in previous_types:
                new_type_def = to_population[new_type_name]
                prev_type_def = from_population[prev_type_name]
                keys_match = new_type_def.has_matching_keys(prev_type_def)
                if keys_match and self.input.ask_type_renamed(
                    prev_type_name, new_type_name
                ):
                    renamed_types_location.add((prev_type_name, new_type_name))
                    break

        # Once we have found the renamed types, we can simply remove them
        # from the new and deleted sets.
        for previous, new in renamed_types_location:
            deleted_types_location.remove(previous)
            new_types_location.remove(new)
