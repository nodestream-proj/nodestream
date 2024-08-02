from abc import ABC, abstractmethod

from ..state import Schema
from .migrations import MigrationGraph
from .migrator import Migrator, OperationTypeRoutingMixin
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
    RelationshipKeyExtended,
    RelationshipKeyPartRenamed,
    RenameNodeProperty,
    RenameNodeType,
    RenameRelationshipProperty,
    RenameRelationshipType,
)


class StateProvider(ABC):
    @abstractmethod
    async def get_schema(self) -> Schema:
        pass


class StaticStateProvider(StateProvider):
    __slots__ = ("schema",)

    def __init__(self, schema: Schema) -> None:
        self.schema = schema

    async def get_schema(self) -> Schema:
        return self.schema


class MigrationGraphStateProvider(StateProvider):
    __slots__ = ("migrations",)

    def __init__(self, migrations: MigrationGraph) -> None:
        self.migrations = migrations

    async def get_schema(self) -> Schema:
        migrator = InMemoryMigrator()
        for migration in self.migrations.get_ordered_migration_plan([]):
            await migrator.execute_migration(migration)
        return migrator.schema


class InMemoryMigrator(OperationTypeRoutingMixin, Migrator):
    """A migrator that executes migrations in memory.

    This migrator executes migrations in memory. It does not actually execute
    migrations against a database. It is used for testing the schema change
    detector as well as for generating the current application state from the
    migrations in the migration graph.
    """

    __slots__ = ("schema",)

    def __init__(self) -> None:
        self.schema = Schema()

    async def execute_create_node_type(self, operation: CreateNodeType) -> None:
        self.schema.put_node_type(operation.as_node_type())

    async def execute_create_relationship_type(
        self, operation: CreateRelationshipType
    ) -> None:
        self.schema.put_relationship_type(operation.as_relationship_type())

    async def execute_drop_node_type(self, operation: DropNodeType) -> None:
        self.schema.drop_node_type_by_name(operation.name)

    async def execute_drop_relationship_type(
        self, operation: DropRelationshipType
    ) -> None:
        self.schema.drop_relationship_type_by_name(operation.name)

    async def execute_rename_node_property(self, operation: RenameNodeProperty) -> None:
        node_type_def = self.schema.get_node_type_by_name(operation.node_type)
        node_type_def.rename_property(
            operation.old_property_name, operation.new_property_name
        )

    async def execute_rename_relationship_property(
        self, operation: RenameRelationshipProperty
    ) -> None:
        relationship_type_def = self.schema.get_relationship_type_by_name(
            operation.relationship_type
        )
        relationship_type_def.rename_property(
            operation.old_property_name, operation.new_property_name
        )

    async def execute_rename_node_type(self, operation: RenameNodeType) -> None:
        current_type = self.schema.get_node_type_by_name(operation.old_type)
        self.schema.drop_node_type_by_name(operation.old_type)
        current_type.name = operation.new_type
        self.schema.put_node_type(current_type)

    async def execute_rename_relationship_type(
        self, operation: RenameRelationshipType
    ) -> None:
        current_type = self.schema.get_relationship_type_by_name(operation.old_type)
        self.schema.drop_relationship_type_by_name(operation.old_type)
        current_type.name = operation.new_type
        self.schema.put_relationship_type(current_type)

    async def execute_add_additional_node_property_index(
        self, operation: AddAdditionalNodePropertyIndex
    ) -> None:
        type_def = self.schema.get_node_type_by_name(operation.node_type)
        type_def.add_index(operation.field_name)

    async def execute_drop_additional_node_property_index(
        self, operation: DropAdditionalNodePropertyIndex
    ) -> None:
        type_def = self.schema.get_node_type_by_name(operation.node_type)
        type_def.drop_index(operation.field_name)

    async def execute_add_additional_relationship_property_index(
        self, operation: AddAdditionalRelationshipPropertyIndex
    ) -> None:
        type_def = self.schema.get_relationship_type_by_name(
            operation.relationship_type
        )
        type_def.add_index(operation.field_name)

    async def execute_drop_additional_relationship_property_index(
        self, operation: DropAdditionalRelationshipPropertyIndex
    ) -> None:
        type_def = self.schema.get_relationship_type_by_name(
            operation.relationship_type
        )
        type_def.drop_index(operation.field_name)

    async def execute_add_node_property(self, operation: AddNodeProperty) -> None:
        type_def = self.schema.get_node_type_by_name(operation.node_type)
        type_def.add_property(operation.property_name)

    async def execute_add_relationship_property(
        self, operation: AddRelationshipProperty
    ) -> None:
        type_def = self.schema.get_relationship_type_by_name(
            operation.relationship_type
        )
        type_def.add_property(operation.property_name)

    async def execute_drop_node_property(self, operation: DropNodeProperty) -> None:
        type_def = self.schema.get_node_type_by_name(operation.node_type)
        type_def.drop_property(operation.property_name)

    async def execute_drop_relationship_property(
        self, operation: DropRelationshipProperty
    ) -> None:
        type_def = self.schema.get_relationship_type_by_name(
            operation.relationship_type
        )
        type_def.drop_property(operation.property_name)

    async def execute_node_key_extended(self, operation: NodeKeyExtended) -> None:
        type_def = self.schema.get_node_type_by_name(operation.node_type)
        type_def.add_key(operation.added_key_property)

    async def execute_relationship_key_extended(
        self, operation: RelationshipKeyExtended
    ) -> None:
        type_def = self.schema.get_relationship_type_by_name(
            operation.relationship_type
        )
        type_def.add_key(operation.added_key_property)

    async def execute_node_key_part_renamed(
        self, operation: NodeKeyPartRenamed
    ) -> None:
        type_def = self.schema.get_node_type_by_name(operation.node_type)
        type_def.rename_key(operation.old_key_part_name, operation.new_key_part_name)

    async def execute_relationship_key_part_renamed(
        self, operation: RelationshipKeyPartRenamed
    ) -> None:
        type_def = self.schema.get_relationship_type_by_name(
            operation.relationship_type
        )
        type_def.rename_key(operation.old_key_part_name, operation.new_key_part_name)
