from typing import Iterable, List, Set

from nodestream.schema.migrations import (
    Migration,
    MigrationGraph,
    Migrator,
    OperationTypeRoutingMixin,
)
from nodestream.schema.migrations.operations import (
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

from .neo4j_database import Neo4jDatabaseConnection
from .query import Query

ADD_LOCK_TYPE = CreateNodeType(
    name="__NodestreamMigrationLock__", keys=["lock"], properties=[]
)

ADD_MIGRATION_TYPE = CreateNodeType(
    name="__NodestreamMigration__", keys=["name"], properties=[]
)

# Inside of acquire_lock, we are going to create a constraint that
# ensures that there is only one lock node in the database. If we
# try to create a second node, it will fail. This is how we ensure
# that only one instance of nodestream is running at a time.
ACQUIRE_LOCK_QUERY = Query.from_statement(
    "CREATE (l:__NodestreamMigrationLock__{lock: 'lock'})"
)
RELEASE_LOCK_QUERY = Query.from_statement(
    "MATCH (l:__NodestreamMigrationLock__{lock: 'lock'}) DELETE l"
)

LIST_MIGRATIONS_QUERY = "MATCH (m:__NodestreamMigration__) RETURN m.name as name"
MARK_MIGRATION_AS_EXECUTED_QUERY = "MERGE (:__NodestreamMigration__ {name: $name})"

DROP_ALL_NODES_OF_TYPE_FORMAT = "MATCH (n:`{type}`) DETACH DELETE n"
DROP_ALL_RELATIONSHIPS_OF_TYPE_FORMAT = "MATCH ()-[r:`{type}`]->() DELETE r"

INDEMPOTENT_DROP_INDEX_FORMAT = "DROP INDEX {index_name} IF EXISTS"
INDEMPOTENT_DROP_CONSTRAINT = "DROP CONSTRAINT {constraint_name} IF EXISTS"

SET_NODE_PROPERTY_FORMAT = "MATCH (n:`{node_type}`) SET n.`{property_name}` = coalesce(n.`{property_name}`, $value)"
SET_RELATIONSHIP_PROPERTY_FORMAT = "MATCH ()-[r:`{relationship_type}`]->() SET r.`{property_name}` = coalesce(r.`{property_name}`, $value)"

KEY_INDEX_QUERY_FORMAT = "CREATE CONSTRAINT {constraint_name} IF NOT EXISTS FOR (n:`{type}`) REQUIRE ({key_pattern}) IS UNIQUE"
ENTERPRISE_KEY_INDEX_QUERY_FORMAT = "CREATE CONSTRAINT {constraint_name} IF NOT EXISTS FOR (n:`{type}`) REQUIRE ({key_pattern}) IS NODE KEY"
NODE_FIELD_INDEX_QUERY_FORMAT = (
    "CREATE INDEX {constraint_name} IF NOT EXISTS FOR (n:`{type}`) ON (n.`{field}`)"
)
REL_FIELD_INDEX_QUERY_FORMAT = "CREATE INDEX {constraint_name} IF NOT EXISTS FOR ()-[r:`{type}`]-() ON (r.`{field}`)"

RENAME_NODE_PROPERTY_FORMAT = "MATCH (n:`{node_type}`) SET n.`{new_property_name}` = n.`{old_property_name}` REMOVE n.`{old_property_name}`"
RENAME_RELATIONSHIP_PROPERTY_FORMAT = "MATCH ()-[r:`{relationship_type}`]->() SET r.`{new_property_name}` = r.`{old_property_name}` REMOVE r.`{old_property_name}`"

GET_PROPERTIES_FOR_CONSTRAINT_QUERY = "SHOW CONSTRAINTS YIELD name, properties WHERE name = $constraint_name RETURN properties"
GET_INDEXES_BY_TYPE_QUERY = "SHOW INDEXES YIELD properties, entityType, labelsOrTypes, owningConstraint WHERE entityType = $entity_type and labelsOrTypes = [$object_type] and owningConstraint is null RETURN properties"

RENAME_NODE_TYPE = "MATCH (n:`{old_type}`) SET n:`{new_type}` REMOVE n:`{old_type}`"
RENAME_REL_TYPE = "MATCH (n)-[r:`{old_type}`]->(m) CREATE (n)-[r2:`{new_type}`]->(m) SET r2 += r WITH r DELETE r"

DROP_REL_PROPERTY_FORMAT = (
    "MATCH ()-[r:`{relationship_type}`]->() REMOVE r.`{property_name}`"
)
DROP_NODE_PROPERTY_FORMAT = "MATCH (n:`{node_type}`) REMOVE n.`{property_name}`"


class CannotAcquireLockException(Exception):
    pass


class Neo4jMigrator(OperationTypeRoutingMixin, Migrator):
    def __init__(
        self,
        database_connection: Neo4jDatabaseConnection,
        use_enterprise_features: bool,
    ) -> None:
        self.database_connection = database_connection
        self.use_enterprise_features = use_enterprise_features

    async def make_node_constraint(
        self, node_type: str, keys: Iterable[str], constraint_name: str
    ) -> None:
        key_pattern = ",".join(f"n.`{p}`" for p in sorted(keys))
        format = (
            ENTERPRISE_KEY_INDEX_QUERY_FORMAT
            if self.use_enterprise_features
            else KEY_INDEX_QUERY_FORMAT
        )
        statement = format.format(
            constraint_name=constraint_name,
            key_pattern=key_pattern,
            type=node_type,
        )
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def drop_constraint_by_name(self, constraint_name: str) -> None:
        statement = INDEMPOTENT_DROP_CONSTRAINT.format(constraint_name=constraint_name)
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def get_properties_by_constraint_name(self, constraint_name: str) -> Set[str]:
        query = Query.from_statement(
            GET_PROPERTIES_FOR_CONSTRAINT_QUERY, constraint_name=constraint_name
        )
        result = await self.database_connection.execute(query)
        return set(result[0]["properties"])

    async def get_indexed_properties_by_type(
        self, entity_type: str, object_type: str
    ) -> Set[str]:
        query = Query.from_statement(
            GET_INDEXES_BY_TYPE_QUERY, entity_type=entity_type, object_type=object_type
        )
        result = await self.database_connection.execute(query)
        return set(record["properties"][0] for record in result)

    async def drop_all_indexes_on_type(self, entity_type: str, object_type: str):
        indexed_properties = await self.get_indexed_properties_by_type(
            entity_type, object_type
        )
        op_factory = (
            DropAdditionalNodePropertyIndex
            if entity_type == "NODE"
            else DropAdditionalRelationshipPropertyIndex
        )
        for field in indexed_properties:
            await self.execute_operation(op_factory(object_type, field))
        return indexed_properties

    async def acquire_lock(self) -> None:
        try:
            await self.execute_create_node_type(ADD_LOCK_TYPE)
            await self.execute_create_node_type(ADD_MIGRATION_TYPE)
            await self.database_connection.execute(ACQUIRE_LOCK_QUERY)
        except Exception as e:
            raise CannotAcquireLockException from e

    async def release_lock(self) -> None:
        await self.database_connection.execute(RELEASE_LOCK_QUERY)

    async def mark_migration_as_executed(self, migration: Migration) -> None:
        query = Query.from_statement(
            MARK_MIGRATION_AS_EXECUTED_QUERY, name=migration.name
        )
        await self.database_connection.execute(query)

    async def get_completed_migrations(self, graph: MigrationGraph) -> List[Migration]:
        query = Query.from_statement(LIST_MIGRATIONS_QUERY)
        return [
            graph.get_migration(record["name"])
            for record in await self.database_connection.execute(query)
        ]

    async def execute_create_node_type(self, operation: CreateNodeType) -> None:
        await self.make_node_constraint(
            node_type=operation.name,
            keys=operation.keys,
            constraint_name=operation.proposed_index_name,
        )

    async def execute_create_relationship_type(self, _: CreateRelationshipType) -> None:
        # Neo4j Does not need us to do anything here.
        pass

    async def execute_drop_node_type(self, operation: DropNodeType) -> None:
        await self.drop_constraint_by_name(operation.proposed_index_name)
        await self.drop_all_indexes_on_type("NODE", operation.name)
        statement = DROP_ALL_NODES_OF_TYPE_FORMAT.format(type=operation.name)
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def execute_drop_relationship_type(
        self, operation: DropRelationshipType
    ) -> None:
        await self.drop_all_indexes_on_type("RELATIONSHIP", operation.name)
        statement = DROP_ALL_RELATIONSHIPS_OF_TYPE_FORMAT.format(type=operation.name)
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def execute_rename_node_property(self, operation: RenameNodeProperty) -> None:
        statement = RENAME_NODE_PROPERTY_FORMAT.format(
            node_type=operation.node_type,
            old_property_name=operation.old_property_name,
            new_property_name=operation.new_property_name,
        )
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def execute_rename_relationship_property(
        self, operation: RenameRelationshipProperty
    ) -> None:
        statement = RENAME_RELATIONSHIP_PROPERTY_FORMAT.format(
            relationship_type=operation.relationship_type,
            old_property_name=operation.old_property_name,
            new_property_name=operation.new_property_name,
        )
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def execute_rename_node_type(self, operation: RenameNodeType) -> None:
        # Drop the old constraint after saving the properties it was on.
        key_properties = await self.get_properties_by_constraint_name(
            operation.old_proposed_index_name
        )
        await self.drop_constraint_by_name(operation.old_proposed_index_name)

        # Drop the old indexes after saving the properties they were on.
        indexed_properties = await self.get_indexed_properties_by_type(
            "NODE", operation.old_type
        )
        for field in indexed_properties:
            await self.execute_drop_additional_node_property_index(
                DropAdditionalNodePropertyIndex(operation.old_type, field)
            )

        # Rename all nodes of the old type to the new type.
        statement = RENAME_NODE_TYPE.format(
            old_type=operation.old_type, new_type=operation.new_type
        )
        await self.database_connection.execute(Query.from_statement(statement))

        # Create the new constraint.
        await self.make_node_constraint(
            node_type=operation.new_type,
            keys=key_properties,
            constraint_name=operation.new_proposed_index_name,
        )

        # Create the new indexes.
        for field in indexed_properties:
            await self.execute_add_additional_node_property_index(
                AddAdditionalNodePropertyIndex(operation.new_type, field)
            )

    async def execute_rename_relationship_type(
        self, operation: RenameRelationshipType
    ) -> None:
        # Drop the old indexes after saving the properties they were on.
        indexed_properties = await self.drop_all_indexes_on_type(
            "RELATIONSHIP", operation.old_type
        )

        # Rename all rels of the old type to the new type.
        statement = RENAME_REL_TYPE.format(
            old_type=operation.old_type, new_type=operation.new_type
        )
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

        # Create the new indexes.
        for field in indexed_properties:
            await self.execute_add_additional_relationship_property_index(
                AddAdditionalRelationshipPropertyIndex(operation.new_type, field)
            )

    async def execute_add_additional_node_property_index(
        self, operation: AddAdditionalNodePropertyIndex
    ) -> None:
        statement = NODE_FIELD_INDEX_QUERY_FORMAT.format(
            constraint_name=operation.proposed_index_name,
            type=operation.node_type,
            field=operation.field_name,
        )
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def execute_drop_additional_node_property_index(
        self, operation: DropAdditionalNodePropertyIndex
    ) -> None:
        statement = INDEMPOTENT_DROP_INDEX_FORMAT.format(
            index_name=operation.proposed_index_name
        )
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def execute_add_additional_relationship_property_index(
        self, operation: AddAdditionalRelationshipPropertyIndex
    ) -> None:
        statement = REL_FIELD_INDEX_QUERY_FORMAT.format(
            constraint_name=operation.proposed_index_name,
            type=operation.relationship_type,
            field=operation.field_name,
        )
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def execute_drop_additional_relationship_property_index(
        self, operation: DropAdditionalRelationshipPropertyIndex
    ) -> None:
        statement = INDEMPOTENT_DROP_INDEX_FORMAT.format(
            index_name=operation.proposed_index_name
        )
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def execute_add_node_property(self, operation: AddNodeProperty) -> None:
        statement = SET_NODE_PROPERTY_FORMAT.format(
            node_type=operation.node_type, property_name=operation.property_name
        )
        query = Query.from_statement(statement, value=operation.default)
        await self.database_connection.execute(query)

    async def execute_add_relationship_property(
        self, operation: AddRelationshipProperty
    ) -> None:
        statement = SET_RELATIONSHIP_PROPERTY_FORMAT.format(
            relationship_type=operation.relationship_type,
            property_name=operation.property_name,
        )
        query = Query.from_statement(statement, value=operation.default)
        await self.database_connection.execute(query)

    async def execute_drop_node_property(self, operation: DropNodeProperty) -> None:
        statement = DROP_NODE_PROPERTY_FORMAT.format(
            node_type=operation.node_type, property_name=operation.property_name
        )
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def execute_drop_relationship_property(
        self, operation: DropRelationshipProperty
    ) -> None:
        statement = DROP_REL_PROPERTY_FORMAT.format(
            relationship_type=operation.relationship_type,
            property_name=operation.property_name,
        )
        query = Query.from_statement(statement)
        await self.database_connection.execute(query)

    async def execute_node_key_extended(self, operation: NodeKeyExtended) -> None:
        constraint_name = operation.proposed_index_name
        as_add = AddNodeProperty(
            operation.node_type, operation.added_key_property, operation.default
        )
        keys = await self.get_properties_by_constraint_name(constraint_name)
        keys.add(operation.added_key_property)
        await self.drop_constraint_by_name(constraint_name)
        await self.execute_add_node_property(as_add)
        await self.make_node_constraint(
            node_type=operation.node_type, keys=keys, constraint_name=constraint_name
        )

    async def execute_relationship_key_extended(
        self, operation: RelationshipKeyExtended
    ) -> None:
        as_add_property = AddRelationshipProperty(
            operation.relationship_type,
            operation.added_key_property,
            operation.default,
        )
        await self.execute_add_relationship_property(as_add_property)

    async def execute_node_key_part_renamed(
        self, operation: NodeKeyPartRenamed
    ) -> None:
        constraint_name = operation.proposed_index_name
        as_rename = RenameNodeProperty(
            operation.node_type,
            operation.old_key_part_name,
            operation.new_key_part_name,
        )
        keys = await self.get_properties_by_constraint_name(constraint_name)
        keys.remove(operation.old_key_part_name)
        keys.add(operation.new_key_part_name)
        await self.drop_constraint_by_name(constraint_name)
        await self.execute_rename_node_property(as_rename)
        await self.make_node_constraint(
            node_type=operation.node_type, keys=keys, constraint_name=constraint_name
        )

    async def execute_relationship_key_part_renamed(
        self, operation: RelationshipKeyPartRenamed
    ) -> None:
        as_rename = RenameRelationshipProperty(
            operation.relationship_type,
            operation.old_key_part_name,
            operation.new_key_part_name,
        )
        await self.execute_rename_relationship_property(as_rename)
