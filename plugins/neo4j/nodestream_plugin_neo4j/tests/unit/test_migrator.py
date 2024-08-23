import pytest
from hamcrest import assert_that
from neo4j import AsyncSession

from nodestream.schema.migrations.operations import (
    AddAdditionalNodePropertyIndex,
    AddAdditionalRelationshipPropertyIndex,
    AddNodeProperty,
    AddRelationshipProperty,
    CreateNodeType,
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
from nodestream_plugin_neo4j.migrator import Neo4jDatabaseConnection, Neo4jMigrator
from nodestream_plugin_neo4j.query import Query

from .matchers import ran_query


@pytest.fixture
def database_connection(mocker):
    connection = mocker.Mock(Neo4jDatabaseConnection)
    connection.session.return_value = mocker.AsyncMock(AsyncSession)
    return connection


@pytest.fixture
def migrator(database_connection):
    return Neo4jMigrator(database_connection, False)


@pytest.mark.asyncio
async def test_execute_relationship_key_part_renamed(migrator):
    operation = RelationshipKeyPartRenamed(
        old_key_part_name="old_key",
        new_key_part_name="new_key",
        relationship_type="RELATIONSHIP_TYPE",
    )
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() SET r.`new_key` = r.`old_key` REMOVE r.`old_key`"
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_execute_relationship_property_renamed(migrator):
    operation = RenameRelationshipProperty(
        old_property_name="old_prop",
        new_property_name="new_prop",
        relationship_type="RELATIONSHIP_TYPE",
    )
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() SET r.`new_prop` = r.`old_prop` REMOVE r.`old_prop`"
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_execute_relationship_key_extended(migrator):
    operation = RelationshipKeyExtended(
        added_key_property="key", relationship_type="RELATIONSHIP_TYPE", default="foo"
    )
    await migrator.execute_operation(operation)
    query = Query(
        "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() SET r.`key` = coalesce(r.`key`, $value)",
        parameters={"value": "foo"},
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_execute_relationship_property_added(migrator):
    operation = AddRelationshipProperty(
        property_name="prop", relationship_type="RELATIONSHIP_TYPE", default="foo"
    )
    await migrator.execute_operation(operation)
    expected_query = Query(
        "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() SET r.`prop` = coalesce(r.`prop`, $value)",
        parameters={"value": "foo"},
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_execute_relationship_property_dropped(migrator):
    operation = DropRelationshipProperty(
        property_name="prop", relationship_type="RELATIONSHIP_TYPE"
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement("MATCH ()-[r:`RELATIONSHIP_TYPE`]->() REMOVE r.`prop`")
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_execute_relationship_type_renamed(migrator):
    operation = RenameRelationshipType(old_type="OLD_TYPE", new_type="NEW_TYPE")
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        "MATCH (n)-[r:`OLD_TYPE`]->(m) CREATE (n)-[r2:`NEW_TYPE`]->(m) SET r2 += r WITH r DELETE r"
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_execute_relationship_type_created(migrator):
    # Neo4j Does not need us to do anything here.
    pass


@pytest.mark.asyncio
async def test_execute_relationship_type_dropped(migrator):
    operation = DropRelationshipType(name="RELATIONSHIP_TYPE")
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        "MATCH ()-[r:`RELATIONSHIP_TYPE`]->() DELETE r"
    )
    assert_that(migrator, expected_query)


@pytest.mark.asyncio
async def test_execute_node_type_dropped(migrator):
    operation = DropNodeType(name="NodeType")
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement("MATCH (n:`NodeType`) DETACH DELETE n")
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_add_additional_node_property_index(migrator):
    operation = AddAdditionalNodePropertyIndex(field_name="prop", node_type="NodeType")
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        "CREATE INDEX NodeType_prop_additional_index IF NOT EXISTS FOR (n:`NodeType`) ON (n.`prop`)"
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_drop_additional_node_property_index(migrator):
    operation = DropAdditionalNodePropertyIndex(field_name="prop", node_type="NodeType")
    await migrator.execute_operation(operation)
    query = Query.from_statement("DROP INDEX NodeType_prop_additional_index IF EXISTS")
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_add_additional_relationship_property_index(migrator):
    operation = AddAdditionalRelationshipPropertyIndex(
        field_name="prop", relationship_type="RelationshipType"
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        "CREATE INDEX RelationshipType_prop_additional_index IF NOT EXISTS FOR ()-[r:`RelationshipType`]-() ON (r.`prop`)"
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_drop_additional_relationship_property_index(migrator):
    operation = DropAdditionalRelationshipPropertyIndex(
        field_name="prop", relationship_type="RelationshipType"
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        "DROP INDEX RelationshipType_prop_additional_index IF EXISTS"
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_rename_node_property(migrator):
    operation = RenameNodeProperty(
        old_property_name="old_prop", new_property_name="new_prop", node_type="NodeType"
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        "MATCH (n:`NodeType`) SET n.`new_prop` = n.`old_prop` REMOVE n.`old_prop`"
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_rename_node_type(migrator):
    operation = RenameNodeType(old_type="OLD_TYPE", new_type="NEW_TYPE")
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        "MATCH (n:`OLD_TYPE`) SET n:`NEW_TYPE` REMOVE n:`OLD_TYPE`"
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_create_node_type_non_enterprise(migrator):
    operation = CreateNodeType(
        name="Person", keys=["first_name", "last_name"], properties=[]
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        "CREATE CONSTRAINT Person_node_key IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`first_name`,n.`last_name`) IS UNIQUE"
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_create_node_type_enterprise(migrator):
    migrator.use_enterprise_features = True
    operation = CreateNodeType(
        name="Person", keys=["first_name", "last_name"], properties=[]
    )
    await migrator.execute_operation(operation)
    query = Query.from_statement(
        "CREATE CONSTRAINT Person_node_key IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`first_name`,n.`last_name`) IS NODE KEY"
    )
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_add_node_property(migrator):
    operation = AddNodeProperty(
        property_name="prop", node_type="NodeType", default="foo"
    )
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        "MATCH (n:`NodeType`) SET n.`prop` = coalesce(n.`prop`, $value)",
        value="foo",
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_drop_node_property(migrator):
    operation = DropNodeProperty(property_name="prop", node_type="NodeType")
    await migrator.execute_operation(operation)
    query = Query.from_statement("MATCH (n:`NodeType`) REMOVE n.`prop`")
    assert_that(migrator, ran_query(query))


@pytest.mark.asyncio
async def test_node_key_extended_with_default(migrator):
    operation = NodeKeyExtended(
        added_key_property="key", node_type="NodeType", default="foo"
    )
    await migrator.execute_operation(operation)
    expected_query = Query.from_statement(
        "MATCH (n:`NodeType`) SET n.`key` = coalesce(n.`key`, $value)",
        value="foo",
    )
    assert_that(migrator, ran_query(expected_query))


@pytest.mark.asyncio
async def test_node_key_renamed(migrator, mocker):
    migrator.get_properties_by_constraint_name = mocker.AsyncMock(return_value={"foo"})
    operation = NodeKeyPartRenamed(
        new_key_part_name="key", node_type="NodeType", old_key_part_name="foo"
    )
    await migrator.execute_operation(operation)
    assert_that(
        migrator,
        ("MATCH (n:`NodeType`) SET n.`key` = n.`foo` REMOVE n.`foo`"),
    )
