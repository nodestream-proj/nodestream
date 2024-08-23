import os

import pytest

from nodestream.schema.migrations import Migration
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
from nodestream_plugin_neo4j.migrator import Neo4jDatabaseConnection, Neo4jMigrator

from .conftest import TESTED_NEO4J_VERSIONS


class Scenario:
    def prepare_schema(self, session):
        pass

    async def apply_opertation(self, migrator):
        migration = Migration("test_migration", [self.get_operation()], [])
        await migrator.execute_migration(migration)

    def get_operation(self):
        pass

    def validate_operation(self, session):
        pass


class CreateNodeTypeScenario(Scenario):
    def get_operation(self):
        return CreateNodeType("TestNode", keys=["id"], properties=["name"])

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW CONSTRAINTS
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["TestNode"]
            RETURN properties
            """
        )

        assert result.single()["properties"] == ["id"]


class RenameNodeTypeScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE CONSTRAINT TestNode_node_key FOR (t:TestNode) REQUIRE t.id IS UNIQUE"
        )

    def get_operation(self):
        return RenameNodeType("TestNode", "RenamedTestNode")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW CONSTRAINTS
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["RenamedTestNode"]
            RETURN properties
            """
        )

        assert result.single()["properties"] == ["id"]


class DropNodeTypeScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE CONSTRAINT TestNode_node_key FOR (t:TestNode) REQUIRE t.id IS UNIQUE"
        )
        session.run("CREATE (n:TestNode{id: 1, name: 'test'})")

    def get_operation(self):
        return DropNodeType("TestNode")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW CONSTRAINTS
            YIELD labelsOrTypes
            WHERE labelsOrTypes = ["TestNode"]
            RETURN labelsOrTypes
            """
        )

        assert result.single() is None

        result = session.run(
            """
            MATCH (n:TestNode)
            RETURN count(n) as count
            """
        )

        assert result.single()["count"] == 0


class AddNodePropertyScenario(Scenario):
    def prepare_schema(self, session):
        session.run("CREATE (n:TestNode{id: 1, name: 'test'})")

    def get_operation(self):
        return AddNodeProperty("TestNode", "age", 4)

    def validate_operation(self, session):
        result = session.run(
            """
            MATCH (n:TestNode)
            RETURN n.age as age
            """
        )

        assert result.single()["age"] == 4


class DropNodePropertyScenario(Scenario):
    def prepare_schema(self, session):
        session.run("CREATE (n:TestNode{id: 1, name: 'test', age: 25})")

    def get_operation(self):
        return DropNodeProperty("TestNode", "age")

    def validate_operation(self, session):
        result = session.run(
            """
            MATCH (n:TestNode)
            RETURN n.age as age
            """
        )

        assert result.single()["age"] is None


class RenameNodePropertyScenario(Scenario):
    def prepare_schema(self, session):
        session.run("CREATE (n:TestNode{id: 1, name: 'test'})")

    def get_operation(self):
        return RenameNodeProperty("TestNode", "name", "new_name")

    def validate_operation(self, session):
        result = session.run(
            """
            MATCH (n:TestNode)
            RETURN n.new_name as new_name
            """
        )

        assert result.single()["new_name"] == "test"


class AddRelationshipPropertyScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE (n1:TestNode{id: 1, name: 'test1'})-[:RELATIONSHIP]->(n2:TestNode{id: 2, name: 'test2'})"
        )

    def get_operation(self):
        return AddRelationshipProperty("RELATIONSHIP", "weight", 12)

    def validate_operation(self, session):
        result = session.run(
            """
            MATCH (:TestNode)-[r:RELATIONSHIP]->(:TestNode)
            RETURN r.weight as weight
            """
        )

        assert result.single()["weight"] == 12


class DropRelationshipPropertyScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE (n1:TestNode{id: 1, name: 'test1'})-[:RELATIONSHIP{weight: 0.5}]->(n2:TestNode{id: 2, name: 'test2'})"
        )

    def get_operation(self):
        return DropRelationshipProperty("RELATIONSHIP", "weight")

    def validate_operation(self, session):
        result = session.run(
            """
            MATCH (:TestNode)-[r:RELATIONSHIP]->(:TestNode)
            RETURN r.weight as weight
            """
        )

        assert result.single()["weight"] is None


class RenameRelationshipPropertyScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE (n1:TestNode{id: 1, name: 'test1'})-[:RELATIONSHIP{weight: 0.5}]->(n2:TestNode{id: 2, name: 'test2'})"
        )

    def get_operation(self):
        return RenameRelationshipProperty("RELATIONSHIP", "weight", "new_weight")

    def validate_operation(self, session):
        result = session.run(
            """
            MATCH (:TestNode)-[r:RELATIONSHIP]->(:TestNode)
            RETURN r.new_weight as new_weight
            """
        )

        assert result.single()["new_weight"] == 0.5


class AddAdditionalNodePropertyIndexScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE CONSTRAINT TestNode_node_key FOR (t:TestNode) REQUIRE t.id IS UNIQUE"
        )

    def get_operation(self):
        return AddAdditionalNodePropertyIndex("TestNode", "name")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW INDEXES
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["TestNode"]
            RETURN properties
            """
        )

        assert result.single()["properties"] == ["name"]


class DropAdditionalNodePropertyIndexScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE INDEX TestNode_name_additional_index FOR (t:TestNode) ON (t.name)"
        )

    def get_operation(self):
        return DropAdditionalNodePropertyIndex("TestNode", "name")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW INDEXES
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["TestNode"]
            RETURN properties
            """
        )

        assert all("name" not in record["properties"] for record in result)


class AddAdditionalRelationshipPropertyIndexScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE (n1:TestNode{id: 1, name: 'test1'})-[:RELATIONSHIP{weight: 0.5}]->(n2:TestNode{id: 2, name: 'test2'})"
        )

    def get_operation(self):
        return AddAdditionalRelationshipPropertyIndex("RELATIONSHIP", "weight")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW INDEXES
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["RELATIONSHIP"]
            RETURN properties
            """
        )

        assert result.single()["properties"] == ["weight"]


class DropAdditionalRelationshipPropertyIndexScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE (n1:TestNode{id: 1, name: 'test1'})-[:RELATIONSHIP{weight: 0.5}]->(n2:TestNode{id: 2, name: 'test2'})"
        )
        session.run(
            "CREATE INDEX RELATIONSHIP_weight_additional_index FOR ()-[r:RELATIONSHIP]-() ON (r.weight)"
        )

    def get_operation(self):
        return DropAdditionalRelationshipPropertyIndex("RELATIONSHIP", "weight")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW INDEXES
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["RELATIONSHIP"]
            RETURN properties
            """
        )

        assert all("weight" not in record["properties"] for record in result)


class CreateRelationshipTypeScenario(Scenario):
    def get_operation(self):
        return CreateRelationshipType(
            "TEST_RELATIONSHIP", keys=["id"], properties=["name"]
        )

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW CONSTRAINTS
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["TestRelationship"]
            RETURN properties
            """
        )

        # We don't create constraints for relationships
        assert result.single() is None


class RenameRelationshipTypeScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE INDEX RELATIONSHIP_weight_additional_index FOR ()-[r:RELATIONSHIP]-() ON (r.weight)"
        )

    def get_operation(self):
        return RenameRelationshipType("RELATIONSHIP", "NEW_RELATIONSHIP")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW INDEXES
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["NEW_RELATIONSHIP"]
            RETURN properties
            """
        )

        assert result.single()["properties"] == ["weight"]


class DropRelationshipTypeScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE INDEX RELATIONSHIP_weight_additional_index FOR ()-[r:RELATIONSHIP]-() ON (r.weight)"
        )
        session.run(
            "CREATE (:TestNode{id: 1, name: 'test1'})-[:RELATIONSHIP{weight: 0.5}]->(:TestNode{id: 2, name: 'test2'})"
        )

    def get_operation(self):
        return DropRelationshipType("RELATIONSHIP")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW CONSTRAINTS
            YIELD labelsOrTypes
            WHERE labelsOrTypes = ["RELATIONSHIP"]
            RETURN labelsOrTypes
            """
        )

        assert result.single() is None

        result = session.run(
            """
            MATCH ()-[r:RELATIONSHIP]->()
            RETURN count(r) as count
            """
        )

        assert result.single()["count"] == 0


class NodeKeyPartRenamedScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE CONSTRAINT TestNode_node_key FOR (t:TestNode) REQUIRE t.id IS UNIQUE"
        )

    def get_operation(self):
        return NodeKeyPartRenamed("TestNode", "id", "new_id")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW CONSTRAINTS
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["TestNode"]
            RETURN properties
            """
        )

        assert result.single()["properties"] == ["new_id"]


class RelationshipKeyPartRenamedScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE (:TestNode{id: 1, name: 'test1'})-[:RELATIONSHIP{weight: 0.5}]->(:TestNode{id: 2, name: 'test2'})"
        )

    def get_operation(self):
        return RelationshipKeyPartRenamed("RELATIONSHIP", "weight", "new_weight")

    def validate_operation(self, session):
        result = session.run(
            """
            MATCH (:TestNode)-[r:RELATIONSHIP]->(:TestNode)
            RETURN r.new_weight as new_weight
            """
        )

        assert result.single()["new_weight"] == 0.5


class NodeKeyExtendedScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE CONSTRAINT TestNode_node_key FOR (t:TestNode) REQUIRE t.id IS UNIQUE"
        )

    def get_operation(self):
        return NodeKeyExtended("TestNode", "name", "bob")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW CONSTRAINTS
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["TestNode"]
            RETURN properties
            """
        )

        assert result.single()["properties"] == ["id", "name"]


class RelationshipKeyExtendedScenario(Scenario):
    def prepare_schema(self, session):
        session.run(
            "CREATE (:TestNode{id: 1, name: 'test1'})-[:RELATIONSHIP{weight: 0.5}]->(:TestNode{id: 2, name: 'test2'})"
        )

    def get_operation(self):
        return RelationshipKeyExtended("RELATIONSHIP", "name", "bob")

    def validate_operation(self, session):
        result = session.run(
            """
            SHOW CONSTRAINTS
            YIELD labelsOrTypes, properties
            WHERE labelsOrTypes = ["RELATIONSHIP"]
            RETURN properties
            """
        )

        assert result.single() is None


@pytest.mark.asyncio
@pytest.mark.e2e
@pytest.mark.parametrize("neo4j_version", TESTED_NEO4J_VERSIONS)
@pytest.mark.parametrize("use_enterprise_features", [True, False])
@pytest.mark.parametrize("scenario", list(Scenario.__subclasses__()))
async def test_neo4j_migration(
    neo4j_container, neo4j_version, use_enterprise_features, scenario
):
    with neo4j_container(
        neo4j_version, use_enterprise_features
    ) as neo4j_container, neo4j_container.get_driver() as driver, driver.session() as session:
        scenario_instance = scenario()
        connection = Neo4jDatabaseConnection.from_configuration(
            uri=os.environ["NEO4J_CONNECT_URI"], username="neo4j", password="password"
        )
        migrator = Neo4jMigrator(connection, use_enterprise_features)
        scenario_instance.prepare_schema(session)
        await scenario_instance.apply_opertation(migrator)
        scenario_instance.validate_operation(session)
