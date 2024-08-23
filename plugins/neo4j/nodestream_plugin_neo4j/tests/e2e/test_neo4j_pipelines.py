from pathlib import Path

import pytest
from neo4j import Session
from pandas import Timedelta, Timestamp

from nodestream.pipeline import (
    PipelineInitializationArguments,
    PipelineProgressReporter,
)
from nodestream.project import Project, RunRequest
from nodestream_plugin_neo4j.query import Query

from .conftest import TESTED_NEO4J_VERSIONS


@pytest.fixture
def project():
    return Project.read_from_file(
        Path("plugins/neo4j/nodestream_plugin_neo4j/tests/project/nodestream.yaml")
    )


def validate_airports(session):
    result = session.run(
        """
        MATCH (a:Airport)
        RETURN count(a) AS count
        """
    )

    assert result.single()["count"] == 1000


def validate_enrichment(session):
    result = session.run(
        """
        MATCH (a:NewObject)
        RETURN count(a) AS count
        """
    )
    assert result.single()["count"] == 30


def valiudate_airport_country(session):
    result = session.run(
        """
        MATCH (a:Airport{identifier: "05pn"})-[:WITHIN]->(c:Country)
        RETURN c.code as country
        """
    )

    assert result.single()["country"] == "us"


def validate_fifa_player_count(session):
    result = session.run(
        """
        MATCH (p:Player:Person)
        RETURN count(p) AS count
        """
    )

    assert result.single()["count"] == 100


def validate_fifa_mo_club(session):
    result = session.run(
        """
        MATCH (p:Player{name: "Mohamed Salah"})-[:PLAYS_FOR]->(t:Team)
        RETURN t.name as club
        """
    )

    assert result.single()["club"] == "Liverpool"


def validate_consistency_in_node_counts(session):
    result = session.run(
        """
        MATCH (n:ObjectA)
        RETURN count(n) as node_count
        """
    )
    assert result.single()["node_count"] == 30
    result = session.run(
        """
        MATCH (n:ObjectB)
        RETURN count(n) as node_count
        """
    )
    assert result.single()["node_count"] == 30


def validate_ttl_seperation_between_relationship_object_types(session):
    result = session.run(
        """
        MATCH ()-[r:CONNECTED_TO]->()
        RETURN count(r) as relationship_count
        """
    )
    assert result.single()["relationship_count"] == 20
    result = session.run(
        """
        MATCH ()-[r:ADJACENT_TO]->()
        RETURN count(r) as relationship_count
        """
    )
    assert result.single()["relationship_count"] == 10


def validate_ttl_seperation_between_node_object_types(session):
    result = session.run(
        """
        MATCH (n:ObjectA)
        RETURN count(n) as node_count
        """
    )
    assert result.single()["node_count"] == 20
    result = session.run(
        """
        MATCH (n:ObjectB)
        RETURN count(n) as node_count
        """
    )
    assert result.single()["node_count"] == 10


PIPELINE_TESTS = [
    ("airports", [validate_airports, valiudate_airport_country]),
    ("fifa", [validate_fifa_player_count, validate_fifa_mo_club]),
]

TTL_TESTS = [
    (
        "relationship-ttls",
        [
            validate_consistency_in_node_counts,
            validate_ttl_seperation_between_relationship_object_types,
        ],
    ),
    ("node-ttls", [validate_ttl_seperation_between_node_object_types]),
]

EXTRACTOR_TESTS = [("extractor_integration", [validate_enrichment])]


NODE_CREATION_QUERY = """
CREATE (n:{node_label}) 
SET n.last_ingested_at = $timestamp
SET n.identifier = $node_id
"""

RELATIONSHIP_CREATION_QUERY = """
MATCH (from_node:{from_node_label}) MATCH (to_node:{to_node_label}) 
WHERE to_node.identifier=$to_node_identifier AND from_node.identifier=$from_node_identifier
CREATE (from_node)-[rel:{relationship_label}]->(to_node) 
SET rel.last_ingested_at = $timestamp
"""

REALLY_OLD_TIMESTAMP = Timestamp.utcnow() - Timedelta(hours=60)
OLD_TIMESTAMP = Timestamp.utcnow() - Timedelta(hours=36)
NEW_TIMESTAMP = Timestamp.utcnow() - Timedelta(hours=12)


def create_node_query_from_params(node_label, node_id, timestamp):
    return Query(
        NODE_CREATION_QUERY.format(node_label=node_label),
        {"timestamp": timestamp, "node_id": node_id},
    )


def create_relationship_query_from_params(
    from_node_label,
    from_node_identifier,
    to_node_label,
    to_node_identifier,
    relationship_label,
    timestamp,
):
    return Query(
        RELATIONSHIP_CREATION_QUERY.format(
            from_node_label=from_node_label,
            to_node_label=to_node_label,
            relationship_label=relationship_label,
        ),
        {
            "timestamp": timestamp,
            "from_node_identifier": from_node_identifier,
            "to_node_identifier": to_node_identifier,
        },
    )


"""
The test works like this:
    We create 60 nodes in total:
        10 ObjectA 60hrs old
        10 ObjectB 60hrs old
        10 ObjectA 36hrs old
        10 ObjectB 36hrs old
        10 ObjectA 12hrs old
        10 ObjectB 12hrs old

    We create 60 pairwise relationships as follows:
        ObjectA -ADJACENT_TO {60hrs old} > Object B
        ObjectA -CONNECTED_TO {60hrs old} > Object B
        ....

    Following the same pattern.

    With our TTL configuration, were able to first get rid of relationships.
    CONNECTED_TO has 48hr expiry leaving 20 relationships of this type post deletion.
    ADJACENT_TO has 24hr expiry leaving 10 relationships of this type post deletion

    We assert that the nodes were untouched in the validate_consistency_in_node_counts  function located above.
    We then do the exact same thing with the nodes.
"""


def create_test_objects(session: Session):
    def create_node(node_label, node_id, timestamp):
        query = create_node_query_from_params(node_label, node_id, timestamp)
        session.run(query.query_statement, query.parameters)

    def create_relationship(
        from_node_label,
        from_node_identifier,
        to_node_label,
        to_node_identifier,
        relationship_label,
        timestamp,
    ):
        query = create_relationship_query_from_params(
            from_node_label,
            from_node_identifier,
            to_node_label,
            to_node_identifier,
            relationship_label,
            timestamp,
        )
        session.run(query.query_statement, query.parameters)

    for i in range(0, 10):
        create_node("ObjectA", str(i), REALLY_OLD_TIMESTAMP)
        create_node("ObjectB", str(i), REALLY_OLD_TIMESTAMP)

    for i in range(10, 20):
        create_node("ObjectA", str(i), OLD_TIMESTAMP)
        create_node("ObjectB", str(i), OLD_TIMESTAMP)

    for i in range(20, 30):
        create_node("ObjectA", str(i), NEW_TIMESTAMP)
        create_node("ObjectB", str(i), NEW_TIMESTAMP)

    for i in range(0, 10):
        create_relationship(
            "ObjectA", str(i), "ObjectB", str(i), "CONNECTED_TO", NEW_TIMESTAMP
        )
        create_relationship(
            "ObjectA", str(i), "ObjectB", str(i), "ADJACENT_TO", NEW_TIMESTAMP
        )

    for i in range(10, 20):
        create_relationship(
            "ObjectA", str(i), "ObjectB", str(i), "CONNECTED_TO", OLD_TIMESTAMP
        )
        create_relationship(
            "ObjectA", str(i), "ObjectB", str(i), "ADJACENT_TO", OLD_TIMESTAMP
        )

    for i in range(20, 30):
        create_relationship(
            "ObjectA", str(i), "ObjectB", str(i), "CONNECTED_TO", REALLY_OLD_TIMESTAMP
        )
        create_relationship(
            "ObjectA", str(i), "ObjectB", str(i), "ADJACENT_TO", REALLY_OLD_TIMESTAMP
        )


@pytest.mark.asyncio
@pytest.mark.e2e
@pytest.mark.parametrize("neo4j_version", TESTED_NEO4J_VERSIONS)
@pytest.mark.parametrize(
    "pipeline_name,validations",
    PIPELINE_TESTS,
)
async def test_neo4j_pipeline(
    project, neo4j_container, pipeline_name, validations, neo4j_version
):
    with neo4j_container(
        neo4j_version
    ) as neo4j_container, neo4j_container.get_driver() as driver, driver.session() as session:
        target = project.get_target_by_name("my-neo4j-db")

        await project.run(
            RunRequest(
                pipeline_name,
                PipelineInitializationArguments(extra_steps=[target.make_writer()]),
                PipelineProgressReporter(),
            )
        )

        for validator in validations:
            validator(session)


@pytest.mark.asyncio
@pytest.mark.e2e
@pytest.mark.parametrize("neo4j_version", TESTED_NEO4J_VERSIONS)
async def test_neo4j_ttls(project, neo4j_container, neo4j_version):
    with neo4j_container(
        neo4j_version
    ) as neo4j_container, neo4j_container.get_driver() as driver, driver.session() as session:
        create_test_objects(session)
        target = project.get_target_by_name("my-neo4j-db")
        for pipeline_name, validations in TTL_TESTS:
            await project.run(
                RunRequest(
                    pipeline_name,
                    PipelineInitializationArguments(extra_steps=[target.make_writer()]),
                    PipelineProgressReporter(),
                )
            )
            for validator in validations:
                validator(session)


@pytest.mark.asyncio
@pytest.mark.e2e
@pytest.mark.parametrize("neo4j_version", TESTED_NEO4J_VERSIONS)
async def test_neo4j_extractor(project, neo4j_container, neo4j_version):
    with neo4j_container(
        neo4j_version
    ) as neo4j_container, neo4j_container.get_driver() as driver, driver.session() as session:
        create_test_objects(session)
        target = project.get_target_by_name("my-neo4j-db")
        for pipeline_name, validations in EXTRACTOR_TESTS:
            await project.run(
                RunRequest(
                    pipeline_name,
                    PipelineInitializationArguments(extra_steps=[target.make_writer()]),
                    PipelineProgressReporter(),
                )
            )
            for validator in validations:
                validator(session)
