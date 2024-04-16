from pathlib import Path

import pytest
from nodestream.pipeline import (
    PipelineInitializationArguments,
    PipelineProgressReporter,
)
from nodestream.project import Project, RunRequest

from .conftest import TESTED_NEO4J_VERSIONS


@pytest.fixture
def project():
    return Project.read_from_file(Path("tests/e2e/project/nodestream.yaml"))


def validate_airports(session):
    result = session.run(
        """
        MATCH (a:Airport)
        RETURN count(a) AS count
        """
    )

    assert result.single()["count"] == 1000


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


def validate_relationship_ttls(session):
    result = session.run(
        """
        MATCH ()-[r]-()
        RETURN count(r) as relationship_count
        """
    )
    assert result.single()["relationship_count"] == 0
    result = session.run(
        """
            MATCH (n)
            RETURN count(n) as node_count
        """
    )
    assert result.single()["relationship_count"] != 0


def validate_node_ttls(session):
    result = session.run(
        """
        MATCH (n)
        RETURN count(n) as node_count
        """
    )
    assert result.single()["node_count"] == 0


@pytest.mark.asyncio
@pytest.mark.e2e
@pytest.mark.parametrize("neo4j_version", TESTED_NEO4J_VERSIONS)
@pytest.mark.parametrize(
    "pipeline_name,validations",
    [
        ("airports", [validate_airports, valiudate_airport_country]),
        ("fifa", [validate_fifa_player_count, validate_fifa_mo_club]),
        ("relatiobship-ttls", [validate_relationship_ttls]),
        ("node-ttls", [validate_node_ttls]),
    ],
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
