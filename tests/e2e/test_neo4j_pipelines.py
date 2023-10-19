from contextlib import contextmanager
from os import environ
from pathlib import Path

import pytest
from testcontainers.neo4j import Neo4jContainer

from nodestream.pipeline import (
    PipelineInitializationArguments,
    PipelineProgressReporter,
)
from nodestream.project import Project, RunRequest

EXPECTED_USER_NAME = "neo4j"
EXPECTED_PASSWORD = "test12345"


@pytest.fixture
def project():
    return Project.read_from_file(Path("tests/e2e/pipelines/nodestream.yaml"))


class Neo4jContainerWithApoc(Neo4jContainer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.with_env("NEO4J_PLUGINS", '["apoc"]]')


@pytest.fixture
def neo4j_container():
    @contextmanager
    def _create_neo4j_container(neo4j_version):
        with Neo4jContainerWithApoc(image=f"neo4j:{neo4j_version}") as neo4j_container:
            environ["NEO4J_CONNECT_URI"] = neo4j_container.get_connection_url()
            yield neo4j_container

    return _create_neo4j_container


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
        MATCH (p:Player)
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


@pytest.mark.asyncio
@pytest.mark.e2e
@pytest.mark.parametrize("neo4j_version", ["4.4", "5.1", "5.10", "5.12"])
@pytest.mark.parametrize(
    "pipeline_name,validations",
    [
        ("airports", [validate_airports, valiudate_airport_country]),
        ("fifa", [validate_fifa_player_count, validate_fifa_mo_club]),
    ],
)
async def test_neo4j_pipeline(
    project, neo4j_container, pipeline_name, validations, neo4j_version
):
    with neo4j_container(
        neo4j_version
    ) as neo4j_container, neo4j_container.get_driver() as driver, driver.session() as session:
        await project.run(
            RunRequest(
                pipeline_name,
                PipelineInitializationArguments(),
                PipelineProgressReporter(),
            )
        )

        for validator in validations:
            validator(session)
