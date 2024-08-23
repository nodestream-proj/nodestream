from contextlib import contextmanager
from os import environ

import pytest
from testcontainers.neo4j import Neo4jContainer

TESTED_NEO4J_VERSIONS = ["5.16"]


class Neo4jContainerWithApoc(Neo4jContainer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.with_env("NEO4J_PLUGINS", '["apoc"]]')
        self.with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")


@pytest.fixture
def neo4j_container():
    @contextmanager
    def _create_neo4j_container(neo4j_version, use_enterprise=False):
        image_tag = f"neo4j:{neo4j_version}"
        if use_enterprise:
            image_tag = f"{image_tag}-enterprise"
        with Neo4jContainerWithApoc(image=image_tag) as neo4j_container:
            environ["NEO4J_CONNECT_URI"] = neo4j_container.get_connection_url()
            yield neo4j_container

    return _create_neo4j_container
