import asyncio
import tempfile
from pathlib import Path

import pytest

from nodestream.model import DesiredIngestion
from nodestream.pipeline.value_providers import ProviderContext
from nodestream.project import PipelineDefinition, PipelineScope, Project
from nodestream.schema import (
    Adjacency,
    AdjacencyCardinality,
    Cardinality,
    GraphObjectSchema,
    PropertyMetadata,
    PropertyType,
    Schema,
    SchemaExpansionCoordinator,
)

DECENT_DOCUMENT = {
    "team": {
        "name": "nodestream",
    },
    "members": [
        {"first_name": "Zach", "last_name": "Probst"},
        {"first_name": "Chad", "last_name": "Cloes"},
    ],
    "project": {"tags": ["graphdb", "python"]},
}


@pytest.fixture
def blank_context():
    return ProviderContext({}, DesiredIngestion())


@pytest.fixture
def blank_context_with_document():
    return ProviderContext(DECENT_DOCUMENT, DesiredIngestion())


@pytest.fixture
def async_return():
    def _async_return(value=None):
        future = asyncio.Future()
        future.set_result(4)
        return future

    return _async_return


@pytest.fixture
def basic_schema():
    schema = Schema()
    person = GraphObjectSchema(
        name="Person",
        properties={
            "name": PropertyMetadata(PropertyType.STRING, is_key=True),
            "age": PropertyMetadata(PropertyType.INTEGER),
        },
    )
    organization = GraphObjectSchema(
        name="Organization",
        properties={
            "name": PropertyMetadata(PropertyType.STRING),
            "industry": PropertyMetadata(PropertyType.STRING),
        },
    )
    best_friend_of = GraphObjectSchema(
        name="BEST_FRIEND_OF",
        properties={
            "since": PropertyMetadata(PropertyType.DATETIME),
        },
    )
    has_employee = GraphObjectSchema(
        name="HAS_EMPLOYEE",
        properties={
            "since": PropertyMetadata(PropertyType.DATETIME),
        },
    )

    schema.put_node_type(person)
    schema.put_node_type(organization)
    schema.put_relationship_type(best_friend_of)
    schema.put_relationship_type(has_employee)

    schema.add_adjacency(
        adjacency=Adjacency("Person", "Person", "BEST_FRIEND_OF"),
        cardinality=AdjacencyCardinality(Cardinality.SINGLE, Cardinality.MANY),
    )
    schema.add_adjacency(
        adjacency=Adjacency("Organization", "Person", "HAS_EMPLOYEE"),
        cardinality=AdjacencyCardinality(Cardinality.MANY, Cardinality.MANY),
    )

    return schema


@pytest.fixture
def pipeline_definition(project_dir):
    return PipelineDefinition("dummy", project_dir / "dummy.yaml")


@pytest.fixture
def project_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def default_scope(pipeline_definition):
    return PipelineScope("default", [pipeline_definition])


@pytest.fixture
def project_with_default_scope(default_scope):
    return Project([default_scope])


@pytest.fixture
def schema_coordinator():
    return SchemaExpansionCoordinator(Schema())
