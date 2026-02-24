from copy import deepcopy

import pytest
from hamcrest import assert_that, has_length

from nodestream.databases.copy import (
    ConcurrentCopier,
    Copier,
)
from nodestream.model import Node, Relationship, RelationshipWithNodes
from nodestream.schema import Adjacency, AdjacencyCardinality, Cardinality


def _build_schema_with_adjacencies(basic_schema):
    """Helper to build a schema with KNOWS (self-ref) and LIVES_AT adjacencies."""
    schema = deepcopy(basic_schema)
    schema.add_adjacency(
        Adjacency("Person", "Person", "KNOWS"),
        AdjacencyCardinality(Cardinality.SINGLE, Cardinality.MANY),
    )
    schema.add_adjacency(
        Adjacency("Person", "Address", "LIVES_AT"),
        AdjacencyCardinality(Cardinality.SINGLE, Cardinality.MANY),
    )
    return schema


@pytest.fixture
def subject(mocker, basic_schema):
    # Use a schema that declares the adjacencies we expect to copy so that the
    # copier always exercises the schema-driven relationship path.
    schema = _build_schema_with_adjacencies(basic_schema)
    return Copier(mocker.Mock(), schema, ["Person", "Address"], ["KNOWS", "LIVES_AT"])


async def async_generator(*items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_extract_records(subject, mocker):
    people = async_generator(
        Node("Person", {"name": "Bob"}),
        Node("Person", {"name": "Alice"}),
    )
    addresses = async_generator(
        Node("Address", {"street": "123 Main St"}),
        Node("Address", {"street": "456 Main St"}),
    )
    knows_rels = async_generator(
        RelationshipWithNodes(
            Relationship("KNOWS", {"since": 2010}),
            Node("Person", {"name": "Bob"}),
            Node("Person", {"name": "Alice"}),
        ),
        RelationshipWithNodes(
            Relationship("KNOWS", {"since": 2015}),
            Node("Person", {"name": "Alice"}),
            Node("Person", {"name": "Bob"}),
        ),
    )
    lives_at_rels = async_generator(
        RelationshipWithNodes(
            Relationship("LIVES_AT", {"since": 2010}),
            Node("Person", {"name": "Bob"}),
            Node("Address", {"street": "123 Main St"}),
        ),
        RelationshipWithNodes(
            Relationship("LIVES_AT", {"since": 2015}),
            Node("Person", {"name": "Alice"}),
            Node("Address", {"street": "456 Main St"}),
        ),
    )

    subject.convert_node_to_ingest = mocker.Mock()
    subject.convert_relationship_to_ingest = mocker.Mock()
    subject.type_retriever.get_nodes_of_type.side_effect = [people, addresses]
    subject.type_retriever.get_relationships_of_type_between.side_effect = [
        knows_rels,
        lives_at_rels,
    ]

    records = [record async for record in subject.extract_records()]

    # 4 nodes + 4 relationships
    assert_that(records, has_length(8))
    assert subject.type_retriever.get_relationships_of_type_between.call_count == 2


def test_convert_node_to_ingest(subject):
    input_node = Node("Person", properties={"name": "bob", "age": 30})
    output_node = Node("Person", key_values={"name": "bob"}, properties={"age": 30})
    ingest = subject.convert_node_to_ingest(input_node)
    assert ingest == output_node.into_ingest()


def test_convert_node_to_ingest_with_unknown_type_does_not_error(subject):
    """reorganize_node_key_properties should be a no-op when the type is unknown."""
    # Use a node type that is not present in the basic_schema fixture.
    input_node = Node("UnknownType", properties={"name": "bob"})
    # This should not raise, and key_values should remain empty.
    ingest = subject.convert_node_to_ingest(input_node)
    assert ingest.source.key_values == {}


def test_convert_node_to_ingest_with_none_type_does_not_error(subject):
    """reorganize_node_key_properties should be a no-op when the node type is None."""
    input_node = Node(type=None, properties={"name": "bob"})
    ingest = subject.convert_node_to_ingest(input_node)
    # When type is None, get_node_type_by_name returns None and the method returns early.
    assert ingest.source.key_values == {}


def test_convert_relationship_to_ingest(subject):
    rel = RelationshipWithNodes(
        Node("Person", properties={"name": "Bob"}),
        Node("Person", properties={"name": "Alice"}),
        Relationship("KNOWS", {"since": 2010}),
    )
    ingest = subject.convert_relationship_to_ingest(rel)
    assert ingest == rel.into_ingest()


# ---------------------------------------------------------------------------
# ConcurrentCopier tests
# ---------------------------------------------------------------------------


@pytest.fixture
def concurrent_subject(mocker, basic_schema):
    schema = _build_schema_with_adjacencies(basic_schema)
    return ConcurrentCopier(
        mocker.Mock(),
        schema,
        ["Person", "Address"],
        ["KNOWS", "LIVES_AT"],
        concurrency_limit=10,
    )


@pytest.mark.asyncio
async def test_concurrent_copier_extract_records(concurrent_subject, mocker):
    """All node types are fetched first, then all relationship types."""
    people = async_generator(
        Node("Person", {"name": "Bob"}),
        Node("Person", {"name": "Alice"}),
    )
    addresses = async_generator(
        Node("Address", {"street": "123 Main St"}),
        Node("Address", {"street": "456 Main St"}),
    )
    knows_rels = async_generator(
        RelationshipWithNodes(
            Relationship("KNOWS", {"since": 2010}),
            Node("Person", {"name": "Bob"}),
            Node("Person", {"name": "Alice"}),
        ),
    )
    lives_at_rels = async_generator(
        RelationshipWithNodes(
            Relationship("LIVES_AT", {"since": 2010}),
            Node("Person", {"name": "Bob"}),
            Node("Address", {"street": "123 Main St"}),
        ),
    )

    concurrent_subject.convert_node_to_ingest = mocker.Mock()
    concurrent_subject.convert_relationship_to_ingest = mocker.Mock()
    concurrent_subject.type_retriever.get_nodes_of_type.side_effect = [
        people,
        addresses,
    ]
    concurrent_subject.type_retriever.get_relationships_of_type_between.side_effect = [
        knows_rels,
        lives_at_rels,
    ]

    records = [record async for record in concurrent_subject.extract_records()]

    # 4 nodes + 2 relationships = 6 records total
    assert_that(records, has_length(6))
    assert (
        concurrent_subject.type_retriever.get_relationships_of_type_between.call_count
        == 2
    )


@pytest.mark.asyncio
async def test_concurrent_copier_no_types(mocker, basic_schema):
    """ConcurrentCopier should handle empty type lists gracefully."""
    schema = deepcopy(basic_schema)
    copier = ConcurrentCopier(mocker.Mock(), schema, [], [], concurrency_limit=5)
    records = [record async for record in copier.extract_records()]
    assert records == []


def test_copier_create_sequential(mocker, basic_schema):
    """Copier.create returns a plain Copier when concurrency_limit is 1."""
    copier = Copier.create(mocker.Mock(), basic_schema, ["Person"], ["KNOWS"])
    assert type(copier) is Copier


def test_copier_create_concurrent(mocker, basic_schema):
    """Copier.create returns a ConcurrentCopier when concurrency_limit > 1."""
    copier = Copier.create(
        mocker.Mock(),
        basic_schema,
        ["Person"],
        ["KNOWS"],
        concurrency_limit=5,
    )
    assert isinstance(copier, ConcurrentCopier)
    assert copier.concurrency_limit == 5
