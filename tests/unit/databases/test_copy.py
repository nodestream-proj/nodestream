from copy import deepcopy

import pytest
from hamcrest import assert_that, has_length

from nodestream.databases.copy import Copier
from nodestream.model import Node, Relationship, RelationshipWithNodes
from nodestream.schema import Adjacency, AdjacencyCardinality, Cardinality


@pytest.fixture
def subject(mocker, basic_schema):
    return Copier(
        mocker.Mock(), basic_schema, ["Person", "Address"], ["KNOWS", "LIVES_AT"]
    )


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
    subject.type_retriever.get_relationships_of_type.side_effect = [
        knows_rels,
        lives_at_rels,
    ]

    records = [record async for record in subject.extract_records()]
    assert_that(records, has_length(8))
    subject.convert_node_to_ingest.call_count == 4
    subject.convert_relationship_to_ingest.call_count == 4


@pytest.mark.asyncio
async def test_extract_records_uses_schema_adjacencies(subject, mocker, basic_schema):
    """When adjacencies exist in the schema, Copier should use the schema-driven path."""
    # Clone the basic schema and add adjacencies for the relationship types we care about.
    schema = deepcopy(basic_schema)
    schema.add_adjacency(
        Adjacency("Person", "Person", "KNOWS"),
        AdjacencyCardinality(Cardinality.SINGLE, Cardinality.MANY),
    )
    schema.add_adjacency(
        Adjacency("Person", "Address", "LIVES_AT"),
        AdjacencyCardinality(Cardinality.SINGLE, Cardinality.MANY),
    )
    subject.schema = schema

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
    # In the adjacency-driven path, we should not be using the schemaless method.
    assert subject.type_retriever.get_relationships_of_type.call_count == 0


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


def test_convert_relationship_to_ingest(subject):
    rel = RelationshipWithNodes(
        Node("Person", properties={"name": "Bob"}),
        Node("Person", properties={"name": "Alice"}),
        Relationship("KNOWS", {"since": 2010}),
    )
    ingest = subject.convert_relationship_to_ingest(rel)
    assert ingest == rel.into_ingest()
