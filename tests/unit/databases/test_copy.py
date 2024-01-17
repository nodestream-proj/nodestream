import pytest
from hamcrest import assert_that, has_length

from nodestream.databases.copy import Copier
from nodestream.model import Node, Relationship, RelationshipWithNodes
from nodestream.schema import GraphObjectSchema, PropertyMetadata


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


def test_convert_node_to_ingest(subject):
    input_node = Node("Person", properties={"name": "bob", "age": 30})
    output_node = Node("Person", key_values={"name": "bob"}, properties={"age": 30})
    ingest = subject.convert_node_to_ingest(input_node)
    assert ingest == output_node.into_ingest()


def test_convert_relationship_to_ingest(subject):
    rel = RelationshipWithNodes(
        Node("Person", properties={"name": "Bob"}),
        Node("Person", properties={"name": "Alice"}),
        Relationship("KNOWS", {"since": 2010}),
    )
    ingest = subject.convert_relationship_to_ingest(rel)
    assert ingest == rel.into_ingest()
