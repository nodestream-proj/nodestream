from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
from hamcrest import assert_that, equal_to, has_length

from nodestream.databases.copy import (
    ConcurrentCopier,
    Copier,
)
from nodestream.model import Node, Relationship, RelationshipWithNodes
from nodestream.pipeline.object_storage import ObjectStore
from nodestream.pipeline.progress_reporter import PipelineProgressReporter
from nodestream.pipeline.step import StepContext
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


def _make_step_context():
    """Build a lightweight StepContext backed by a null ObjectStore."""
    return StepContext("test", 0, PipelineProgressReporter(), ObjectStore.null())


@pytest.mark.asyncio
async def test_start_sorts_node_types_descending_by_count(subject):
    """start() should reorder node_types so the largest types come first."""
    subject.type_retriever.preview_node_count = AsyncMock(
        side_effect=lambda t: {"Person": 50, "Address": 200}[t]
    )
    subject.type_retriever.preview_relationship_count = AsyncMock(
        side_effect=lambda t: {"KNOWS": 10, "LIVES_AT": 30}[t]
    )

    await subject.start(_make_step_context())

    assert_that(subject.node_types, equal_to(["Address", "Person"]))
    assert_that(subject.relationship_types, equal_to(["LIVES_AT", "KNOWS"]))


@pytest.mark.asyncio
async def test_start_persists_counts_for_logging(subject):
    """start() should store preview counts as instance state."""
    subject.type_retriever.preview_node_count = AsyncMock(
        side_effect=lambda t: {"Person": 100, "Address": 50}[t]
    )
    subject.type_retriever.preview_relationship_count = AsyncMock(
        side_effect=lambda t: {"KNOWS": 75, "LIVES_AT": 25}[t]
    )

    await subject.start(_make_step_context())

    assert subject._node_counts == {"Person": 100, "Address": 50}
    assert subject._relationship_counts == {"KNOWS": 75, "LIVES_AT": 25}


@pytest.mark.asyncio
async def test_extract_records_respects_start_sort_order(subject, mocker):
    """extract_records should iterate types in the order established by start()."""
    # Make Address larger so it sorts first after start().
    subject.type_retriever.preview_node_count = AsyncMock(
        side_effect=lambda t: {"Person": 10, "Address": 500}[t]
    )
    subject.type_retriever.preview_relationship_count = AsyncMock(
        side_effect=lambda t: {"KNOWS": 5, "LIVES_AT": 100}[t]
    )
    await subject.start(_make_step_context())

    # Now set up the generators in the *sorted* order (Address first, Person second).
    addresses = async_generator(Node("Address", {"street": "123 Main St"}))
    people = async_generator(Node("Person", {"name": "Bob"}))
    lives_at_rels = async_generator(
        RelationshipWithNodes(
            Node("Person", {"name": "Bob"}),
            Node("Address", {"street": "123 Main St"}),
            Relationship("LIVES_AT", {"since": 2010}),
        ),
    )
    knows_rels = async_generator(
        RelationshipWithNodes(
            Node("Person", {"name": "Bob"}),
            Node("Person", {"name": "Alice"}),
            Relationship("KNOWS", {"since": 2010}),
        ),
    )

    subject.convert_node_to_ingest = mocker.Mock(side_effect=lambda n: ("node", n.type))
    subject.convert_relationship_to_ingest = mocker.Mock(
        side_effect=lambda r: ("rel", r.relationship.type)
    )
    # Side effects are consumed in call order — Address first, then Person.
    subject.type_retriever.get_nodes_of_type.side_effect = [addresses, people]
    subject.type_retriever.get_relationships_of_type_between.side_effect = [
        lives_at_rels,
        knows_rels,
    ]

    records = [record async for record in subject.extract_records()]

    assert_that(records, has_length(4))
    # Verify the ordering: Address nodes come before Person nodes.
    assert records[0] == ("node", "Address")
    assert records[1] == ("node", "Person")
    assert records[2] == ("rel", "LIVES_AT")
    assert records[3] == ("rel", "KNOWS")


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
            Node("Person", {"name": "Bob"}),
            Node("Person", {"name": "Alice"}),
            Relationship("KNOWS", {"since": 2010}),
        ),
        RelationshipWithNodes(
            Node("Person", {"name": "Alice"}),
            Node("Person", {"name": "Bob"}),
            Relationship("KNOWS", {"since": 2015}),
        ),
    )
    lives_at_rels = async_generator(
        RelationshipWithNodes(
            Node("Person", {"name": "Bob"}),
            Node("Address", {"street": "123 Main St"}),
            Relationship("LIVES_AT", {"since": 2010}),
        ),
        RelationshipWithNodes(
            Node("Person", {"name": "Alice"}),
            Node("Address", {"street": "456 Main St"}),
            Relationship("LIVES_AT", {"since": 2015}),
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
            Node("Person", {"name": "Bob"}),
            Node("Person", {"name": "Alice"}),
            Relationship("KNOWS", {"since": 2010}),
        ),
    )
    lives_at_rels = async_generator(
        RelationshipWithNodes(
            Node("Person", {"name": "Bob"}),
            Node("Address", {"street": "123 Main St"}),
            Relationship("LIVES_AT", {"since": 2010}),
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


@pytest.mark.asyncio
async def test_concurrent_copier_propagates_producer_error(mocker, basic_schema):
    """If a producer raises, the error should propagate instead of hanging."""
    schema = _build_schema_with_adjacencies(basic_schema)
    copier = ConcurrentCopier(
        mocker.Mock(), schema, ["Person"], [], concurrency_limit=2
    )

    async def failing_generator(*_):
        yield Node("Person", {"name": "Bob"})
        raise RuntimeError("database went away")

    copier.convert_node_to_ingest = mocker.Mock()
    copier.type_retriever.get_nodes_of_type.side_effect = [failing_generator()]

    with pytest.raises(RuntimeError, match="database went away"):
        async for _ in copier.extract_records():
            pass


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
