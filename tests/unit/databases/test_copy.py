from copy import deepcopy

import pytest
from hamcrest import assert_that, has_length

from nodestream.databases.copy import (
    ConcurrentCopier,
    Copier,
    _adjacency_progress_key,
    _node_progress_key,
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
    """Copier.create returns a plain Copier when run_concurrently is False."""
    copier = Copier.create(
        mocker.Mock(), basic_schema, ["Person"], ["KNOWS"], run_concurrently=False
    )
    assert type(copier) is Copier


def test_copier_create_concurrent(mocker, basic_schema):
    """Copier.create returns a ConcurrentCopier when run_concurrently is True."""
    copier = Copier.create(
        mocker.Mock(),
        basic_schema,
        ["Person"],
        ["KNOWS"],
        run_concurrently=True,
        concurrency_limit=5,
    )
    assert isinstance(copier, ConcurrentCopier)
    assert copier.concurrency_limit == 5


# ---------------------------------------------------------------------------
# Checkpoint / resume tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_copier_make_checkpoint_empty(subject):
    """When no records have been yielded, make_checkpoint returns None."""
    checkpoint = await subject.make_checkpoint()
    assert checkpoint is None


@pytest.mark.asyncio
async def test_copier_make_checkpoint_after_extraction(subject, mocker):
    """After yielding records, make_checkpoint returns progress state."""
    people = async_generator(
        Node("Person", {"name": "Bob"}),
        Node("Person", {"name": "Alice"}),
    )
    addresses = async_generator()  # empty

    subject.convert_node_to_ingest = mocker.Mock(side_effect=lambda n: n)
    subject.convert_relationship_to_ingest = mocker.Mock(side_effect=lambda r: r)
    subject.type_retriever.get_nodes_of_type.side_effect = [people, addresses]
    subject.type_retriever.get_relationships_of_type_between.side_effect = [
        async_generator(),
        async_generator(),
    ]

    _ = [record async for record in subject.extract_records()]

    checkpoint = await subject.make_checkpoint()
    assert checkpoint is not None
    assert checkpoint["progress"][_node_progress_key("Person")] == 2


@pytest.mark.asyncio
async def test_copier_resume_skips_already_copied_nodes(subject, mocker):
    """After resuming from checkpoint, already-copied records are skipped."""
    # Simulate a checkpoint where 1 Person node was already copied.
    await subject.resume_from_checkpoint(
        {"progress": {_node_progress_key("Person"): 1}}
    )

    people = async_generator(
        Node("Person", {"name": "Bob"}),  # index 1 — should be skipped
        Node("Person", {"name": "Alice"}),  # index 2 — should be yielded
    )
    addresses = async_generator(
        Node("Address", {"street": "123 Main St"}),
    )

    subject.convert_node_to_ingest = mocker.Mock(side_effect=lambda n: n)
    subject.convert_relationship_to_ingest = mocker.Mock(side_effect=lambda r: r)
    subject.type_retriever.get_nodes_of_type.side_effect = [people, addresses]
    subject.type_retriever.get_relationships_of_type_between.side_effect = [
        async_generator(),
        async_generator(),
    ]

    records = [record async for record in subject.extract_records()]

    # 1 Person skipped + 1 Person yielded + 1 Address yielded = 2 records
    assert_that(records, has_length(2))


@pytest.mark.asyncio
async def test_copier_resume_skips_already_copied_relationships(subject, mocker):
    """After resuming, already-copied relationship records are skipped."""
    adj_key = _adjacency_progress_key(Adjacency("Person", "Person", "KNOWS"))
    await subject.resume_from_checkpoint({"progress": {adj_key: 1}})

    # No nodes to copy (all done).
    subject.convert_node_to_ingest = mocker.Mock(side_effect=lambda n: n)
    subject.convert_relationship_to_ingest = mocker.Mock(side_effect=lambda r: r)
    subject.type_retriever.get_nodes_of_type.side_effect = [
        async_generator(),
        async_generator(),
    ]

    knows_rels = async_generator(
        RelationshipWithNodes(
            Relationship("KNOWS", {"since": 2010}),
            Node("Person", {"name": "Bob"}),
            Node("Person", {"name": "Alice"}),
        ),  # index 1 — skipped
        RelationshipWithNodes(
            Relationship("KNOWS", {"since": 2015}),
            Node("Person", {"name": "Alice"}),
            Node("Person", {"name": "Bob"}),
        ),  # index 2 — yielded
    )
    lives_at_rels = async_generator(
        RelationshipWithNodes(
            Relationship("LIVES_AT", {"since": 2020}),
            Node("Person", {"name": "Bob"}),
            Node("Address", {"street": "123 Main St"}),
        ),
    )
    subject.type_retriever.get_relationships_of_type_between.side_effect = [
        knows_rels,
        lives_at_rels,
    ]

    records = [record async for record in subject.extract_records()]

    # 1 KNOWS skipped + 1 KNOWS yielded + 1 LIVES_AT yielded = 2 records
    assert_that(records, has_length(2))


@pytest.mark.asyncio
async def test_copier_checkpoint_roundtrip(subject, mocker):
    """make_checkpoint -> resume_from_checkpoint preserves progress state."""
    people_run1 = async_generator(
        Node("Person", {"name": "Bob"}),
        Node("Person", {"name": "Alice"}),
        Node("Person", {"name": "Charlie"}),
    )

    subject.convert_node_to_ingest = mocker.Mock(side_effect=lambda n: n)
    subject.convert_relationship_to_ingest = mocker.Mock(side_effect=lambda r: r)
    subject.type_retriever.get_nodes_of_type.side_effect = [
        people_run1,
        async_generator(),
    ]
    subject.type_retriever.get_relationships_of_type_between.side_effect = [
        async_generator(),
        async_generator(),
    ]

    _ = [record async for record in subject.extract_records()]

    checkpoint = await subject.make_checkpoint()
    assert checkpoint["progress"][_node_progress_key("Person")] == 3

    # Create a fresh copier and resume from the checkpoint.
    fresh = Copier(
        subject.type_retriever,
        subject.schema,
        subject.node_types,
        subject.relationship_types,
    )
    await fresh.resume_from_checkpoint(checkpoint)
    assert fresh._progress[_node_progress_key("Person")] == 3


# ---------------------------------------------------------------------------
# ConcurrentCopier checkpoint tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_copier_make_checkpoint_after_extraction(
    concurrent_subject, mocker
):
    """ConcurrentCopier tracks per-producer progress for checkpointing."""
    people = async_generator(
        Node("Person", {"name": "Bob"}),
        Node("Person", {"name": "Alice"}),
    )
    addresses = async_generator(
        Node("Address", {"street": "123 Main St"}),
    )

    concurrent_subject.convert_node_to_ingest = mocker.Mock(side_effect=lambda n: n)
    concurrent_subject.convert_relationship_to_ingest = mocker.Mock(
        side_effect=lambda r: r
    )
    concurrent_subject.type_retriever.get_nodes_of_type.side_effect = [
        people,
        addresses,
    ]
    concurrent_subject.type_retriever.get_relationships_of_type_between.side_effect = [
        async_generator(),
        async_generator(),
    ]

    _ = [record async for record in concurrent_subject.extract_records()]

    checkpoint = await concurrent_subject.make_checkpoint()
    assert checkpoint is not None
    assert checkpoint["progress"][_node_progress_key("Person")] == 2
    assert checkpoint["progress"][_node_progress_key("Address")] == 1


@pytest.mark.asyncio
async def test_concurrent_copier_resume_skips_records(concurrent_subject, mocker):
    """ConcurrentCopier skips already-copied records per producer on resume."""
    await concurrent_subject.resume_from_checkpoint(
        {"progress": {_node_progress_key("Person"): 1}}
    )

    people = async_generator(
        Node("Person", {"name": "Bob"}),  # index 1 — skipped
        Node("Person", {"name": "Alice"}),  # index 2 — yielded
    )
    addresses = async_generator(
        Node("Address", {"street": "123 Main St"}),
    )

    concurrent_subject.convert_node_to_ingest = mocker.Mock(side_effect=lambda n: n)
    concurrent_subject.convert_relationship_to_ingest = mocker.Mock(
        side_effect=lambda r: r
    )
    concurrent_subject.type_retriever.get_nodes_of_type.side_effect = [
        people,
        addresses,
    ]
    concurrent_subject.type_retriever.get_relationships_of_type_between.side_effect = [
        async_generator(),
        async_generator(),
    ]

    records = [record async for record in concurrent_subject.extract_records()]

    # 1 Person skipped + 1 Person yielded + 1 Address yielded = 2 records
    assert_that(records, has_length(2))
