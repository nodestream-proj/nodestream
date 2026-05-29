from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock

import pytest
from hamcrest import assert_that, has_length

from nodestream.databases.copy import (
    ConcurrentCopier,
    Copier,
    TypeHistogram,
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


async def _empty_async_gen():
    return
    yield  # pragma: no cover


def _make_mock_retriever(
    mocker, node_types=None, relationship_types=None, concurrency_limit=1
):
    """Build a mock TypeRetriever with the new ABC interface wired up."""
    retriever = mocker.Mock()
    histogram = TypeHistogram(
        node_counts={t: 0 for t in (node_types or [])},
        relationship_counts={t: 0 for t in (relationship_types or [])},
    )
    retriever.build_histogram = AsyncMock(return_value=histogram)
    retriever.fetchNodes = MagicMock(return_value=_empty_async_gen())
    retriever.fetchRelationships = MagicMock(return_value=_empty_async_gen())
    retriever.concurrency_limit = concurrency_limit
    retriever.orchestrator_queue_size = 0
    retriever.relationships_only = False
    return retriever


@pytest.fixture
def subject(mocker, basic_schema):
    schema = _build_schema_with_adjacencies(basic_schema)
    retriever = _make_mock_retriever(
        mocker,
        node_types=["Person", "Address"],
        relationship_types=["KNOWS", "LIVES_AT"],
    )
    return Copier(retriever, schema)


async def async_generator(*items):
    for item in items:
        yield item


def _make_step_context():
    return StepContext("test", 0, PipelineProgressReporter(), ObjectStore.null())


# ---------------------------------------------------------------------------
# TypeHistogram tests
# ---------------------------------------------------------------------------


def test_histogram_sorted_node_types():
    h = TypeHistogram({"Person": 10, "Address": 200, "Device": 5})
    assert h.sorted_node_types() == ["Address", "Person", "Device"]


def test_histogram_sorted_relationship_types():
    h = TypeHistogram(relationship_counts={"KNOWS": 5, "LIVES_AT": 100})
    assert h.sorted_relationship_types() == ["LIVES_AT", "KNOWS"]


def test_histogram_log(mocker):
    h = TypeHistogram({"Person": 10, "Address": 200}, {"KNOWS": 5, "LIVES_AT": 100})
    logger = mocker.Mock()
    h.log(logger)
    rendered = []
    for call in logger.info.call_args_list:
        fmt = call.args[0]
        args = call.args[1:]
        rendered.append(fmt % args if args else fmt)
    assert any("Address" in msg and "200" in msg for msg in rendered)
    assert any("Total nodes: 210" in msg for msg in rendered)
    assert any("Total relationships: 105" in msg for msg in rendered)


# ---------------------------------------------------------------------------
# Copier.start tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_delegates_histogram_to_retriever(subject):
    """start() should call build_histogram on the retriever and log it."""
    histogram = TypeHistogram(
        {"Person": 50, "Address": 200}, {"KNOWS": 10, "LIVES_AT": 30}
    )
    subject.type_retriever.build_histogram = AsyncMock(return_value=histogram)

    await subject.start(_make_step_context())

    subject.type_retriever.build_histogram.assert_called_once_with(subject.schema)


@pytest.mark.asyncio
async def test_start_logs_histogram(subject, mocker):
    histogram = TypeHistogram(
        {"Person": 10, "Address": 200}, {"KNOWS": 5, "LIVES_AT": 100}
    )
    subject.type_retriever.build_histogram = AsyncMock(return_value=histogram)
    mock_logger = mocker.patch.object(subject, "logger")

    await subject.start(_make_step_context())

    rendered = []
    for call in mock_logger.info.call_args_list:
        fmt = call.args[0]
        args = call.args[1:]
        rendered.append(fmt % args if args else fmt)

    assert any("Node type histogram" in msg for msg in rendered)
    assert any("Relationship type histogram" in msg for msg in rendered)
    assert any("Address" in msg and "200" in msg for msg in rendered)
    assert any("Total nodes: 210" in msg for msg in rendered)
    assert any("Total relationships: 105" in msg for msg in rendered)


# ---------------------------------------------------------------------------
# Copier.extract_records tests
# ---------------------------------------------------------------------------


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

    async def mockFetchNodes(schema):
        async for n in people:
            yield n
        async for n in addresses:
            yield n

    async def mockFetchRelationships(schema):
        async for r in knows_rels:
            yield r

    subject.type_retriever.fetchNodes = mockFetchNodes
    subject.type_retriever.fetchRelationships = mockFetchRelationships
    subject.convert_node_to_ingest = mocker.Mock(side_effect=lambda n: ("node", n.type))
    subject.convert_relationship_to_ingest = mocker.Mock(
        side_effect=lambda r: ("rel", r.relationship.type)
    )

    records = [record async for record in subject.extract_records()]

    assert_that(records, has_length(6))
    assert records[0] == ("node", "Person")
    assert records[1] == ("node", "Person")
    assert records[2] == ("node", "Address")
    assert records[3] == ("node", "Address")
    assert records[4] == ("rel", "KNOWS")
    assert records[5] == ("rel", "KNOWS")


# ---------------------------------------------------------------------------
# convert helpers
# ---------------------------------------------------------------------------


def test_convert_node_to_ingest(subject):
    input_node = Node("Person", properties={"name": "bob", "age": 30})
    output_node = Node("Person", key_values={"name": "bob"}, properties={"age": 30})
    ingest = subject.convert_node_to_ingest(input_node)
    assert ingest == output_node.into_ingest()


def test_convert_node_to_ingest_with_unknown_type_does_not_error(subject):
    input_node = Node("UnknownType", properties={"name": "bob"})
    ingest = subject.convert_node_to_ingest(input_node)
    assert ingest.source.key_values == {}


def test_convert_node_to_ingest_with_none_type_does_not_error(subject):
    input_node = Node(type=None, properties={"name": "bob"})
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


# ---------------------------------------------------------------------------
# Concurrent path tests (retriever.concurrency_limit > 1)
# ---------------------------------------------------------------------------


@pytest.fixture
def concurrent_subject(mocker, basic_schema):
    schema = _build_schema_with_adjacencies(basic_schema)
    retriever = _make_mock_retriever(
        mocker,
        node_types=["Person", "Address"],
        relationship_types=["KNOWS", "LIVES_AT"],
        concurrency_limit=10,
    )
    return Copier(retriever, schema)


@pytest.mark.asyncio
async def test_concurrent_copier_nodes_before_relationships(concurrent_subject, mocker):
    """Nodes are fully drained before relationships start in the concurrent path."""
    order = []

    async def node_gen(schema):
        order.append("nodes")
        return
        yield  # pragma: no cover

    async def rel_gen(schema):
        order.append("rels")
        return
        yield  # pragma: no cover

    concurrent_subject.type_retriever.fetchNodes = node_gen
    concurrent_subject.type_retriever.fetchRelationships = rel_gen

    async for _ in concurrent_subject.extract_records():
        pass

    assert order == ["nodes", "rels"]


@pytest.mark.asyncio
async def test_concurrent_copier_no_types(mocker, basic_schema):
    """Concurrent path handles empty generators gracefully."""
    schema = deepcopy(basic_schema)
    retriever = _make_mock_retriever(mocker, concurrency_limit=5)
    copier = Copier(retriever, schema)
    records = [record async for record in copier.extract_records()]
    assert records == []


@pytest.mark.asyncio
async def test_concurrent_copier_propagates_producer_error(mocker, basic_schema):
    """If fetch_nodes raises, the error propagates out of extract_records."""
    schema = _build_schema_with_adjacencies(basic_schema)
    retriever = _make_mock_retriever(mocker, node_types=["Person"], concurrency_limit=2)

    async def failing_nodes(schema):
        raise RuntimeError("database went away")
        yield  # pragma: no cover

    retriever.fetchNodes = failing_nodes

    copier = Copier(retriever, schema)
    with pytest.raises(RuntimeError, match="database went away"):
        async for _ in copier.extract_records():
            pass


def test_copier_is_single_class(mocker, basic_schema):
    """Copier and ConcurrentCopier are the same class; mode is retriever-driven."""
    assert ConcurrentCopier is Copier


def test_type_histogram_empty():
    h = TypeHistogram.empty()
    assert h.node_counts == {}
    assert h.relationship_counts == {}


@pytest.mark.asyncio
async def test_type_retriever_build_histogram_default(mocker, basic_schema):
    """Default build_histogram returns an empty TypeHistogram."""
    from nodestream.databases.copy import TypeRetriever

    class MinimalRetriever(TypeRetriever):
        async def fetchNodes(self, schema):
            return
            yield  # pragma: no cover

        async def fetchRelationships(self, schema):
            return
            yield  # pragma: no cover

    retriever = MinimalRetriever()
    histogram = await retriever.build_histogram(basic_schema)
    assert isinstance(histogram, TypeHistogram)
    assert histogram.node_counts == {}
    assert histogram.relationship_counts == {}


@pytest.mark.asyncio
async def test_extract_sequential_relationships_only(mocker, basic_schema):
    """_extract_sequential skips nodes when relationships_only=True."""
    schema = _build_schema_with_adjacencies(basic_schema)
    retriever = _make_mock_retriever(
        mocker, relationship_types=["KNOWS"], concurrency_limit=1
    )
    retriever.relationships_only = True

    rel = RelationshipWithNodes(
        Node("Person", {"name": "A"}),
        Node("Person", {"name": "B"}),
        Relationship("KNOWS", {}),
    )

    async def rel_gen(schema):
        yield rel

    retriever.fetchRelationships = rel_gen
    copier = Copier(retriever, schema)
    records = [r async for r in copier.extract_records()]
    # Only relationship records, no nodes
    assert len(records) == 1
    retriever.fetchNodes.assert_not_called()


@pytest.mark.asyncio
async def test_concurrent_copier_relationships_only(mocker, basic_schema):
    """Concurrent path skips node queue entirely when relationships_only=True."""
    schema = _build_schema_with_adjacencies(basic_schema)
    retriever = _make_mock_retriever(
        mocker, relationship_types=["KNOWS"], concurrency_limit=4
    )
    retriever.relationships_only = True

    rel = RelationshipWithNodes(
        Node("Person", {"name": "A"}),
        Node("Person", {"name": "B"}),
        Relationship("KNOWS", {}),
    )

    async def rel_gen(schema):
        yield rel

    retriever.fetchRelationships = rel_gen
    copier = Copier(retriever, schema)
    records = [r async for r in copier.extract_records()]
    assert len(records) == 1
    retriever.fetchNodes.assert_not_called()


@pytest.mark.asyncio
async def test_concurrent_copier_yields_nodes_and_rels(mocker, basic_schema):
    """Concurrent path yields both node and relationship records."""
    schema = _build_schema_with_adjacencies(basic_schema)
    retriever = _make_mock_retriever(
        mocker,
        node_types=["Person"],
        relationship_types=["KNOWS"],
        concurrency_limit=4,
    )

    node = Node("Person", {"name": "Alice"})
    rel = RelationshipWithNodes(
        Node("Person", {"name": "A"}),
        Node("Person", {"name": "B"}),
        Relationship("KNOWS", {}),
    )

    async def node_gen(schema):
        yield node

    async def rel_gen(schema):
        yield rel

    retriever.fetchNodes = node_gen
    retriever.fetchRelationships = rel_gen
    copier = Copier(retriever, schema)
    copier.convert_node_to_ingest = lambda n: ("node", n.type)
    copier.convert_relationship_to_ingest = lambda r: ("rel", r.relationship.type)
    records = [r async for r in copier.extract_records()]
    assert ("node", "Person") in records
    assert ("rel", "KNOWS") in records


@pytest.mark.asyncio
async def test_concurrent_copier_propagates_rel_producer_error(mocker, basic_schema):
    """If fetch_relationships raises, the error propagates out of extract_records."""
    schema = _build_schema_with_adjacencies(basic_schema)
    retriever = _make_mock_retriever(mocker, concurrency_limit=2)
    retriever.relationships_only = True

    async def failing_rels(schema):
        raise RuntimeError("rel fetch failed")
        yield  # pragma: no cover

    retriever.fetchRelationships = failing_rels
    copier = Copier(retriever, schema)
    with pytest.raises(RuntimeError, match="rel fetch failed"):
        async for _ in copier.extract_records():
            pass
