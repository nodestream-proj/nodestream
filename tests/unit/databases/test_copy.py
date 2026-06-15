from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock

import pytest
from hamcrest import assert_that, has_length

from nodestream.databases.copy import (
    ConcurrentCopier,
    Copier,
    TypeHistogram,
    TypeRetriever,
)
from nodestream.model import Node, Relationship, RelationshipWithNodes
from nodestream.pipeline import Extractor
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


def _make_extractor(*records):
    """Build a mock Extractor that yields the given records."""
    extractor = MagicMock(spec=Extractor)

    async def _extract():
        for r in records:
            yield r

    extractor.extract_records = _extract
    return extractor


def _make_mock_retriever(
    mocker, basic_schema=None, node_extractors=(), rel_extractors=(), concurrency_limit=1
):
    """Build a mock TypeRetriever with fetchNodeExtractors/fetchRelationshipExtractors."""
    retriever = mocker.Mock()

    async def _node_extractors():
        for e in node_extractors:
            yield e

    async def _rel_extractors():
        for e in rel_extractors:
            yield e

    retriever.fetchNodeExtractors = _node_extractors
    retriever.fetchRelationshipExtractors = _rel_extractors
    retriever.build_histogram = AsyncMock(return_value=TypeHistogram())
    retriever.concurrency_limit = concurrency_limit
    retriever.orchestrator_queue_size = 0
    retriever.schema = basic_schema
    return retriever


@pytest.fixture
def subject(mocker, basic_schema):
    schema = _build_schema_with_adjacencies(basic_schema)
    retriever = _make_mock_retriever(mocker, basic_schema=schema)
    return Copier(retriever)


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

    subject.type_retriever.build_histogram.assert_called_once_with()


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
async def test_extract_records_yields_node_then_rel_extractor_records(mocker, basic_schema):
    schema = _build_schema_with_adjacencies(basic_schema)
    nodeExtractor = _make_extractor("node-a", "node-b")
    relExtractor = _make_extractor("rel-x")
    retriever = _make_mock_retriever(
        mocker, basic_schema=schema,
        node_extractors=[nodeExtractor],
        rel_extractors=[relExtractor],
    )
    copier = Copier(retriever)
    records = [r async for r in copier.extract_records()]
    assert set(records) == {"node-a", "node-b", "rel-x"}


@pytest.mark.asyncio
async def test_extract_records_empty(mocker, basic_schema):
    retriever = _make_mock_retriever(mocker, basic_schema=basic_schema)
    copier = Copier(retriever)
    records = [r async for r in copier.extract_records()]
    assert records == []


@pytest.mark.asyncio
async def test_extract_records_propagates_extractor_error(mocker, basic_schema):
    schema = _build_schema_with_adjacencies(basic_schema)

    async def _failing_extract():
        raise RuntimeError("database went away")
        yield  # pragma: no cover

    badExtractor = MagicMock(spec=Extractor)
    badExtractor.extract_records = _failing_extract
    retriever = _make_mock_retriever(mocker, basic_schema=schema, node_extractors=[badExtractor])
    copier = Copier(retriever)
    with pytest.raises(RuntimeError, match="database went away"):
        async for _ in copier.extract_records():
            pass


# ---------------------------------------------------------------------------
# convert helpers (still on Copier for key reorganization)
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
# Concurrent / concurrency_limit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_copier_runs_multiple_extractors(mocker, basic_schema):
    """Copier runs all extractors and collects their records."""
    schema = _build_schema_with_adjacencies(basic_schema)
    e1 = _make_extractor("a", "b")
    e2 = _make_extractor("c")
    e3 = _make_extractor("d", "e")
    retriever = _make_mock_retriever(
        mocker, basic_schema=schema,
        node_extractors=[e1, e2],
        rel_extractors=[e3],
        concurrency_limit=4,
    )
    copier = Copier(retriever)
    records = [r async for r in copier.extract_records()]
    assert set(records) == {"a", "b", "c", "d", "e"}


@pytest.mark.asyncio
async def test_concurrent_copier_propagates_error(mocker, basic_schema):
    """Error inside an extractor propagates out of extract_records."""
    schema = _build_schema_with_adjacencies(basic_schema)

    async def _fail():
        raise RuntimeError("rel fetch failed")
        yield  # pragma: no cover

    badExtractor = MagicMock(spec=Extractor)
    badExtractor.extract_records = _fail
    retriever = _make_mock_retriever(
        mocker, basic_schema=schema, rel_extractors=[badExtractor], concurrency_limit=2
    )
    copier = Copier(retriever)
    with pytest.raises(RuntimeError, match="rel fetch failed"):
        async for _ in copier.extract_records():
            pass


def test_copier_and_concurrent_copier_are_same_class():
    """ConcurrentCopier is just an alias for Copier."""
    assert ConcurrentCopier is Copier


# ---------------------------------------------------------------------------
# TypeHistogram default
# ---------------------------------------------------------------------------


def test_type_histogram_default():
    h = TypeHistogram()
    assert h.node_counts == {}
    assert h.relationship_counts == {}


# ---------------------------------------------------------------------------
# TypeRetriever.build_histogram default
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_type_retriever_build_histogram_default(mocker, basic_schema):
    """Default build_histogram returns an empty TypeHistogram."""

    class MinimalRetriever(TypeRetriever):
        async def fetchNodeExtractors(self):
            return
            yield  # pragma: no cover

        async def fetchRelationshipExtractors(self):
            return
            yield  # pragma: no cover

    retriever = MinimalRetriever(schema=basic_schema)
    histogram = await retriever.build_histogram()
    assert isinstance(histogram, TypeHistogram)
    assert histogram.node_counts == {}
    assert histogram.relationship_counts == {}
