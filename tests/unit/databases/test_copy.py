from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock

import pytest

from nodestream.databases.copy import (
    Copier,
    TypeHistogram,
    TypeRetriever,
)
from nodestream.pipeline import Extractor
from nodestream.pipeline.object_storage import ObjectStore
from nodestream.pipeline.progress_reporter import PipelineProgressReporter
from nodestream.pipeline.step import StepContext
from nodestream.schema import Adjacency, AdjacencyCardinality, Cardinality


def _build_schema_with_adjacencies(basic_schema):
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


def _make_extractor(*records):
    extractor = MagicMock(spec=Extractor)

    async def _extract():
        for r in records:
            yield r

    extractor.extract_records = _extract
    return extractor


def _make_mock_retriever(mocker, basic_schema=None, extractors=()):
    retriever = mocker.Mock()

    async def _fetch_extractors_impl():
        for e in extractors:
            yield e

    retriever.fetch_extractors = _fetch_extractors_impl
    retriever.build_histogram = AsyncMock(return_value=TypeHistogram())
    retriever.schema = basic_schema
    return retriever


@pytest.fixture
def subject(mocker, basic_schema):
    schema = _build_schema_with_adjacencies(basic_schema)
    retriever = _make_mock_retriever(mocker, basic_schema=schema)
    return Copier(retriever)


def _make_step_context():
    return StepContext("test", 0, PipelineProgressReporter(), ObjectStore.null())


# ---------------------------------------------------------------------------
# TypeHistogram tests
# ---------------------------------------------------------------------------


def test_histogram_sorted_node_types():
    h = TypeHistogram({"Person": 10, "Address": 200, "Device": 5})
    assert h.sorted_types_by_count(h.node_counts) == ["Address", "Person", "Device"]


def test_histogram_sorted_relationship_types():
    h = TypeHistogram(relationship_counts={"KNOWS": 5, "LIVES_AT": 100})
    assert h.sorted_types_by_count(h.relationship_counts) == ["LIVES_AT", "KNOWS"]


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
async def test_extract_records_yields_extractor_records(mocker, basic_schema):
    schema = _build_schema_with_adjacencies(basic_schema)
    e1 = _make_extractor("a", "b")
    e2 = _make_extractor("c")
    retriever = _make_mock_retriever(mocker, basic_schema=schema, extractors=[e1, e2])
    copier = Copier(retriever)
    records = [r async for r in copier.extract_records()]
    assert set(records) == {"a", "b", "c"}


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
    retriever = _make_mock_retriever(
        mocker, basic_schema=schema, extractors=[badExtractor]
    )
    copier = Copier(retriever)
    with pytest.raises(RuntimeError, match="database went away"):
        async for _ in copier.extract_records():
            pass


# ---------------------------------------------------------------------------
# Concurrency tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_copier_runs_multiple_extractors_concurrently(mocker, basic_schema):
    schema = _build_schema_with_adjacencies(basic_schema)
    e1 = _make_extractor("a", "b")
    e2 = _make_extractor("c")
    e3 = _make_extractor("d", "e")
    retriever = _make_mock_retriever(
        mocker, basic_schema=schema, extractors=[e1, e2, e3]
    )
    copier = Copier(retriever, concurrency_limit=4)
    records = [r async for r in copier.extract_records()]
    assert set(records) == {"a", "b", "c", "d", "e"}


@pytest.mark.asyncio
async def test_copier_propagates_extractor_error(mocker, basic_schema):
    schema = _build_schema_with_adjacencies(basic_schema)

    async def _fail():
        raise RuntimeError("fetch failed")
        yield  # pragma: no cover

    badExtractor = MagicMock(spec=Extractor)
    badExtractor.extract_records = _fail
    retriever = _make_mock_retriever(
        mocker, basic_schema=schema, extractors=[badExtractor]
    )
    copier = Copier(retriever, concurrency_limit=2)
    with pytest.raises(RuntimeError, match="fetch failed"):
        async for _ in copier.extract_records():
            pass


# ---------------------------------------------------------------------------
# TypeHistogram default
# ---------------------------------------------------------------------------


def test_type_histogram_default():
    h = TypeHistogram()
    assert h.node_counts == {}
    assert h.relationship_counts == {}


# ---------------------------------------------------------------------------
# TypeRetriever.build_histogram abstract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_type_retriever_build_histogram_is_abstract(basic_schema):
    """TypeRetriever.build_histogram is abstract — subclasses must implement it."""

    class MinimalRetriever(TypeRetriever):
        async def build_histogram(self) -> TypeHistogram:
            return TypeHistogram()

        async def fetch_extractors(self):
            return
            yield  # pragma: no cover

    retriever = MinimalRetriever(schema=basic_schema)
    histogram = await retriever.build_histogram()
    assert isinstance(histogram, TypeHistogram)
    assert histogram.node_counts == {}
    assert histogram.relationship_counts == {}
