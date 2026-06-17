import pytest
from hamcrest import assert_that, empty, instance_of

from nodestream.databases.null import (
    NullConnector,
    NullMigrator,
    NullQueryExecutor,
    NullRetriever,
)


@pytest.fixture
def connector():
    return NullConnector()


@pytest.fixture
def retriever():
    return NullRetriever()


def test_make_migrator(connector):
    assert_that(connector.make_migrator(), instance_of(NullMigrator))


def test_make_executor(connector):
    assert_that(connector.make_query_executor(), instance_of(NullQueryExecutor))


def test_make_retriever(connector):
    assert_that(connector.make_type_retriever(), instance_of(NullRetriever))


@pytest.mark.asyncio
async def test_retriever_fetch_extractors(retriever):
    results = [result async for result in retriever.fetch_extractors()]
    assert_that(results, empty())


def test_make_type_retriever_accepts_kwargs(connector):
    """make_type_retriever should accept and ignore arbitrary kwargs."""
    retriever = connector.make_type_retriever(limit=500, sample_ratio=50)
    assert_that(retriever, instance_of(NullRetriever))


def test_connector_get_type_retriever_forwards_kwargs(connector):
    """get_type_retriever should forward kwargs to make_type_retriever."""
    retriever = connector.get_type_retriever(limit=100, extra_param="value")
    assert_that(retriever, instance_of(NullRetriever))
