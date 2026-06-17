import pytest
from hamcrest import assert_that, empty, instance_of

from nodestream.databases.null import (
    NullConnector,
    NullMigrator,
    NullQueryExecutor,
    NullRetriver,
)


@pytest.fixture
def connector():
    return NullConnector()


@pytest.fixture
def retriver():
    return NullRetriver()


def test_make_migrator(connector):
    assert_that(connector.make_migrator(), instance_of(NullMigrator))


def test_make_executor(connector):
    assert_that(connector.make_query_executor(), instance_of(NullQueryExecutor))


def test_make_retriever(connector):
    assert_that(connector.make_type_retriever(), instance_of(NullRetriver))


@pytest.mark.asyncio
async def test_retriever_fetch_extractors(retriver):
    results = [r async for r in retriver.fetch_extractors()]
    assert_that(results, empty())


def test_make_type_retriever_accepts_kwargs(connector):
    """make_type_retriever should accept and ignore arbitrary kwargs."""
    retriever = connector.make_type_retriever(limit=500, sample_ratio=50)
    assert_that(retriever, instance_of(NullRetriver))


def test_connector_get_type_retriever_forwards_kwargs(connector):
    """get_type_retriever should forward kwargs to make_type_retriever."""
    retriever = connector.get_type_retriever(limit=100, extra_param="value")
    assert_that(retriever, instance_of(NullRetriver))
