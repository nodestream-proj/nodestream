import pytest
from hamcrest import assert_that, instance_of, empty

from nodestream.databases.null import (
    NullQueryExecutor,
    NullMigrator,
    NullConnector,
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
async def test_retriever_get_nodes_of_type(retriver):
    results = [r async for r in retriver.get_nodes_of_type("Foo")]
    assert_that(results, empty())


@pytest.mark.asyncio
async def test_retriever_get_relationships_of_type(retriver):
    results = [r async for r in retriver.get_relationships_of_type("IS_FOO")]
    assert_that(results, empty())
