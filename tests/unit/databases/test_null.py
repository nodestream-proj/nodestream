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
async def test_retriever_get_nodes_of_type(retriver):
    results = [r async for r in retriver.get_nodes_of_type("Foo")]
    assert_that(results, empty())


@pytest.mark.asyncio
async def test_retriever_get_relationships_of_type(retriver):
    results = [
        r
        async for r in retriver.get_relationships_of_type_between(
            "Person", "Person", "KNOWS"
        )
    ]
    assert_that(results, empty())


@pytest.mark.asyncio
async def test_retriever_preview_node_count(retriver):
    count = await retriver.preview_node_count("Person")
    assert count == 0


@pytest.mark.asyncio
async def test_retriever_preview_relationship_count(retriver):
    count = await retriver.preview_relationship_count("KNOWS")
    assert count == 0


def test_make_type_retriever_accepts_kwargs(connector):
    """make_type_retriever should accept and ignore arbitrary kwargs."""
    retriever = connector.make_type_retriever(limit=500, sample_ratio=50)
    assert_that(retriever, instance_of(NullRetriver))


def test_connector_get_type_retriever_forwards_kwargs(connector):
    """get_type_retriever should forward kwargs to make_type_retriever."""
    retriever = connector.get_type_retriever(limit=100, extra_param="value")
    assert_that(retriever, instance_of(NullRetriver))
