import pytest

from nodestream.model import (
    Node,
    Relationship,
    RelationshipWithNodes,
    KeyIndex,
    FieldIndex,
    GraphObjectType,
    TimeToLiveConfiguration,
    IngestionHookRunRequest,
    GraphObjectShape,
    NodeIdentityShape,
    MatchStrategy,
)
from nodestream.databases import DebouncedIngestStrategy
from nodestream.databases.query_executor import OperationOnNodeIdentity

from hamcrest import assert_that, equal_to


@pytest.fixture
def ingest_strategy(mocker):
    return DebouncedIngestStrategy(mocker.AsyncMock(), mocker.Mock())


@pytest.mark.asyncio
async def test_ingest_source_node(ingest_strategy):
    node = Node("test", "test", {"test": "test"})
    await ingest_strategy.ingest_source_node(node)
    ingest_strategy.debouncer.debounce_node.assert_called_once_with(node)


@pytest.mark.asyncio
async def test_ingest_relationship(ingest_strategy):
    rel = RelationshipWithNodes(
        from_node=Node("test", "test", {"test": "test"}),
        to_node=Node("test2", "test2", {"test2": "test"}),
        relationship=Relationship("test_rel"),
    )
    await ingest_strategy.ingest_relationship(rel)
    ingest_strategy.debouncer.debounce_relationship.assert_called_once_with(rel)


@pytest.mark.asyncio
async def test_run_hook_before_ingest(ingest_strategy, mocker):
    hook = mocker.AsyncMock()
    await ingest_strategy.run_hook(IngestionHookRunRequest(hook, before_ingest=True))
    ingest_strategy.executor.execute_hook.assert_called_once_with(hook)


@pytest.mark.asyncio
async def test_run_hook_after_ingest(ingest_strategy, mocker):
    hook = mocker.AsyncMock()
    await ingest_strategy.run_hook(IngestionHookRunRequest(hook, before_ingest=False))
    assert_that(ingest_strategy.hooks_saved_for_after_ingest, equal_to([hook]))


@pytest.mark.asyncio
async def test_upsert_key_index(ingest_strategy):
    index = KeyIndex("test", frozenset(("key",)))
    await ingest_strategy.upsert_key_index(index)
    ingest_strategy.executor.upsert_key_index.assert_called_once_with(index)


@pytest.mark.asyncio
async def test_upsert_field_index(ingest_strategy):
    index = FieldIndex("test", "field", GraphObjectType.NODE)
    await ingest_strategy.upsert_field_index(index)
    ingest_strategy.executor.upsert_field_index.assert_called_once_with(index)


@pytest.mark.asyncio
async def test_upsert_ttl_config(ingest_strategy):
    config = TimeToLiveConfiguration(GraphObjectType.NODE, "Type")
    await ingest_strategy.perform_ttl_operation(config)
    ingest_strategy.executor.upsert_field_index.assert_called_once_with(config)


@pytest.mark.asyncio
async def test_flush_nodes(ingest_strategy):
    ingest_strategy.debouncer.drain_relationship_groups.return_value = []
    ingest_strategy.debouncer.drain_node_groups.return_value = [
        (
            OperationOnNodeIdentity(
                NodeIdentityShape("type", ("key",)), MatchStrategy.EAGER
            ),
            [Node("type", {"key": "test"})],
        )
    ]
    await ingest_strategy.flush()
    ingest_strategy.executor.upsert_nodes_in_bulk_with_same_operation.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush_relationships(ingest_strategy):
    ingest_strategy.debouncer.drain_node_groups.return_value = []
    ingest_strategy.debouncer.drain_relationship_groups.return_value = [
        (
            GraphObjectShape(GraphObjectType.RELATIONSHIP, "type", frozenset(("key",))),
            [
                RelationshipWithNodes(
                    from_node=Node("type", {"key": "test"}),
                    to_node=Node("type", {"key": "test"}),
                    relationship=Relationship("type", {"key": "test"}),
                )
            ],
        )
    ]
    await ingest_strategy.flush()
    ingest_strategy.executor.upsert_relationships_in_bulk_of_same_shape.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush_hooks_after_ingest_calls_executor(ingest_strategy, mocker):
    ingest_strategy.debouncer.drain_node_groups.return_value = []
    ingest_strategy.debouncer.drain_relationship_groups.return_value = []
    ingest_strategy.hooks_saved_for_after_ingest = [mocker.AsyncMock()]
    await ingest_strategy.flush()
    ingest_strategy.executor.execute_hook.assert_awaited_once_with(
        ingest_strategy.hooks_saved_for_after_ingest[0]
    )
