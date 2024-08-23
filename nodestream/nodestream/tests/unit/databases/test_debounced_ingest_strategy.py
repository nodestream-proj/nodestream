import pytest
from hamcrest import assert_that, empty, equal_to

from nodestream.databases import DebouncedIngestStrategy
from nodestream.databases.query_executor import OperationOnNodeIdentity
from nodestream.model import (
    IngestionHookRunRequest,
    Node,
    NodeCreationRule,
    NodeIdentityShape,
    Relationship,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)
from nodestream.schema import GraphObjectType


@pytest.fixture
def ingest_strategy(mocker):
    debouncer = DebouncedIngestStrategy(mocker.AsyncMock())
    debouncer.debouncer = mocker.Mock()
    return debouncer


@pytest.mark.asyncio
async def test_ingest_source_node(ingest_strategy):
    node = Node("test", "test", {"test": "test"})
    await ingest_strategy.ingest_source_node(node)
    ingest_strategy.debouncer.debounce_node_operation.assert_called_once_with(
        node, node_creation_rule=NodeCreationRule.EAGER
    )


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
async def test_upsert_ttl_config(ingest_strategy):
    config = TimeToLiveConfiguration(GraphObjectType.NODE, "Type")
    await ingest_strategy.perform_ttl_operation(config)
    ingest_strategy.executor.perform_ttl_op.assert_called_once_with(config)


@pytest.mark.asyncio
async def test_flush_nodes(ingest_strategy):
    ingest_strategy.debouncer.drain_relationship_groups.return_value = []
    ingest_strategy.debouncer.drain_node_groups.return_value = [
        (
            OperationOnNodeIdentity(
                NodeIdentityShape("type", ("key",)), NodeCreationRule.EAGER
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
            "Type",
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
    ingest_strategy.executor.upsert_relationships_in_bulk_of_same_operation.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush_hooks_after_ingest_calls_executor(ingest_strategy, mocker):
    ingest_strategy.debouncer.drain_node_groups.return_value = []
    ingest_strategy.debouncer.drain_relationship_groups.return_value = []
    ingest_strategy.hooks_saved_for_after_ingest = [hook := mocker.AsyncMock()]
    await ingest_strategy.flush()
    ingest_strategy.executor.execute_hook.assert_awaited_once_with(hook)
    assert_that(ingest_strategy.hooks_saved_for_after_ingest, empty())
