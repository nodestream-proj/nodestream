from unittest.mock import AsyncMock, patch

import pytest

from nodestream.databases import DebouncedIngestStrategy
from nodestream.model import (
    DesiredIngestion,
    IngestionHookRunRequest,
    Node,
    NodeCreationRule,
    Relationship,
)


@pytest.fixture
def desired_ingestion():
    return DesiredIngestion()


@pytest.fixture
def valid_node():
    return Node("Foo", {"bar": "baz"})


@pytest.fixture
def valid_node2():
    return Node("Zoom", {"zam": "zom"})


@pytest.fixture
def invalid_node():
    return Node()


@pytest.fixture
def valid_relationship():
    return Relationship("IS_RELATED_TO")


@pytest.fixture
def ingest_strategy(mocker):
    debouncer = DebouncedIngestStrategy(mocker.AsyncMock())
    debouncer.debouncer = mocker.Mock()
    return debouncer


def add_source_node(
    desired_ingestion: DesiredIngestion, node: Node, creation_rule: NodeCreationRule
):
    desired_ingestion.add_source_node(
        source_type=node.type,
        creation_rule=creation_rule,
        key_values=node.key_values,
        additional_types=node.additional_types,
        properties=node.properties,
    )


def test_add_relationship_valid_node(desired_ingestion, valid_node, valid_relationship):
    desired_ingestion.add_relationship(
        related_node=valid_node,
        relationship=valid_relationship,
        outbound=True,
        node_creation_rule=NodeCreationRule.EAGER,
    )
    assert len(desired_ingestion.relationships) == 1


def test_add_relationship_invalid_node(
    desired_ingestion, invalid_node, valid_relationship
):
    desired_ingestion.add_relationship(
        related_node=invalid_node,
        relationship=valid_relationship,
        outbound=True,
        node_creation_rule=NodeCreationRule.EAGER,
    )
    assert len(desired_ingestion.relationships) == 0


@pytest.mark.asyncio
async def test_run_ingest_hooks(desired_ingestion, mocker):
    desired_ingestion.hook_requests = [
        IngestionHookRunRequest(mocker.Mock(), before_ingest=True),
        IngestionHookRunRequest(mocker.Mock(), before_ingest=False),
    ]

    await desired_ingestion.run_ingest_hooks(strategy := mocker.AsyncMock())
    strategy.run_hook.assert_has_awaits(
        [
            mocker.call(desired_ingestion.hook_requests[0]),
            mocker.call(desired_ingestion.hook_requests[1]),
        ]
    )


def test_relationship_is_finalized_after_source_node_added(
    desired_ingestion, valid_node, valid_relationship, valid_node2
):
    desired_ingestion.add_relationship(
        related_node=valid_node2,
        relationship=valid_relationship,
        outbound=True,
        node_creation_rule=NodeCreationRule.EAGER,
    )
    assert len(desired_ingestion.relationships) == 1

    add_source_node(desired_ingestion, valid_node, NodeCreationRule.MATCH_ONLY)

    subject = desired_ingestion.relationships[0]
    assert subject.from_side_node_creation_rule == NodeCreationRule.MATCH_ONLY
    assert subject.from_node.additional_types == valid_node.additional_types
    assert subject.from_node.type == valid_node.type


def test_inbound_relationship_is_finalized_after_source_node_added(
    desired_ingestion, valid_node, valid_relationship, valid_node2
):
    desired_ingestion.add_relationship(
        related_node=valid_node2,
        relationship=valid_relationship,
        outbound=False,
        node_creation_rule=NodeCreationRule.EAGER,
    )
    assert len(desired_ingestion.relationships) == 1

    add_source_node(desired_ingestion, valid_node, NodeCreationRule.MATCH_ONLY)

    subject = desired_ingestion.relationships[0]
    assert subject.to_side_node_creation_rule == NodeCreationRule.MATCH_ONLY
    assert subject.to_node.additional_types == valid_node.additional_types
    assert subject.to_node.type == valid_node.type


def test_relationships_are_finalized_when_source_node_exists(
    desired_ingestion, valid_node, valid_relationship, valid_node2
):
    add_source_node(desired_ingestion, valid_node, NodeCreationRule.MATCH_ONLY)

    desired_ingestion.add_relationship(
        related_node=valid_node2,
        relationship=valid_relationship,
        outbound=True,
        node_creation_rule=NodeCreationRule.EAGER,
    )

    subject = desired_ingestion.relationships[0]
    assert subject.from_side_node_creation_rule == NodeCreationRule.MATCH_ONLY
    assert subject.from_node.additional_types == valid_node.additional_types
    assert subject.from_node.type == valid_node.type


def test_relationships_are_finalized_when_source_node_is_inbound(
    desired_ingestion, valid_node, valid_relationship, valid_node2
):
    add_source_node(desired_ingestion, valid_node, NodeCreationRule.MATCH_ONLY)

    desired_ingestion.add_relationship(
        related_node=valid_node2,
        relationship=valid_relationship,
        outbound=False,
        node_creation_rule=NodeCreationRule.EAGER,
    )

    subject = desired_ingestion.relationships[0]
    assert subject.to_side_node_creation_rule == NodeCreationRule.MATCH_ONLY
    assert subject.to_node.additional_types == valid_node.additional_types
    assert subject.to_node.type == valid_node.type


@patch("nodestream.model.DesiredIngestion.ingest_source_node", new_callable=AsyncMock)
@patch("nodestream.model.DesiredIngestion.ingest_relationships", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_ingestion_with_valid_source_node(
    ingest_source_node, ingest_relationships, valid_node, ingest_strategy
):
    desired_ingestion = DesiredIngestion(source=valid_node)

    await desired_ingestion.ingest(ingest_strategy)

    ingest_source_node.assert_called_once()
    ingest_relationships.assert_called_once()


@patch("nodestream.model.DesiredIngestion.ingest_source_node", new_callable=AsyncMock)
@patch("nodestream.model.DesiredIngestion.ingest_relationships", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_ingestion_with_invalid_node(
    ingest_source_node, ingest_relationships, invalid_node, ingest_strategy
):
    desired_ingestion = DesiredIngestion(source=invalid_node)

    await desired_ingestion.ingest(ingest_strategy)

    ingest_source_node.assert_not_called()
    ingest_relationships.assert_not_called()


@patch("nodestream.model.DesiredIngestion.ingest_source_node", new_callable=AsyncMock)
@patch("nodestream.model.DesiredIngestion.ingest_relationships", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_ingestion_without_source_node_creation_rule(
    ingest_source_node, ingest_relationships, invalid_node, ingest_strategy
):
    desired_ingestion = DesiredIngestion(source=invalid_node)
    desired_ingestion.source_node_creation_rule = None

    await desired_ingestion.ingest(ingest_strategy)

    ingest_source_node.assert_not_called()
    ingest_relationships.assert_not_called()
