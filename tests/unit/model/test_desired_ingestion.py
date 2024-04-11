import pytest

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


def test_add_relationship_valid_node(desired_ingestion, valid_node, valid_relationship):
    desired_ingestion.add_relationship(
        related_node=valid_node,
        relationship=valid_relationship,
        outbound=True,
        node_creation_rule=NodeCreationRule.EAGER,
    )
    assert len(desired_ingestion.relationship_drafts) == 1


def test_add_relationship_invalid_node(
        desired_ingestion, invalid_node, valid_relationship
):
    desired_ingestion.add_relationship(
        related_node=invalid_node,
        relationship=valid_relationship,
        outbound=True,
        node_creation_rule=NodeCreationRule.EAGER,
    )
    assert len(desired_ingestion.relationship_drafts) == 0


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


def test_relationship_is_finalized_after_source_node_added(desired_ingestion, valid_node, valid_relationship,
                                                           valid_node2):
    desired_ingestion.add_relationship(
        related_node=valid_node2,
        relationship=valid_relationship,
        outbound=True,
        node_creation_rule=NodeCreationRule.EAGER,
    )
    assert len(desired_ingestion.relationship_drafts) == 1
    assert len(desired_ingestion.relationships) == 0

    desired_ingestion.add_source_node(
        source_type=valid_node.type,
        additional_types=valid_node.additional_types,
        creation_rule=NodeCreationRule.EAGER,
        key_value_generator=((key, value) for key, value in valid_node.key_values.items()),
        properties_generator=None)
    assert len(desired_ingestion.relationship_drafts) == 0
    assert len(desired_ingestion.relationships) == 1


def test_drafts_are_empty_when_source_node_exists(desired_ingestion, valid_node, valid_relationship, valid_node2):
    desired_ingestion.add_source_node(
        source_type=valid_node.type,
        additional_types=valid_node.additional_types,
        creation_rule=NodeCreationRule.EAGER,
        key_value_generator=((key, value) for key, value in valid_node.key_values.items()),
        properties_generator=None)

    desired_ingestion.add_relationship(
        related_node=valid_node2,
        relationship=valid_relationship,
        outbound=True,
        node_creation_rule=NodeCreationRule.EAGER,
    )
    assert len(desired_ingestion.relationship_drafts) == 0
    assert len(desired_ingestion.relationships) == 1

    assert len(desired_ingestion.relationship_drafts) == 0
    assert len(desired_ingestion.relationships) == 1
