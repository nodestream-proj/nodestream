import pytest

from nodestream.model import DesiredIngestion, NodeCreationRule, Node, Relationship


@pytest.fixture
def desired_ingestion():
    return DesiredIngestion()


@pytest.fixture
def valid_node():
    return Node("Foo", {"bar": "baz"})


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
