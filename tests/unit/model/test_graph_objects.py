import pytest
from hamcrest import assert_that, equal_to

from nodestream.model import (
    Node,
    NodeCreationRule,
    Relationship,
    RelationshipCreationRule,
    RelationshipWithNodes,
)


def test_node_into_ingest():
    node = Node("Person", {"name": "John"})
    ingest = node.into_ingest()
    assert_that(ingest.source, equal_to(node))
    assert_that(ingest.relationships, equal_to([]))


def test_relationship_into_ingest():
    relationship = Relationship("KNOWS", {"since": 2010})
    from_node = Node("Person", {"name": "John"})
    to_node = Node("Person", {"name": "Mary"})
    relationship_with_nodes = RelationshipWithNodes(
        from_node=from_node, to_node=to_node, relationship=relationship
    )
    ingest = relationship_with_nodes.into_ingest()
    assert_that(ingest.source, equal_to(from_node))
    assert_that(
        ingest.relationships,
        equal_to(
            [
                RelationshipWithNodes(
                    from_node=from_node,
                    to_node=to_node,
                    relationship=relationship,
                    from_side_node_creation_rule=NodeCreationRule.EAGER,
                    to_side_node_creation_rule=NodeCreationRule.MATCH_ONLY,
                    relationship_creation_rule=RelationshipCreationRule.CREATE,
                )
            ]
        ),
    )


@pytest.mark.parametrize(
    "keys,expected",
    [
        ({"name": "John"}, True),
        ({"name": None}, False),
        ({"name": "John", "age": 30}, True),
        ({"name": "John", "age": None}, False),
        ({"name": None, "age": None}, False),
    ],
)
def test_node_key_validity(keys, expected):
    node = Node("Person", keys)
    assert_that(node.has_valid_id, equal_to(expected))
