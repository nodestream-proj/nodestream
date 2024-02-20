import time

import pytest
from hamcrest import assert_that, equal_to, not_

from nodestream.model.graph_objects import (
    Node,
    NodeCreationRule,
    Relationship,
    RelationshipCreationRule,
    RelationshipWithNodes,
    get_cached_timestamp,
)


def test_node_with_no_keys_is_invalid():
    node = Node("Person", {})
    assert_that(node.has_valid_id, equal_to(False))


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


def test_get_cached_timestamp():
    # The first and second timestamp should be the same if called in quick
    # succession.
    first = get_cached_timestamp()
    second = get_cached_timestamp()
    assert_that(first, equal_to(second))

    # After 2 seconds, the timestamp should be the same.
    time.sleep(2)
    third = get_cached_timestamp()
    assert_that(third, not_(equal_to(first)))
